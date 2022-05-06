using App.Metrics;
using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.Logging;
using Picton.Managers;
using Picton.Messaging.Messages;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	/// Designed to monitor an Azure storage queue and process the message as quickly and efficiently as possible.
	/// </summary>
	public class AsyncMessagePumpWithHandlers
	{
		#region FIELDS

		private static readonly IDictionary<Type, Type[]> _messageHandlers = GetMessageHandlers();

		private readonly ILogger _logger;
		private readonly AsyncMessagePump _messagePump;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when an error occurs.
		/// </summary>
		/// <example>
		/// <code>
		/// OnError = (message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);
		/// </code>
		/// </example>
		/// <remarks>
		/// When isPoison is set to true, you should copy this message to a poison queue because it will be deleted from the original queue.
		/// </remarks>
		public Action<CloudMessage, Exception, bool> OnError
		{
			get { return _messagePump.OnError; }
			set { _messagePump.OnError = value; }
		}

		/// <summary>
		/// Gets or sets the logic to execute when queue is empty.
		/// </summary>
		/// <example>
		/// <code>
		/// OnQueueEmpty = cancellationToken => Task.Delay(2500, cancellationToken).Wait();
		/// </code>
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to pause for 2 seconds.
		/// </remarks>
		public Action<CancellationToken> OnQueueEmpty
		{
			get { return _messagePump.OnQueueEmpty; }
			set { _messagePump.OnQueueEmpty = value; }
		}

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="connectionString">
		/// A connection string includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </param>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poinson' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		[ExcludeFromCodeCoverage]
		public AsyncMessagePumpWithHandlers(string connectionString, string queueName, int concurrentTasks = 25, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
			: this(new QueueManager(connectionString, queueName), string.IsNullOrEmpty(poisonQueueName) ? null : new QueueManager(connectionString, poisonQueueName), concurrentTasks, visibilityTimeout, maxDequeueCount, logger, metrics)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="queueManager">The queue manager.</param>
		/// <param name="poisonQueueManager">The poison queue manager.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMessagePumpWithHandlers(QueueManager queueManager, QueueManager poisonQueueManager, int concurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
		{
			_logger = logger;

			_messagePump = new AsyncMessagePump(queueManager, poisonQueueManager, concurrentTasks, visibilityTimeout, maxDequeueCount, logger, metrics)
			{
				OnMessage = (message, cancellationToken) =>
				{
					var contentType = message.Content.GetType();

					if (!_messageHandlers.TryGetValue(contentType, out Type[] handlers))
					{
						throw new Exception($"Received a message of type {contentType.FullName} but could not find a class implementing IMessageHandler<{contentType.FullName}>");
					}

					foreach (var handlerType in handlers)
					{
						var handler = Activator.CreateInstance(handlerType);
						var handlerMethod = handlerType.GetMethod("Handle", new[] { contentType });
						handlerMethod.Invoke(handler, new[] { message.Content });
					}
				}
			};
		}

		#endregion

		#region PUBLIC METHODS

		/// <summary>
		/// Starts the message pump.
		/// </summary>
		public void Start()
		{
			_messagePump.Start();
		}

		/// <summary>
		/// Stops the message pump.
		/// </summary>
		public void Stop()
		{
			_messagePump.Stop();
		}

		#endregion

		#region PRIVATE METHODS

		private static IDictionary<Type, Type[]> GetMessageHandlers()
		{
			_logger.Trace("Discovering message handlers.");

			var assemblies = GetLocalAssemblies();

			var assembliesCount = assemblies.Length;
			if (assembliesCount == 0) _logger.Trace($"Did not find any local assembly.");
			else if (assembliesCount == 1) _logger.Trace("Found 1 local assembly.");
			else _logger.Trace($"Found {assemblies.Count()} local assemblies.");

			var typesWithMessageHandlerInterfaces = assemblies
				.SelectMany(x => x.GetTypes())
				.Where(t => !GetTypeInfo(t).IsInterface)
				.Select(type => new
				{
					Type = type,
					MessageTypes = type
						.GetInterfaces()
							.Where(i => GetTypeInfo(i).IsGenericType)
							.Where(i => i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
							.SelectMany(i => i.GetGenericArguments())
				})
				.Where(t => t.MessageTypes != null && t.MessageTypes.Any())
				.ToArray();

			var classesCount = typesWithMessageHandlerInterfaces.Length;
			if (classesCount == 0) _logger.Trace($"Did not find any class implementing the 'IMessageHandler' interface.");
			else if (classesCount == 1) _logger.Trace("Found 1 class implementing the 'IMessageHandler' interface.");
			else _logger.Trace($"Found {typesWithMessageHandlerInterfaces.Count()} classes implementing the 'IMessageHandler' interface.");

			var oneTypePerMessageHandler = typesWithMessageHandlerInterfaces
				.SelectMany(t => t.MessageTypes, (t, messageType) =>
				new
				{
					t.Type,
					MessageType = messageType
				})
				.ToArray();

			var messageHandlers = oneTypePerMessageHandler
				.GroupBy(h => h.MessageType)
				.ToDictionary(group => group.Key, group => group.Select(t => t.Type)
				.ToArray());

			return messageHandlers;
		}

		private static TypeInfo GetTypeInfo(Type type)
		{
			return type.GetTypeInfo();
		}

		private static Assembly[] GetLocalAssemblies()
		{
			var dependencies = DependencyContext.Default.RuntimeLibraries;

			var assemblies = new List<Assembly>();
			foreach (var library in dependencies)
			{
				if (IsCandidateLibrary(library))
				{
					var assembly = Assembly.Load(new AssemblyName(library.Name));
					assemblies.Add(assembly);
				}
			}

			return assemblies.ToArray();
		}

		private static bool IsCandidateLibrary(RuntimeLibrary library)
		{
			return !library.Name.StartsWith("Microsoft.", StringComparison.OrdinalIgnoreCase) &&
				!library.Name.StartsWith("System.", StringComparison.OrdinalIgnoreCase) &&
				!library.Name.StartsWith("NetStandard.", StringComparison.OrdinalIgnoreCase) &&
				!string.Equals(library.Type, "package", StringComparison.OrdinalIgnoreCase) &&
				!string.Equals(library.Type, "referenceassembly", StringComparison.OrdinalIgnoreCase);
		}

		#endregion
	}
}
