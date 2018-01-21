using Microsoft.Extensions.DependencyModel;
using Microsoft.WindowsAzure.Storage;
using Picton.Interfaces;
using Picton.Messaging.Logging;
using Picton.Messaging.Messages;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
#if NETSTANDARD
using System.Runtime.Loader;
#endif
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

		private static readonly ILog _logger = LogProvider.GetLogger(typeof(AsyncMessagePumpWithHandlers));
		private static readonly IDictionary<Type, Type[]> _messageHandlers = GetMessageHandlers();
		private readonly AsyncMessagePump _messagePump;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when an error occurs.
		/// </summary>
		/// <example>
		/// OnError = (message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);
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
		/// Here's an example:
		/// OnQueueEmpty = cancellationToken => Task.Delay(2500, cancellationToken).Wait();
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
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="cloudStorageAccount">The cloud storage account.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		public AsyncMessagePumpWithHandlers(string queueName, CloudStorageAccount cloudStorageAccount, int concurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
			: this(queueName, StorageAccount.FromCloudStorageAccount(cloudStorageAccount), concurrentTasks, visibilityTimeout, maxDequeueCount)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="storageAccount">The storage account</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="visibilityTimeout">The queue visibility timeout</param>
		/// <param name="maxDequeueCount">The number of times to try processing a given message before giving up</param>
		public AsyncMessagePumpWithHandlers(string queueName, IStorageAccount storageAccount, int concurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
		{
			_messagePump = new AsyncMessagePump(queueName, storageAccount, concurrentTasks, visibilityTimeout, maxDequeueCount)
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
			var assemblies = GetLocalAssemblies();

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

			var oneTypePerMessageHandler = typesWithMessageHandlerInterfaces
				.SelectMany(t => t.MessageTypes, (t, messageType) =>
				new
				{
					t.Type,
					MessageType = messageType
				}).ToArray();

			var messageHandlers = oneTypePerMessageHandler
				.GroupBy(h => h.MessageType)
				.ToDictionary(group => group.Key, group => group.Select(t => t.Type).ToArray());
			return messageHandlers;
		}

#if NETFULL
		private static Type GetTypeInfo(Type type)
		{
			return type;
		}

		private static IEnumerable<Assembly> GetLocalAssemblies()
		{
			var callingAssembly = Assembly.GetCallingAssembly();
			var path = new Uri(Path.GetDirectoryName(callingAssembly.Location)).AbsolutePath;

			return AppDomain.CurrentDomain.GetAssemblies()
				.Where(x => !x.IsDynamic && new Uri(x.CodeBase).AbsolutePath.Contains(path)).ToList();
		}
#else
		private static TypeInfo GetTypeInfo(Type type)
		{
			return type.GetTypeInfo();
		}

		private static IEnumerable<Assembly> GetLocalAssemblies()
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

			return assemblies;
		}

		private static bool IsCandidateLibrary(RuntimeLibrary library)
		{
			return !library.Name.StartsWith("Microsoft.", StringComparison.OrdinalIgnoreCase) &&
				!library.Name.StartsWith("System.", StringComparison.OrdinalIgnoreCase) &&
				!library.Name.StartsWith("NetStandard.", StringComparison.OrdinalIgnoreCase) &&
				!string.Equals(library.Type, "package", StringComparison.OrdinalIgnoreCase) &&
				!string.Equals(library.Type, "referenceassembly", StringComparison.OrdinalIgnoreCase);
		}
#endif

		#endregion
	}
}
