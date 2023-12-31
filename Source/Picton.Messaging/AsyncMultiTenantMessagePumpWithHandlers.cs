using App.Metrics;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.Logging;
using Picton.Messaging.Messages;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	/// Designed to monitor an Azure storage queue and process the message as quickly and efficiently as possible.
	/// </summary>
	public class AsyncMultiTenantMessagePumpWithHandlers
	{
		#region FIELDS

		private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

		private static IDictionary<Type, Type[]> _messageHandlers;

		private readonly ILogger _logger;
		private readonly AsyncMultiTenantMessagePump _messagePump;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when an error occurs.
		/// </summary>
		/// <example>
		/// <code>
		/// OnError = (tenantId, message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);
		/// </code>
		/// </example>
		/// <remarks>
		/// When isPoison is set to true, you should copy this message to a poison queue because it will be deleted from the original queue.
		/// </remarks>
		public Action<string, CloudMessage, Exception, bool> OnError
		{
			get { return _messagePump.OnError; }
			set { _messagePump.OnError = value; }
		}

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMultiTenantMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="connectionString">
		/// A connection string includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </param>
		/// <param name="queueNamePrefix">Queues name prefix.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poinson' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="queueClientOptions">
		/// Optional client options that define the transport pipeline
		/// policies for authentication, retries, etc., that are applied to
		/// every request to the queue.
		/// </param>
		/// <param name="blobClientOptions">
		/// Optional client options that define the transport pipeline
		/// policies for authentication, retries, etc., that are applied to
		/// every request to the blob storage.
		/// </param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		[ExcludeFromCodeCoverage]
		public AsyncMultiTenantMessagePumpWithHandlers(string connectionString, string queueNamePrefix, int concurrentTasks = 25, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, QueueClientOptions queueClientOptions = null, BlobClientOptions blobClientOptions = null, ILogger logger = null, IMetrics metrics = null)
		{
			_logger = logger;

			_messagePump = new AsyncMultiTenantMessagePump(connectionString, queueNamePrefix, concurrentTasks, poisonQueueName, visibilityTimeout, maxDequeueCount, queueClientOptions, blobClientOptions, logger, metrics)
			{
				OnMessage = (tenantId, message, cancellationToken) =>
				{
					var contentType = message.Content.GetType();

					if (!_messageHandlers.TryGetValue(contentType, out Type[] handlers))
					{
						throw new Exception($"Received a message of type {contentType.FullName} but could not find a class implementing IMessageHandler<{contentType.FullName}>");
					}

					foreach (var handlerType in handlers)
					{
						object handler = null;
						if (handlerType.GetConstructor(new[] { typeof(ILogger) }) != null)
						{
							handler = Activator.CreateInstance(handlerType, new[] { (object)logger });
						}
						else
						{
							handler = Activator.CreateInstance(handlerType);
						}

						var handlerMethod = handlerType.GetMethod("Handle", new[] { contentType });
						handlerMethod.Invoke(handler, new[] { message.Content });
					}
				}
			};

			DiscoverMessageHandlersIfNecessary(logger);
		}

		#endregion

		#region PUBLIC METHODS

		/// <summary>
		/// Starts the message pump.
		/// </summary>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <exception cref="System.ArgumentNullException">OnMessage.</exception>
		/// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
		public Task StartAsync(CancellationToken cancellationToken)
		{
			return _messagePump.StartAsync(cancellationToken);
		}

		#endregion

		#region PRIVATE METHODS

		private static void DiscoverMessageHandlersIfNecessary(ILogger logger)
		{
			try
			{
				_lock.EnterUpgradeableReadLock();

				if (_messageHandlers == null)
				{
					try
					{
						_lock.EnterWriteLock();

						_messageHandlers ??= GetMessageHandlers(null);
					}
					finally
					{
						if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
					}
				}
			}
			finally
			{
				if (_lock.IsUpgradeableReadLockHeld) _lock.ExitUpgradeableReadLock();
			}
		}

		private static IDictionary<Type, Type[]> GetMessageHandlers(ILogger logger)
		{
			logger?.LogTrace("Discovering message handlers.");

			var assemblies = GetLocalAssemblies();

			var assembliesCount = assemblies.Length;
			if (assembliesCount == 0) logger?.LogTrace($"Did not find any local assembly.");
			else if (assembliesCount == 1) logger?.LogTrace("Found 1 local assembly.");
			else logger?.LogTrace($"Found {assemblies.Length} local assemblies.");

			var typesWithMessageHandlerInterfaces = assemblies
				.SelectMany(x => x.GetTypes())
				.Where(t => !t.GetTypeInfo().IsInterface)
				.Select(type => new
				{
					Type = type,
					MessageTypes = type
						.GetInterfaces()
							.Where(i => i.GetTypeInfo().IsGenericType)
							.Where(i => i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
							.SelectMany(i => i.GetGenericArguments())
				})
				.Where(t => t.MessageTypes != null && t.MessageTypes.Any())
				.ToArray();

			var classesCount = typesWithMessageHandlerInterfaces.Length;
			if (classesCount == 0) logger?.LogTrace("Did not find any class implementing the 'IMessageHandler' interface.");
			else if (classesCount == 1) logger?.LogTrace("Found 1 class implementing the 'IMessageHandler' interface.");
			else logger?.LogTrace($"Found {typesWithMessageHandlerInterfaces.Length} classes implementing the 'IMessageHandler' interface.");

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