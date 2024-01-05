using App.Metrics;
using Microsoft.Extensions.Logging;
using Picton.Managers;
using Picton.Messaging.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	/// Designed to monitor either a single queue or a fixed list of queues and process messages as
	/// quickly and efficiently as possible.
	/// </summary>
	public class AsyncMessagePumpWithHandlers
	{
		#region FIELDS

		private static IDictionary<Type, Type[]> _messageHandlers;

		private readonly AsyncMessagePump _messagePump;
		private readonly ILogger _logger;

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
		public Action<string, CloudMessage, Exception, bool> OnError { get; set; }

		/// <summary>
		/// Gets or sets the logic to execute when all queues are empty.
		/// </summary>
		/// <example>
		/// <code>
		/// OnEmpty = cancellationToken => Task.Delay(2500, cancellationToken).Wait();
		/// </code>
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to do nothing.
		/// </remarks>
		public Action<CancellationToken> OnEmpty { get; set; }

		#endregion

		#region CONSTRUCTORS

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueConfigs">The configuration options for each queue to be monitored.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		internal AsyncMessagePumpWithHandlers(MessagePumpOptions options, IEnumerable<(QueueManager QueueManager, QueueManager PoisonQueueManager, TimeSpan? VisibilityTimeout, int MaxDequeueCount)> queueConfigs, ILogger logger = null, IMetrics metrics = null)
		{
			_messageHandlers = MessageHandlersDiscoverer.GetMessageHandlers(logger);
			_messagePump = new AsyncMessagePump(options, queueConfigs, logger, metrics);
			_logger = logger;

		}

		#endregion

		#region STATIC METHODS

		/// <summary>
		/// Returns a message pump that will process messages in a single Azure storage queue.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poison' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		/// <returns>The message pump.</returns>
		public AsyncMessagePumpWithHandlers ForSingleQueue(MessagePumpOptions options, string queueName, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
		{
			var queueConfig = new QueueConfig(queueName, poisonQueueName, visibilityTimeout, maxDequeueCount);

			ValidateOptions(options);
			ValidateQueueConfig(queueConfig);

			return ForMultipleQueues(options, new[] { queueConfig }, logger, metrics);
		}

		/// <summary>
		/// Returns a message pump that will process messages in a single Azure storage queue.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueConfig"></param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		/// <returns>The message pump.</returns>
		public AsyncMessagePumpWithHandlers ForSingleQueue(MessagePumpOptions options, QueueConfig queueConfig, ILogger logger = null, IMetrics metrics = null)
		{
			ValidateOptions(options);
			ValidateQueueConfig(queueConfig);

			return ForMultipleQueues(options, new[] { queueConfig }, logger, metrics);
		}

		/// <summary>
		/// Returns a message pump that will process messages in multiple Azure storage queues.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueConfigs"></param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		/// <returns>The message pump.</returns>
		public AsyncMessagePumpWithHandlers ForMultipleQueues(MessagePumpOptions options, IEnumerable<QueueConfig> queueConfigs, ILogger logger = null, IMetrics metrics = null)
		{
			ValidateOptions(options);
			ValidateQueueConfigs(queueConfigs);

			var configs = queueConfigs
				.Select(queueConfig =>
				{
					var queueManager = new QueueManager(options.ConnectionString, queueConfig.QueueName, true, options.QueueClientOptions, options.BlobClientOptions);
					var poisonQueueManager = string.IsNullOrEmpty(queueConfig.PoisonQueueName) ? null : new QueueManager(options.ConnectionString, queueConfig.PoisonQueueName);
					return (queueManager, poisonQueueManager, queueConfig.VisibilityTimeout, queueConfig.MaxDequeueCount);
				})
				.ToArray();

			return new AsyncMessagePumpWithHandlers(options, configs, logger, metrics);
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
			_messagePump.OnEmpty = OnEmpty;
			_messagePump.OnError = OnError;
			_messagePump.OnMessage = (queueName, message, cancellationToken) =>
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
						handler = Activator.CreateInstance(handlerType, new[] { (object)_logger });
					}
					else
					{
						handler = Activator.CreateInstance(handlerType);
					}

					var handlerMethod = handlerType.GetMethod("Handle", new[] { contentType });
					handlerMethod.Invoke(handler, new[] { message.Content });
				}
			};

			return _messagePump.StartAsync(cancellationToken);
		}

		#endregion

		#region PRIVATE METHODS

		private void ValidateOptions(MessagePumpOptions options)
		{
			if (options == null) throw new ArgumentNullException(nameof(options));
			if (string.IsNullOrEmpty(options.ConnectionString)) throw new ArgumentNullException(nameof(options.ConnectionString));
			if (options.ConcurrentTasks < 1) throw new ArgumentOutOfRangeException(nameof(options.ConcurrentTasks), "Number of concurrent tasks must be greather than zero");
		}

		private void ValidateQueueConfig(QueueConfig queueConfig)
		{
			if (queueConfig == null) throw new ArgumentNullException(nameof(queueConfig));
			if (string.IsNullOrEmpty(queueConfig.QueueName)) throw new ArgumentNullException(nameof(queueConfig.QueueName));
			if (queueConfig.MaxDequeueCount < 1) throw new ArgumentOutOfRangeException(nameof(queueConfig.MaxDequeueCount), $"Number of retries for {queueConfig.QueueName} must be greater than zero.");
		}

		private void ValidateQueueConfigs(IEnumerable<QueueConfig> queueConfigs)
		{
			foreach (var queueConfig in queueConfigs)
			{
				ValidateQueueConfig(queueConfig);
			}
		}

		#endregion
	}
}
