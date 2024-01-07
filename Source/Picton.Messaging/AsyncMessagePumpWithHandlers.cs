using App.Metrics;
using Microsoft.Extensions.Logging;
using Picton.Managers;
using Picton.Messaging.Utilities;
using System;
using System.Collections.Generic;
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
		/// <param name="options">Options for the mesage pump.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMessagePumpWithHandlers(MessagePumpOptions options, ILogger logger = null, IMetrics metrics = null)
		{
			_messageHandlers = MessageHandlersDiscoverer.GetMessageHandlers(logger);
			_messagePump = new AsyncMessagePump(options, logger, metrics);
			_logger = logger;
		}

		#endregion

		#region PUBLIC METHODS


		/// <summary>
		/// Add a queue to be monitored.
		/// </summary>
		/// <param name="queueName">The name of the queue.</param>
		/// <param name="poisonQueueName">Optional. The name of the queue where poison messages are automatically moved.</param>
		/// <param name="visibilityTimeout">Optional. Specifies the visibility timeout value. The default value is 30 seconds.</param>
		/// <param name="maxDequeueCount">Optional. A nonzero integer value that specifies the number of time we try to process a message before giving up and declaring the message to be "poison". The default value is 3.</param>
		public void AddQueue(string queueName, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
		{
			_messagePump.AddQueue(queueName, poisonQueueName, visibilityTimeout, maxDequeueCount);
		}

		/// <summary>
		/// Add a queue to be monitored.
		/// </summary>
		/// <param name="queueConfig">Queue configuration.</param>
		public void AddQueue(QueueConfig queueConfig)
		{
			_messagePump.AddQueue(queueConfig);
		}

		/// <summary>
		/// Remove a queue from the list of queues that are monitored.
		/// </summary>
		/// <param name="queueName">The name of the queue.</param>
		public void RemoveQueue(string queueName)
		{
			_messagePump.RemoveQueue(queueName);
		}

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

		// This internal method is primarily for unit testing purposes. It allows me to inject mocked queue managers
		internal void AddQueue(QueueManager queueManager, QueueManager poisonQueueManager, TimeSpan? visibilityTimeout, int maxDequeueCount)
		{
			_messagePump.AddQueue(queueManager, poisonQueueManager, visibilityTimeout, maxDequeueCount);
		}

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
