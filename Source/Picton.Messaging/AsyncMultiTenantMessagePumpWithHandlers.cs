using App.Metrics;
using Microsoft.Extensions.Logging;
using Picton.Messaging.Utilities;
using System;
using System.Collections.Generic;
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

		private static IDictionary<Type, Type[]> _messageHandlers;

		private readonly MessagePumpOptions _messagePumpOptions;
		private readonly string _queueNamePrefix;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private readonly ILogger _logger;

		private readonly AsyncMultiTenantMessagePump _messagePump;

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

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMultiTenantMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="options">Options for the mesage pump.</param>
		/// <param name="queueNamePrefix">The common prefix in the naming convention.</param>
		/// <param name="discoverQueuesInterval">The frequency we check for queues in the Azure storage account matching the naming convention. Default is 30 seconds.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMultiTenantMessagePumpWithHandlers(MessagePumpOptions options, string queueNamePrefix, TimeSpan? discoverQueuesInterval = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
		{
			_messageHandlers = MessageHandlersDiscoverer.GetMessageHandlers(logger);

			_messagePumpOptions = options;
			_queueNamePrefix = queueNamePrefix;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;
			_logger = logger;

			_messagePump = new AsyncMultiTenantMessagePump(options, queueNamePrefix, discoverQueuesInterval, visibilityTimeout, maxDequeueCount, logger, metrics);
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
			_messagePump.OnError = (queueName, message, exception, isPoison) => OnError?.Invoke(queueName.TrimStart(_queueNamePrefix), message, exception, isPoison);
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
	}
}
