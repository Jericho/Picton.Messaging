using App.Metrics;
using Microsoft.Extensions.Logging;
using Picton.Messaging.Utilities;
using System;
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

		private readonly string _queueNamePrefix;
		private readonly ILogger _logger;
		private readonly CloudMessageHandler _cloudMessageHandler;
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
		/// Gets or sets the logic to execute when a queue is empty.
		/// </summary>
		/// <example>
		/// <code>
		/// OnQueueEmpty = (queueName, cancellationToken) => _logger.LogInformation("Queue {queueName} is empty", queueName);
		/// </code>
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to do nothing.
		/// </remarks>
		public Action<string, CancellationToken> OnQueueEmpty { get; set; }

		/// <summary>
		/// Gets or sets the logic to execute when all queues are empty.
		/// </summary>
		/// <example>
		/// <code>
		/// OnAllQueuesEmpty = (cancellationToken) => _logger.LogInformation("All queues are empty");
		/// </code>
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to do nothing.
		/// </remarks>
		public Action<CancellationToken> OnAllQueuesEmpty { get; set; }

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMultiTenantMessagePumpWithHandlers"/> class.
		/// </summary>
		/// <param name="options">Options for the mesage pump.</param>
		/// <param name="serviceProvider">DI.</param>
		/// <param name="queueNamePrefix">The common prefix in the naming convention.</param>
		/// <param name="discoverQueuesInterval">The frequency we check for queues in the Azure storage account matching the naming convention. Default is 30 seconds.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMultiTenantMessagePumpWithHandlers(MessagePumpOptions options, IServiceProvider serviceProvider, string queueNamePrefix, TimeSpan? discoverQueuesInterval = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
		{
			_queueNamePrefix = queueNamePrefix;
			_logger = logger;
			_cloudMessageHandler = new CloudMessageHandler(serviceProvider);
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
			_messagePump.OnQueueEmpty = (queueName, cancellationToken) => OnQueueEmpty?.Invoke(queueName.TrimStart(_queueNamePrefix), cancellationToken);
			_messagePump.OnAllQueuesEmpty = OnAllQueuesEmpty;
			_messagePump.OnError = (queueName, message, exception, isPoison) => OnError?.Invoke(queueName.TrimStart(_queueNamePrefix), message, exception, isPoison);
			_messagePump.OnMessage = async (queueName, message, cancellationToken) =>
			{
				await _cloudMessageHandler.HandleMessageAsync(message, cancellationToken).ConfigureAwait(false);
			};

			return _messagePump.StartAsync(cancellationToken);
		}

		#endregion
	}
}
