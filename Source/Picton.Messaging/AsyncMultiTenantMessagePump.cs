using App.Metrics;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Logging;
using Picton.Messaging.Utilities;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	///
	/// Designed to monitor multiple Azure storage queues that follow the following naming convention:
	/// a common prefix followed by a unique tenant identifier. For example, if the prefix is "myqueue",
	/// this message pump will monitor queues such as "myqueue001", myqueue002" and "myqueueabc".
	///
	/// Please note that the message pump specifically ignores queue that follow the following naming convention:
	/// - the common prefix without a postfix. For example "myqueue". Notice the absence of a tenant identifier
	/// after the "myqueue" part in the name.
	/// - The common prefix followed by "-poison". For example "myqueue-poison".
	///
	/// Furthermore, the list of queues matching the naming convention is refreshed at regular interval in order
	/// to discover new tenant queues that might have been created in the Azure storage account.
	/// </summary>
	public class AsyncMultiTenantMessagePump
	{
		#region FIELDS

		private readonly MessagePumpOptions _messagePumpOptions;
		private readonly string _queueNamePrefix;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private readonly ILogger _logger;
		private readonly AsyncMessagePump _messagePump;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when a message is retrieved from the queue.
		/// </summary>
		/// <remarks>
		/// If exception is thrown when calling OnMessage, it will regard this queue message as failed.
		/// </remarks>
		public Action<string, CloudMessage, CancellationToken> OnMessage { get; set; }

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
		/// Initializes a new instance of the <see cref="AsyncMultiTenantMessagePump"/> class.
		/// </summary>
		/// <param name="options">Options for the mesage pump.</param>
		/// <param name="queueNamePrefix">The common prefix in the naming convention.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMultiTenantMessagePump(MessagePumpOptions options, string queueNamePrefix, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
		{
			if (options == null) throw new ArgumentNullException(nameof(options));
			if (string.IsNullOrEmpty(options.ConnectionString)) throw new ArgumentNullException(nameof(options.ConnectionString));
			if (options.ConcurrentTasks < 1) throw new ArgumentOutOfRangeException(nameof(options.ConcurrentTasks), "Number of concurrent tasks must be greather than zero");
			if (string.IsNullOrEmpty(queueNamePrefix)) throw new ArgumentNullException(nameof(queueNamePrefix));

			_messagePumpOptions = options;
			_queueNamePrefix = queueNamePrefix;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;
			_logger = logger;
			_messagePump = new AsyncMessagePump(options, logger, metrics);
		}

		#endregion

		#region PUBLIC METHODS

		/// <summary>
		/// Starts the message pump.
		/// </summary>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <exception cref="System.ArgumentNullException">OnMessage.</exception>
		/// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			if (OnMessage == null) throw new ArgumentNullException(nameof(OnMessage));

			_messagePump.OnEmpty = OnEmpty;
			_messagePump.OnError = (queueName, message, exception, isPoison) => OnError?.Invoke(queueName.TrimStart(_queueNamePrefix), message, exception, isPoison);
			_messagePump.OnMessage = (queueName, message, cancellationToken) => OnMessage?.Invoke(queueName.TrimStart(_queueNamePrefix), message, cancellationToken);

			// Define the task that discovers queues that follow the naming convention
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					try
					{
						var queueServiceClient = new QueueServiceClient(_messagePumpOptions.ConnectionString);
						var response = queueServiceClient.GetQueuesAsync(QueueTraits.None, _queueNamePrefix, cancellationToken);
						await foreach (Page<QueueItem> queues in response.AsPages())
						{
							foreach (var queue in queues.Values)
							{
								if (!queue.Name.Equals(_queueNamePrefix, StringComparison.OrdinalIgnoreCase) &&
									!queue.Name.Equals($"{_queueNamePrefix}-poison", StringComparison.OrdinalIgnoreCase))
								{
									// AddQueue will make sure to add the queue only if it's not already in the round-robin list of queues.
									_messagePump.AddQueue(queue.Name, $"{_queueNamePrefix}-poison", _visibilityTimeout, _maxDequeueCount);
								}
							}
						}

						// Please note there is no need to remove queues that no longer exist from the message
						// pump round-robin list. The reason is: message pump will get a RequestFailedException
						// with ErrorCode == "QueueNotFound" next time the message pump tries to query those
						// queues and it will automatically remove them at that time.
					}
					catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
					{
						// The message pump is shutting down.
						// This exception can be safely ignored.
					}
					catch (Exception e)
					{
						_logger?.LogError(e.GetBaseException(), "An error occured while fetching the Azure queues that match the naming convention. The error was caught and ignored.");
					}
				},
				TimeSpan.FromMilliseconds(30000),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Brief pause to ensure the task defined above runs at least once before we start processing messages
			await Task.Delay(500, cancellationToken).ConfigureAwait(false);

			await _messagePump.StartAsync(cancellationToken).ConfigureAwait(false);
		}

		#endregion
	}
}
