using App.Metrics;
using Microsoft.Extensions.Logging;
using Picton.Interfaces;
using Picton.Managers;
using Picton.Messaging.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	/// Designed to monitor an Azure storage queue and process the message as quickly and efficiently as possible.
	/// </summary>
	public class AsyncMessagePump
	{
		#region FIELDS

		private readonly IQueueManager _queueManager;
		private readonly IQueueManager _poisonQueueManager;
		private readonly int _concurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private readonly ILogger _logger;
		private readonly IMetrics _metrics;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when a message is retrieved from the queue.
		/// </summary>
		/// <remarks>
		/// If exception is thrown when calling OnMessage, it will regard this queue message as failed.
		/// </remarks>
		public Action<CloudMessage, CancellationToken> OnMessage { get; set; }

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
		public Action<CloudMessage, Exception, bool> OnError { get; set; }

		/// <summary>
		/// Gets or sets the logic to execute when queue is empty.
		/// </summary>
		/// <example>
		/// <code>
		/// OnQueueEmpty = cancellationToken => Task.Delay(2500, cancellationToken).Wait();
		/// </code>
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to pause for 1.5 seconds.
		/// </remarks>
		public Action<CancellationToken> OnQueueEmpty { get; set; }

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePump"/> class.
		/// </summary>
		/// <param name="connectionString">
		/// A connection string includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </param>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poison' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		[ExcludeFromCodeCoverage]
		public AsyncMessagePump(string connectionString, string queueName, int concurrentTasks = 25, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
			: this(new QueueManager(connectionString, queueName), string.IsNullOrEmpty(poisonQueueName) ? null : new QueueManager(connectionString, poisonQueueName), concurrentTasks, visibilityTimeout, maxDequeueCount, logger, metrics)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePump"/> class.
		/// </summary>
		/// <param name="queueManager">The queue manager.</param>
		/// <param name="poisonQueueManager">The poison queue manager.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMessagePump(QueueManager queueManager, QueueManager poisonQueueManager, int concurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
		{
			if (concurrentTasks < 1) throw new ArgumentOutOfRangeException("Number of concurrent tasks must be greather than zero", nameof(concurrentTasks));
			if (maxDequeueCount < 1) throw new ArgumentOutOfRangeException("Number of retries must be greather than zero", nameof(maxDequeueCount));

			_queueManager = queueManager ?? throw new ArgumentNullException(nameof(queueManager));
			_poisonQueueManager = poisonQueueManager;
			_concurrentTasks = concurrentTasks;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;
			_logger = logger;
			_metrics = metrics ?? TurnOffMetrics();

			InitDefaultActions();
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

			_logger?.LogTrace("AsyncMessagePump starting...");
			await ProcessMessagesAsync(_visibilityTimeout, cancellationToken).ConfigureAwait(false);
			_logger?.LogTrace("AsyncMessagePump stopping...");
		}

		#endregion

		#region PRIVATE METHODS

		private void InitDefaultActions()
		{
			OnError = (message, exception, isPoison) => _logger?.LogError(exception, "An error occured when processing a message");
			OnQueueEmpty = (cancellationToken) => Task.Delay(1500, cancellationToken).Wait();
		}

		private IMetrics TurnOffMetrics()
		{
			var metricsTurnedOff = new MetricsBuilder();
			metricsTurnedOff.Configuration.Configure(new MetricsOptions()
			{
				Enabled = false,
				ReportingEnabled = false
			});
			return metricsTurnedOff.Build();
		}

		private async Task ProcessMessagesAsync(TimeSpan? visibilityTimeout, CancellationToken cancellationToken)
		{
			var runningTasks = new ConcurrentDictionary<Task, Task>();
			var semaphore = new SemaphoreSlim(_concurrentTasks, _concurrentTasks);
			var queuedMessages = new ConcurrentQueue<CloudMessage>();

			// Define the task that fetches messages from the Azure queue
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					// Fetch messages from the Azure queue when the number of items in the concurrent queue falls below an "acceptable" level.
					if (!cancellationToken.IsCancellationRequested && queuedMessages.Count <= _concurrentTasks / 2)
					{
						IEnumerable<CloudMessage> messages = null;
						using (_metrics.Measure.Timer.Time(Metrics.MessagesFetchingTimer))
						{
							try
							{
								messages = await _queueManager.GetMessagesAsync(_concurrentTasks, visibilityTimeout, cancellationToken).ConfigureAwait(false);
							}
							catch (TaskCanceledException)
							{
								// The message pump is shutting down.
								// This exception can be safely ignored.
							}
							catch (Exception e)
							{
								_logger?.LogError(e.GetBaseException(), "An error occured while fetching messages from the Azure queue. The error was caught and ignored.");
							}
						}

						if (messages == null) return;

						if (messages.Any())
						{
							var messagesCount = messages.Count();
							_logger?.LogTrace("Fetched {messagesCount} message(s) from the queue.", messagesCount);

							foreach (var message in messages)
							{
								queuedMessages.Enqueue(message);
							}
						}
						else
						{
							_logger?.LogTrace("The queue is empty, no messages fetched.");
							try
							{
								// The queue is empty
								_metrics.Measure.Counter.Increment(Metrics.QueueEmptyCounter);
								OnQueueEmpty?.Invoke(cancellationToken);
							}
							catch (Exception e)
							{
								_logger?.LogError(e.GetBaseException(), "An error occured when handling an empty queue. The error was caught and ignored.");
							}
						}
					}
				},
				TimeSpan.FromMilliseconds(500),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task that checks how many messages are queued in the Azure queue
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					try
					{
						var count = await _queueManager.GetApproximateMessageCountAsync(cancellationToken).ConfigureAwait(false);
						_metrics.Measure.Gauge.SetValue(Metrics.QueuedCloudMessagesGauge, count);
					}
					catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
					{
						// The message pump is shutting down.
						// This exception can be safely ignored.
					}
					catch (Exception e)
					{
						_logger?.LogError(e.GetBaseException(), "An error occured while checking how many message are waiting in the Azure queue. The error was caught and ignored.");
					}
				},
				TimeSpan.FromMilliseconds(5000),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task that checks how many messages are queued in memory
			RecurrentCancellableTask.StartNew(
				() =>
				{
					try
					{
						_metrics.Measure.Gauge.SetValue(Metrics.QueuedMemoryMessagesGauge, queuedMessages.Count);
					}
					catch (Exception e)
					{
						_logger?.LogError(e.GetBaseException(), "An error occured while checking how many message are waiting in the memory queue. The error was caught and ignored.");
					}

					return Task.CompletedTask;
				},
				TimeSpan.FromMilliseconds(5000),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task pump
			var pumpTask = Task.Run(async () =>
			{
				// We process messages until cancellation is requested.
				// When cancellation is requested, we continue processing messages until the memory queue is drained.
				while (!cancellationToken.IsCancellationRequested || !queuedMessages.IsEmpty)
				{
					await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

					// Retrieved the next message from the queue and process it
					var runningTask = Task.Run(
						async () =>
						{
							var messageProcessed = false;

							using (_metrics.Measure.Timer.Time(Metrics.MessageProcessingTimer))
							{
								queuedMessages.TryDequeue(out CloudMessage message);

								if (message != null)
								{
									try
									{
										// Process the message
										OnMessage?.Invoke(message, cancellationToken);

										// Delete the processed message from the queue
										// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue even when the message pump is shutting down
										await _queueManager.DeleteMessageAsync(message, CancellationToken.None).ConfigureAwait(false);
									}
									catch (Exception ex)
									{
										var isPoison = message.DequeueCount >= _maxDequeueCount;
										OnError?.Invoke(message, ex, isPoison);
										if (isPoison)
										{
											// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue and moved to poison queue even when the message pump is shutting down
											if (_poisonQueueManager != null)
											{
												message.Metadata["PoisonExceptionMessage"] = ex.GetBaseException().Message;
												message.Metadata["PoisonExceptionDetails"] = ex.GetBaseException().ToString();

												await _poisonQueueManager.AddMessageAsync(message.Content, message.Metadata, null, null, CancellationToken.None).ConfigureAwait(false);
											}

											await _queueManager.DeleteMessageAsync(message, CancellationToken.None).ConfigureAwait(false);
										}
									}

									messageProcessed = true;
								}
							}

							// Increment the counter if we processed a message
							if (messageProcessed) _metrics.Measure.Counter.Increment(Metrics.MessagesProcessedCounter);

							// Return a value indicating whether we processed a message or not
							return messageProcessed;
						},
						CancellationToken.None);

					// Add the task to the dictionary of tasks (allows us to keep track of the running tasks)
					runningTasks.TryAdd(runningTask, runningTask);

					// Complete the task
					runningTask.ContinueWith(
						t =>
						{
							semaphore.Release();
							runningTasks.TryRemove(t, out Task taskToBeRemoved);
						},
						TaskContinuationOptions.ExecuteSynchronously)
					.IgnoreAwait();
				}
			});

			// Run the task pump until canceled
			await pumpTask.UntilCancelled().ConfigureAwait(false);

			// Task pump has been canceled, wait for the currently running tasks to complete
			await Task.WhenAll(runningTasks.Values).UntilCancelled().ConfigureAwait(false);
		}
	}

	#endregion
}
