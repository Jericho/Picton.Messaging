using App.Metrics;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Picton.Interfaces;
using Picton.Managers;
using Picton.Messaging.Logging;
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

		private static readonly ILog _logger = LogProvider.GetLogger(typeof(AsyncMessagePump));

		private readonly IQueueManager _queueManager;
		private readonly IQueueManager _poisonQueueManager;
		private readonly int _concurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private readonly IMetrics _metrics;

		private CancellationTokenSource _cancellationTokenSource;
		private ManualResetEvent _safeToExitHandle;

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
		/// OnError = (message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);
		/// </example>
		/// <remarks>
		/// When isPoison is set to true, you should copy this message to a poison queue because it will be deleted from the original queue.
		/// </remarks>
		public Action<CloudMessage, Exception, bool> OnError { get; set; }

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
		public Action<CancellationToken> OnQueueEmpty { get; set; }

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePump"/> class.
		/// </summary>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="storageAccount">The cloud storage account.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poison' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="metrics">The system where metrics are published</param>
		public AsyncMessagePump(string queueName, CloudStorageAccount storageAccount, int concurrentTasks = 25, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, IMetrics metrics = null)
		{
			if (storageAccount == null) throw new ArgumentNullException(nameof(storageAccount));

			if (concurrentTasks < 1) throw new ArgumentException("Number of concurrent tasks must be greather than zero", nameof(concurrentTasks));
			if (maxDequeueCount < 1) throw new ArgumentException("Number of retries must be greather than zero", nameof(maxDequeueCount));

			var queueClient = storageAccount.CreateCloudQueueClient();
			var blobClient = storageAccount.CreateCloudBlobClient();

			_queueManager = new QueueManager(queueName, queueClient, blobClient);
			_concurrentTasks = concurrentTasks;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;
			_metrics = EnsureValidMetrics(metrics);

			if (!string.IsNullOrEmpty(poisonQueueName))
			{
				_poisonQueueManager = new QueueManager(poisonQueueName, queueClient, blobClient);
			}

			InitMessagePump();
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePump"/> class.
		/// </summary>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="queueClient">The queue client.</param>
		/// <param name="blobClient">The blob client.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poison' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="metrics">The system where metrics are published</param>
		public AsyncMessagePump(string queueName, CloudQueueClient queueClient, CloudBlobClient blobClient, int concurrentTasks = 25, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, IMetrics metrics = null)
		{
			if (concurrentTasks < 1) throw new ArgumentException("Number of concurrent tasks must be greather than zero", nameof(concurrentTasks));
			if (maxDequeueCount < 1) throw new ArgumentException("Number of retries must be greather than zero", nameof(maxDequeueCount));

			_queueManager = new QueueManager(queueName, queueClient, blobClient);
			_concurrentTasks = concurrentTasks;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;
			_metrics = EnsureValidMetrics(metrics);

			if (!string.IsNullOrEmpty(poisonQueueName))
			{
				_poisonQueueManager = new QueueManager(poisonQueueName, queueClient, blobClient);
			}

			InitMessagePump();
		}

		#endregion

		#region PUBLIC METHODS

		/// <summary>
		/// Starts the message pump.
		/// </summary>
		/// <exception cref="System.ArgumentNullException">OnMessage</exception>
		public void Start()
		{
			if (OnMessage == null) throw new ArgumentNullException(nameof(OnMessage));

			_logger.Trace($"{nameof(AsyncMessagePump)} starting...");

			_cancellationTokenSource = new CancellationTokenSource();
			_safeToExitHandle = new ManualResetEvent(false);

			ProcessMessages(_visibilityTimeout, _cancellationTokenSource.Token).Wait();

			_cancellationTokenSource.Dispose();

			_logger.Trace($"{nameof(AsyncMessagePump)} ready to exit");
			_safeToExitHandle.Set();
		}

		/// <summary>
		/// Stops the message pump.
		/// </summary>
		public void Stop()
		{
			// Don't attempt to stop the message pump if it's already in the process of stopping
			if (_cancellationTokenSource?.IsCancellationRequested ?? false) return;

			// Stop the message pump
			_logger.Trace($"{nameof(AsyncMessagePump)} stopping...");
			if (_cancellationTokenSource != null) _cancellationTokenSource.Cancel();
			if (_safeToExitHandle != null) _safeToExitHandle.WaitOne();
			_logger.Trace($"{nameof(AsyncMessagePump)} stopped, exiting safely");
		}

		#endregion

		#region PRIVATE METHODS

		private IMetrics EnsureValidMetrics(IMetrics metrics)
		{
			if (metrics != null)
			{
				return metrics;
			}

			var metricsTurnedOff = new MetricsBuilder();
			metricsTurnedOff.Configuration.Configure(new MetricsOptions()
			{
				Enabled = false,
				ReportingEnabled = false
			});
			return metricsTurnedOff.Build();
		}

		private void InitMessagePump()
		{
			OnQueueEmpty = cancellationToken => Task.Delay(1500, cancellationToken).Wait();
			OnError = (message, exception, isPoison) => _logger.ErrorException("An error occured when processing a message", exception);
		}

		private async Task ProcessMessages(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default(CancellationToken))
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
						using (_metrics.Measure.Timer.Time(Metrics.MessageFetchingTimer))
						{
							IEnumerable<CloudMessage> messages = null;
							try
							{
								messages = await _queueManager.GetMessagesAsync(_concurrentTasks, visibilityTimeout, null, null, cancellationToken).ConfigureAwait(false);
							}
							catch (TaskCanceledException)
							{
								// The message pump is shutting down.
								// This exception can be safely ignored.
							}
							catch (Exception e)
							{
								_logger.InfoException("An error occured while fetching messages from the Azure queue. The error was caught and ignored.", e.GetBaseException());
							}

							if (messages == null) return;

							if (messages.Any())
							{
								_logger.Trace($"Fetched {messages.Count()} message(s) from the queue.");

								foreach (var message in messages)
								{
									queuedMessages.Enqueue(message);
								}
							}
							else
							{
								_logger.Trace("The queue is empty, no messages fetched.");
								try
								{
									// The queue is empty
									OnQueueEmpty?.Invoke(cancellationToken);
									_metrics.Measure.Counter.Increment(Metrics.QueueEmptyCounter);
								}
								catch (Exception e)
								{
									_logger.InfoException("An error occured when handling an empty queue. The error was caught and ignored.", e.GetBaseException());
								}
							}
						}
					}
				},
				TimeSpan.FromMilliseconds(500),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task that checks how many messages are queued
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					try
					{
						var count = await _queueManager.GetApproximateMessageCountAsync(cancellationToken).ConfigureAwait(false);
						count += queuedMessages.Count;
						_metrics.Measure.Gauge.SetValue(Metrics.QueuedMessagesGauge, count);
					}
					catch (TaskCanceledException)
					{
						// The message pump is shutting down.
						// This exception can be safely ignored.
					}
					catch (Exception e)
					{
						_logger.InfoException("An error occured while checking how many message are waiting in the queue. The error was caught and ignored.", e.GetBaseException());
					}
				},
				TimeSpan.FromMilliseconds(5000),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task pump
			var pumpTask = Task.Run(async () =>
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

					// Retrieved the next message from the queue and process it
					var runningTask = Task.Run(
						async () =>
						{
							var messageProcessed = false;

							if (cancellationToken.IsCancellationRequested) return messageProcessed;

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
										await _queueManager.DeleteMessageAsync(message, null, null, CancellationToken.None).ConfigureAwait(false);
									}
									catch (Exception ex)
									{
										var isPoison = message.DequeueCount > _maxDequeueCount;
										OnError?.Invoke(message, ex, isPoison);
										if (isPoison)
										{
											// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue and moved to poison queue even when the message pump is shutting down
											if (_poisonQueueManager != null)
											{
												message.Metadata["PoisonExceptionMessage"] = ex.GetBaseException().Message;
												message.Metadata["PoisonExceptionDetails"] = ex.GetBaseException().ToString();

												await _poisonQueueManager.AddMessageAsync(message.Content, message.Metadata, null, null, null, null, CancellationToken.None).ConfigureAwait(false);
											}
											await _queueManager.DeleteMessageAsync(message, null, null, CancellationToken.None).ConfigureAwait(false);
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
						}, TaskContinuationOptions.ExecuteSynchronously)
					.IgnoreAwait();
				}
			});

			// Run the task pump until canceled
			await pumpTask.UntilCancelled().ConfigureAwait(false);

			// Task pump has been canceled, wait for the currently running tasks to complete
			await Task.WhenAll(runningTasks.Values).UntilCancelled().ConfigureAwait(false);
		}

		#endregion
	}
}
