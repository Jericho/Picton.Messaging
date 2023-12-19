using App.Metrics;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Logging;
using Picton.Interfaces;
using Picton.Managers;
using Picton.Messaging.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	/// Designed to monitor multiple Azure storage queues that follow the following naming convention:
	/// a common prefix followed by a unique tenant identifier.
	/// </summary>
	public class AsyncMultiTenantMessagePump
	{
		#region FIELDS

		private readonly Func<string, QueueManager> _queueManagerFactory;

		private readonly ConcurrentDictionary<string, Lazy<(QueueManager QueueManager, DateTime LastFetched, TimeSpan FetchDelay)>> _tenantQueueManagers = new();
		private readonly RoundRobinList<string> _tenantIds = new RoundRobinList<string>(Array.Empty<string>());

		private readonly IQueueManager _poisonQueueManager;
		private readonly string _connectionString;
		private readonly string _queueNamePrefix;
		private readonly int _concurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private readonly ILogger _logger;
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
		/// Gets or sets the logic to execute when all tenant queues are empty.
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
		/// <param name="connectionString">
		/// A connection string includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </param>
		/// <param name="queueNamePrefix">Queues name prefix.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks.</param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poison' messages.</param>
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
		public AsyncMultiTenantMessagePump(string connectionString, string queueNamePrefix, int concurrentTasks = 25, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, QueueClientOptions queueClientOptions = null, BlobClientOptions blobClientOptions = null, ILogger logger = null, IMetrics metrics = null)
		{
			if (concurrentTasks < 1) throw new ArgumentOutOfRangeException("Number of concurrent tasks must be greather than zero", nameof(concurrentTasks));
			if (maxDequeueCount < 1) throw new ArgumentOutOfRangeException("Number of retries must be greather than zero", nameof(maxDequeueCount));

			_queueManagerFactory = (tenantId) =>
			{
				var blobContainerClient = new BlobContainerClient(connectionString, $"{queueNamePrefix}{tenantId}-oversized-messages", blobClientOptions);
				var queueClient = new QueueClient(connectionString, $"{queueNamePrefix}{tenantId}", queueClientOptions);
				return new QueueManager(blobContainerClient, queueClient, true);
			};

			_connectionString = connectionString ?? throw new ArgumentNullException(connectionString);
			_queueNamePrefix = queueNamePrefix ?? throw new ArgumentNullException(queueNamePrefix);
			_concurrentTasks = concurrentTasks;
			_poisonQueueManager = string.IsNullOrEmpty(poisonQueueName) ? null : new QueueManager(connectionString, poisonQueueName);
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
		/// <exception cref="System.ArgumentNullException">OnMessage.</exception>
		public void Start()
		{
			if (OnMessage == null) throw new ArgumentNullException(nameof(OnMessage));

			_logger?.LogTrace("AsyncMultiTenantMessagePump starting message pump...");

			_cancellationTokenSource = new CancellationTokenSource();
			_safeToExitHandle = new ManualResetEvent(false);

			ProcessMessages(_visibilityTimeout, _cancellationTokenSource.Token).Wait();

			_cancellationTokenSource.Dispose();

			_logger?.LogTrace("AsyncMultiTenantMessagePump ready to exit");
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
			_logger?.LogTrace("AsyncMultiTenantMessagePump stopping message pump...");
			if (_cancellationTokenSource != null) _cancellationTokenSource.Cancel();
			if (_safeToExitHandle != null) _safeToExitHandle.WaitOne();
			_logger?.LogTrace("AsyncMultiTenantMessagePump stopped, exiting safely");
		}

		#endregion

		#region PRIVATE METHODS

		private void InitDefaultActions()
		{
			OnError = (tenantId, message, exception, isPoison) => _logger?.LogError(exception, "An error occured when processing a message for tenant {tenantId}", tenantId);
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

		private async Task ProcessMessages(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default)
		{
			var runningTasks = new ConcurrentDictionary<Task, Task>();
			var semaphore = new SemaphoreSlim(_concurrentTasks, _concurrentTasks);
			var queuedMessages = new ConcurrentQueue<(string TenantId, CloudMessage Message)>();

			// Define the task that discovers queues that follow the naming convention
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					try
					{
						var queueServiceClient = new QueueServiceClient(_connectionString);
						var response = queueServiceClient.GetQueuesAsync(QueueTraits.None, _queueNamePrefix, cancellationToken);
						await foreach (Page<QueueItem> queues in response.AsPages())
						{
							foreach (var queue in queues.Values)
							{
								if (!queue.Name.Equals(_queueNamePrefix, StringComparison.OrdinalIgnoreCase))
								{
									_tenantIds.AddIfNotPresent(queue.Name.TrimStart(_queueNamePrefix));
								}
							}
						}
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
				TimeSpan.FromMilliseconds(15000),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Brief pause to ensure the task defined above runs at least once before we start processing messages
			await Task.Delay(500, cancellationToken).ConfigureAwait(false);

			// Define the task that fetches messages from the Azure queue
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					// Fetch messages from Azure when the number of items in the concurrent queue falls below an "acceptable" level.
					if (!cancellationToken.IsCancellationRequested && queuedMessages.Count <= _concurrentTasks / 2)
					{
						await foreach (var message in FetchMessages(visibilityTimeout, cancellationToken))
						{
							queuedMessages.Enqueue(message);
						}
					}
				},
				TimeSpan.FromMilliseconds(500),
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task that checks how many messages are queued in Azure
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					var count = 0;
					foreach (var kvp in _tenantQueueManagers)
					{
						var tenantId = kvp.Key;
						(var queueManager, var lastFetched, var fetchDelay) = kvp.Value.Value;

						try
						{
							var properties = await queueManager.GetPropertiesAsync(cancellationToken).ConfigureAwait(false);

							count += properties.ApproximateMessagesCount;
						}
						catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
						{
							// The message pump is shutting down.
							// This exception can be safely ignored.
						}
						catch (RequestFailedException rfe) when (rfe.ErrorCode == "QueueNotFound")
						{
							// The queue has been deleted
							_tenantIds.Remove(tenantId);
							_tenantQueueManagers.Remove(tenantId, out _);
						}
						catch (Exception e)
						{
							_logger?.LogError(e.GetBaseException(), "An error occured while checking how many message are waiting in Azure. The error was caught and ignored.");
						}
					}

					_metrics.Measure.Gauge.SetValue(Metrics.QueuedCloudMessagesGauge, count);
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
								if (queuedMessages.TryDequeue(out (string TenantId, CloudMessage Message) result))
								{
									var tenantInfo = GetTenantInfo(result.TenantId);

									try
									{
										// Process the message
										OnMessage?.Invoke(result.TenantId, result.Message, cancellationToken);

										// Delete the processed message from the queue
										// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue even when the message pump is shutting down
										await tenantInfo.QueueManager.DeleteMessageAsync(result.Message, CancellationToken.None).ConfigureAwait(false);
									}
									catch (Exception ex)
									{
										var isPoison = result.Message.DequeueCount > _maxDequeueCount;
										OnError?.Invoke(result.TenantId, result.Message, ex, isPoison);
										if (isPoison)
										{
											// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue and moved to poison queue even when the message pump is shutting down
											if (_poisonQueueManager != null)
											{
												result.Message.Metadata["PoisonExceptionMessage"] = ex.GetBaseException().Message;
												result.Message.Metadata["PoisonExceptionDetails"] = ex.GetBaseException().ToString();

												await _poisonQueueManager.AddMessageAsync(result.Message.Content, result.Message.Metadata, null, null, CancellationToken.None).ConfigureAwait(false);
											}

											await tenantInfo.QueueManager.DeleteMessageAsync(result.Message, CancellationToken.None).ConfigureAwait(false);
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

		private async IAsyncEnumerable<(string TenantId, CloudMessage Message)> FetchMessages(TimeSpan? visibilityTimeout = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
		{
			var messageCount = 0;
			var originalTenant = _tenantIds.Current;

			using (_metrics.Measure.Timer.Time(Metrics.MessagesFetchingTimer))
			{
				do
				{
					var tenantId = _tenantIds.MoveToNextItem();
					var tenantInfo = GetTenantInfo(tenantId);

					if (!cancellationToken.IsCancellationRequested && tenantInfo.LastFetched.Add(tenantInfo.FetchDelay) < DateTime.UtcNow)
					{
						IEnumerable<CloudMessage> messages = null;

						try
						{
							messages = await tenantInfo.QueueManager.GetMessagesAsync(_concurrentTasks, visibilityTimeout, cancellationToken).ConfigureAwait(false);
						}
						catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
						{
							// The message pump is shutting down.
							// This exception can be safely ignored.
						}
						catch (RequestFailedException rfe) when (rfe.ErrorCode == "QueueNotFound")
						{
							// The queue has been deleted
							_tenantIds.Remove(tenantId);
							_tenantQueueManagers.Remove(tenantId, out _);
						}
						catch (Exception e)
						{
							_logger?.LogError(e.GetBaseException(), "An error occured while fetching messages for tenant {tenantId}. The error was caught and ignored.", tenantId);
						}

						if (messages != null && messages.Any())
						{
							var messagesCount = messages.Count();
							_logger?.LogTrace("Fetched {messagesCount} message(s) for tenant {tenantId}.", messagesCount, tenantId);

							foreach (var message in messages)
							{
								Interlocked.Increment(ref messageCount);
								yield return (tenantId, message);
							}

							// Reset the Fetch delay to zero to indicate that we can fetch more messages from this queue as soon as possible
							_tenantQueueManagers[tenantId] = new Lazy<(QueueManager, DateTime, TimeSpan)>((tenantInfo.QueueManager, DateTime.UtcNow, TimeSpan.Zero));
						}
						else
						{
							_logger?.LogTrace("There are no messages for tenant {tenantId}.", tenantId);
							_metrics.Measure.Counter.Increment(Metrics.QueueEmptyCounter);

							// Set a "resonable" fetch delay to ensure we don't query an empty queue too often
							var delay = tenantInfo.FetchDelay.Add(TimeSpan.FromSeconds(5));
							if (delay.TotalSeconds > 15) delay = TimeSpan.FromSeconds(15);

							_tenantQueueManagers[tenantId] = new Lazy<(QueueManager, DateTime, TimeSpan)>((tenantInfo.QueueManager, DateTime.UtcNow, delay));
						}
					}
				}

				// Stop when we either retrieved the desired number of messages OR we have looped through all the known tenants
				while (messageCount < _concurrentTasks && (string.IsNullOrEmpty(originalTenant) || originalTenant != _tenantIds.Current));
			}

			if (messageCount == 0)
			{
				_logger?.LogTrace("All tenant queues are empty, no messages fetched.");
				try
				{
					// All queues are empty
					OnEmpty?.Invoke(cancellationToken);
					_metrics.Measure.Counter.Increment(Metrics.QueueEmptyCounter);
				}
				catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
				{
					// The message pump is shutting down.
					// This exception can be safely ignored.
				}
				catch (Exception e)
				{
					_logger?.LogError(e.GetBaseException(), "An error occured when handling empty queues. The error was caught and ignored.");
				}
			}
		}

		private (QueueManager QueueManager, DateTime LastFetched, TimeSpan FetchDelay) GetTenantInfo(string tenantId)
		{
			var lazyQueueManager = _tenantQueueManagers.GetOrAdd(tenantId, tenantId =>
			{
				return new Lazy<(QueueManager, DateTime, TimeSpan)>(() =>
				{
					_tenantIds.AddIfNotPresent(tenantId);
					return (_queueManagerFactory.Invoke(tenantId), DateTime.MinValue, TimeSpan.Zero);
				});
			});

			return lazyQueueManager.Value;
		}

		#endregion
	}
}
