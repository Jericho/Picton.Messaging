using App.Metrics;
using Azure;
using Microsoft.Extensions.Logging;
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
	/// Designed to monitor either a single queue or a fixed list of queues and process messages as
	/// quickly and efficiently as possible.
	/// </summary>
	public class AsyncMessagePump
	{
		#region FIELDS

		private readonly ConcurrentDictionary<string, (QueueConfig Config, QueueManager QueueManager, QueueManager PoisonQueueManager, DateTime LastFetched, TimeSpan FetchDelay)> _queueManagers = new ConcurrentDictionary<string, (QueueConfig Config, QueueManager QueueManager, QueueManager PoisonQueueManager, DateTime LastFetched, TimeSpan FetchDelay)>();
		private readonly RoundRobinList<string> _queueNames;

		private readonly MessagePumpOptions _mesagePumpOptions;
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
		/// Initializes a new instance of the <see cref="AsyncMessagePump"/> class.
		/// </summary>
		/// <param name="connectionString">
		/// A connection string includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </param>
		/// <param name="queueName">Name of the queue.</param>
		/// <param name="options"></param>
		/// <param name="poisonQueueName">Name of the queue where messages are automatically moved to when they fail to be processed after 'maxDequeueCount' attempts. You can indicate that you do not want messages to be automatically moved by leaving this value empty. In such a scenario, you are responsible for handling so called 'poison' messages.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		[ExcludeFromCodeCoverage]
		public AsyncMessagePump(MessagePumpOptions options, string queueName, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
			: this(options, new QueueConfig(queueName, poisonQueueName, visibilityTimeout, maxDequeueCount), logger, metrics)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMessagePump"/> class.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueConfig"></param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		[ExcludeFromCodeCoverage]
		public AsyncMessagePump(MessagePumpOptions options, QueueConfig queueConfig, ILogger logger = null, IMetrics metrics = null)
			: this(options, new[] { queueConfig }, logger, metrics)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMultiQueueMessagePump"/> class.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueConfigs">The configuration options for each queue to be monitored.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMessagePump(MessagePumpOptions options, IEnumerable<QueueConfig> queueConfigs, ILogger logger = null, IMetrics metrics = null)
		{
			ValidateOptions(options);
			ValidateQueueConfigs(queueConfigs);

			foreach (var queueConfig in queueConfigs)
			{
				var queueManager = new QueueManager(options.ConnectionString, queueConfig.QueueName, true, options.QueueClientOptions, options.BlobClientOptions);
				var poisonQueueManager = string.IsNullOrEmpty(queueConfig.PoisonQueueName) ? null : new QueueManager(options.ConnectionString, queueConfig.PoisonQueueName);
				_queueManagers.TryAdd(queueConfig.QueueName, (queueConfig, queueManager, poisonQueueManager, DateTime.MinValue, TimeSpan.Zero));
			}

			_queueNames = new RoundRobinList<string>(queueConfigs.Select(config => config.QueueName));
			_logger = logger;
			_metrics = metrics ?? TurnOffMetrics();
			_mesagePumpOptions = options;

			InitDefaultActions();
			RandomizeRoundRobinStart();
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMultiQueueMessagePump"/> class.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueManager">The queue manager.</param>
		/// <param name="poisonQueueManager">The poison queue manager.</param>
		/// <param name="visibilityTimeout">The visibility timeout.</param>
		/// <param name="maxDequeueCount">The maximum dequeue count.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		internal AsyncMessagePump(MessagePumpOptions options, QueueManager queueManager, QueueManager poisonQueueManager = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3, ILogger logger = null, IMetrics metrics = null)
			: this(options, new[] { (queueManager, poisonQueueManager, visibilityTimeout, maxDequeueCount) }, logger, metrics)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncMultiQueueMessagePump"/> class.
		/// </summary>
		/// <param name="options"></param>
		/// <param name="queueConfigs">The configuration options for each queue to be monitored.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		internal AsyncMessagePump(MessagePumpOptions options, IEnumerable<(QueueManager QueueManager, QueueManager PoisonQueueManager, TimeSpan? VisibilityTimeout, int MaxDequeueCount)> queueConfigs, ILogger logger = null, IMetrics metrics = null)
		{
			ValidateOptions(options);
			ValidateQueueConfigs(queueConfigs);

			foreach (var queueConfig in queueConfigs)
			{
				var queueName = queueConfig.QueueManager.QueueName;
				var poisonQueueName = queueConfig.PoisonQueueManager?.QueueName;
				var config = new QueueConfig(queueName, poisonQueueName, queueConfig.VisibilityTimeout, queueConfig.MaxDequeueCount);
				_queueManagers.TryAdd(queueName, (config, queueConfig.QueueManager, queueConfig.PoisonQueueManager, DateTime.MinValue, TimeSpan.Zero));
			}

			_queueNames = new RoundRobinList<string>(queueConfigs.Select(config => config.QueueManager.QueueName));
			_logger = logger;
			_metrics = metrics ?? TurnOffMetrics();
			_mesagePumpOptions = options;

			InitDefaultActions();
			RandomizeRoundRobinStart();
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
			await ProcessMessagesAsync(cancellationToken).ConfigureAwait(false);
		}

		#endregion

		#region PRIVATE METHODS

		private void InitDefaultActions()
		{
			OnError = (queueName, message, exception, isPoison) => _logger?.LogError(exception, "An error occured when processing a message in {queueName}", queueName);
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

		private void ValidateOptions(MessagePumpOptions options)
		{
			if (options == null) throw new ArgumentNullException(nameof(options));
			if (string.IsNullOrEmpty(options.ConnectionString)) throw new ArgumentNullException(nameof(options.ConnectionString));
			if (options.ConcurrentTasks < 1) throw new ArgumentOutOfRangeException(nameof(options.ConcurrentTasks), "Number of concurrent tasks must be greather than zero");
		}

		private void ValidateQueueConfigs(IEnumerable<QueueConfig> queueConfigs)
		{
			if (queueConfigs == null || !queueConfigs.Any()) throw new ArgumentNullException(nameof(queueConfigs), "You must specify the configuration options for at least one queue");

			var dequeueCountTooSmall = queueConfigs.Where(config => config.MaxDequeueCount < 1);
			if (dequeueCountTooSmall.Any())
			{
				var misConfiguredQueues = string.Join(", ", dequeueCountTooSmall.Select(config => config.QueueName));
				throw new ArgumentOutOfRangeException(nameof(queueConfigs), $"Number of retries is misconfigured for the following queues: {misConfiguredQueues}");
			}
		}

		private void ValidateQueueConfigs(IEnumerable<(QueueManager QueueManager, QueueManager PoisonQueueManager, TimeSpan? VisibilityTimeout, int MaxDequeueCount)> queueConfigs)
		{
			if (queueConfigs == null || !queueConfigs.Any()) throw new ArgumentNullException(nameof(queueConfigs), "You must specify the configuration options for at least one queue");

			if (queueConfigs.Any(config => config.QueueManager == null))
			{
				throw new ArgumentNullException(nameof(queueConfigs), "At least one of the specified queue managers is null.");
			}

			var dequeueCountTooSmall = queueConfigs.Where(config => config.MaxDequeueCount < 1);
			if (dequeueCountTooSmall.Any())
			{
				var misConfiguredQueues = string.Join(", ", dequeueCountTooSmall.Select(config => config.QueueManager.QueueName));
				throw new ArgumentOutOfRangeException(nameof(queueConfigs), $"Number of retries is misconfigured for the following queues: {misConfiguredQueues}");
			}
		}

		private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
		{
			var runningTasks = new ConcurrentDictionary<Task, Task>();
			var semaphore = new SemaphoreSlim(_mesagePumpOptions.ConcurrentTasks, _mesagePumpOptions.ConcurrentTasks);
			var queuedMessages = new ConcurrentQueue<(string QueueName, CloudMessage Message)>();

			// Define the task that fetches messages from the Azure queue
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					// Fetch messages from Azure when the number of items in the concurrent queue falls below an "acceptable" level.
					if (!cancellationToken.IsCancellationRequested && queuedMessages.Count <= _mesagePumpOptions.ConcurrentTasks / 2)
					{
						await foreach (var message in FetchMessages(cancellationToken))
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
					foreach (var kvp in _queueManagers)
					{
						var queueName = kvp.Key;
						(var queueConfig, var queueManager, var poisonQueueManager, var lastFetched, var fetchDelay) = kvp.Value;

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
							_queueNames.Remove(queueName);
							_queueManagers.TryRemove(queueName, out _);
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

							if (queuedMessages.TryDequeue(out (string QueueName, CloudMessage Message) result))
							{
								if (_queueManagers.TryGetValue(result.QueueName, out (QueueConfig Config, QueueManager QueueManager, QueueManager PoisonQueueManager, DateTime LastFetched, TimeSpan FetchDelay) queueInfo))
								{
									using (_metrics.Measure.Timer.Time(Metrics.MessageProcessingTimer))
									{
										try
										{
											// Process the message
											OnMessage?.Invoke(result.QueueName, result.Message, cancellationToken);

											// Delete the processed message from the queue
											// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue even when the message pump is shutting down
											await queueInfo.QueueManager.DeleteMessageAsync(result.Message, CancellationToken.None).ConfigureAwait(false);
										}
										catch (Exception ex)
										{
											var isPoison = result.Message.DequeueCount >= queueInfo.Config.MaxDequeueCount;

											try
											{
												OnError?.Invoke(result.QueueName, result.Message, ex, isPoison);
											}
											catch (Exception e)
											{
												_logger?.LogError(e.GetBaseException(), "An error occured when handling an exception for {queueName}. The error was caught and ignored.", result.QueueName);
											}

											if (isPoison)
											{
												// PLEASE NOTE: we use "CancellationToken.None" to ensure a processed message is deleted from the queue and moved to poison queue even when the message pump is shutting down
												if (queueInfo.PoisonQueueManager != null)
												{
													result.Message.Metadata["PoisonExceptionMessage"] = ex.GetBaseException().Message;
													result.Message.Metadata["PoisonExceptionDetails"] = ex.GetBaseException().ToString();

													await queueInfo.PoisonQueueManager.AddMessageAsync(result.Message.Content, result.Message.Metadata, null, null, CancellationToken.None).ConfigureAwait(false);
												}

												await queueInfo.QueueManager.DeleteMessageAsync(result.Message, CancellationToken.None).ConfigureAwait(false);
											}
										}

										messageProcessed = true;
									}
								}
								else
								{
									_queueNames.Remove(result.QueueName);
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

		private async IAsyncEnumerable<(string QueueName, CloudMessage Message)> FetchMessages([EnumeratorCancellation] CancellationToken cancellationToken)
		{
			var messageCount = 0;

			if (string.IsNullOrEmpty(_queueNames.Current)) _queueNames.Reset();
			var originalQueue = _queueNames.Current;

			using (_metrics.Measure.Timer.Time(Metrics.MessagesFetchingTimer))
			{
				do
				{
					var queueName = _queueNames.MoveToNextItem();

					if (_queueManagers.TryGetValue(queueName, out (QueueConfig Config, QueueManager QueueManager, QueueManager PoisonQueueManager, DateTime LastFetched, TimeSpan FetchDelay) queueInfo))
					{
						if (!cancellationToken.IsCancellationRequested && queueInfo.LastFetched.Add(queueInfo.FetchDelay) < DateTime.UtcNow)
						{
							IEnumerable<CloudMessage> messages = null;

							try
							{
								messages = await queueInfo.QueueManager.GetMessagesAsync(_mesagePumpOptions.ConcurrentTasks, queueInfo.Config.VisibilityTimeout, cancellationToken).ConfigureAwait(false);
							}
							catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
							{
								// The message pump is shutting down.
								// This exception can be safely ignored.
							}
							catch (RequestFailedException rfe) when (rfe.ErrorCode == "QueueNotFound")
							{
								// The queue has been deleted
								_queueNames.Remove(queueName);
								_queueManagers.TryRemove(queueName, out _);
							}
							catch (Exception e)
							{
								_logger?.LogError(e.GetBaseException(), "An error occured while fetching messages from {queueName}. The error was caught and ignored.", queueName);
							}

							if (messages != null && messages.Any())
							{
								var messagesCount = messages.Count();
								_logger?.LogTrace("Fetched {messagesCount} message(s) in {queueName}.", messagesCount, queueName);

								foreach (var message in messages)
								{
									Interlocked.Increment(ref messageCount);
									yield return (queueName, message);
								}

								// Reset the Fetch delay to zero to indicate that we can fetch more messages from this queue as soon as possible
								_queueManagers[queueName] = (queueInfo.Config, queueInfo.QueueManager, queueInfo.PoisonQueueManager, DateTime.UtcNow, TimeSpan.Zero);
							}
							else
							{
								_logger?.LogTrace("There are no messages in {queueName}.", queueName);
								_metrics.Measure.Counter.Increment(Metrics.QueueEmptyCounter);

								// Set a "resonable" fetch delay to ensure we don't query an empty queue too often
								var delay = queueInfo.FetchDelay.Add(TimeSpan.FromSeconds(5));
								if (delay.TotalSeconds > 15) delay = TimeSpan.FromSeconds(15);

								_queueManagers[queueName] = (queueInfo.Config, queueInfo.QueueManager, queueInfo.PoisonQueueManager, DateTime.UtcNow, delay);
							}
						}
					}
					else
					{
						_queueNames.Remove(queueName);
					}
				}

				// Stop when we either retrieved the desired number of messages OR we have looped through all the queues
				while (messageCount < (_mesagePumpOptions.ConcurrentTasks * 2) && originalQueue != _queueNames.Current);
			}

			if (messageCount == 0)
			{
				_logger?.LogTrace("All tenant queues are empty, no messages fetched.");
				try
				{
					// All queues are empty
					_metrics.Measure.Counter.Increment(Metrics.AllQueuesEmptyCounter);
					OnEmpty?.Invoke(cancellationToken);
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

		private void RandomizeRoundRobinStart()
		{
			if (_queueNames.Current == null)
			{
				var randomIndex = RandomGenerator.Instance.GetInt32(0, _queueNames.Count);
				_queueNames.ResetTo(randomIndex);
			}
		}

		#endregion
	}
}
