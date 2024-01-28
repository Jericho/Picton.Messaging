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
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// High performance message processor (also known as a message "pump") for Azure storage queues.
	/// Designed to monitor either a single queue or a list of queues and process messages as
	/// quickly and efficiently as possible.
	/// </summary>
	public class AsyncMessagePump
	{
		#region FIELDS

		private readonly ConcurrentDictionary<string, (QueueConfig Config, QueueManager QueueManager, QueueManager PoisonQueueManager, DateTime LastFetched, TimeSpan FetchDelay)> _queueManagers = new();
		private readonly RoundRobinList<string> _queueNames = new(Enumerable.Empty<string>());

		private readonly MessagePumpOptions _messagePumpOptions;
		private readonly ILogger _logger;
		private readonly IMetrics _metrics;
		private readonly bool _metricsTurnedOff;

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
		/// <param name="options">Options for the mesage pump.</param>
		/// <param name="logger">The logger.</param>
		/// <param name="metrics">The system where metrics are published.</param>
		public AsyncMessagePump(MessagePumpOptions options, ILogger logger = null, IMetrics metrics = null)
		{
			if (options == null) throw new ArgumentNullException(nameof(options));
			if (string.IsNullOrEmpty(options.ConnectionString)) throw new ArgumentNullException(nameof(options.ConnectionString));
			if (options.ConcurrentTasks < 1) throw new ArgumentOutOfRangeException(nameof(options.ConcurrentTasks), "Number of concurrent tasks must be greather than zero");
			if (options.FetchMessagesInterval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.FetchMessagesInterval), "Fetch messages interval must be greather than zero");
			if (options.EmptyQueueFetchDelay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.EmptyQueueFetchDelay), "Emnpty queue fetch delay must be greather than zero");
			if (options.EmptyQueueMaxFetchDelay < options.EmptyQueueFetchDelay) throw new ArgumentOutOfRangeException(nameof(options.EmptyQueueMaxFetchDelay), "Max fetch delay can not be smaller than fetch delay");

			_messagePumpOptions = options;
			_logger = logger;
			_metrics = metrics ?? TurnOffMetrics();
			_metricsTurnedOff = metrics == null;

			InitDefaultActions();
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
			AddQueue(new QueueConfig(queueName, poisonQueueName, visibilityTimeout, maxDequeueCount));
		}

		/// <summary>
		/// Add a queue to be monitored.
		/// </summary>
		/// <param name="queueConfig">Queue configuration.</param>
		public void AddQueue(QueueConfig queueConfig)
		{
			if (string.IsNullOrEmpty(queueConfig.QueueName)) throw new ArgumentNullException(nameof(queueConfig.QueueName));
			if (queueConfig.MaxDequeueCount < 1) throw new ArgumentOutOfRangeException(nameof(queueConfig.MaxDequeueCount), "Number of retries must be greater than zero.");

			var queueManager = new QueueManager(_messagePumpOptions.ConnectionString, queueConfig.QueueName, true, _messagePumpOptions.QueueClientOptions, _messagePumpOptions.BlobClientOptions);
			var poisonQueueManager = string.IsNullOrEmpty(queueConfig.PoisonQueueName) ? null : new QueueManager(_messagePumpOptions.ConnectionString, queueConfig.PoisonQueueName, true, _messagePumpOptions.QueueClientOptions, _messagePumpOptions.BlobClientOptions);

			AddQueue(queueManager, poisonQueueManager, queueConfig.VisibilityTimeout, queueConfig.MaxDequeueCount);
		}

		/// <summary>
		/// Remove a queue from the list of queues that are monitored.
		/// </summary>
		/// <param name="queueName">The name of the queue.</param>
		public void RemoveQueue(string queueName)
		{
			/*
			 * Do not remove from _queuManagers because there could messages still in the memory queue that need to be processed
			 * _queueManagers.TryRemove(queueName, out _);
			 */

			_queueNames.RemoveItem(queueName);
		}

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

		// This internal method is primarily for unit testing purposes. It allows me to inject mocked queue managers
		internal void AddQueue(QueueManager queueManager, QueueManager poisonQueueManager, TimeSpan? visibilityTimeout, int maxDequeueCount)
		{
			if (queueManager == null) throw new ArgumentNullException(nameof(queueManager));
			if (string.IsNullOrEmpty(queueManager.QueueName)) throw new ArgumentNullException(nameof(queueManager.QueueName));
			if (maxDequeueCount < 1) throw new ArgumentOutOfRangeException(nameof(maxDequeueCount), "Number of retries must be greater than zero.");

			var queueConfig = new QueueConfig(queueManager.QueueName, poisonQueueManager?.QueueName, visibilityTimeout, maxDequeueCount);

			_queueManagers.AddOrUpdate(
				queueManager.QueueName,
				(queueName) => (queueConfig, queueManager, poisonQueueManager, DateTime.MinValue, TimeSpan.Zero),
				(queueName, oldConfig) => (queueConfig, queueManager, poisonQueueManager, oldConfig.LastFetched, oldConfig.FetchDelay));
			_queueNames.AddItem(queueManager.QueueName);
		}

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

		private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
		{
			var runningTasks = new ConcurrentDictionary<Task, Task>();
			var semaphore = new SemaphoreSlim(_messagePumpOptions.ConcurrentTasks, _messagePumpOptions.ConcurrentTasks);
			var channelOptions = new UnboundedChannelOptions() { SingleReader = false, SingleWriter = true };
			var channel = Channel.CreateUnbounded<(string QueueName, CloudMessage Message)>(channelOptions);
			var channelCompleted = false;

			// Define the task that fetches messages from the Azure queue
			RecurrentCancellableTask.StartNew(
				async () =>
				{
					// Fetch messages from Azure when the number of items in the concurrent queue falls below an "acceptable" level.
					if (!cancellationToken.IsCancellationRequested &&
						!channelCompleted &&
						channel.Reader.Count <= _messagePumpOptions.ConcurrentTasks / 2)
					{
						await foreach (var message in FetchMessages(cancellationToken).ConfigureAwait(false))
						{
							await channel.Writer.WriteAsync(message).ConfigureAwait(false);
						}
					}

					// Mark the channel as "complete" which means that no more messages will be written to it
					else if (!channelCompleted)
					{
						channelCompleted = channel.Writer.TryComplete();
					}
				},
				_messagePumpOptions.FetchMessagesInterval,
				cancellationToken,
				TaskCreationOptions.LongRunning);

			// Define the task that checks how many messages are queued in Azure
			if (!_metricsTurnedOff && _messagePumpOptions.CountAzureMessagesInterval > TimeSpan.Zero)
			{
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
								RemoveQueue(queueName);
							}
							catch (Exception e)
							{
								_logger?.LogError(e.GetBaseException(), "An error occured while checking how many message are waiting in Azure. The error was caught and ignored.");
							}
						}

						_metrics.Measure.Gauge.SetValue(Metrics.QueuedCloudMessagesGauge, count);
					},
					_messagePumpOptions.CountAzureMessagesInterval,
					cancellationToken,
					TaskCreationOptions.LongRunning);
			}

			// Define the task that checks how many messages are queued in memory
			if (!_metricsTurnedOff && _messagePumpOptions.CountMemoryMessagesInterval > TimeSpan.Zero)
			{
				RecurrentCancellableTask.StartNew(
					() =>
					{
						try
						{
							_metrics.Measure.Gauge.SetValue(Metrics.QueuedMemoryMessagesGauge, channel.Reader.Count);
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
			}

			// Define the task pump
			var pumpTask = Task.Run(async () =>
			{
				// We process messages until cancellation is requested.
				// When cancellation is requested, we continue processing messages until the memory queue is drained.
				while (!cancellationToken.IsCancellationRequested || channel.Reader.Count > 0)
				{
					await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

					// Retrieved the next message from the queue and process it
					var runningTask = Task.Run(
						async () =>
						{
							var messageProcessed = false;

							if (channel.Reader.TryRead(out (string QueueName, CloudMessage Message) result))
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
													result.Message.Metadata["PoisonOriginalQueue"] = queueInfo.QueueManager.QueueName;

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
									_queueNames.RemoveItem(result.QueueName);
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
							runningTasks.TryRemove(t, out Task _);
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

			if (_queueNames.Count == 0)
			{
				_logger?.LogTrace("There are no queues being monitored. Therefore no messages could be fetched.");
				yield break;
			}

			var originalQueue = _queueNames.Current;

			using (_metrics.Measure.Timer.Time(Metrics.MessagesFetchingTimer))
			{
				do
				{
					var queueName = _queueNames.MoveToNextItem();
					originalQueue ??= queueName; // This is important because originalQueue will be null the very first time we fetch messages

					if (_queueManagers.TryGetValue(queueName, out (QueueConfig Config, QueueManager QueueManager, QueueManager PoisonQueueManager, DateTime LastFetched, TimeSpan FetchDelay) queueInfo))
					{
						if (!cancellationToken.IsCancellationRequested && queueInfo.LastFetched.Add(queueInfo.FetchDelay) < DateTime.UtcNow)
						{
							IEnumerable<CloudMessage> messages = null;

							try
							{
								messages = await queueInfo.QueueManager.GetMessagesAsync(_messagePumpOptions.ConcurrentTasks, queueInfo.Config.VisibilityTimeout, cancellationToken).ConfigureAwait(false);
							}
							catch (Exception e) when (e is TaskCanceledException || e is OperationCanceledException)
							{
								// The message pump is shutting down.
								// This exception can be safely ignored.
							}
							catch (RequestFailedException rfe) when (rfe.ErrorCode == "QueueNotFound")
							{
								// The queue has been deleted
								RemoveQueue(queueName);
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

								// Set a "reasonable" fetch delay to ensure we don't query an empty queue too often
								var delay = queueInfo.FetchDelay.Add(_messagePumpOptions.EmptyQueueFetchDelay);
								if (delay > _messagePumpOptions.EmptyQueueMaxFetchDelay) delay = _messagePumpOptions.EmptyQueueMaxFetchDelay;

								_queueManagers[queueName] = (queueInfo.Config, queueInfo.QueueManager, queueInfo.PoisonQueueManager, DateTime.UtcNow, delay);
							}
						}
					}
					else
					{
						_queueNames.RemoveItem(queueName);
					}
				}

				// Stop when we either retrieved the desired number of messages OR we have looped through all the queues
				while (messageCount < (_messagePumpOptions.ConcurrentTasks * 2) && originalQueue != _queueNames.Next);
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

		#endregion
	}
}
