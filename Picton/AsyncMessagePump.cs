using Microsoft.WindowsAzure.Storage.Queue;
using Picton.Logging;
using Picton.Utils;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Picton
{
	public class AsyncMessagePump
	{
		#region FIELDS

		private readonly CloudQueue _cloudQueue;
		private readonly int _minConcurrentTasks;
		private readonly int _maxConcurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private CancellationTokenSource _cancellationTokenSource;
		private ManualResetEvent _safeToExitHandle;

		private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when a message is retrieved from the queue.
		/// </summary>
		/// <remarks>
		/// If exception is thrown when calling OnMessage, it will regard this queue message as failed.
		/// </remarks>
		public Action<CloudQueueMessage, CancellationToken> OnMessage { get; set; }

		/// <summary>
		/// Gets or sets the logic to execute when an error occurs.
		/// </summary>
		/// <example>
		/// OnError = (message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);
		/// </example>
		/// <remarks>
		/// When isPoison is set to true, you should copy this message to a poison queue because it will be deleted from the original queue.
		/// </remarks>
		public Action<CloudQueueMessage, Exception, bool> OnError { get; set; }

		/// <summary>
		/// Gets or sets the logic to execute when queue is empty.
		/// </summary>
		/// <example>
		/// Here's an example:
		/// OnQueueEmpty = cancellationToken => Task.Delay(2500, cancellationToken).Wait();
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to pause for 1 second.
		/// </remarks>
		public Action<CancellationToken> OnQueueEmpty { get; set; }

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// High performance message processor (also known as a message "pump") for Azure storage queues. Designed to monitor an Azure storage queue and process the message as quickly and efficiently as possible.
		/// When messages are present in the queue, this message pump will increase the number of tasks that can concurrently process messages.
		/// Conversly, this message pump will reduce the number of tasks that can concurrently process messages when the queue is empty.
		/// </summary>
		/// <param name="minConcurrentTasks">The minimum number of concurrent tasks. The message pump will not scale down below this value</param>
		/// <param name="maxConcurrentTasks">The maximum number of concurrent tasks. The message pump will not scale up above this value</param>
		/// <param name="visibilityTimeout">The queue visibility timeout</param>
		/// <param name="maxDequeueCount">The number of times to try processing a given message before giving up</param>
		public AsyncMessagePump(CloudQueue cloudQueue, int minConcurrentTasks = 1, int maxConcurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
		{
			if (cloudQueue == null) throw new ArgumentNullException(nameof(cloudQueue));
			if (minConcurrentTasks < 1) throw new ArgumentException("Minimum number of concurrent tasks must be greather than zero", nameof(minConcurrentTasks));
			if (maxConcurrentTasks < minConcurrentTasks) throw new ArgumentException("Maximum number of concurrent tasks must be greather than or equal to the minimum", nameof(maxConcurrentTasks));
			if (maxDequeueCount < 1) throw new ArgumentException("Number of retries must be greather than zero", nameof(maxDequeueCount));

			_cloudQueue = cloudQueue;
			_minConcurrentTasks = minConcurrentTasks;
			_maxConcurrentTasks = maxConcurrentTasks;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;

			OnQueueEmpty = cancellationToken => Task.Delay(1000, cancellationToken).Wait();
			OnError = (message, exception, isPoison) => _logger.ErrorException("An error occured when processing a message", exception);
		}

		#endregion

		#region PUBLIC METHODS

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

		private async Task ProcessMessages(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default(CancellationToken))
		{
			var runningTasks = new ConcurrentDictionary<Task, Task>();
			var semaphore = new SemaphoreSlimEx(_minConcurrentTasks, _minConcurrentTasks, _maxConcurrentTasks);

			// Define the task pump
			var pumpTask = Task.Run(async () =>
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

					var runningTask = Task.Run(async () =>
					{
						if (cancellationToken.IsCancellationRequested) return false;

						CloudQueueMessage message = null;
						try
						{
							message = await _cloudQueue.GetMessageAsync(visibilityTimeout, null, null, cancellationToken);
						}
						catch (Exception e)
						{
							_logger.ErrorException("An error occured when attempting to get a message from the queue", e);
						}

						if (message == null)
						{
							try
							{
								// The queue is empty
								OnQueueEmpty?.Invoke(cancellationToken);
							}
#pragma warning disable RECS0022 // A catch clause that catches System.Exception and has an empty body
							catch
#pragma warning restore RECS0022 // A catch clause that catches System.Exception and has an empty body
							{
								// Intentionally left empty. We ignore errors from OnQueueEmpty.
							}

							// False indicates that no message was processed
							return false;
						}
						else
						{
							try
							{
								// Process the message
								OnMessage?.Invoke(message, cancellationToken);

								// Delete the processed message from the queue
								await _cloudQueue.DeleteMessageAsync(message);
							}
							catch (Exception ex)
							{
								var isPoison = (message.DequeueCount > _maxDequeueCount);
								OnError?.Invoke(message, ex, isPoison);
								if (isPoison) await _cloudQueue.DeleteMessageAsync(message);
							}

							// True indicates that a message was processed
							return true;
						}
					}, CancellationToken.None);

					runningTasks.TryAdd(runningTask, runningTask);

					runningTask.ContinueWith(async t =>
					{
						// Decide if we need to scale up or down
						if (!cancellationToken.IsCancellationRequested)
						{
							if (await t)
							{
								// The queue is not empty, therefore increase the number of concurrent tasks
								semaphore.TryIncrease();
							}
							else
							{
								// The queue is empty, therefore reduce the number of concurrent tasks
								semaphore.TryDecrease();
							}
						}

						// Complete the task
						semaphore.Release();
						Task taskToBeRemoved;
						runningTasks.TryRemove(t, out taskToBeRemoved);
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
