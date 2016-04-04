using Microsoft.WindowsAzure.Storage.Queue;
using Picton.Utils;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.WorkerRoles
{
	public class AsyncMessagePump
	{
		#region FIELDS

		private readonly int _minConcurrentTasks;
		private readonly int _maxConcurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private CancellationTokenSource _cancellationTokenSource;
		private ManualResetEvent _safeToExitHandle;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets the queue to process.
		/// </summary>
		public Func<CloudQueue> GetQueue { get; set; }

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
		/// When messages are present in the queue, this worker will increase the number of tasks that can concurrently process messages.
		/// Conversly, this worker will reduce the number of tasks that can concurrently process messages when the queue is empty.
		/// </summary>
		/// <param name="minConcurrentTasks">The minimum number of tasks. The AsyncQueueworker will not scale down below this value.</param>
		/// <param name="maxConcurrentTasks">The maximum number of tasks. The AsyncQueueworker will not scale up above this value.</param>
		/// <param name="visibilityTimeout">The queue visibility timeout</param>
		/// <param name="maxDequeuecount">The number of times to retry before giving up</param>
		public AsyncMessagePump(int minConcurrentTasks = 1, int maxConcurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
		{
			if (minConcurrentTasks < 1) throw new ArgumentException("minConcurrentTasks must be greather than zero");
			if (maxConcurrentTasks < minConcurrentTasks) throw new ArgumentException("maxConcurrentTasks must be greather than or equal to minConcurrentTasks");

			_minConcurrentTasks = minConcurrentTasks;
			_maxConcurrentTasks = maxConcurrentTasks;
			_visibilityTimeout = visibilityTimeout;
			_maxDequeueCount = maxDequeueCount;

			_cancellationTokenSource = new CancellationTokenSource();

			OnQueueEmpty = cancellationToken => Task.Delay(1000, cancellationToken).Wait();
			OnError = (message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);

		}

		#endregion

		#region PUBLIC METHODS

		public void Start()
		{
			Trace.TraceInformation("AsyncMessagePump starting...");

			_cancellationTokenSource = new CancellationTokenSource();
			_safeToExitHandle = new ManualResetEvent(false);

			ProcessMessages(_visibilityTimeout, _cancellationTokenSource.Token).Wait();

			_cancellationTokenSource.Dispose();

			Trace.TraceInformation("AsyncMessagePump ready to exit");
			_safeToExitHandle.Set();
		}

		public void Stop()
		{
			Trace.TraceInformation("AsyncMessagePump stopping...");
			_cancellationTokenSource.Cancel();
			_safeToExitHandle.WaitOne();
			Trace.TraceInformation("AsyncMessagePump stopped, exiting safely");
		}

		#endregion

		#region PRIVATE METHODS

		private async Task ProcessMessages(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default(CancellationToken))
		{
			if (this.GetQueue == null) throw new NotImplementedException("The GetQueue property must be provided");
			if (this.OnMessage == null) throw new NotImplementedException("The OnMessage property must be provided");

			var runningTasks = new ConcurrentDictionary<Task, Task>();
			var semaphore = new SemaphoreSlimEx(_minConcurrentTasks, _minConcurrentTasks, _maxConcurrentTasks);
			var queue = GetQueue();

			// Define the task pump
			var pumpTask = Task.Run(async () =>
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

					var runningTask = Task.Run(() =>
					{
						var message = queue.GetMessage(visibilityTimeout);
						if (message == null)
						{
							try
							{
								// The queue is empty
								if (this.OnQueueEmpty != null) OnQueueEmpty(cancellationToken);
							}
							catch
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
								if (this.OnMessage != null) OnMessage(message, cancellationToken);

								// Delete the processed message from the queue
								queue.DeleteMessage(message);
							}
							catch (Exception ex)
							{
								if (message.DequeueCount > _maxDequeueCount)
								{
									OnError(message, ex, true);
									queue.DeleteMessage(message);
								}
								else
								{
									OnError(message, ex, false);
								}
							}

							// True indicates that a message was processed
							return true;
						}
					}, CancellationToken.None);

					runningTasks.TryAdd(runningTask, runningTask);

					runningTask.ContinueWith(t =>
					{
						if (t.Result)
						{
							// The queue is not empty, therefore increase the number of concurrent tasks
							var scaleUp = Task.Run(() =>
							{
								var increased = semaphore.TryIncrease();
								if (increased) Debug.WriteLine("Semaphone slots increased: {0}", semaphore.AvailableSlotsCount);
							});
							runningTasks.TryAdd(scaleUp, scaleUp);
						}
						else
						{
							// The queue is empty, therefore reduce the number of concurrent tasks
							var scaleDown = Task.Run(() =>
							{
								var decreased = semaphore.TryDecrease();
								if (decreased) Debug.WriteLine("Semaphone slots decreased: {0}", semaphore.AvailableSlotsCount);
							});
							runningTasks.TryAdd(scaleDown, scaleDown);
						}
					}, TaskContinuationOptions.ExecuteSynchronously)
					.IgnoreAwait();

					runningTask.ContinueWith(t =>
					{
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
