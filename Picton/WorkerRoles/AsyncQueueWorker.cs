using Microsoft.WindowsAzure.Storage.Queue;
using Picton.Utils;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.WorkerRoles
{
	public class AsyncQueueWorker : BaseWorker
	{
		#region FIELDS

		private readonly int _minConcurrentTasks;
		private readonly int _maxConcurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly int _maxDequeueCount;
		private CancellationTokenSource _cancellationTokenSource;
		private ManualResetEvent _safeToExitHandle;

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// High performance worker role that is designed to monitor a Azure storage queue and process the message as quickly and efficiently as possible.
		/// When messages are present in the queue, this worker will increase the number of tasks that can concurrently process messages.
		/// Conversly, this worker will reduce the number of tasks that can concurrently process messages when the queue is empty.
		/// </summary>
		/// <param name="workerName">The name of the worker. This information is used when writing to the Trace.</param>
		/// <param name="minConcurrentTasks">The minimum number of tasks. The AsyncQueueworker will not scale down below this value.</param>
		/// <param name="maxConcurrentTasks">The maximum number of tasks. The AsyncQueueworker will not scale up above this value.</param>
		/// <param name="visibilityTimeout">The queue visibility timeout</param>
		/// <param name="maxDequeuecount">The number of times to retry before giving up</param>
		public AsyncQueueWorker(string workerName, int minConcurrentTasks = 1, int maxConcurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeuecount = 5)
			: base(workerName)
		{
			if (minConcurrentTasks < 1) throw new ArgumentException("minConcurrentTasks must be greather than zero");
			if (maxConcurrentTasks < minConcurrentTasks) throw new ArgumentException("maxConcurrentTasks must be greather than or equal to minConcurrentTasks");

			_minConcurrentTasks = minConcurrentTasks;
			_maxConcurrentTasks = maxConcurrentTasks;
			_visibilityTimeout = visibilityTimeout;
			_cancellationTokenSource = new CancellationTokenSource();
			_maxConcurrentTasks = maxConcurrentTasks;
		}

		#endregion

		#region PUBLIC METHODS

		/// <summary>
		/// Must be overridden on derived classes to provide the storage queue.
		/// </summary>
		/// <example>
		/// Here's an example when you are running in the emulator:
		/// public override CloudQueue GetQueue()
		/// {
		///		var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
		///		var cloudQueueClient = storageAccount.CreateCloudQueueClient();
		///		cloudQueueClient.DefaultRequestOptions.RetryPolicy = new NoRetry();
		///		var cloudQueue = cloudQueueClient.GetQueueReference("myqueue");
		///		cloudQueue.CreateIfNotExists();
		///		return cloudQueue;
		///	}
		/// 
		/// Here's an example when runing in Azure:
		/// public override CloudQueue GetQueue()
		/// {
		///		var storageCredentials = new StorageCredentials("your account name", "your shared key");
		///		var storageAccount = new CloudStorageAccount(storageCredentials, true);
		///		
		/// 	// Improve transfer speeds for small requests
		/// 	// http://blogs.msdn.com/b/windowsazurestorage/archive/2010/06/25/nagle-s-algorithm-is-not-friendly-towards-small-requests.aspx
		/// 	ServicePointManager.FindServicePoint(storageAccount.BlobEndpoint).UseNagleAlgorithm = false;
		/// 	ServicePointManager.FindServicePoint(storageAccount.QueueEndpoint).UseNagleAlgorithm = false;
		/// 	ServicePointManager.FindServicePoint(storageAccount.TableEndpoint).UseNagleAlgorithm = false;
		/// 	ServicePointManager.FindServicePoint(storageAccount.FileEndpoint).UseNagleAlgorithm = false;
		/// 	
		///		var cloudQueueClient = storageAccount.CreateCloudQueueClient();
		///		cloudQueueClient.DefaultRequestOptions.RetryPolicy = new ExponentialRetry();
		///		var cloudQueue = cloudQueueClient.GetQueueReference("myqueue");
		///		cloudQueue.CreateIfNotExists();
		///		return cloudQueue;
		///	}
		/// </example>
		/// <returns>The storage queue</returns>
		public virtual CloudQueue GetQueue()
		{
			throw new NotImplementedException("The GetQueue method must be overridden");
		}

		/// <summary>
		/// Must be overridden on derived classes to provide the logic to execute when a message is retrieved from the queue.
		/// </summary>
		public virtual void OnMessage(CloudQueueMessage message, CancellationToken cancellationToken = default(CancellationToken))
		{
			throw new NotImplementedException("The ProcessMessage method must be overridden");
		}

		/// <summary>
		/// Must be overridden on derived classes to provide the logic to execute when an error occurs.
		/// </summary>
		/// <remarks>
		/// When isPoison is set to true, you will want to copy this message to a poison queue because it will be deleted from the original queue.
		/// </remarks>
		public virtual void OnError(CloudQueueMessage message, Exception exception, bool isPoison)
		{
			Trace.TraceInformation("OnError: {0}", exception);
		}

		/// <summary>
		/// Povides the logic to execute when the queue is empty.
		/// </summary>
		/// <example>
		/// Here's an example:
		/// public override void OnEmptyQueue(CancellationToken cancellationToken = default(CancellationToken))
		/// {
		///		Task.Delay(2500, cancellationToken).Wait();
		///	}
		/// </example>
		/// <remarks>
		/// If this method is not overwrittn in a derived class, the default logic is to pause for 1 second.
		/// </remarks>
		/// <returns>The storage queue</returns>
		public virtual void OnQueueEmpty(CancellationToken cancellationToken = default(CancellationToken))
		{
			// Wait a little bit in order to avoid overwhelming the queue storage
			Task.Delay(1000, cancellationToken).Wait();
		}

		public override void Run()
		{
			Trace.TraceInformation("'{0}'.Run called", this.WorkerName);

			_cancellationTokenSource = new CancellationTokenSource();
			_safeToExitHandle = new ManualResetEvent(false);

			ProcessMessages(_visibilityTimeout, _cancellationTokenSource.Token).Wait();

			_cancellationTokenSource.Dispose();

			Trace.TraceInformation("'{0}' ready to exit", this.WorkerName);
			_safeToExitHandle.Set();
		}

		public override void OnStop()
		{
			Trace.TraceInformation("'{0}'.OnStop called", this.WorkerName);
			_cancellationTokenSource.Cancel();
			base.OnStop();
			_safeToExitHandle.WaitOne();
			Trace.TraceInformation("'{0}'.OnStop complete, exiting safely", this.WorkerName);
		}

		#endregion

		#region PRIVATE METHODS

		private async Task ProcessMessages(TimeSpan? visibilityTimeout = null, CancellationToken cancellationToken = default(CancellationToken))
		{
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
								OnQueueEmpty(cancellationToken);
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
								OnMessage(message, cancellationToken);

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
							var scaleUp = Task.Run(() => semaphore.TryIncrease());
							runningTasks.TryAdd(scaleUp, scaleUp);
						}
						else
						{
							// The queue is empty, therefore reduce the number of concurrent tasks
							var scaleDown = Task.Run(() => semaphore.TryDecrease());
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
