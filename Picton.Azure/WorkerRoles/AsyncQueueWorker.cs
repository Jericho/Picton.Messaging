using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Picton.Azure.Utils;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Azure.WorkerRoles
{
	public class AsyncQueueWorker : BaseWorker
	{
		#region FIELDS

		private readonly string _queueName;
		private readonly int _minConcurrentTasks;
		private readonly int _maxConcurrentTasks;
		private readonly TimeSpan? _visibilityTimeout;
		private readonly CloudQueueClient _cloudQueueClient;
		private readonly CloudQueue _cloudQueue;
		private CancellationTokenSource _cancellationTokenSource;
		private ManualResetEvent _safeToExitHandle;

		#endregion

		#region CONSTRUCTOR

		public AsyncQueueWorker(string workerName, string queueName, int minConcurrentTasks = 1, int maxConcurrentTasks = 25, TimeSpan? visibilityTimeout = null)
			: base(workerName)
		{
			if (minConcurrentTasks < 1) throw new ArgumentException("minConcurrentTasks must be greather than zero");
			if (maxConcurrentTasks < minConcurrentTasks) throw new ArgumentException("maxConcurrentTasks must be greather than or equal to minConcurrentTasks");

			_queueName = queueName;
			_minConcurrentTasks = minConcurrentTasks;
			_maxConcurrentTasks = maxConcurrentTasks;
			_visibilityTimeout = visibilityTimeout;

			var storageAccount = GetStorageAccount();

			// Improve transfer speeds for small requests
			// http://blogs.msdn.com/b/windowsazurestorage/archive/2010/06/25/nagle-s-algorithm-is-not-friendly-towards-small-requests.aspx
			if (storageAccount.BlobEndpoint != null) ServicePointManager.FindServicePoint(storageAccount.BlobEndpoint).UseNagleAlgorithm = false;
			if (storageAccount.QueueEndpoint != null) ServicePointManager.FindServicePoint(storageAccount.QueueEndpoint).UseNagleAlgorithm = false;
			if (storageAccount.TableEndpoint != null) ServicePointManager.FindServicePoint(storageAccount.TableEndpoint).UseNagleAlgorithm = false;
			if (storageAccount.FileEndpoint != null) ServicePointManager.FindServicePoint(storageAccount.FileEndpoint).UseNagleAlgorithm = false;

			_cloudQueueClient = storageAccount.CreateCloudQueueClient();
			_cloudQueueClient.DefaultRequestOptions.RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(2), 5);

			_cloudQueue = _cloudQueueClient.GetQueueReference(queueName);
			_cloudQueue.CreateIfNotExists();
		}

		#endregion

		#region PUBLIC METHODS

		/// <summary>
		/// Must be overridden on derived classes to provide the storage account.
		/// </summary>
		/// <example>
		/// Here's an example when you are running in the emulator
		/// return CloudStorageAccount.DevelopmentStorageAccount;
		/// 
		/// Here's an example when runing in Azure
		/// var storageCredentials = new StorageCredentials("your account name", "your shared key");
		/// return new CloudStorageAccount(storageCredentials, true);
		/// </example>
		/// <returns>The storage acocunt</returns>
		public virtual CloudStorageAccount GetStorageAccount()
		{
			throw new NotImplementedException("The GetStorageAccount method must be overridden");
		}

		public virtual void ProcessMessage(CloudQueueMessage message)
		{
			throw new NotImplementedException("The ProcessMessage method must be overridden");
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

			// Define the task pump
			var pumpTask = Task.Run(async () =>
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

					var runningTask = Task.Run(() =>
					{
						var message = _cloudQueue.GetMessage(visibilityTimeout);
						if (message != null)
						{
							// Process the message
							ProcessMessage(message);

							// Delete the processed message from the queue
							_cloudQueue.DeleteMessage(message);

							// True indicates that a message was processed
							return true;
						}
						else
						{
							// The queue is empty, wait a little bit in order to avoid overwhelming the queue storage
							Task.Delay(1000, cancellationToken).Wait();

							// False indicates that no message was processed
							return false;
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