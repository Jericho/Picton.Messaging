using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Moq;
using Picton.WorkerRoles;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.IntegrationTests
{
	class Program
	{
		static void Main(string[] args)
		{
			AzureStorageEmulatorManager.StartStorageEmulator();

			var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
			var cloudQueueClient = storageAccount.CreateCloudQueueClient();
			cloudQueueClient.DefaultRequestOptions.RetryPolicy = new NoRetry();
			var cloudQueue = cloudQueueClient.GetQueueReference("myqueue");
			cloudQueue.CreateIfNotExists();

			var lockObject = new Object();
			var stopping = false;
			Stopwatch sw = null;

			for (var i = 0; i < 100; i++)
			{
				cloudQueue.AddMessage(new CloudQueueMessage(string.Format("Hello world {0}", i)));
			}


			var mockWorker = new Mock<AsyncQueueWorker>("TestWorker", 1, 25, TimeSpan.FromMilliseconds(500), 5) { CallBase = true };
			mockWorker.Setup(m => m.GetQueue()).Returns(() =>
			{
				sw = Stopwatch.StartNew();
				return cloudQueue;
			});
			mockWorker.Setup(m => m.OnMessage(It.IsAny<CloudQueueMessage>(), It.IsAny<CancellationToken>())).Callback((CloudQueueMessage msg, CancellationToken cancellationToken) =>
			{
				Console.WriteLine(msg.AsString);
			});
			mockWorker.Setup(m => m.OnQueueEmpty(It.IsAny<CancellationToken>())).Callback(() =>
			{
				// Stop the worker role when the queue is empty.
				// However, ensure that we try to stop the role only once (otherwise each concurrent task would try to top the role)
				if (!stopping)
				{
					lock (lockObject)
					{
						if (sw.IsRunning) sw.Stop();
						if (!stopping)
						{
							// Indicate that the role is stopping
							stopping = true;

							// Run the 'OnStop' on a different thread so we don't block it
							Task.Run(() =>
							{
								mockWorker.Object.OnStop();
							}).ConfigureAwait(false);
						}
					}
				}
			});


			mockWorker.Object.OnStart();
			mockWorker.Object.Run();

			Console.WriteLine("Elapsed Milliseconds: " + sw.Elapsed.ToDurationString());

			Console.WriteLine("");
			Console.WriteLine("Press any key to exit...");
			Console.ReadKey();

		}
	}
}
