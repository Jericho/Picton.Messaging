using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Diagnostics;
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

			// Add messages to our testing queue
			for (var i = 0; i < 100; i++)
			{
				cloudQueue.AddMessage(new CloudQueueMessage(string.Format("Hello world {0}", i)));
			}

			// Configure the message pump
			var messagePump = new AsyncMessagePump(cloudQueue, 1, 25, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Console.WriteLine(message.AsString);
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Stop the message pump when the queue is empty.
				// However, ensure that we try to stop it only once (otherwise each concurrent task would try to stop it)
				if (!stopping)
				{
					lock (lockObject)
					{
						if (sw.IsRunning) sw.Stop();
						if (!stopping)
						{
							// Indicate that the message pump is stopping
							stopping = true;

							// Run the 'OnStop' on a different thread so we don't block it
							Task.Run(() =>
							{
								messagePump.Stop();
							}).ConfigureAwait(false);
						}
					}
				}
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			messagePump.Start();

			// Display how long it took to process the messages that were in the queue
			Console.WriteLine("Elapsed Milliseconds: " + sw.Elapsed.ToDurationString());
			Console.WriteLine("");
			Console.WriteLine("Press any key to exit...");

			// Flush the console key buffer
			while (Console.KeyAvailable) Console.ReadKey(true);

			// Wait for user to press a key
			Console.ReadKey();
		}
	}
}
