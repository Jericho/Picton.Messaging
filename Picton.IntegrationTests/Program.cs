using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Picton.Logging;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Picton.IntegrationTests
{
	class Program
	{
		static void Main(string[] args)
		{
			// Ensure the storage emulator is running
			AzureStorageEmulatorManager.StartStorageEmulator();

			// If you want to see tracing from the Picton libary, change the LogLevel to 'Trace'
			var minLogLevel = Logging.LogLevel.Debug;

			// Configure logging to the console
			var logProvider = new ColoredConsoleLogProvider(minLogLevel);
			var logger = logProvider.GetLogger("IntegrationTests");
			LogProvider.SetCurrentLogProvider(logProvider);

			// Ensure the Console is tall enough
			Console.WindowHeight = 60;

			// Setup the message queue in Azure storage emulator
			var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
			var cloudQueueClient = storageAccount.CreateCloudQueueClient();
			cloudQueueClient.DefaultRequestOptions.RetryPolicy = new NoRetry();
			var cloudQueue = cloudQueueClient.GetQueueReference("myqueue");
			cloudQueue.CreateIfNotExists();

			var lockObject = new Object();
			var stopping = false;
			Stopwatch sw = null;

			// Add messages to our testing queue
			for (var i = 0; i < 50; i++)
			{
				cloudQueue.AddMessage(new CloudQueueMessage($"Hello world {i}"));
			}

			// Configure the message pump
			var messagePump = new AsyncMessagePump(cloudQueue, 1, 25, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				logger(Logging.LogLevel.Debug, () => message.AsString);
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

							// Log to console
							logger(Logging.LogLevel.Debug, () => "Asking the message pump to stop");

							// Run the 'OnStop' on a different thread so we don't block it
							Task.Run(() =>
							{
								messagePump.Stop();
								logger(Logging.LogLevel.Debug, () => "Message pump has been stopped");
							}).ConfigureAwait(false);
						}
					}
				}
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			logger(Logging.LogLevel.Debug, () => "Message pump is starting");
			messagePump.Start();

			// Display summary
			logger(Logging.LogLevel.Info, () => "Elapsed Milliseconds: " + sw.Elapsed.ToDurationString());
			logger(Logging.LogLevel.Info, () => "Press any key to exit...");

			// Flush the console key buffer
			while (Console.KeyAvailable) Console.ReadKey(true);

			// Wait for user to press a key
			Console.ReadKey();
		}
	}
}
