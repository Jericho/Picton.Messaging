using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Picton.Interfaces;
using Picton.Logging;
using Picton.Managers;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Picton.IntegrationTests
{
	class Program
	{
#pragma warning disable RECS0154 // Parameter is never used
		static void Main(string[] args)
#pragma warning restore RECS0154 // Parameter is never used
		{
			// Ensure the storage emulator is running
			AzureStorageEmulatorManager.StartStorageEmulator();

			// If you want to see tracing from the Picton libary, change the LogLevel to 'Trace'
			var minLogLevel = Logging.LogLevel.Debug;

			// Configure logging to the console
			var logProvider = new ColoredConsoleLogProvider(minLogLevel);
			var logger = logProvider.GetLogger("Main");
			LogProvider.SetCurrentLogProvider(logProvider);

			// Ensure the Console is tall enough
			Console.WindowHeight = Math.Min(60, Console.LargestWindowHeight);

			// Setup the message queue in Azure storage emulator
			var storageAccount = StorageAccount.FromCloudStorageAccount(CloudStorageAccount.DevelopmentStorageAccount);
			var queueName = "myqueue";

			// Proces some mesages
			logger(Logging.LogLevel.Info, () => "Begin integration tests...");
			ProcessSimpleMessages(queueName, storageAccount, logProvider);
			ProcessMessagesWithHandlers(queueName, storageAccount, logProvider);

			// Flush the console key buffer
			while (Console.KeyAvailable) Console.ReadKey(true);

			// Wait for user to press a key
			logger(Logging.LogLevel.Info, () => "Press any key to exit...");
			Console.ReadKey();
		}

		public static void ProcessSimpleMessages(string queueName, IStorageAccount storageAccount, ILogProvider logProvider)
		{
			var logger = logProvider.GetLogger("ProcessSimpleMessages");

			var lockObject = new Object();
			var stopping = false;
			Stopwatch sw = null;

			// Add messages to our testing queue
			var cloudQueueClient = storageAccount.CreateCloudQueueClient();
			var cloudQueue = cloudQueueClient.GetQueueReference(queueName);
			cloudQueue.CreateIfNotExists();
			for (var i = 0; i < 5; i++)
			{
				cloudQueue.AddMessage(new CloudQueueMessage($"Hello world {i}"));
			}

			// Configure the message pump
			var messagePump = new AsyncMessagePump(queueName, storageAccount, 1, 25, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				logger(Logging.LogLevel.Debug, () => message.Content.ToString());
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
							logger(Logging.LogLevel.Debug, () => "Asking the 'simple' message pump to stop");

							// Run the 'OnStop' on a different thread so we don't block it
							Task.Run(() =>
							{
								messagePump.Stop();
								logger(Logging.LogLevel.Debug, () => "The 'simple' message pump has been stopped");
							}).ConfigureAwait(false);
						}
					}
				}
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			logger(Logging.LogLevel.Debug, () => "The 'simple' message pump is starting");
			messagePump.Start();

			// Display summary
			logger(Logging.LogLevel.Info, () => "Elapsed Milliseconds: " + sw.Elapsed.ToDurationString());
		}

		public static void ProcessMessagesWithHandlers(string queueName, IStorageAccount storageAccount, ILogProvider logProvider)
		{
			var logger = logProvider.GetLogger("ProcessMessagesWithHandlers");

			var lockObject = new Object();
			var stopping = false;
			Stopwatch sw = null;

			// Add messages to our testing queue
			var queueManager = new QueueManager(queueName, storageAccount);
			for (var i = 0; i < 5; i++)
			{
				queueManager.AddMessageAsync(new MyMessage { MessageContent = $"Hello world {i}" });
			}

			// Configure the message pump
			var messagePump = new AsyncMessagePumpWithHandlers(queueName, storageAccount, 1, 25, TimeSpan.FromMinutes(1), 3);
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
							logger(Logging.LogLevel.Debug, () => "Asking the message pump with handlers to stop");

							// Run the 'OnStop' on a different thread so we don't block it
							Task.Run(() =>
							{
								messagePump.Stop();
								logger(Logging.LogLevel.Debug, () => "The message pump with handlers has been stopped");
							}).ConfigureAwait(false);
						}
					}
				}
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			logger(Logging.LogLevel.Debug, () => "The message pump with handlers is starting");
			messagePump.Start();

			// Display summary
			logger(Logging.LogLevel.Info, () => "Elapsed Milliseconds: " + sw.Elapsed.ToDurationString());
		}
	}
}
