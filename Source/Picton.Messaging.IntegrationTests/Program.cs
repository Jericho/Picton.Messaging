using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Picton.Interfaces;
using Picton.Managers;
using Picton.Messaging.Logging;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	class Program
	{
		static void Main()
		{
			// Ensure the storage emulator is running
			AzureEmulatorManager.EnsureStorageEmulatorIsStarted();

			// If you want to see tracing from the Picton libary, change the LogLevel to 'Trace'
			var minLogLevel = Logging.LogLevel.Trace;

			// Configure logging to the console
			var logProvider = new ColoredConsoleLogProvider(minLogLevel);
			var logger = logProvider.GetLogger("Main");
			LogProvider.SetCurrentLogProvider(logProvider);

			// Ensure the Console is tall enough
			Console.WindowHeight = Math.Min(60, Console.LargestWindowHeight);

			// Setup the message queue in Azure storage emulator
			var storageAccount = StorageAccount.FromCloudStorageAccount(CloudStorageAccount.DevelopmentStorageAccount);
			var queueName = "myqueue";

			logger(Logging.LogLevel.Info, () => "Begin integration tests...");

			var numberOfMessages = 50;

			logger(Logging.LogLevel.Info, () => $"Adding {numberOfMessages} simple messages to the queue...");
			AddSimpleMessagesToQueue(numberOfMessages, queueName, storageAccount, logProvider).Wait();
			logger(Logging.LogLevel.Info, () => "Processing the messages in the queue...");
			ProcessSimpleMessages(queueName, storageAccount, logProvider);

			logger(Logging.LogLevel.Info, () => $"Adding {numberOfMessages} messages with handlers to the queue...");
			AddMessagesWithHandlerToQueue(numberOfMessages, queueName, storageAccount, logProvider).Wait();
			logger(Logging.LogLevel.Info, () => "Processing the messages in the queue...");
			ProcessMessagesWithHandlers(queueName, storageAccount, logProvider);

			// Flush the console key buffer
			while (Console.KeyAvailable) Console.ReadKey(true);

			// Wait for user to press a key
			logger(Logging.LogLevel.Info, () => "Press any key to exit...");
			Console.ReadKey();
		}

		public static async Task AddSimpleMessagesToQueue(int numberOfMessages, string queueName, IStorageAccount storageAccount, ILogProvider logProvider)
		{
			// Add messages to our testing queue
			var cloudQueueClient = storageAccount.CreateCloudQueueClient();
			var cloudQueue = cloudQueueClient.GetQueueReference(queueName);
			await cloudQueue.CreateIfNotExistsAsync().ConfigureAwait(false);
			await cloudQueue.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await cloudQueue.AddMessageAsync(new CloudQueueMessage($"Hello world {i}")).ConfigureAwait(false);
			}
		}

		public static void ProcessSimpleMessages(string queueName, IStorageAccount storageAccount, ILogProvider logProvider)
		{
			var logger = logProvider.GetLogger("ProcessSimpleMessages");
			Stopwatch sw = null;

			var lockObject = new Object();
			var stopping = false;

			// Configure the message pump
			var messagePump = new AsyncMessagePump(queueName, storageAccount, 1, 25, TimeSpan.FromMinutes(1), 3)
			{
				OnMessage = (message, cancellationToken) =>
				{
					logger(Logging.LogLevel.Debug, () => message.Content.ToString());
				}
			};

			// Stop the message pump when the queue is empty.
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Make sure we try to stop it only once (otherwise each concurrent task would try to stop it)
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
			logger(Logging.LogLevel.Info, () => $"\tDone in {sw.Elapsed.ToDurationString()}");
		}

		public static async Task AddMessagesWithHandlerToQueue(int numberOfMessages, string queueName, IStorageAccount storageAccount, ILogProvider logProvider)
		{
			var queueManager = new QueueManager(queueName, storageAccount);
			await queueManager.CreateIfNotExistsAsync().ConfigureAwait(false);
			await queueManager.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync(new MyMessage { MessageContent = $"Hello world {i}" }).ConfigureAwait(false);
			}
		}

		public static void ProcessMessagesWithHandlers(string queueName, IStorageAccount storageAccount, ILogProvider logProvider)
		{
			var logger = logProvider.GetLogger("ProcessMessagesWithHandlers");

			var lockObject = new Object();
			var stopping = false;
			Stopwatch sw = null;

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
			logger(Logging.LogLevel.Info, () => $"\tDone in {sw.Elapsed.ToDurationString()}");
		}
	}
}
