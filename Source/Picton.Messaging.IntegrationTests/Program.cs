using App.Metrics;
using App.Metrics.Scheduling;
using Picton.Managers;
using Picton.Messaging.IntegrationTests.Datadog;
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
			var minLogLevel = Logging.LogLevel.Debug;

			// Configure logging to the console
			var logProvider = new ColoredConsoleLogProvider(minLogLevel);
			var logger = logProvider.GetLogger("Main");
			LogProvider.SetCurrentLogProvider(logProvider);

			// Ensure the Console is tall enough and centered on the screen
			if (OperatingSystem.IsWindows()) Console.WindowHeight = Math.Min(60, Console.LargestWindowHeight);
			ConsoleUtils.CenterConsole();

			// Configure where metrics are published to. By default, don't publish metrics
			var metrics = (IMetricsRoot)null;

			// In this example, I'm publishing metrics to a DataDog account
			var datadogApiKey = Environment.GetEnvironmentVariable("DATADOG_APIKEY");
			if (!string.IsNullOrEmpty(datadogApiKey))
			{
				metrics = new MetricsBuilder()
					.Report.OverHttp(o =>
					{
						o.HttpSettings.RequestUri = new Uri($"https://app.datadoghq.com/api/v1/series?api_key={datadogApiKey}");
						o.MetricsOutputFormatter = new DatadogFormatter(new DatadogFormatterOptions { Hostname = Environment.MachineName });
						o.FlushInterval = TimeSpan.FromSeconds(2);
					})
					.Build();

				// Send metrics to Datadog
				var sendMetricsJob = new AppMetricsTaskScheduler(
					TimeSpan.FromSeconds(2),
					async () =>
					{
						await Task.WhenAll(metrics.ReportRunner.RunAllAsync());
					});
				sendMetricsJob.Start();
			}

			// Setup the message queue in Azure storage emulator
			var connectionString = "UseDevelopmentStorage=true";
			var queueName = "myqueue";
			var numberOfMessages = 25;

			logger(Logging.LogLevel.Info, () => "Begin integration tests...");

			var stringMessagesLogger = logProvider.GetLogger("StringMessages");
			AddStringMessagesToQueue(numberOfMessages, queueName, connectionString, stringMessagesLogger).Wait();
			ProcessSimpleMessages(queueName, connectionString, stringMessagesLogger, metrics);

			var simpleMessagesLogger = logProvider.GetLogger("SimpleMessages");
			AddSimpleMessagesToQueue(numberOfMessages, queueName, connectionString, simpleMessagesLogger).Wait();
			ProcessSimpleMessages(queueName, connectionString, simpleMessagesLogger, metrics);

			var messagesWithHandlerLogger = logProvider.GetLogger("MessagesWithHandler");
			AddMessagesWithHandlerToQueue(numberOfMessages, queueName, connectionString, messagesWithHandlerLogger).Wait();
			ProcessMessagesWithHandlers(queueName, connectionString, messagesWithHandlerLogger, metrics);

			// Flush the console key buffer
			while (Console.KeyAvailable) Console.ReadKey(true);

			// Wait for user to press a key
			logger(Logging.LogLevel.Info, () => "Press any key to exit...");
			Console.ReadKey();
		}

		public static async Task AddStringMessagesToQueue(int numberOfMessages, string queueName, string connectionString, Logger logger)
		{
			logger(Logging.LogLevel.Info, () => $"Adding {numberOfMessages} string messages to the queue...");

			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync($"Hello world {i}").ConfigureAwait(false);
			}
		}

		public static async Task AddSimpleMessagesToQueue(int numberOfMessages, string queueName, string connectionString, Logger logger)
		{
			logger(Logging.LogLevel.Info, () => $"Adding {numberOfMessages} simple messages to the queue...");

			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync($"Hello world {i}").ConfigureAwait(false);
			}
		}

		public static void ProcessSimpleMessages(string queueName, string connectionString, Logger logger, IMetrics metrics)
		{
			Stopwatch sw = null;

			// Configure the message pump
			var messagePump = new AsyncMessagePump(connectionString, queueName, 10, null, TimeSpan.FromMinutes(1), 3, metrics)
			{
				OnMessage = (message, cancellationToken) =>
				{
					logger(Logging.LogLevel.Debug, () => message.Content.ToString());
				}
			};

			// Stop the message pump when the queue is empty.
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Stop the timer
				if (sw.IsRunning) sw.Stop();

				// Stop the message pump
				logger(Logging.LogLevel.Debug, () => "Asking the 'simple' message pump to stop");
				messagePump.Stop();
				logger(Logging.LogLevel.Debug, () => "The 'simple' message pump has been stopped");
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			logger(Logging.LogLevel.Debug, () => "The 'simple' message pump is starting");
			messagePump.Start();

			// Display summary
			logger(Logging.LogLevel.Info, () => $"\tDone in {sw.Elapsed.ToDurationString()}");
		}

		public static async Task AddMessagesWithHandlerToQueue(int numberOfMessages, string queueName, string connectionString, Logger logger)
		{
			logger(Logging.LogLevel.Info, () => $"Adding {numberOfMessages} messages with handlers to the queue...");

			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync(new MyMessage { MessageContent = $"Hello world {i}" }).ConfigureAwait(false);
			}
		}

		public static void ProcessMessagesWithHandlers(string queueName, string connectionString, Logger logger, IMetrics metrics)
		{
			Stopwatch sw = null;

			// Configure the message pump
			var messagePump = new AsyncMessagePumpWithHandlers(connectionString, queueName, 10, null, TimeSpan.FromMinutes(1), 3, metrics);
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Stop the timer
				if (sw.IsRunning) sw.Stop();

				// Stop the message pump
				logger(Logging.LogLevel.Debug, () => "Asking the message pump with handlers to stop");
				messagePump.Stop();
				logger(Logging.LogLevel.Debug, () => "The message pump with handlers has been stopped");
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
