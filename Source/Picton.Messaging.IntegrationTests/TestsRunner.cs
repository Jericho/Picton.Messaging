using App.Metrics;
using App.Metrics.Scheduling;
using Microsoft.Extensions.Logging;
using Picton.Managers;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	internal class TestsRunner
	{
		private enum ResultCodes
		{
			Success = 0,
			Exception = 1,
			Cancelled = 1223
		}

		private readonly ILogger _logger;

		public TestsRunner(ILogger<TestsRunner> logger)
		{
			_logger = logger;
		}

		public async Task<int> RunAsync(CancellationToken cancellationToken = default)
		{
			// Configure Console
			var source = new CancellationTokenSource();
			Console.CancelKeyPress += (s, e) =>
			{
				e.Cancel = true;
				source.Cancel();
			};

			// Ensure the Console is tall enough and centered on the screen
			if (OperatingSystem.IsWindows()) Console.WindowHeight = Math.Min(60, Console.LargestWindowHeight);
			Utils.CenterConsole();

			// Configure where metrics are published to. By default, don't publish metrics
			var metrics = (IMetricsRoot)null;

			// In this example, I'm publishing metrics to a DataDog account
			var datadogApiKey = Environment.GetEnvironmentVariable("DATADOG_APIKEY");
			if (!string.IsNullOrEmpty(datadogApiKey))
			{
				metrics = new MetricsBuilder()
					.Report.ToDatadogHttp(
						options =>
						{
							options.Datadog.BaseUri = new Uri("https://app.datadoghq.com/api/v1/series");
							options.Datadog.ApiKey = datadogApiKey;
							options.FlushInterval = TimeSpan.FromSeconds(2);
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

			// Start Azurite before running the tests. It will be automaticaly stopped when "emulator" goes out of scope
			using (var emulator = new AzuriteManager())
			{
				var connectionString = "UseDevelopmentStorage=true";
				var queueName = "myqueue";
				var numberOfMessages = 25;
				var numberOfTenants = 5;

				// Run the integration tests
				await RunAsyncMessagePumpTests(connectionString, queueName, numberOfMessages, metrics, cancellationToken).ConfigureAwait(false);
				await RunAsyncMessagePumpWithHandlersTests(connectionString, queueName, numberOfMessages, metrics, cancellationToken).ConfigureAwait(false);
				await RunMultiTenantAsyncMessagePumpTests(connectionString, queueName, numberOfTenants, numberOfMessages, metrics, cancellationToken).ConfigureAwait(false);
			}

			// Prompt user to press a key in order to allow reading the log in the console
			var promptLog = new StringWriter();
			await promptLog.WriteLineAsync("\n\n**************************************************").ConfigureAwait(false);
			await promptLog.WriteLineAsync("Press any key to exit...").ConfigureAwait(false);
			Utils.Prompt(promptLog.ToString());

			// Return code indicating success/failure
			var resultCode = (int)ResultCodes.Success;

			return await Task.FromResult(resultCode);
		}

		private async Task RunAsyncMessagePumpTests(string connectionString, string queueName, int numberOfMessages, IMetrics metrics, CancellationToken cancellationToken)
		{
			_logger.LogInformation("**************************************************");
			_logger.LogInformation("Testing AsyncMessagePump...");

			// Add messages to the queue
			_logger.LogInformation("Adding {numberOfMessages} string messages to the {queueName} queue...", numberOfMessages, queueName);
			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync($"Hello world {i}").ConfigureAwait(false);
			}

			// Process the messages
			Stopwatch sw = null;

			// Configure the message pump
			var messagePump = new AsyncMessagePump(connectionString, queueName, 10, null, TimeSpan.FromMinutes(1), 3, _logger, metrics)
			{
				OnMessage = (message, cancellationToken) =>
				{
					_logger.LogInformation(message.Content.ToString());
				}
			};

			// Stop the message pump when the queue is empty.
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Stop the timer
				if (sw.IsRunning) sw.Stop();

				// Stop the message pump
				_logger.LogDebug("Asking the message pump to stop...");
				messagePump.Stop();
				_logger.LogDebug("The message pump has been stopped");
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			_logger.LogDebug("The message pump is starting...");
			messagePump.Start();

			// Display summary
			_logger.LogInformation($"\tDone in {sw.Elapsed.ToDurationString()}");
		}

		private async Task RunAsyncMessagePumpWithHandlersTests(string connectionString, string queueName, int numberOfMessages, IMetrics metrics, CancellationToken cancellationToken)
		{
			_logger.LogInformation("**************************************************");
			_logger.LogInformation("Testing AsyncMessagePumpWithHandlers...");

			// Add messages to the queue
			_logger.LogInformation("Adding {numberOfMessages} messages with handlers to the {queueName} queue...", numberOfMessages, queueName);
			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync().ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync(new MyMessage { MessageContent = $"Hello world {i}" }).ConfigureAwait(false);
			}

			// Process the messages
			Stopwatch sw = null;

			// Configure the message pump
			var messagePump = new AsyncMessagePumpWithHandlers(connectionString, queueName, 10, null, TimeSpan.FromMinutes(1), 3, _logger, metrics);
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Stop the timer
				if (sw.IsRunning) sw.Stop();

				// Stop the message pump
				_logger.LogDebug("Asking the message pump with handlers to stop...");
				messagePump.Stop();
				_logger.LogDebug("The message pump with handlers has been stopped");
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			_logger.LogDebug("The message pump with handlers is starting...");
			messagePump.Start();

			// Display summary
			_logger.LogInformation($"\tDone in {sw.Elapsed.ToDurationString()}");
		}

		private async Task RunMultiTenantAsyncMessagePumpTests(string connectionString, string queueNamePrefix, int numberOfTenants, int numberOfMessages, IMetrics metrics, CancellationToken cancellationToken)
		{
			_logger.LogInformation("**************************************************");
			_logger.LogInformation("Testing AsyncMultiTenantMessagePump...");

			// Add messages to the tenant queues
			for (int i = 0; i < numberOfTenants; i++)
			{
				var queueManager = new QueueManager(connectionString, $"{queueNamePrefix}{i:00}");
				await queueManager.ClearAsync().ConfigureAwait(false);
				var numberOfMessagesForThisTenant = numberOfMessages * (i + 1); // Each tenant receives a different number of messages
				for (var j = 0; j < numberOfMessagesForThisTenant; j++)
				{
					await queueManager.AddMessageAsync($"Hello world {j} to tenant {i:00}").ConfigureAwait(false);
				}
			}

			// Process the messages
			Stopwatch sw = null;

			// Configure the message pump
			var messagePump = new AsyncMultiTenantMessagePump(connectionString, queueNamePrefix, 10, null, TimeSpan.FromMinutes(1), 3, null, null, _logger, metrics)
			{
				OnMessage = (tenantId, message, cancellationToken) =>
				{
					var messageContent = message.Content.ToString();
					_logger.LogInformation($"{tenantId} - {messageContent}", tenantId, messageContent);
				}
			};

			// Stop the message pump when all tenant queues are empty.
			messagePump.OnEmpty = cancellationToken =>
			{
				// Stop the timer
				if (sw.IsRunning) sw.Stop();

				// Stop the message pump
				_logger.LogDebug("Asking the multi-tenant message pump to stop...");
				messagePump.Stop();
				_logger.LogDebug("The multi-tenant message pump has been stopped");
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			_logger.LogDebug("The multi-tenant message pump is starting...");
			messagePump.Start();

			// Display summary
			_logger.LogInformation($"\tDone in {sw.Elapsed.ToDurationString()}");
		}
	}
}
