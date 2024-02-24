using App.Metrics;
using App.Metrics.Scheduling;
using Microsoft.Extensions.Logging;
using Picton.Managers;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
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

		private readonly ILogger<TestsRunner> _logger;
		private readonly IServiceProvider _serviceProvider;

		public TestsRunner(ILogger<TestsRunner> logger, IServiceProvider serviceProvider)
		{
			_logger = logger;
			_serviceProvider = serviceProvider;
		}

		public async Task<int> RunAsync()
		{
			ServicePointManager.DefaultConnectionLimit = 1000;
			ServicePointManager.UseNagleAlgorithm = false;

			// Configure Console
			var cts = new CancellationTokenSource();
			Console.CancelKeyPress += (s, e) =>
			{
				e.Cancel = true;
				cts.Cancel();
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
				var concurrentTasks = 5;

				// Run the integration tests
				await RunAsyncMessagePumpTests(connectionString, queueName, concurrentTasks, 25, metrics, cts.Token).ConfigureAwait(false);
				await RunAsyncMessagePumpWithHandlersTests(connectionString, queueName, concurrentTasks, 25, metrics, cts.Token).ConfigureAwait(false);
				await RunMultiTenantAsyncMessagePumpTests(connectionString, queueName, concurrentTasks, [6, 12, 18, 24], metrics, cts.Token).ConfigureAwait(false);
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

		private async Task RunAsyncMessagePumpTests(string connectionString, string queueName, int concurrentTasks, int numberOfMessages, IMetrics metrics, CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested) return;

			_logger.LogInformation("**************************************************");
			_logger.LogInformation("Testing AsyncMessagePump...");

			// Add messages to the queue
			_logger.LogInformation("Adding {numberOfMessages} string messages to the {queueName} queue...", numberOfMessages, queueName);
			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync(cancellationToken).ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync($"Hello world {i}", cancellationToken: cancellationToken).ConfigureAwait(false);
			}

			// Configure the message pump
			Stopwatch sw = null;
			var cts = new CancellationTokenSource();
			var options = new MessagePumpOptions(connectionString, concurrentTasks, null, null);
			var messagePump = new AsyncMessagePump(options, _logger, metrics)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					_logger.LogInformation("{messageContent}", message.Content.ToString());
				},

				// Stop the message pump when there are no more messages to process.
				OnAllQueuesEmpty = cancellationToken =>
				{
					if (sw.IsRunning) sw.Stop();
					_logger.LogDebug("Asking the message pump to stop...");
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueName, null, TimeSpan.FromMinutes(1), 3);

			// Start the message pump
			sw = Stopwatch.StartNew();
			_logger.LogDebug("The message pump is starting...");
			await messagePump.StartAsync(cts.Token).ConfigureAwait(false);

			// Display summary
			_logger.LogInformation("\tDone in {duration}", sw.Elapsed.ToDurationString());
		}

		private async Task RunAsyncMessagePumpWithHandlersTests(string connectionString, string queueName, int concurrentTasks, int numberOfMessages, IMetrics metrics, CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested) return;

			_logger.LogInformation("**************************************************");
			_logger.LogInformation("Testing AsyncMessagePumpWithHandlers...");

			// Add messages to the queue
			_logger.LogInformation("Adding {numberOfMessages} messages with handlers to the {queueName} queue...", numberOfMessages, queueName);
			var queueManager = new QueueManager(connectionString, queueName);
			await queueManager.ClearAsync(cancellationToken).ConfigureAwait(false);
			for (var i = 0; i < numberOfMessages; i++)
			{
				await queueManager.AddMessageAsync(new MyMessage { MessageContent = $"Hello world {i}" }, cancellationToken: cancellationToken).ConfigureAwait(false);
			}

			// Configure the message pump
			Stopwatch sw = null;
			var cts = new CancellationTokenSource();
			var options = new MessagePumpOptions(connectionString, concurrentTasks, null, null);
			var messagePump = new AsyncMessagePumpWithHandlers(options, _serviceProvider, _logger, metrics)
			{
				// Stop the message pump when there are no more messages to process.
				OnAllQueuesEmpty = cancellationToken =>
				{
					if (sw.IsRunning) sw.Stop();
					_logger.LogDebug("Asking the message pump with handlers to stop...");
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueName, null, TimeSpan.FromMinutes(1), 3);

			// Start the message pump
			sw = Stopwatch.StartNew();
			_logger.LogDebug("The message pump with handlers is starting...");
			await messagePump.StartAsync(cts.Token);

			// Display summary
			_logger.LogInformation("\tDone in {duration}", sw.Elapsed.ToDurationString());
		}

		private async Task RunMultiTenantAsyncMessagePumpTests(string connectionString, string queueNamePrefix, int concurrentTasks, int[] numberOfMessagesForTenant, IMetrics metrics, CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested) return;

			_logger.LogInformation("**************************************************");
			_logger.LogInformation("Testing AsyncMultiTenantMessagePump...");

			// Add messages to the tenant queues
			for (int i = 0; i < numberOfMessagesForTenant.Length; i++)
			{
				var queueManager = new QueueManager(connectionString, $"{queueNamePrefix}{i:00}");
				await queueManager.ClearAsync(cancellationToken).ConfigureAwait(false);
				for (var j = 0; j < numberOfMessagesForTenant[i]; j++)
				{
					await queueManager.AddMessageAsync($"Hello world {j:00} to tenant {i:00}", cancellationToken: cancellationToken).ConfigureAwait(false);
				}
			}

			// Process the messages
			Stopwatch sw = null;

			// Configure the message pump
			var cts = new CancellationTokenSource();
			var options = new MessagePumpOptions(connectionString, concurrentTasks, null, null);
			var messagePump = new AsyncMultiTenantMessagePump(options, queueNamePrefix, logger: _logger, metrics: metrics)
			{
				OnMessage = (tenantId, message, cancellationToken) =>
				{
					_logger.LogInformation("{tenantId} - {messageContent}", tenantId, message.Content.ToString());
				},

				// Stop the message pump when there are no more messages to process.
				OnAllQueuesEmpty = cancellationToken =>
				{
					if (sw.IsRunning) sw.Stop();
					_logger.LogDebug("Asking the multi-tenant message pump to stop...");
					cts.Cancel();
				}
			};

			// Start the message pump
			sw = Stopwatch.StartNew();
			_logger.LogDebug("The multi-tenant message pump is starting...");
			await messagePump.StartAsync(cts.Token);

			// Display summary
			_logger.LogInformation("\tDone in {duration}", sw.Elapsed.ToDurationString());
		}
	}
}
