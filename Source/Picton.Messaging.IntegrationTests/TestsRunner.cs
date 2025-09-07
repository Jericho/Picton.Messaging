using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Picton.Managers;
using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	internal class TestsRunner : IHostedService
	{
		private enum ResultCodes
		{
			Success = 0,
			Exception = 1,
			Cancelled = 1223
		}

		private readonly IServiceProvider _serviceProvider;
		private readonly IHostApplicationLifetime _hostApplicationLifetime;
		private readonly ILogger<TestsRunner> _logger;
		private readonly IMeterFactory _meterFactory;

		public TestsRunner(IServiceProvider serviceProvider, IHostApplicationLifetime hostApplicationLifetime, ILogger<TestsRunner> logger, IMeterFactory meterFactory)
		{
			_serviceProvider = serviceProvider;
			_hostApplicationLifetime = hostApplicationLifetime;
			_logger = logger;
			_meterFactory = meterFactory;
		}

		public async Task StartAsync(CancellationToken cancellationToken)
		{
			// Start Azurite before running the tests. It will be automaticaly stopped when "emulator" goes out of scope
			using (var emulator = new AzuriteManager())
			{
				var connectionString = "UseDevelopmentStorage=true";
				var queueName = "myqueue";
				var concurrentTasks = 5;

				// Run the integration tests
				await RunAsyncMessagePumpTests(connectionString, queueName, concurrentTasks, 25, cancellationToken).ConfigureAwait(false);
				await RunAsyncMessagePumpWithHandlersTests(connectionString, queueName, concurrentTasks, 25, cancellationToken).ConfigureAwait(false);
				await RunMultiTenantAsyncMessagePumpTests(connectionString, queueName, concurrentTasks, [6, 12, 18, 24], cancellationToken).ConfigureAwait(false);
			}

			// Shutdown the application
			_hostApplicationLifetime.StopApplication();
		}

		public Task StopAsync(CancellationToken cancellationToken)
		{
			return Task.CompletedTask;
		}

		private async Task RunAsyncMessagePumpTests(string connectionString, string queueName, int concurrentTasks, int numberOfMessages, CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested) return;

			_logger.LogDebug("**************************************************");
			_logger.LogDebug("Testing AsyncMessagePump...");

			if (cancellationToken.IsCancellationRequested)
			{
				_logger.LogInformation("Skipping tests due to cancellation");
				return;
			}

			try
			{
				// Add messages to the queue
				_logger.LogDebug("Adding {numberOfMessages} string messages to the {queueName} queue...", numberOfMessages, queueName);
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
				var messagePump = new AsyncMessagePump(options, _logger, _meterFactory)
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
				_logger.LogDebug("\tDone in {duration}", sw.Elapsed.ToDurationString());
			}
			catch (OperationCanceledException)
			{
				_logger.LogInformation("Tests interrupted due to cancellation");
			}
		}

		private async Task RunAsyncMessagePumpWithHandlersTests(string connectionString, string queueName, int concurrentTasks, int numberOfMessages, CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested) return;

			_logger.LogDebug("**************************************************");
			_logger.LogDebug("Testing AsyncMessagePumpWithHandlers...");

			if (cancellationToken.IsCancellationRequested)
			{
				_logger.LogInformation("Skipping tests due to cancellation");
				return;
			}

			try
			{
				// Add messages to the queue
				_logger.LogDebug("Adding {numberOfMessages} messages with handlers to the {queueName} queue...", numberOfMessages, queueName);
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
				var messagePump = new AsyncMessagePumpWithHandlers(options, _serviceProvider, _logger, _meterFactory)
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
				_logger.LogDebug("\tDone in {duration}", sw.Elapsed.ToDurationString());
			}
			catch (OperationCanceledException)
			{
				_logger.LogInformation("Tests interrupted due to cancellation");
			}
		}

		private async Task RunMultiTenantAsyncMessagePumpTests(string connectionString, string queueNamePrefix, int concurrentTasks, int[] numberOfMessagesForTenant, CancellationToken cancellationToken)
		{
			if (cancellationToken.IsCancellationRequested) return;

			_logger.LogDebug("**************************************************");
			_logger.LogDebug("Testing AsyncMultiTenantMessagePump...");

			if (cancellationToken.IsCancellationRequested)
			{
				_logger.LogInformation("Skipping tests due to cancellation");
				return;
			}

			try
			{
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
				var messagePump = new AsyncMultiTenantMessagePump(options, queueNamePrefix, logger: _logger, meterFactory: _meterFactory)
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
				_logger.LogDebug("\tDone in {duration}", sw.Elapsed.ToDurationString());
			}
			catch (OperationCanceledException)
			{
				_logger.LogInformation("Tests interrupted due to cancellation");
			}
		}
	}
}
