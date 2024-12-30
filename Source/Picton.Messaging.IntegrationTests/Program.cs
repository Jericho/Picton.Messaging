using Formitable.BetterStack.Logger.Microsoft;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	class Program
	{
		public static async Task Main()
		{
			var cts = new CancellationTokenSource();
			Console.CancelKeyPress += (s, e) =>
			{
				e.Cancel = true;
				cts.Cancel();
			};

			var services = new ServiceCollection();
			ConfigureServices(services);
			using var serviceProvider = services.BuildServiceProvider();
			var app = serviceProvider.GetService<IHostedService>();
			await app.StartAsync(cts.Token).ConfigureAwait(false);
		}

		private static void ConfigureServices(ServiceCollection services)
		{
			services.AddHostedService<TestsRunner>();
			services.AddPictonMessageHandlers();

			services
				.AddMetrics(metrics =>
				{
					metrics.AddDebugConsole();
				});

			services
				.AddLogging(logging =>
				{
					var betterStackToken = Environment.GetEnvironmentVariable("BETTERSTACK_TOKEN");
					if (!string.IsNullOrEmpty(betterStackToken))
					{
						logging.AddBetterStackLogger(options =>
						{
							options.SourceToken = betterStackToken;
							options.Context["source"] = "Picton_messaging_integration_tests";
							options.Context["Picton-Version"] = typeof(CloudMessage).Assembly.GetName().Version.ToString(3);
						});
					}

					logging.AddSimpleConsole(options =>
					{
						options.SingleLine = true;
						options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
					});

					logging.AddFilter("*", LogLevel.Debug);
				});
		}
	}
}
