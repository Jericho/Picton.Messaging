using Formitable.BetterStack.Logger.Microsoft;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	class Program
	{
		public static async Task Main()
		{
			var source = new CancellationTokenSource();
			Console.CancelKeyPress += (s, e) =>
			{
				e.Cancel = true;
				source.Cancel();
			};

			var services = new ServiceCollection();
			ConfigureServices(services);
			using var serviceProvider = services.BuildServiceProvider();
			var app = serviceProvider.GetService<IHostedService>();
			await app.StartAsync(source.Token).ConfigureAwait(false);
		}

		private static void ConfigureServices(ServiceCollection services)
		{
			services.AddHostedService<TestsRunner>();
			services.AddPictonMessageHandlers();
			services.AddMetrics();

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

			// Configure metrics
			var logzioMetricsToken = Environment.GetEnvironmentVariable("LOGZIO_METRICS_TOKEN");
			if (!string.IsNullOrEmpty(logzioMetricsToken))
			{
				services.AddOpenTelemetry()
					.WithMetrics(metrics =>
					{
						metrics.AddMeter("Picton.Messaging");

						metrics.AddConsoleExporter();

						metrics.AddOtlpExporter("logzio", (exporterConfig, readerConfig) =>
						{
							// Either "http" or "https".
							var scheme = "https";

							// The Logz.io Listener URL for your region: https://docs.logz.io/docs/user-guide/admin/hosting-regions/account-region/
							var url = "listener.logz.io";

							// I found some documentation on Logz.io web site that says:
							//     - The required port depends whether HTTP or HTTPS is used: HTTP = 8070, HTTPS = 8071.
							// And I also found documentation that says:
							//     - ... port 8052 for http traffic, or port 8053 for https traffic.
							// This conflicting information is confusing but I suspect that 8070 and 8071 are the correct values
							// because I am successfully publishing logs to 8071 via HTTPS.
							var port = scheme switch
							{
								"http" => 8070,
								"https" => 8071,
								_ => throw new Exception($"Unknown scheme: {scheme}")
							};

							exporterConfig.Endpoint = (new UriBuilder(scheme, url, port)).Uri;
							exporterConfig.Protocol = OtlpExportProtocol.HttpProtobuf;
							exporterConfig.Headers = $"Authorization=Bearer {logzioMetricsToken}";

							// The default export interval is is 60 seconds, but our integration tests take less than 60 seconds to complete.
							// Also, several of the tasks performed by Picton.Messaging default to a 5 second interval.
							// Therefore, it makes sense to change the metrics export interval to a value between 5 and 60 seconds.
							readerConfig.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 5000;
						});
					});
			}
		}
	}
}
