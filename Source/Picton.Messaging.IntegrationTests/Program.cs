using Logzio.DotNet.NLog;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Config;
using NLog.Extensions.Logging;
using NLog.Targets;
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
		public static async Task Main(string[] args)
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
			var app = serviceProvider.GetService<TestsRunner>();
			return await app.RunAsync(source.Token).ConfigureAwait(false);
		}

			builder.Services.AddPictonMessageHandlers();
			builder.Services.AddHostedService<TestsRunner>();

			// Configure logging
			builder.Logging.ClearProviders(); // Remove the built-in providers (which include the Console)
			builder.Logging.AddNLog(GetNLogConfiguration()); // Add our desired custom providers (which include the Colored Console)

			// Configure metrics
			var logzioMetricsToken = Environment.GetEnvironmentVariable("LOGZIO_METRICS_TOKEN");
			if (!string.IsNullOrEmpty(logzioMetricsToken))
			{
				builder.Services.AddOpenTelemetry()
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

			var host = builder.Build();
			await host.StartAsync(CancellationToken.None).ConfigureAwait(false);

			// Stop NLog (which has the desirable side-effect of flushing any pending logs)
			LogManager.Shutdown();
		}

		private static LoggingConfiguration GetNLogConfiguration()
		{
			// Configure logging
			var nLogConfig = new LoggingConfiguration();

			// Send logs to logz.io
			var logzioToken = Environment.GetEnvironmentVariable("LOGZIO_TOKEN");
			if (!string.IsNullOrEmpty(logzioToken))
			{
				var logzioTarget = new LogzioTarget
				{
					Name = "Logzio",
					Token = logzioToken,
					LogzioType = "nlog",
					JsonKeysCamelCase = true,
					// ProxyAddress = "http://localhost:8888",
				};
				logzioTarget.ContextProperties.Add(new TargetPropertyWithContext("Source", "PictonMessaging_integration_tests"));
				logzioTarget.ContextProperties.Add(new TargetPropertyWithContext("PictonMessaging-Version", typeof(AsyncMessagePump).Assembly.GetName().Version.ToString(3)));

				nLogConfig.AddTarget("Logzio", logzioTarget);
				nLogConfig.AddRule(NLog.LogLevel.Info, NLog.LogLevel.Fatal, "Logzio", "*");
			}

			// Send logs to console
			var consoleTarget = new ColoredConsoleTarget();
			nLogConfig.AddTarget("ColoredConsole", consoleTarget);
			nLogConfig.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, "ColoredConsole", "*");

			return nLogConfig;
		}
	}
}
