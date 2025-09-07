using Formitable.BetterStack.Logger.Microsoft;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	class Program
	{
		public static async Task Main()
		{
			var builder = Host.CreateApplicationBuilder();

			ConfigureLogging(builder.Logging);
			ConfigureServices(builder.Services);

			var host = builder.Build();
			await host.StartAsync().ConfigureAwait(false);
		}

		private static void ConfigureLogging(ILoggingBuilder logging)
		{
			logging.ClearProviders();

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
		}

		private static void ConfigureServices(IServiceCollection services)
		{
			services.AddHostedService<TestsRunner>();
			services.AddPictonMessageHandlers();

			services
				.AddMetrics(metrics =>
				{
					metrics.AddDebugConsole();
				});
		}
	}
}
