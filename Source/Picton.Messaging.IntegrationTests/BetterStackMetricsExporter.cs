using Microsoft.Extensions.Diagnostics.Metrics;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	public class BetterStackMetricsExporter : MetricsCollector
	{
		private readonly HttpClient _httpClient;
		private readonly string _sourceToken;
		private readonly Uri _endpointForLogs;
		private readonly Uri _endpointForMetrics;

		/// <summary>
		/// Gets the name of the listener used in configuration and enabling instruments.
		/// </summary>
		public static string ListenerName => nameof(BetterStackMetricsExporter);

		public BetterStackMetricsExporter(HttpClient httpClient, string sourceToken)
		{
			_httpClient = httpClient;
			_sourceToken = sourceToken;
			_endpointForLogs = new Uri("https://in.logs.betterstack.com");
			_endpointForMetrics = new Uri("https://s1143848.eu-fsn-3.betterstackdata.com/metrics");
		}

		protected override MeasurementHandlers GetMeasurementHandlers() => new()
		{
			ByteHandler = OnMeasurement,
			ShortHandler = OnMeasurement,
			IntHandler = OnMeasurement,
			LongHandler = OnMeasurement,
			FloatHandler = OnMeasurement,
			DoubleHandler = OnMeasurement,
			DecimalHandler = OnMeasurement
		};

		public void OnInstrumentPublished(Instrument instrument)
		{
			Console.WriteLine($"Instrument published: {instrument.Name}");
		}

		public void OnMeasurement<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object>> tags, object state)
		{
			SendAsLogAsync(instrument, measurement, tags, state).GetAwaiter().GetResult();
			SendAsMetricAsync(instrument, measurement, tags, state).GetAwaiter().GetResult();
		}

		private Task SendAsLogAsync<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object>> tags, object state)
		{
			var payload = new
			{
				message = $"Metric: {instrument.Name}",
				timestamp = DateTime.UtcNow.ToString("o"),
				context = new
				{
					meter = instrument.Meter.Name,
					value = measurement,
					tags = tags.ToArray()
				}
			};

			var json = JsonSerializer.Serialize(payload);
			var request = new HttpRequestMessage(HttpMethod.Post, _endpointForLogs)
			{
				Content = new StringContent(json, Encoding.UTF8, "application/json")
			};

			request.Headers.Add("Authorization", $"Bearer {_sourceToken}");

			return _httpClient.SendAsync(request);
		}

		private Task SendAsMetricAsync<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object>> tags, object state)
		{
			var json = string.Empty;
			switch (instrument)
			{
				case Counter<int> intCounter:
					var payload = new
					{
						name = intCounter.Name,
						counter = new
						{
							value = intCounter
						}
					};
					json = JsonSerializer.Serialize(payload);
					break;
				//case Histogram<T>:
				//case ObservableCounter<T>:
				//case ObservableGauge<T>:
				//	break;
				default:
					// Only gauges, counters and histograms are supported
					return Task.CompletedTask;
			}

			if (!string.IsNullOrEmpty(json))
			{
				var request = new HttpRequestMessage(HttpMethod.Post, _endpointForMetrics)
				{
					Content = new StringContent(json, Encoding.UTF8, "application/json")
				};

				request.Headers.Add("Authorization", $"Bearer {_sourceToken}");

				return _httpClient.SendAsync(request);
			}

			return Task.CompletedTask;
		}

		public override string Name { get; } = InMemoryMetricsCollector.ListenerName;
	}
}
