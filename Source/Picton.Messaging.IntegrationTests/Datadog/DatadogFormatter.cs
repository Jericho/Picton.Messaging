using App.Metrics;
using App.Metrics.Formatters;
using App.Metrics.Serialization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests.Datadog
{
	/// <summary>
	/// Formatter for encoding Metrics in Datadog JSON
	/// </summary>
	public class DatadogFormatter : IMetricsOutputFormatter
	{
		private readonly DatadogFormatterOptions _options;

		/// <summary>
		/// Constructor
		/// </summary>
		public DatadogFormatter(DatadogFormatterOptions options)
		{
			_options = options;
		}

		/// <inheritdoc />
		public Task WriteAsync(Stream output, MetricsDataValueSource metricsData, CancellationToken cancellationToken = new CancellationToken())
		{
			var serializer = new MetricSnapshotSerializer();
			using (var streamWriter = new StreamWriter(output))
			{
				using (var writer = new MetricSnapshotDatadogWriter(streamWriter, _options))
					serializer.Serialize(writer, metricsData, this.MetricFields);
			}

			return Task.CompletedTask;
		}

		/// <inheritdoc />
		public MetricsMediaTypeValue MediaType => new MetricsMediaTypeValue("application", "com.datadoghq.metrics", "v1", "json");

		/// <inheritdoc />
		public MetricFields MetricFields { get; set; }
	}
}
