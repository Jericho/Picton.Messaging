using App.Metrics;
using App.Metrics.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Picton.Messaging.IntegrationTests.Datadog
{
	class MetricSnapshotDatadogWriter : IMetricSnapshotWriter
	{
		private StreamWriter _streamWriter;
		private readonly DatadogFormatterOptions _options;
		private readonly List<MetricJson> _metrics = new List<MetricJson>();

		static readonly JsonSerializerSettings JsonSettings = new JsonSerializerSettings
		{
			ContractResolver = new LowercaseContractResolver()
		};

		private class LowercaseContractResolver : DefaultContractResolver
		{
			protected override string ResolvePropertyName(string propertyName)
			{
				return propertyName.ToLower();
			}
		}

		public MetricSnapshotDatadogWriter(StreamWriter streamWriter, DatadogFormatterOptions options)
		{
			_streamWriter = streamWriter;
			_options = options;
		}

		public void Dispose()
		{
			if (_streamWriter != null)
			{
				Flush();

				_streamWriter?.Dispose();
				_streamWriter = null;
			}
		}

		/// <inheritdoc />
		public void Write(string context, string name, string field, object value, MetricTags tags, DateTime timestamp)
		{
			Write(context, name, new[] { field }, new[] { value }, tags, timestamp);
		}

		/// <inheritdoc />
		public void Write(string context, string name, IEnumerable<string> columns, IEnumerable<object> values, MetricTags tags, DateTime timestamp)
		{
			var posixTimestamp = (timestamp - new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)).TotalSeconds; //TODO: Check this

			var dict = columns.Zip(values, (column, value) => new { column, value }).ToDictionary(p => p.column, p => p.value);

			switch (tags.Values[Array.IndexOf(tags.Keys, "mtype")])
			{
				case "apdex":
					Write(posixTimestamp, context, name, "count", dict["samples"]);
					Write(posixTimestamp, context, name, "score", dict["score"]);
					Write(posixTimestamp, context, name, "satisfied", dict["satisfied"]);
					Write(posixTimestamp, context, name, "tolerating", dict["tolerating"]);
					Write(posixTimestamp, context, name, "frustrating", dict["frustrating"]);
					break;
				case "gauge":
					Write(posixTimestamp, context, name, dict["value"]);
					break;
				case "counter":
					if (dict.ContainsKey("value"))
						Write(posixTimestamp, context, name, dict["value"]);
					break;
				case "meter":
					Write(posixTimestamp, context, name, "count", dict["count.meter"]);
					Write(posixTimestamp, context, name, "15m", dict["rate15m"]);
					Write(posixTimestamp, context, name, "5m", dict["rate5m"]);
					Write(posixTimestamp, context, name, "1m", dict["rate1m"]);
					Write(posixTimestamp, context, name, "avg", dict["rate.mean"]);
					break;
				case "timer":
					Write(posixTimestamp, context, name, "1mrate", dict["rate1m"]);
					Write(posixTimestamp, context, name, "5mrate", dict["rate5m"]);
					Write(posixTimestamp, context, name, "15mrate", dict["rate15m"]);
					WriteHistogram(posixTimestamp, context, name, dict);
					break;
				case "histogram":
					WriteHistogram(posixTimestamp, context, name, dict);
					break;
			}
		}

		private void WriteHistogram(double posixTimestamp, string context, string name, Dictionary<string, object> dict)
		{
			Write(posixTimestamp, context, name, "count", dict["count.hist"]);

			Write(posixTimestamp, context, name, "max", dict["max"]);
			Write(posixTimestamp, context, name, "avg", dict["mean"]);
			Write(posixTimestamp, context, name, "median", dict["median"]);
			Write(posixTimestamp, context, name, "min", dict["min"]);
			Write(posixTimestamp, context, name, "stdDev", dict["stddev"]);

			Write(posixTimestamp, context, name, "75percentile", dict["p75"]);
			Write(posixTimestamp, context, name, "95percentile", dict["p95"]);
			Write(posixTimestamp, context, name, "98percentile", dict["p98"]);
			Write(posixTimestamp, context, name, "99percentile", dict["p99"]);
			Write(posixTimestamp, context, name, "999percentile", dict["p999"]);
		}

		private void Write(double timestamp, string context, string name, string subname, object value)
		{
			Write(timestamp, context, name + "." + subname, value);
		}

		private void Write(double timestamp, string context, string name, object value)
		{
			_metrics.Add(new MetricJson
			{
				Host = _options.Hostname,
				Metric = context + "." + name,
				//Tags = tags.Values,
				Points = new[]
				{
					new[] { timestamp, value }
				}
			});
		}

		private void Flush()
		{
			_streamWriter.Write(JsonConvert.SerializeObject(new SeriesJson { Series = _metrics.ToArray() }, JsonSettings));
		}
	}
}
