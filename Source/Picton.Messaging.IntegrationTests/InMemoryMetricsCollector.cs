using Microsoft.Extensions.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;

namespace Picton.Messaging.IntegrationTests
{
	/// <summary>
	/// Provides simple implementation of <see cref="IMetricsListener"/> which listens to metrics 
	/// emited from system and stores measurements in memory to the typed dictionaries 
	/// so they can be accessed by corresponding <see cref="Instrument.Name"/> key.
	/// This implementation also provides periodic polling for observable instruments.
	/// </summary>
	internal sealed class InMemoryMetricsCollector : MetricsCollector
	{
		private readonly Dictionary<string, byte> byteValues = [];
		private readonly Dictionary<string, short> shortValues = [];
		private readonly Dictionary<string, int> intValues = [];
		private readonly Dictionary<string, long> longValues = [];
		private readonly Dictionary<string, float> floatValues = [];
		private readonly Dictionary<string, double> doubleValues = [];
		private readonly Dictionary<string, decimal> decimalValues = [];

		/// <summary>
		/// Gets the name of the listener used in configuration and enabling instruments.
		/// </summary>
		public static string ListenerName => nameof(InMemoryMetricsCollector);

		public InMemoryMetricsCollector([NotNull] IOptionsMonitor<MetricsCollectorOptions> options)
		{
			RecordInterval = options.CurrentValue.RecordInterval;
			options.OnChange(OnOptionsChanged);
		}

		private void OnByteMeasurement(Instrument instrument, byte measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			byteValues[instrument.Name] = measurement;
		private void OnShortMeasurement(Instrument instrument, short measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			shortValues[instrument.Name] = measurement;

		private void OnIntMeasurement(Instrument instrument, int measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			intValues[instrument.Name] = measurement;

		private void OnLongMeasurement(Instrument instrument, long measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			longValues[instrument.Name] = measurement;

		private void OnFloatMeasurement(Instrument instrument, float measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			floatValues[instrument.Name] = measurement;

		private void OnDoubleMeasurement(Instrument instrument, double measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			doubleValues[instrument.Name] = measurement;

		private void OnDecimalMeasurement(Instrument instrument, decimal measurement,
			ReadOnlySpan<KeyValuePair<string, object>> tags, object state) =>
			decimalValues[instrument.Name] = measurement;


		private void OnOptionsChanged(MetricsCollectorOptions options, string arg2) => RecordInterval = options.RecordInterval;

		protected override MeasurementHandlers GetMeasurementHandlers() => new()
		{
			ByteHandler = OnByteMeasurement,
			ShortHandler = OnShortMeasurement,
			IntHandler = OnIntMeasurement,
			LongHandler = OnLongMeasurement,
			FloatHandler = OnFloatMeasurement,
			DoubleHandler = OnDoubleMeasurement,
			DecimalHandler = OnDecimalMeasurement
		};

		public IReadOnlyDictionary<string, byte> ByteValues => byteValues;
		public IReadOnlyDictionary<string, short> ShortValues => shortValues;
		public IReadOnlyDictionary<string, int> IntValues => intValues;
		public IReadOnlyDictionary<string, long> LongValues => longValues;
		public IReadOnlyDictionary<string, float> FloatValues => floatValues;
		public IReadOnlyDictionary<string, double> DoubleValues => doubleValues;
		public IReadOnlyDictionary<string, decimal> DecimalValues => decimalValues;

		public override string Name { get; } = InMemoryMetricsCollector.ListenerName;
	}
}
