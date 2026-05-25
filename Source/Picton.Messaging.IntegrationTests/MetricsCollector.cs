using Microsoft.Extensions.Diagnostics.Metrics;
using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	/// <summary>
	/// Provides base implementation of <see cref="IMetricsListener"/> which listens to metrics 
	/// emited from system. It has periodical polling for observable instruments
	/// with <see cref="RecordInterval"/> time interval built-in.
	/// </summary>
	/// <remarks>
	/// From the https://github.com/Lapiniot/OOs.Common project.
	/// </remarks>
	public abstract class MetricsCollector : IMetricsListener, IDisposable
	{
		private int instruments;
		private TimeSpan recordInterval = TimeSpan.FromSeconds(5);
		private Task recordTask;
		private IObservableInstrumentsSource source;
		private PeriodicTimer timer;

		#region IMetricsListener implementation

		MeasurementHandlers IMetricsListener.GetMeasurementHandlers() => GetMeasurementHandlers();

		void IMetricsListener.Initialize(IObservableInstrumentsSource source)
		{
			this.source = source;
			if (instruments > 0 && recordTask is null)
				Start();
			Initialize(source);
		}

		bool IMetricsListener.InstrumentPublished(Instrument instrument, out object userState)
		{
			if (InstrumentPublished(instrument, out userState))
			{
				if (instrument.IsObservable && ++instruments > 0 && source is not null && recordTask is null)
					Start();
				return true;
			}

			userState = null;
			return true;
		}

		void IMetricsListener.MeasurementsCompleted(Instrument instrument, object userState)
		{
			if (instrument.IsObservable && --instruments is 0)
				Stop();

			MeasurementsCompleted(instrument, userState);
		}

		#endregion

		private static async Task RunObserverAsync(PeriodicTimer timer, IObservableInstrumentsSource source)
		{
			source.RecordObservableInstruments();
			while (await timer.WaitForNextTickAsync().ConfigureAwait(false))
			{
				source.RecordObservableInstruments();
			}
		}

		private void Start()
		{
			timer?.Dispose();
			timer = new(RecordInterval);
			recordTask = RunObserverAsync(timer, source!);
		}

		private void Stop()
		{
			timer?.Dispose();
			timer = null;
			recordTask = null;
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
				timer?.Dispose();
		}

		protected abstract MeasurementHandlers GetMeasurementHandlers();

		protected virtual void Initialize(IObservableInstrumentsSource source) { }

		protected virtual bool InstrumentPublished(Instrument instrument, out object userState)
		{
			userState = null;
			return true;
		}

		protected virtual void MeasurementsCompleted(Instrument instrument, object userState) { }

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public abstract string Name { get; }

		public TimeSpan RecordInterval
		{
			get => recordInterval;
			protected set
			{
				if (recordInterval == value)
					return;
				recordInterval = value;
				timer?.Period = recordInterval;
			}
		}
	}
}
