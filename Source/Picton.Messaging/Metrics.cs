using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using App.Metrics.Timer;

namespace Picton.Messaging
{
	internal static class Metrics
	{
		/// <summary>
		/// Gets the counter indicating the number of messages processed by the message pump.
		/// </summary>
		public static CounterOptions MessagesProcessedCounter => new CounterOptions
		{
			Context = "Picton",
			Name = "MessagesProcessedCount",
			MeasurementUnit = Unit.Items
		};

		/// <summary>
		/// Gets the timer indicating the time it takes to process a message.
		/// </summary>
		public static TimerOptions MessageProcessingTimer => new TimerOptions
		{
			Context = "Picton",
			Name = "MessageProcessingTime"
		};

		/// <summary>
		/// Gets the timer indicating the time it takes to fetch a batch of messages from the Azure queue.
		/// </summary>
		public static TimerOptions MessagesFetchingTimer => new TimerOptions
		{
			Context = "Picton",
			Name = "MessagesFetchingTime"
		};

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from an Azure queue but it was empty.
		/// </summary>
		public static CounterOptions QueueEmptyCounter => new CounterOptions
		{
			Context = "Picton",
			Name = "QueueEmptyCount"
		};

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from Azure but all the queues are empty.
		/// </summary>
		public static CounterOptions AllQueuesEmptyCounter => new CounterOptions
		{
			Context = "Picton",
			Name = "AllQueuesEmptyCount"
		};

		/// <summary>
		/// Gets the gauge indicating the number of messages waiting in the Azure queue over time.
		/// </summary>
		public static GaugeOptions QueuedCloudMessagesGauge => new GaugeOptions
		{
			Context = "Picton",
			Name = "QueuedCloudMessages",
			MeasurementUnit = Unit.Items
		};

		/// <summary>
		/// Gets the gauge indicating the number of messages waiting in the memory queue over time.
		/// </summary>
		public static GaugeOptions QueuedMemoryMessagesGauge => new GaugeOptions
		{
			Context = "Picton",
			Name = "QueuedMemoryMessages",
			MeasurementUnit = Unit.Items
		};
	}
}
