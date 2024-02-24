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
		public static CounterOptions MessagesProcessedCounter => new()
		{
			Context = "Picton.Messaging",
			Name = "MessagesProcessedCount",
			MeasurementUnit = Unit.Items
		};

		/// <summary>
		/// Gets the timer indicating how long a message was in queue, waiting to be processed.
		/// </summary>
		public static TimerOptions MessageWaitBeforeProcessTimer => new()
		{
			Context = "Picton.Messaging",
			Name = "MessageWaitBeforeProcessTimer"
		};

		/// <summary>
		/// Gets the timer indicating the time it takes to process a message.
		/// </summary>
		public static TimerOptions MessageProcessingTimer => new()
		{
			Context = "Picton.Messaging",
			Name = "MessageProcessingTime"
		};

		/// <summary>
		/// Gets the timer indicating the time it takes to fetch a batch of messages from the Azure queue.
		/// </summary>
		public static TimerOptions MessagesFetchingTimer => new()
		{
			Context = "Picton.Messaging",
			Name = "MessagesFetchingTime"
		};

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from an Azure queue but it was empty.
		/// </summary>
		public static CounterOptions QueueEmptyCounter => new()
		{
			Context = "Picton.Messaging",
			Name = "QueueEmptyCount"
		};

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from Azure but all the queues are empty.
		/// </summary>
		public static CounterOptions AllQueuesEmptyCounter => new()
		{
			Context = "Picton.Messaging",
			Name = "AllQueuesEmptyCount"
		};

		/// <summary>
		/// Gets the gauge indicating the number of messages waiting in the Azure queue over time.
		/// </summary>
		public static GaugeOptions QueuedCloudMessagesGauge => new()
		{
			Context = "Picton.Messaging",
			Name = "QueuedCloudMessages",
			MeasurementUnit = Unit.Items
		};

		/// <summary>
		/// Gets the gauge indicating the number of messages waiting in the memory queue over time.
		/// </summary>
		public static GaugeOptions QueuedMemoryMessagesGauge => new()
		{
			Context = "Picton.Messaging",
			Name = "QueuedMemoryMessages",
			MeasurementUnit = Unit.Items
		};
	}
}
