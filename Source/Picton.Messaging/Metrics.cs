using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Timer;

namespace Picton.Messaging
{
	internal static class Metrics
	{
		/// <summary>
		/// Gets the counter indicating the number of messages processed by the message pump
		/// </summary>
		public static CounterOptions MessagesProcessedCounter => new CounterOptions
		{
			Name = "Messages processed",
			MeasurementUnit = Unit.Items
		};

		/// <summary>
		/// Gets the counter indicating the time it takes to process a message
		/// </summary>
		public static TimerOptions MessageProcessingTimer => new TimerOptions
		{
			Name = "Message processing timer"
		};

		/// <summary>
		/// Gets the counter indicating the number of times messages have been fetched from the Azure queue
		/// </summary>
		public static CounterOptions FetchMessagesCounter => new CounterOptions
		{
			Name = "Fetch Messages",
			MeasurementUnit = Unit.Items
		};

		public static TimerOptions MessageFetchingTimer => new TimerOptions
		{
			Name = "Message fetching timer"
		};

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from the Azure queue but the queue was empty
		/// </summary>
		public static CounterOptions QueueEmptyCounter => new CounterOptions
		{
			Name = "Queue Empty"
		};
	}
}
