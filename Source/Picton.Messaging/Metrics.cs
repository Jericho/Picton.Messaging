using System.Diagnostics.Metrics;

namespace Picton.Messaging
{
	internal class Metrics
	{
		public Metrics(IMeterFactory meterFactory)
		{
			var pictonMessagingVersion =
#if DEBUG
				"DEBUG";
#else
				typeof(AsyncMessagePump).GetTypeInfo().Assembly.GetName().Version.ToString(3);
#endif

			var meter = meterFactory.Create("Picton.Messaging", pictonMessagingVersion);

			MessagesProcessed = meter.CreateCounter<int>("picton.messaging.messages_processed", null, "The number of messages processed by the message pump.");
			MessageWaitBeforeProcess = meter.CreateHistogram<long>("picton.messaging.message_wait_before_process", null, "How long a message was in queue, waiting to be processed.");
			MessageProcessing = meter.CreateHistogram<long>("picton.messaging.message_processing", null, "The time it takes to process a message.");
			MessagesFetching = meter.CreateHistogram<long>("picton.messaging.messages_fetching", null, "The time it takes to fetch a batch of messages from the Azure queue.");
			QueueEmpty = meter.CreateCounter<int>("picton.messaging.queue_empty", null, "The number of times we attempted to fetch messages from an Azure queue but it was empty.");
			AllQueuesEmpty = meter.CreateCounter<int>("picton.messaging.all_queues_empty", null, "The number of times we attempted to fetch messages from Azure but all the queues are empty.");
			QueuedCloudMessages = meter.CreateHistogram<int>("picton.messaging.queued_cloud_messages", null, "The number of messages waiting in the Azure queue over time.");
			QueuedMemoryMessages = meter.CreateHistogram<int>("picton.messaging.queued_memory_messages", null, "The number of messages waiting in the memory queue over time.");
		}

		/// <summary>
		/// Gets the counter indicating the number of messages processed by the message pump.
		/// </summary>
		public Counter<int> MessagesProcessed { get; private set; }

		/// <summary>
		/// Gets the timer indicating how long a message was in queue, waiting to be processed.
		/// </summary>
		public Histogram<long> MessageWaitBeforeProcess { get; private set; }

		/// <summary>
		/// Gets the timer indicating the time it takes to process a message.
		/// </summary>
		public Histogram<long> MessageProcessing { get; private set; }

		/// <summary>
		/// Gets the timer indicating the time it takes to fetch a batch of messages from the Azure queue.
		/// </summary>
		public Histogram<long> MessagesFetching { get; private set; }

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from an Azure queue but it was empty.
		/// </summary>
		public Counter<int> QueueEmpty { get; private set; }

		/// <summary>
		/// Gets the counter indicating the number of times we attempted to fetch messages from Azure but all the queues are empty.
		/// </summary>
		public Counter<int> AllQueuesEmpty { get; private set; }

		/// <summary>
		/// Gets the gauge indicating the number of messages waiting in the Azure queue over time.
		/// </summary>
		public Histogram<int> QueuedCloudMessages { get; private set; }

		/// <summary>
		/// Gets the gauge indicating the number of messages waiting in the memory queue over time.
		/// </summary>
		public Histogram<int> QueuedMemoryMessages { get; private set; }
	}
}
