using System;

namespace Picton.Messaging
{
	public record QueueConfig
	{
		public QueueConfig(string queueName, string poisonQueueName = null, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
		{
			QueueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
			PoisonQueueName = poisonQueueName;
			VisibilityTimeout = visibilityTimeout;
			MaxDequeueCount = maxDequeueCount;
		}

		public string QueueName { get; set; }

		public string PoisonQueueName { get; set; } = null;

		public TimeSpan? VisibilityTimeout { get; set; } = null;

		public int MaxDequeueCount { get; set; } = 3;
	}
}
