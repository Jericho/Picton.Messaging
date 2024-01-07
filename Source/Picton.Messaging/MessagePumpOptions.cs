using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using System;

namespace Picton.Messaging
{
	/// <summary>
	/// Configuration options for a message pump.
	/// </summary>
	public record MessagePumpOptions
	{
		private const int _defaultConcurrentTasks = 25;
		private static readonly TimeSpan _defaultFetchMessagesInterval = TimeSpan.FromSeconds(1);
		private static readonly TimeSpan _defaultCountAzureMessagesInterval = TimeSpan.FromSeconds(5);
		private static readonly TimeSpan _defaultCountMemoryMessagesInterval = TimeSpan.FromSeconds(5);

		/// <summary>
		/// Initializes a new instance of the <see cref="MessagePumpOptions"/> class.
		/// </summary>
		public MessagePumpOptions()
		{ }

		/// <summary>
		/// Initializes a new instance of the <see cref="MessagePumpOptions"/> class.
		/// </summary>
		/// <param name="connectionString">The connection string.</param>
		/// <param name="concurrentTasks">The number of concurrent tasks. In other words: the number of messages that can be processed at a time.</param>
		/// <param name="queueClientOptions">The client options that define the transport pipeline policies for authentication, retries, etc., that are applied to every request to the queue.</param>
		/// <param name="blobClientOptions">The client options that define the transport pipeline policies for authentication, retries, etc., that are applied to every request to the blob storage.</param>
		/// <param name="fetchMessagesInterval">The frequency at which messages are fetched from the Azure queues. The default value is 1 second.</param>
		/// <param name="countAzureMessagesInterval">The frequency at which we count how many messages are queue in Azure, waiting to be fetched. Default is 5 seconds.</param>
		/// <param name="countMemoryMessagesInterval">the frequency at which we count how many messages have been fetched from Azure and are queued in memory, waiting to be processed. Default is 5 seconds.</param>
		public MessagePumpOptions(string connectionString, int? concurrentTasks, QueueClientOptions queueClientOptions = null, BlobClientOptions blobClientOptions = null, TimeSpan? fetchMessagesInterval = null, TimeSpan? countAzureMessagesInterval = null, TimeSpan? countMemoryMessagesInterval = null)
		{
			ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
			ConcurrentTasks = concurrentTasks ?? _defaultConcurrentTasks;
			QueueClientOptions = queueClientOptions;
			BlobClientOptions = blobClientOptions;
			FetchMessagesInterval = fetchMessagesInterval ?? _defaultFetchMessagesInterval;
			CountAzureMessagesInterval = countAzureMessagesInterval ?? _defaultCountAzureMessagesInterval;
			CountMemoryMessagesInterval = countMemoryMessagesInterval ?? _defaultCountMemoryMessagesInterval;
		}

		/// <summary>
		/// Gets or sets the connection string which includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </summary>
		public string ConnectionString { get; set; }

		/// <summary>
		/// Gets or sets the number of concurrent tasks. In other words: the number of messages that can be processed at a time.
		/// </summary>
		public int ConcurrentTasks { get; set; } = _defaultConcurrentTasks;

		/// <summary>
		/// Gets or sets the optional client options that define the transport
		/// pipeline policies for authentication, retries, etc., that are applied
		/// to every request to the queue.
		/// </summary>
		public QueueClientOptions QueueClientOptions { get; set; } = null;

		/// <summary>
		/// Gets or sets the optional client options that define the transport
		/// pipeline policies for authentication, retries, etc., that are applied
		/// to every request to the blob storage.
		/// </summary>
		public BlobClientOptions BlobClientOptions { get; set; } = null;

		/// <summary>
		/// Gets or sets the frequency at which messages are fetched from the Azure queues. The default value is 1 second.
		/// </summary>
		public TimeSpan FetchMessagesInterval { get; set; } = _defaultFetchMessagesInterval;

		/// <summary>
		/// Gets or sets the frequency at which we count how many messages are queue in Azure, waiting to be processed.
		/// The count is subsequently published to the metric system you have configured.
		///
		/// You can turn off this behavior by setting this interval to `TimeSpan.Zero`.
		///
		/// Default value is 5 seconds.
		/// </summary>
		/// <remarks>This setting is ignored if you don't specify the sytem where metrics are published.</remarks>
		public TimeSpan CountAzureMessagesInterval { get; set; } = _defaultCountAzureMessagesInterval;

		/// <summary>
		/// Gets or sets the frequency at which we count how many messages have been fetched from Azure and are queued in memory, waiting to be fetched.
		/// The count is subsequently published to the metric system you have configured.
		///
		/// You can turn off this behavior by setting this interval to `TimeSpan.Zero`.
		///
		/// Default value is 5 seconds.
		/// </summary>
		/// <remarks>This setting is ignored if you don't specify the sytem where metrics are published.</remarks>
		public TimeSpan CountMemoryMessagesInterval { get; set; } = _defaultCountMemoryMessagesInterval;
	}
}
