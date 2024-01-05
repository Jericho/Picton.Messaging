using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using System;

namespace Picton.Messaging
{
	public record MessagePumpOptions
	{
		public MessagePumpOptions(string connectionString, int concurrentTasks, QueueClientOptions queueClientOptions = null, BlobClientOptions blobClientOptions = null)
		{
			ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
			ConcurrentTasks = concurrentTasks;
			QueueClientOptions = queueClientOptions;
			BlobClientOptions = blobClientOptions;
		}

		/// <summary>
		/// Gets or sets the connection string which includes the authentication information required for your application to access data in an Azure Storage account at runtime.
		/// For more information, https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string.
		/// </summary>
		public string ConnectionString { get; set; }

		/// <summary>
		/// Gets or sets the number of concurrent tasks. In other words: the number of messages that can be processed at a time.
		/// </summary>
		public int ConcurrentTasks { get; set; } = 25;

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
	}
}
