using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Picton.Messaging.UnitTests
{
	internal static class MockUtils
	{
		private static readonly string QUEUE_STORAGE_URL = "http://bogus:10001/devstoreaccount1/";
		private static readonly string BLOB_STORAGE_URL = "http://bogus:10002/devstoreaccount1/";

		internal static Mock<BlobContainerClient> GetMockBlobContainerClient(string containerName = "mycontainer", IEnumerable<Mock<BlobClient>> mockBlobClients = null)
		{
			var mockContainerUri = new Uri(BLOB_STORAGE_URL + containerName);
			var blobContainerInfo = BlobsModelFactory.BlobContainerInfo(ETag.All, DateTimeOffset.UtcNow);
			var mockBlobContainer = new Mock<BlobContainerClient>(MockBehavior.Strict);

			mockBlobContainer
				.SetupGet(m => m.Name)
				.Returns(containerName);

			mockBlobContainer
				.SetupGet(m => m.Uri)
				.Returns(mockContainerUri);

			mockBlobContainer
				.Setup(c => c.CreateIfNotExists(It.IsAny<PublicAccessType>(), It.IsAny<IDictionary<string, string>>(), It.IsAny<BlobContainerEncryptionScopeOptions>(), It.IsAny<CancellationToken>()))
				.Returns(Response.FromValue(blobContainerInfo, new MockAzureResponse(200, "ok")))
				.Verifiable();

			foreach (var blobClient in mockBlobClients?.Select(m => m.Object) ?? Enumerable.Empty<BlobClient>())
			{
				mockBlobContainer
					.Setup(c => c.GetBlobClient(blobClient.Name))
					.Returns(blobClient)
					.Verifiable();
			}

			return mockBlobContainer;
		}

		internal static Mock<BlobClient> GetMockBlobClient(string blobName)
		{
			var mockBlobUri = new Uri(BLOB_STORAGE_URL + blobName);
			var mockBlobClient = new Mock<BlobClient>(MockBehavior.Strict);

			mockBlobClient
				.SetupGet(m => m.Name)
				.Returns(blobName);

			mockBlobClient
				.SetupGet(m => m.Uri)
				.Returns(mockBlobUri);

			return mockBlobClient;
		}

		internal static Mock<QueueClient> GetMockQueueClient(string queueName = "myqueue")
		{
			var mockQueueStorageUri = new Uri(QUEUE_STORAGE_URL + queueName);
			var mockQueueClient = new Mock<QueueClient>(MockBehavior.Strict);

			mockQueueClient
				.SetupGet(m => m.MaxPeekableMessages)
				.Returns(32);

			mockQueueClient
				.SetupGet(m => m.MessageMaxBytes)
				.Returns(65536);

			mockQueueClient
				.SetupGet(m => m.Uri)
				.Returns(mockQueueStorageUri);

			mockQueueClient
				.Setup(c => c.CreateIfNotExists(It.IsAny<IDictionary<string, string>>(), It.IsAny<CancellationToken>()))
				.Returns((Response)null)
				.Verifiable();

			return mockQueueClient;
		}
	}
}
