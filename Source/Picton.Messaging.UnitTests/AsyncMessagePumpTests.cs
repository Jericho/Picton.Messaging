using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Moq;
using Picton.Interfaces;
using Shouldly;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Picton.Messaging.UnitTests
{
	public class AsyncMessagePumpTests
	{
		private static readonly string QUEUE_STORAGE_URL = "http://bogus:10001/devstoreaccount1/";
		private static readonly string BLOB_STORAGE_URL = "http://bogus:10002/devstoreaccount1/";

		[Fact]
		public void Null_cloudQueue_throws()
		{
			Should.Throw<ArgumentNullException>(() =>
			{
				var messagePump = new AsyncMessagePump("myqueue", (IStorageAccount)null, 1, 1, TimeSpan.FromMinutes(1), 3);
			});
		}

		[Fact]
		public void Min_too_small_throws()
		{
			Should.Throw<ArgumentException>(() =>
			{
				var mockStorageAccount = new Mock<IStorageAccount>(MockBehavior.Strict);
				var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 0, 1, TimeSpan.FromMinutes(1), 3);
			});
		}

		[Fact]
		public void Max_too_small_throws()
		{
			Should.Throw<ArgumentException>(() =>
			{
				var mockStorageAccount = new Mock<IStorageAccount>(MockBehavior.Strict);
				var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 2, 1, TimeSpan.FromMinutes(1), 3);
			});
		}

		[Fact]
		public void DequeueCount_too_small_throws()
		{
			Should.Throw<ArgumentException>(() =>
			{
				var mockStorageAccount = new Mock<IStorageAccount>(MockBehavior.Strict);
				var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), 0);
			});
		}

		[Fact]
		public void Start_without_OnMessage_throws()
		{
			// Arrange
			var queueName = "myqueue";
			var mockQueue = GetMockQueue(queueName);
			var mockQueueClient = GetMockQueueClient(mockQueue);
			var mockBlobContainer = GetMockBlobContainer();
			var mockBlobClient = GetMockBlobClient(mockBlobContainer);
			var mockStorageAccount = GetMockStorageAccount(mockBlobClient, mockQueueClient);

			var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), 3);

			// Act
			Should.Throw<ArgumentNullException>(() => messagePump.Start());
		}

		[Fact]
		public void Stopping_without_starting()
		{
			// Arrange
			var queueName = "myqueue";
			var mockQueue = GetMockQueue(queueName);
			var mockQueueClient = GetMockQueueClient(mockQueue);
			var mockBlobContainer = GetMockBlobContainer();
			var mockBlobClient = GetMockBlobClient(mockBlobContainer);
			var mockStorageAccount = GetMockStorageAccount(mockBlobClient, mockQueueClient);

			var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), 3);

			// Act
			messagePump.Stop();

			// Nothing to assert.
			// We simply want to make sure that no exception is thrown
		}

		[Fact]
		public void No_message_processed_when_queue_is_empty()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var queueName = "myqueue";
			var mockQueue = GetMockQueue(queueName);
			var mockQueueClient = GetMockQueueClient(mockQueue);
			var mockBlobContainer = GetMockBlobContainer();
			var mockBlobClient = GetMockBlobClient(mockBlobContainer);
			var mockStorageAccount = GetMockStorageAccount(mockBlobClient, mockQueueClient);

			mockQueue.Setup(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>())).Returns<CloudQueueMessage>(null);

			var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Interlocked.Increment(ref onMessageInvokeCount);
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);

				// Run the 'OnStop' on a different thread so we don't block it
				Task.Run(() =>
				{
					messagePump.Stop();
				}).ConfigureAwait(false);
			};
			messagePump.OnError = (message, exception, isPoison) =>
			{
				Interlocked.Increment(ref onErrorInvokeCount);
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			onQueueEmptyInvokeCount.ShouldBeGreaterThan(0);
			onErrorInvokeCount.ShouldBe(0);

			// You would expect the 'GetMessageAsync' method to be invoked only once, but unfortunately we can't be sure.
			// It will be invoked a small number of times (probably once or twice, maybe three times but not more than that).
			// However we can't be more precise because we stop the message pump on another thread and 'GetMessageAsync' may be invoked
			// a few times while we wait for the message pump to stop.
			//
			// What this means is that there is no way to precisely assert the number of times the method has been invoked. The only
			// thing we know for sure, is that 'GetMessageAsync' has been invoked at least once
			mockQueue.Verify(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()), Times.AtLeast(1));
		}

		[Fact]
		public void Message_processed()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var lockObject = new Object();
			var cloudMessage = new CloudQueueMessage("Message");

			var queueName = "myqueue";
			var mockQueue = GetMockQueue(queueName);
			var mockQueueClient = GetMockQueueClient(mockQueue);
			var mockBlobContainer = GetMockBlobContainer();
			var mockBlobClient = GetMockBlobClient(mockBlobContainer);
			var mockStorageAccount = GetMockStorageAccount(mockBlobClient, mockQueueClient);

			mockQueue.Setup(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>())).Returns((TimeSpan? visibilityTimeout, QueueRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken) =>
			{
				if (cloudMessage == null) return null;

				lock (lockObject)
				{
					if (cloudMessage != null)
					{
						// DequeueCount is a private property. Therefore we must use reflection to change its value
						var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
						dequeueCountProperty.SetValue(cloudMessage, cloudMessage.DequeueCount + 1);
					}
					return Task.FromResult(cloudMessage);
				}
			});
			mockQueue.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>())).Returns((string messageId, string popReceipt, QueueRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken) =>
			{
				lock (lockObject)
				{
					cloudMessage = null;
				}
				return Task.FromResult(true);
			});

			var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Interlocked.Increment(ref onMessageInvokeCount);
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);

				// Run the 'OnStop' on a different thread so we don't block it
				Task.Run(() =>
				{
					messagePump.Stop();
				}).ConfigureAwait(false);
			};
			messagePump.OnError = (message, exception, isPoison) =>
			{
				Interlocked.Increment(ref onErrorInvokeCount);
				if (isPoison)
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
				}
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBeGreaterThan(0);
			onErrorInvokeCount.ShouldBe(0);
			mockQueue.Verify(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()), Times.AtLeast(1));
			mockQueue.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
		}

		[Fact]
		public void Poison_message_is_rejected()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var isRejected = false;
			var retries = 3;
			var lockObject = new Object();
			var cloudMessage = new CloudQueueMessage("Message");

			var queueName = "myqueue";
			var mockQueue = GetMockQueue(queueName);
			var mockQueueClient = GetMockQueueClient(mockQueue);
			var mockBlobContainer = GetMockBlobContainer();
			var mockBlobClient = GetMockBlobClient(mockBlobContainer);
			var mockStorageAccount = GetMockStorageAccount(mockBlobClient, mockQueueClient);

			mockQueue.Setup(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>())).Returns((TimeSpan? visibilityTimeout, QueueRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken) =>
			{
				if (cloudMessage == null) return null;

				lock (lockObject)
				{
					if (cloudMessage != null)
					{
						// DequeueCount is a private property. Therefore we must use reflection to change its value
						var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
						dequeueCountProperty.SetValue(cloudMessage, cloudMessage.DequeueCount + 1);
					}
					return Task.FromResult(cloudMessage);
				}
			});
			mockQueue.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>())).Returns((string messageId, string popReceipt, QueueRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken) =>
			{
				lock (lockObject)
				{
					cloudMessage = null;
				}
				return Task.FromResult(true);
			});

			var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), retries);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Interlocked.Increment(ref onMessageInvokeCount);
				throw new Exception("An error occured when attempting to process the message");
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);

				// Run the 'OnStop' on a different thread so we don't block it
				Task.Run(() =>
				{
					messagePump.Stop();
				}).ConfigureAwait(false);
			};
			messagePump.OnError = (message, exception, isPoison) =>
			{
				Interlocked.Increment(ref onErrorInvokeCount);
				if (isPoison)
				{
					lock (lockObject)
					{
						isRejected = true;
						cloudMessage = null;
					}
				}
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(retries + 1);
			onQueueEmptyInvokeCount.ShouldBeGreaterThan(0);
			onErrorInvokeCount.ShouldBe(retries + 1);
			isRejected.ShouldBeTrue();
			mockQueue.Verify(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()), Times.AtLeast(retries));
		}

		[Fact]
		public void Exceptions_in_OnQueueEmpty_are_ignored()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var exceptionSimulated = false;
			var lockObject = new Object();

			var queueName = "myqueue";
			var mockQueue = GetMockQueue(queueName);
			var mockQueueClient = GetMockQueueClient(mockQueue);
			var mockBlobContainer = GetMockBlobContainer();
			var mockBlobClient = GetMockBlobClient(mockBlobContainer);
			var mockStorageAccount = GetMockStorageAccount(mockBlobClient, mockQueueClient);

			mockQueue.Setup(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>())).Returns<CloudQueueMessage>(null);

			var messagePump = new AsyncMessagePump("myqueue", mockStorageAccount.Object, 1, 1, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Interlocked.Increment(ref onMessageInvokeCount);
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);

				// Simulate an exception (only the first time)
				lock (lockObject)
				{
					if (!exceptionSimulated)
					{
						exceptionSimulated = true;
						throw new Exception("This dummy exception should be ignored");
					}
				}

				// Run the 'OnStop' on a different thread so we don't block it
				Task.Run(() =>
				{
					messagePump.Stop();
				}).ConfigureAwait(false);
			};
			messagePump.OnError = (message, exception, isPoison) =>
			{
				Interlocked.Increment(ref onErrorInvokeCount);
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			onQueueEmptyInvokeCount.ShouldBeGreaterThan(0);
			onErrorInvokeCount.ShouldBe(0);
			mockQueue.Verify(q => q.GetMessageAsync(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()), Times.AtLeast(1));
		}

		private static Mock<CloudBlobContainer> GetMockBlobContainer(string containerName = "mycontainer")
		{
			var mockContainerUri = new Uri(BLOB_STORAGE_URL + containerName);
			var mockBlobContainer = new Mock<CloudBlobContainer>(MockBehavior.Strict, mockContainerUri);
			mockBlobContainer
				.Setup(c => c.CreateIfNotExistsAsync(It.IsAny<BlobContainerPublicAccessType>(), It.IsAny<BlobRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(true)
				.Verifiable();
			return mockBlobContainer;
		}

		private static Mock<CloudBlobClient> GetMockBlobClient(Mock<CloudBlobContainer> mockBlobContainer)
		{
			var mockBlobStorageUri = new Uri(BLOB_STORAGE_URL);
			var mockBlobClient = new Mock<CloudBlobClient>(MockBehavior.Strict, mockBlobStorageUri);
			mockBlobClient
				.Setup(c => c.GetContainerReference(It.IsAny<string>()))
				.Returns(mockBlobContainer.Object)
				.Verifiable();
			return mockBlobClient;
		}

		private static Mock<CloudQueue> GetMockQueue(string queueName)
		{
			var queueAddres = new Uri(QUEUE_STORAGE_URL + queueName);
			var mockQueue = new Mock<CloudQueue>(MockBehavior.Strict, queueAddres);
			mockQueue
				.Setup(c => c.CreateIfNotExistsAsync(It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(false)
				.Verifiable();
			return mockQueue;
		}

		private static Mock<CloudQueueClient> GetMockQueueClient(Mock<CloudQueue> mockQueue)
		{
			var mockQueueStorageUri = new Uri(QUEUE_STORAGE_URL);
			var mockCredentials = new StorageCredentials("mySasToken");
			var mockQueueClient = new Mock<CloudQueueClient>(MockBehavior.Strict, mockQueueStorageUri, mockCredentials);
			mockQueueClient
				.Setup(c => c.GetQueueReference(mockQueue.Object.Name))
				.Returns(mockQueue.Object)
				.Verifiable();
			return mockQueueClient;
		}

		private static Mock<IStorageAccount> GetMockStorageAccount(Mock<CloudBlobClient> mockBlobClient, Mock<CloudQueueClient> mockQueueClient)
		{
			var storageAccount = new Mock<IStorageAccount>(MockBehavior.Strict);
			storageAccount
				.Setup(s => s.CreateCloudBlobClient())
				.Returns(mockBlobClient.Object)
				.Verifiable();
			storageAccount
				.Setup(s => s.CreateCloudQueueClient())
				.Returns(mockQueueClient.Object)
				.Verifiable();
			return storageAccount;
		}
	}
}
