using Azure;
using Azure.Storage.Queues.Models;
using Moq;
using Picton.Managers;
using Shouldly;
using System;
using System.Linq;
using System.Threading;
using Xunit;

namespace Picton.Messaging.UnitTests
{
	public class AsyncMessagePumpTests
	{
		[Fact]
		public void Null_cloudQueue_throws()
		{
			Should.Throw<ArgumentNullException>(() =>
			{
				var messagePump = new AsyncMessagePump((QueueManager)null, null, 1, TimeSpan.FromMinutes(1), 1, null, null);
			});
		}

		[Fact]
		public void Number_of_concurrent_tasks_too_small_throws()
		{
			Should.Throw<ArgumentException>(() =>
			{
				var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
				var mockQueueClient = MockUtils.GetMockQueueClient();
				var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object, false);

				var messagePump = new AsyncMessagePump(queueManager, null, 0, TimeSpan.FromMinutes(1), 1, null, null);
			});
		}

		[Fact]
		public void DequeueCount_too_small_throws()
		{
			Should.Throw<ArgumentException>(() =>
			{
				var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
				var mockQueueClient = MockUtils.GetMockQueueClient();
				var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object, false);

				var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 0, null, null);
			});
		}

		[Fact]
		public void Start_without_OnMessage_throws()
		{
			// Arrange
			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();
			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object, true);

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null, null);

			// Act
			Should.Throw<ArgumentNullException>(() => messagePump.Start());
		}

		[Fact]
		public void Stopping_without_starting()
		{
			// Arrange
			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();
			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object, true);

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null, null);

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

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			mockQueueClient
				.Setup(q => q.GetPropertiesAsync(It.IsAny<CancellationToken>()))
				.ReturnsAsync((CancellationToken cancellationToken) =>
				{
					var queueProperties = QueuesModelFactory.QueueProperties(null, 0);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok")))
				.Verifiable();

			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object);

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null)
			{
				OnMessage = (message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
				},
				OnError = (message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
				}
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);
				messagePump.Stop();
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(0);

			// You would expect the 'GetMessagesAsync' method to be invoked only once, but unfortunately we can't be sure.
			// It will be invoked a small number of times (probably once or twice, maybe three times but not more than that).
			// However we can't be more precise because we stop the message pump on another thread and 'GetMessagesAsync' may
			// be invoked a few times while we wait for the message pump to stop.
			//
			// What this means is that there is no way to precisely assert the number of times the method has been invoked.
			// The only thing we know for sure, is that 'GetMessagesAsync' has been invoked at least once
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.AtLeast(1));
		}

		[Fact]
		public void Message_processed()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var lockObject = new Object();
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			mockQueueClient
				.Setup(q => q.GetPropertiesAsync(It.IsAny<CancellationToken>()))
				.ReturnsAsync((CancellationToken cancellationToken) =>
				{
					var messageCount = cloudMessage == null ? 0 : 1;
					var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((int? maxMessages, TimeSpan? visibilityTimeout, CancellationToken cancellationToken) =>
				{
					if (cloudMessage != null)
					{
						lock (lockObject)
						{
							if (cloudMessage != null)
							{
								// DequeueCount is a private property. Therefore we must use reflection to change its value
								var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
								dequeueCountProperty.SetValue(cloudMessage, cloudMessage.DequeueCount + 1);

								return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
							}
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageId, string popReceipt, CancellationToken cancellationToken) =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
					return new MockAzureResponse(200, "ok");
				});

			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object);

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null)
			{
				OnMessage = (message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
				},
				OnError = (message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
					if (isPoison)
					{
						lock (lockObject)
						{
							cloudMessage = null;
						}
					}
				}
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);
				messagePump.Stop();
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(0);
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.AtLeast(2));
			mockQueueClient.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
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
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			mockQueueClient
				.Setup(q => q.GetPropertiesAsync(It.IsAny<CancellationToken>()))
				.ReturnsAsync((CancellationToken cancellationToken) =>
				{
					var messageCount = cloudMessage == null ? 0 : 1;
					var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((int? maxMessages, TimeSpan? visibilityTimeout, CancellationToken cancellationToken) =>
				{
					if (cloudMessage != null)
					{
						lock (lockObject)
						{
							if (cloudMessage != null)
							{
								// DequeueCount is a private property. Therefore we must use reflection to change its value
								var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
								dequeueCountProperty.SetValue(cloudMessage, retries + 1);   // intentionally set 'DequeueCount' to a value exceeding maxRetries to simulate a poison message

								return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
							}
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageId, string popReceipt, CancellationToken cancellationToken) =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
					return new MockAzureResponse(200, "ok");
				});

			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object);

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null)
			{
				OnMessage = (message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
					throw new Exception("An error occured when attempting to process the message");
				},
				OnError = (message, exception, isPoison) =>
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
				}
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);
				messagePump.Stop();
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(1);
			isRejected.ShouldBeTrue();
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.AtLeast(2));
			mockQueueClient.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
		}

		[Fact]
		public void Poison_message_is_moved()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var isRejected = false;
			var retries = 3;

			var lockObject = new Object();
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();
			var mockPoisonQueueClient = MockUtils.GetMockQueueClient("my_poison_queue");

			mockQueueClient
				.Setup(q => q.GetPropertiesAsync(It.IsAny<CancellationToken>()))
				.ReturnsAsync((CancellationToken cancellationToken) =>
				{
					var messageCount = cloudMessage == null ? 0 : 1;
					var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((int? maxMessages, TimeSpan? visibilityTimeout, CancellationToken cancellationToken) =>
				{
					if (cloudMessage != null)
					{
						lock (lockObject)
						{
							if (cloudMessage != null)
							{
								// DequeueCount is a private property. Therefore we must use reflection to change its value
								var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
								dequeueCountProperty.SetValue(cloudMessage, retries + 1);   // intentionally set 'DequeueCount' to a value exceeding maxRetries to simulate a poison message

								return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
							}
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageId, string popReceipt, CancellationToken cancellationToken) =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
					return new MockAzureResponse(200, "ok");
				});

			mockPoisonQueueClient
				.Setup(q => q.SendMessageAsync(It.IsAny<string>(), It.IsAny<TimeSpan?>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageText, TimeSpan? visibilityTimeout, TimeSpan? timeToLive, CancellationToken cancellationToken) =>
				{
					// Nothing to do. We just want to ensure this method is invoked.
					var sendReceipt = QueuesModelFactory.SendReceipt("abc123", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(7), "xyz", DateTimeOffset.UtcNow);
					return Response.FromValue(sendReceipt, new MockAzureResponse(200, "ok"));
				});

			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object);
			var poisonQueueManager = new QueueManager(mockBlobContainerClient.Object, mockPoisonQueueClient.Object);

			var messagePump = new AsyncMessagePump(queueManager, poisonQueueManager, 1, TimeSpan.FromMinutes(1), 3, null)
			{
				OnMessage = (message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
					throw new Exception("An error occured when attempting to process the message");
				},
				OnError = (message, exception, isPoison) =>
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
				}
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				Interlocked.Increment(ref onQueueEmptyInvokeCount);
				messagePump.Stop();
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(1);
			isRejected.ShouldBeTrue();
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.AtLeast(2));
			mockQueueClient.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
			mockPoisonQueueClient.Verify(q => q.SendMessageAsync(It.IsAny<string>(), It.IsAny<TimeSpan?>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
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

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			mockQueueClient
				.Setup(q => q.GetPropertiesAsync(It.IsAny<CancellationToken>()))
				.ReturnsAsync((CancellationToken cancellationToken) =>
				{
					var queueProperties = QueuesModelFactory.QueueProperties(null, 0);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync(Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok")));

			var queueManager = new QueueManager(mockBlobContainerClient.Object, mockQueueClient.Object);

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null, null)
			{
				OnMessage = (message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
				},
				OnError = (message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
				}
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

				// Stop the message pump
				messagePump.Stop();
			};

			// Act
			messagePump.Start();

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			onQueueEmptyInvokeCount.ShouldBeGreaterThan(0);
			onErrorInvokeCount.ShouldBe(0);
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.AtLeast(1));
		}
	}
}
