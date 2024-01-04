using Azure;
using Azure.Storage.Queues.Models;
using Moq;
using Picton.Managers;
using Shouldly;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

			var cts = new CancellationTokenSource();

			var messagePump = new AsyncMessagePump(queueManager, null, 1, TimeSpan.FromMinutes(1), 3, null, null);

			// Act
			Should.ThrowAsync<ArgumentNullException>(() => messagePump.StartAsync(cts.Token));
		}

		[Fact]
		public async Task No_message_processed_when_queue_is_empty()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

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
				},
				OnQueueEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref onQueueEmptyInvokeCount);
					cts.Cancel();
				}
			};

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(0);
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Once());
		}

		[Fact]
		public async Task Message_processed()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var lockObject = new Object();
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

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
				},
				OnQueueEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref onQueueEmptyInvokeCount);
					cts.Cancel();
				}
			};

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(0);
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
			mockQueueClient.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
		}

		[Fact]
		public async Task Poison_message_is_rejected()
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

			var cts = new CancellationTokenSource();

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
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageId, string popReceipt, CancellationToken cancellationToken) =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
						isRejected = true;
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
				},
				OnQueueEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref onQueueEmptyInvokeCount);
					cts.Cancel();
				}
			};

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(1);
			isRejected.ShouldBeTrue();
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
			mockQueueClient.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
		}

		[Fact]
		public async Task Poison_message_is_moved()
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

			var cts = new CancellationTokenSource();

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
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.Setup(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageId, string popReceipt, CancellationToken cancellationToken) =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
						isRejected = true;
					}
					return new MockAzureResponse(200, "ok");
				});

			mockPoisonQueueClient
				.Setup(q => q.SendMessageAsync(It.IsAny<string>(), It.IsAny<TimeSpan?>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
				.ReturnsAsync((string messageText, TimeSpan? visibilityTimeout, TimeSpan? timeToLive, CancellationToken cancellationToken) =>
				{
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
				},
				OnQueueEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref onQueueEmptyInvokeCount);
					cts.Cancel();
				}
			};

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			onQueueEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(1);
			isRejected.ShouldBeTrue();
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
			mockQueueClient.Verify(q => q.DeleteMessageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
			mockPoisonQueueClient.Verify(q => q.SendMessageAsync(It.IsAny<string>(), It.IsAny<TimeSpan?>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
		}

		[Fact]
		public async Task Exceptions_in_OnQueueEmpty_are_ignored()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var onQueueEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var exceptionSimulated = false;
			var lockObject = new Object();

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

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
				},
				OnQueueEmpty = cancellationToken =>
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
					cts.Cancel();
				}
			};

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			onQueueEmptyInvokeCount.ShouldBe(2); // First time we throw an exception, second time we stop the message pump
			onErrorInvokeCount.ShouldBe(0);
			mockQueueClient.Verify(q => q.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
		}
	}
}
