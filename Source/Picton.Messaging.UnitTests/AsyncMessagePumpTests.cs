using Azure;
using Azure.Storage.Queues.Models;
using NSubstitute;
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
		public void Throws_when_options_is_null()
		{
			// Arrange
			var options = (MessagePumpOptions)null;

			//Act
			Should.Throw<ArgumentNullException>(() => new AsyncMessagePump(options));
		}

		[Fact]
		public void Throws_when_zero_queues()
		{
			// Arrange
			var cts = new CancellationTokenSource();

			var options = new MessagePumpOptions("bogus connection string", 1);
			var messagePump = new AsyncMessagePump(options);

			// Act
			Should.ThrowAsync<ArgumentNullException>(() => messagePump.StartAsync(cts.Token));

		}

		[Fact]
		public void Throws_when_number_of_concurrent_tasks_too_small()
		{
			// Arrange
			var options = new MessagePumpOptions("bogus connection string", 0);

			//Act
			Should.Throw<ArgumentException>(() => new AsyncMessagePump(options));
		}

		[Fact]
		public void Throws_when_DequeueCount_too_small()
		{
			// Arrange
			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();
			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient, false);
			var options = new MessagePumpOptions("bogus connection string", 1);
			var messagePump = new AsyncMessagePump(options);

			// Act
			Should.Throw<ArgumentOutOfRangeException>(() => messagePump.AddQueue(queueManager, null, null, 0));
		}

		[Fact]
		public void Throws_when_OnMessage_not_set()
		{
			// Arrange
			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();
			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient, true);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var cts = new CancellationTokenSource();

			var messagePump = new AsyncMessagePump(options);
			messagePump.AddQueue(queueManager, null, null, 3);

			// Act
			Should.ThrowAsync<ArgumentNullException>(() => messagePump.StartAsync(cts.Token));
		}

		[Fact]
		public async Task No_message_processed_when_queue_is_empty()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var OnEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

			mockQueueClient
				.GetPropertiesAsync(Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var queueProperties = QueuesModelFactory.QueueProperties(null, 0);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.ReceiveMessagesAsync(Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo => Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok")));

			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var messagePump = new AsyncMessagePump(options)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
				},
				OnError = (queueName, message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
				},
				OnAllQueuesEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref OnEmptyInvokeCount);
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueManager, null, null, 3);

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			OnEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(0);
		}

		[Fact]
		public async Task Message_processed()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var OnEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var lockObject = new Object();
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

			mockQueueClient
				.GetPropertiesAsync(Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var messageCount = cloudMessage == null ? 0 : 1;
					var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.ReceiveMessagesAsync(Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var maxMessages = callInfo.ArgAt<int?>(0);
					var visibilityTimeout = callInfo.ArgAt<TimeSpan?>(1);
					var cancellationToken = callInfo.ArgAt<CancellationToken>(2);

					if (cloudMessage != null)
					{
						lock (lockObject)
						{
							if (cloudMessage != null)
							{
								// DequeueCount is a read-only property but we can use reflection to change its value
								var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
								dequeueCountProperty.SetValue(cloudMessage, cloudMessage.DequeueCount + 1);

								return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
							}
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.DeleteMessageAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
					return new MockAzureResponse(200, "ok");
				});

			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var messagePump = new AsyncMessagePump(options)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
				},
				OnError = (queueName, message, exception, isPoison) =>
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
				OnAllQueuesEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref OnEmptyInvokeCount);
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueManager, null, null, 3);

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			OnEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(0);
		}

		[Fact]
		public async Task Poison_message_is_rejected()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var OnEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var isRejected = false;
			var retries = 3;

			var lockObject = new Object();
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

			mockQueueClient
				.GetPropertiesAsync(Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				 {
					 var messageCount = cloudMessage == null ? 0 : 1;
					 var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					 return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				 });
			mockQueueClient
				.ReceiveMessagesAsync(Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						if (cloudMessage != null)
						{
							// DequeueCount is a read-only property but we can use reflection to change its value
							var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
							dequeueCountProperty.SetValue(cloudMessage, retries + 1);   // intentionally set 'DequeueCount' to a value exceeding maxRetries to simulate a poison message

							return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.DeleteMessageAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
						isRejected = true;
					}
					return new MockAzureResponse(200, "ok");
				});

			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var messagePump = new AsyncMessagePump(options)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
					throw new Exception("An error occured when attempting to process the message");
				},
				OnError = (queueName, message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
				},
				OnAllQueuesEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref OnEmptyInvokeCount);
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueManager, null, null, retries);

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			OnEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(1);
			isRejected.ShouldBeTrue();
		}

		[Fact]
		public async Task Poison_message_is_moved()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var OnEmptyInvokeCount = 0;
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
				.GetPropertiesAsync(Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var messageCount = cloudMessage == null ? 0 : 1;
					var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.ReceiveMessagesAsync(Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						if (cloudMessage != null)
						{
							// DequeueCount is a read-only property but we can use reflection to change its value
							var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
							dequeueCountProperty.SetValue(cloudMessage, retries + 1);   // intentionally set 'DequeueCount' to a value exceeding maxRetries to simulate a poison message

							return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.DeleteMessageAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
						isRejected = true;
					}
					return new MockAzureResponse(200, "ok");
				});

			mockPoisonQueueClient
				.SendMessageAsync(Arg.Any<string>(), Arg.Any<TimeSpan?>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var sendReceipt = QueuesModelFactory.SendReceipt("abc123", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(7), "xyz", DateTimeOffset.UtcNow);
					return Response.FromValue(sendReceipt, new MockAzureResponse(200, "ok"));
				});

			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient);
			var poisonQueueManager = new QueueManager(mockBlobContainerClient, mockPoisonQueueClient);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var messagePump = new AsyncMessagePump(options)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
					throw new Exception("An error occured when attempting to process the message");
				},
				OnError = (queueName, message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
				},
				OnAllQueuesEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref OnEmptyInvokeCount);
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueManager, poisonQueueManager, null, retries);

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(1);
			OnEmptyInvokeCount.ShouldBe(1);
			onErrorInvokeCount.ShouldBe(1);
			isRejected.ShouldBeTrue();
		}

		[Fact]
		public async Task Exceptions_in_OnEmpty_are_ignored()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var OnEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var exceptionSimulated = false;
			var lockObject = new Object();

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

			mockQueueClient
				.GetPropertiesAsync(Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var queueProperties = QueuesModelFactory.QueueProperties(null, 0);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.ReceiveMessagesAsync(Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo => Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok")));

			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var messagePump = new AsyncMessagePump(options)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
				},
				OnError = (queueName, message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
				},
				OnAllQueuesEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref OnEmptyInvokeCount);

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
			messagePump.AddQueue(queueManager, null, null, 3);

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(0);
			OnEmptyInvokeCount.ShouldBe(2); // First time we throw an exception, second time we stop the message pump
			onErrorInvokeCount.ShouldBe(0);
		}

		[Fact]
		public async Task Exceptions_in_OnError_are_ignored()
		{
			// Arrange
			var onMessageInvokeCount = 0;
			var OnEmptyInvokeCount = 0;
			var onErrorInvokeCount = 0;

			var lockObject = new Object();
			var cloudMessage = QueuesModelFactory.QueueMessage("abc123", "xyz", "Hello World!", 0, null, DateTimeOffset.UtcNow, null);

			var mockBlobContainerClient = MockUtils.GetMockBlobContainerClient();
			var mockQueueClient = MockUtils.GetMockQueueClient();

			var cts = new CancellationTokenSource();

			mockQueueClient
				.GetPropertiesAsync(Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					var messageCount = cloudMessage == null ? 0 : 1;
					var queueProperties = QueuesModelFactory.QueueProperties(null, messageCount);
					return Response.FromValue(queueProperties, new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.ReceiveMessagesAsync(Arg.Any<int>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						if (cloudMessage != null)
						{
							// DequeueCount is a read-only property but we can use reflection to change its value
							var dequeueCountProperty = cloudMessage.GetType().GetProperty("DequeueCount");
							dequeueCountProperty.SetValue(cloudMessage, cloudMessage.DequeueCount + 1);

							return Response.FromValue(new[] { cloudMessage }, new MockAzureResponse(200, "ok"));
						}
					}
					return Response.FromValue(Enumerable.Empty<QueueMessage>().ToArray(), new MockAzureResponse(200, "ok"));
				});
			mockQueueClient
				.DeleteMessageAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
				.Returns(callInfo =>
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
					return new MockAzureResponse(200, "ok");
				});

			var queueManager = new QueueManager(mockBlobContainerClient, mockQueueClient);
			var options = new MessagePumpOptions("bogus connection string", 1);

			var messagePump = new AsyncMessagePump(options)
			{
				OnMessage = (queueName, message, cancellationToken) =>
				{
					Interlocked.Increment(ref onMessageInvokeCount);
					throw new Exception("Simulate a problem while processing the message in order to unit test the error handler");
				},
				OnError = (queueName, message, exception, isPoison) =>
				{
					Interlocked.Increment(ref onErrorInvokeCount);
					throw new Exception("This dummy exception should be ignored");
				},
				OnAllQueuesEmpty = cancellationToken =>
				{
					Interlocked.Increment(ref OnEmptyInvokeCount);
					cts.Cancel();
				}
			};
			messagePump.AddQueue(queueManager, null, null, 3);

			// Act
			await messagePump.StartAsync(cts.Token);

			// Assert
			onMessageInvokeCount.ShouldBe(3); // <-- message is retried three times
			onErrorInvokeCount.ShouldBe(3); // <-- we throw a dummy exception every time the mesage is processed, until we give up and move the message to the poison queue
			OnEmptyInvokeCount.ShouldBe(1);
		}
	}
}
