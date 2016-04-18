using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Moq;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.UnitTests
{
	[TestClass]
	public class AsyncMessagePumpTests
	{
		[TestMethod]
		public void No_message_processed_when_queue_is_empty()
		{
			// Arrange
			var messagesProcessed = 0;

			var mockStorageUri = new Uri("http://bogus/myaccount");
			var mockQueue = new Mock<CloudQueue>(MockBehavior.Strict, mockStorageUri);
			mockQueue.Setup(q => q.GetMessage(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>())).Returns<CloudQueueMessage>(null);

			var messagePump = new AsyncMessagePump(mockQueue.Object, 1, 1, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Interlocked.Increment(ref messagesProcessed);
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Run the 'OnStop' on a different thread so we don't block it
				Task.Run(() =>
				{
					messagePump.Stop();
				}).ConfigureAwait(false);
			};

			// Act
			messagePump.Start();

			// Assert
			Assert.AreEqual(0, messagesProcessed);

			// You would expect the 'GetMessage' method to be invoked only once, but unfortunately we can't be sure.
			// It will be invoked a small number of times (probably once or twice, maybe three times but not more than that).
			// However we can't be more precise because we stop the message pump on another thread and 'GetMessage' may be invoked
			// a few times while we wait for the message pump to stop.
			//
			// What this means is that there is no way to precisely assert the number of times the method will invoked. The only
			// thing we know for sure, is that 'GetMessage' will be invoked at least once
			mockQueue.Verify(q => q.GetMessage(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>()), Times.AtLeast(1));
		}

		[TestMethod]
		public void Message_processed()
		{
			// Arrange
			var messagesProcessed = 0;
			var lockObject = new Object();
			var cloudMessage = new CloudQueueMessage("Message");

			var mockStorageUri = new Uri("http://bogus/myaccount");
			var mockQueue = new Mock<CloudQueue>(MockBehavior.Strict, mockStorageUri);
			mockQueue.Setup(q => q.GetMessage(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>())).Returns((TimeSpan? visibilityTimeout, QueueRequestOptions options, OperationContext operationContext) =>
			{
				lock (lockObject)
				{
					if (cloudMessage != null)
					{
						var t = cloudMessage.GetType();
						t.InvokeMember("DequeueCount", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.SetProperty | BindingFlags.Instance, null, cloudMessage, new object[] { cloudMessage.DequeueCount + 1 });
					}

					return cloudMessage;
				}
			});
			mockQueue.Setup(q => q.DeleteMessage(It.IsAny<CloudQueueMessage>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>())).Callback(() => cloudMessage = null);

			var messagePump = new AsyncMessagePump(mockQueue.Object, 1, 1, TimeSpan.FromMinutes(1), 3);
			messagePump.OnMessage = (message, cancellationToken) =>
			{
				Interlocked.Increment(ref messagesProcessed);
			};
			messagePump.OnError = (message, exception, isPoison) =>
			{
				if (isPoison)
				{
					lock (lockObject)
					{
						cloudMessage = null;
					}
				}
			};
			messagePump.OnQueueEmpty = cancellationToken =>
			{
				// Run the 'OnStop' on a different thread so we don't block it
				Task.Run(() =>
				{
					messagePump.Stop();
				}).ConfigureAwait(false);
			};

			// Act
			messagePump.Start();

			// Assert
			Assert.AreEqual(1, messagesProcessed);
			mockQueue.Verify(q => q.GetMessage(It.IsAny<TimeSpan?>(), It.IsAny<QueueRequestOptions>(), It.IsAny<OperationContext>()), Times.AtLeast(1));
		}
	}
}
