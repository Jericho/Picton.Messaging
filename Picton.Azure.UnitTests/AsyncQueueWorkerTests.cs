using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Moq;
using Picton.Azure.WorkerRoles;
using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Azure.UnitTests
{
	[TestClass]
	public class AsyncQueueWorkerTests
	{
		[TestMethod]
		public void TestMethod1()
		{
			var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
			var cloudQueueClient = storageAccount.CreateCloudQueueClient();
			cloudQueueClient.DefaultRequestOptions.RetryPolicy = new NoRetry();
			var cloudQueue = cloudQueueClient.GetQueueReference("myqueue");
			cloudQueue.CreateIfNotExists();

			var lockObject = new Object();
			var stopping = false;
			Stopwatch sw = null;

			for (var i = 0; i < 100; i++)
			{
				cloudQueue.AddMessage(new CloudQueueMessage(string.Format("Hello world {0}", i)));
			}


			var mockWorker = new Mock<AsyncQueueWorker>("TestWorker", 1, 25, TimeSpan.FromMilliseconds(500)) { CallBase = true };
			mockWorker.Setup(m => m.GetQueue()).Returns(() =>
			{
				sw = Stopwatch.StartNew();
				return cloudQueue;
			});
			mockWorker.Setup(m => m.OnMessage(It.IsAny<CloudQueueMessage>(), It.IsAny<CancellationToken>())).Callback((CloudQueueMessage msg, CancellationToken cancellationToken) =>
			{
				Debug.WriteLine(msg.AsString);
			});
			mockWorker.Setup(m => m.OnQueueEmpty(It.IsAny<CancellationToken>())).Callback(() =>
			{
				// Stop the worker role when the queue is empty.
				// However, ensure that we try to stop the role only once (otherwise each concurrent task would try to top the role)
				if (!stopping)
				{
					lock (lockObject)
					{
						if (sw.IsRunning) sw.Stop();
						if (!stopping)
						{
							// Indicate that the role is stopping
							stopping = true;

							// Run the 'OnStop' on a different thread so we don't block it
							Task.Run(() =>
							{
								mockWorker.Object.OnStop();
							}).ConfigureAwait(false);
						}
					}
				}
			});


			mockWorker.Object.OnStart();
			mockWorker.Object.Run();

			Debug.Write("Elapsed Milliseconds: " + ToDurationString(sw.Elapsed));
		}

		private static string ToDurationString(TimeSpan timeSpan)
		{
			// In case the TimeSpan is extremely short
			if (timeSpan.TotalMilliseconds <= 1) return "1 millisecond";

			var result = new StringBuilder();

			if (timeSpan.Days == 1) result.Append(" 1 day");
			else if (timeSpan.Days > 1) result.AppendFormat(" {0} days", timeSpan.Days);

			if (timeSpan.Hours == 1) result.Append(" 1 hour");
			else if (timeSpan.Hours > 1) result.AppendFormat(" {0} hours", timeSpan.Hours);

			if (timeSpan.Minutes == 1) result.Append(" 1 minute");
			else if (timeSpan.Minutes > 1) result.AppendFormat(" {0} minutes", timeSpan.Minutes);

			if (timeSpan.Seconds == 1) result.Append(" 1 second");
			else if (timeSpan.Seconds > 1) result.AppendFormat(" {0} seconds", timeSpan.Seconds);

			if (timeSpan.Milliseconds == 1) result.Append(" 1 millisecond");
			else if (timeSpan.Milliseconds > 1) result.AppendFormat(" {0} milliseconds", timeSpan.Milliseconds);

			return result.ToString().Trim();
		}
	}
}
