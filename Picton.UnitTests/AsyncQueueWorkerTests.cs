using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Picton.WorkerRoles;

namespace Picton.UnitTests
{
	[TestClass]
	public class AsyncQueueWorkerTests
	{
		[TestMethod]
		[ExpectedException(typeof(ArgumentException))]
		public void Negative_min_concurrent_tasks_throws()
		{
			var worker = new AsyncQueueWorker("unittestworker", -1);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentException))]
		public void Too_small_max_concurrent_tasks_throws()
		{
			var worker = new AsyncQueueWorker("unittestworker", 2, 1);
		}

		[TestMethod]
		public void Name()
		{
			var worker = new AsyncQueueWorker("unittestworker");

			Assert.AreEqual("unittestworker", worker.WorkerName);
		}
	}
}
