﻿using System;
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
	}
}
