using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Picton.UnitTests
{
	[TestClass]
	public class AsyncMessagePumpTests
	{
		[TestMethod]
		[ExpectedException(typeof(ArgumentException))]
		public void Negative_min_concurrent_tasks_throws()
		{
			var worker = new AsyncMessagePump(-1);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentException))]
		public void Too_small_max_concurrent_tasks_throws()
		{
			var worker = new AsyncMessagePump(2, 1);
		}
	}
}
