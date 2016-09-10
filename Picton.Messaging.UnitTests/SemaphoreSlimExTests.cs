using Microsoft.VisualStudio.TestTools.UnitTesting;
using Picton.Messaging.Utils;

namespace Picton.Messaging.UnitTests
{
	[TestClass]
	public class SemaphoreSlimExTests
	{
		[TestMethod]
		public void Increase_allowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 2);

			// Act
			var increased = semaphore.TryIncrease();

			// Assert
			Assert.IsTrue(increased);
			Assert.AreEqual(2, semaphore.AvailableSlotsCount);
		}

		[TestMethod]
		public void Increase_disallowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 2, 2);

			// Act
			var increased = semaphore.TryIncrease();

			// Assert
			Assert.IsFalse(increased);
			Assert.AreEqual(2, semaphore.AvailableSlotsCount);
		}

		[TestMethod]
		public void Decrease_allowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 2, 2);

			// Act
			var decreased = semaphore.TryDecrease();

			// Assert
			Assert.IsTrue(decreased);
			Assert.AreEqual(1, semaphore.AvailableSlotsCount);
		}

		[TestMethod]
		public void Decrease_disallowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 2);

			// Act
			var decreased = semaphore.TryDecrease();

			// Assert
			Assert.IsFalse(decreased);
			Assert.AreEqual(1, semaphore.AvailableSlotsCount);
		}
	}
}
