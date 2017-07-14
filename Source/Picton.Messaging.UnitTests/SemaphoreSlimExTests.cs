using Picton.Messaging.Utils;
using Shouldly;
using Xunit;

namespace Picton.Messaging.UnitTests
{
	public class SemaphoreSlimExTests
	{
		[Fact]
		public void Increase_allowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 2);

			// Act
			var increased = semaphore.TryIncrease();

			// Assert
			increased.ShouldBeTrue();
			semaphore.AvailableSlotsCount.ShouldBe(2);
		}

		[Fact]
		public void Increase_disallowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 2, 2);

			// Act
			var increased = semaphore.TryIncrease();

			// Assert
			increased.ShouldBeFalse();
			semaphore.AvailableSlotsCount.ShouldBe(2);
		}

		[Fact]
		public void Decrease_allowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 2, 2);

			// Act
			var decreased = semaphore.TryDecrease();

			// Assert
			decreased.ShouldBeTrue();
			semaphore.AvailableSlotsCount.ShouldBe(1);
		}

		[Fact]
		public void Decrease_disallowed()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 2);

			// Act
			var decreased = semaphore.TryDecrease();

			// Assert
			decreased.ShouldBeFalse();
			semaphore.AvailableSlotsCount.ShouldBe(1);
		}
	}
}
