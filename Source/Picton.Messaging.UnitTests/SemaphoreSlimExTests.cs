using System;
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

		[Fact]
		public void Increase_multiple()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 5);

			// Act
			var increased = semaphore.TryIncrease(increaseCount: 3);

			// Assert
			increased.ShouldBeTrue();
			semaphore.AvailableSlotsCount.ShouldBe(4);
		}

		[Fact]
		public void Decrease_multiple()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 5, 5);

			// Act
			var decreased = semaphore.TryDecrease(decreaseCount: 3);

			// Assert
			decreased.ShouldBeTrue();
			semaphore.AvailableSlotsCount.ShouldBe(2);
		}

		[Fact]
		public void Increase_negative_count_throws_exception()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 5);

			// Act
			Should.Throw<ArgumentOutOfRangeException>(() => semaphore.TryIncrease(increaseCount: -1));
		}

		[Fact]
		public void Decrease_negative_count_throws_exception()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 5, 5);

			// Act
			Should.Throw<ArgumentOutOfRangeException>(() => semaphore.TryDecrease(decreaseCount: -1));
		}

		[Fact]
		public void Increase_zero_returns_false()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 1, 5);

			// Act
			var increased = semaphore.TryIncrease(increaseCount: 0);

			// Assert
			increased.ShouldBeFalse();
			semaphore.AvailableSlotsCount.ShouldBe(1);
		}

		[Fact]
		public void Decrease_zero_returns_false()
		{
			// Arrange
			var semaphore = new SemaphoreSlimEx(1, 5, 5);

			// Act
			var increased = semaphore.TryDecrease(decreaseCount: 0);

			// Assert
			increased.ShouldBeFalse();
			semaphore.AvailableSlotsCount.ShouldBe(5);
		}
	}
}
