using Shouldly;
using System;
using Xunit;

namespace Picton.Messaging.UnitTests
{
	public class ExtensionsTests
	{
		[Fact]
		public void ToDurationString_less_than_one_millisecond()
		{
			// Arrange
			var duration = TimeSpan.FromMilliseconds(0);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("1 millisecond");
		}

		[Fact]
		public void ToDurationString_one_day()
		{
			// Arrange
			var duration = TimeSpan.FromDays(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("1 day");
		}

		[Fact]
		public void ToDurationString_multiple_days()
		{
			// Arrange
			var duration = TimeSpan.FromDays(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("2 days");
		}

		[Fact]
		public void ToDurationString_one_hour()
		{
			// Arrange
			var duration = TimeSpan.FromHours(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("1 hour");
		}

		[Fact]
		public void ToDurationString_multiple_hours()
		{
			// Arrange
			var duration = TimeSpan.FromHours(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("2 hours");
		}

		[Fact]
		public void ToDurationString_one_minute()
		{
			// Arrange
			var duration = TimeSpan.FromMinutes(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("1 minute");
		}

		[Fact]
		public void ToDurationString_multiple_minutes()
		{
			// Arrange
			var duration = TimeSpan.FromMinutes(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("2 minutes");
		}

		[Fact]
		public void ToDurationString_one_second()
		{
			// Arrange
			var duration = TimeSpan.FromSeconds(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("1 second");
		}

		[Fact]
		public void ToDurationString_multiple_seconds()
		{
			// Arrange
			var duration = TimeSpan.FromSeconds(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("2 seconds");
		}

		[Fact]
		public void ToDurationString_one_millisecond()
		{
			// Arrange
			var duration = TimeSpan.FromMilliseconds(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("1 millisecond");
		}

		[Fact]
		public void ToDurationString_multiple_milliseconds()
		{
			// Arrange
			var duration = TimeSpan.FromMilliseconds(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			durationString.ShouldBe("2 milliseconds");
		}
	}
}
