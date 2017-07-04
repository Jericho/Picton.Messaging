using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Picton.Messaging.UnitTests
{
	[TestClass]
	public class ExtensionsTests
	{
		[TestMethod]
		public void ToDurationString_less_than_one_millisecond()
		{
			// Arrange
			var duration = TimeSpan.FromMilliseconds(0);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("1 millisecond", durationString);
		}

		[TestMethod]
		public void ToDurationString_one_day()
		{
			// Arrange
			var duration = TimeSpan.FromDays(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("1 day", durationString);
		}

		[TestMethod]
		public void ToDurationString_multiple_days()
		{
			// Arrange
			var duration = TimeSpan.FromDays(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("2 days", durationString);
		}

		[TestMethod]
		public void ToDurationString_one_hour()
		{
			// Arrange
			var duration = TimeSpan.FromHours(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("1 hour", durationString);
		}

		[TestMethod]
		public void ToDurationString_multiple_hours()
		{
			// Arrange
			var duration = TimeSpan.FromHours(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("2 hours", durationString);
		}

		[TestMethod]
		public void ToDurationString_one_minute()
		{
			// Arrange
			var duration = TimeSpan.FromMinutes(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("1 minute", durationString);
		}

		[TestMethod]
		public void ToDurationString_multiple_minutes()
		{
			// Arrange
			var duration = TimeSpan.FromMinutes(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("2 minutes", durationString);
		}

		[TestMethod]
		public void ToDurationString_one_second()
		{
			// Arrange
			var duration = TimeSpan.FromSeconds(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("1 second", durationString);
		}

		[TestMethod]
		public void ToDurationString_multiple_seconds()
		{
			// Arrange
			var duration = TimeSpan.FromSeconds(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("2 seconds", durationString);
		}

		[TestMethod]
		public void ToDurationString_one_millisecond()
		{
			// Arrange
			var duration = TimeSpan.FromMilliseconds(1);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("1 millisecond", durationString);
		}

		[TestMethod]
		public void ToDurationString_multiple_milliseconds()
		{
			// Arrange
			var duration = TimeSpan.FromMilliseconds(2);

			// Act
			var durationString = duration.ToDurationString();

			// Assert
			Assert.AreEqual("2 milliseconds", durationString);
		}
	}
}
