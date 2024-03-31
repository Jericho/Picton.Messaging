using Microsoft.Extensions.Logging;
using System;

namespace Picton.Messaging
{
	/// <summary>
	/// Internal extension methods for logging purposes.
	/// </summary>
	internal static partial class Logging
	{
		[LoggerMessage(LogLevel.Error, "An exception occured when {operationDescription}. It was caught and ignored.")]
		public static partial void ErrorIgnored(this ILogger logger, string operationDescription, Exception exception);

		[LoggerMessage(LogLevel.Error, "An exception occured when {operationDescription} for {queueName}. It was caught and ignored.")]
		public static partial void ErrorIgnoredForQueue(this ILogger logger, string operationDescription, string queueName, Exception exception);

		[LoggerMessage(LogLevel.Trace, "There are no queues being monitored. Therefore no messages could be fetched.")]
		public static partial void NoQueuesMonitored(this ILogger logger);

		[LoggerMessage(LogLevel.Trace, "Fetched {messageCount} message(s) in {queueName}.")]
		public static partial void FetchedMessagesForQueue(this ILogger logger, int messageCount, string queueName);

		[LoggerMessage(LogLevel.Trace, "There are no messages in {queueName}.")]
		public static partial void NoMessagesInQueue(this ILogger logger, string queueName);

		[LoggerMessage(LogLevel.Trace, "All tenant queues are empty, no messages fetched.")]
		public static partial void TenantQueuesAreEmpty(this ILogger logger);
	}
}
