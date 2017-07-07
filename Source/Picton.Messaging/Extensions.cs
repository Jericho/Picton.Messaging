using System;
using System.Text;
using System.Threading.Tasks;

namespace Picton.Messaging
{
	/// <summary>
	/// Extension methods
	/// </summary>
	public static class Extensions
	{
		#region PUBLIC EXTENSION METHODS

#pragma warning disable RECS0154 // Parameter is never used
		/// <summary>
		/// The purpose of this extension method is to avoid a Visual Studio warning about async calls that are not awaited
		/// </summary>
		/// <param name="task">The task.</param>
		public static void IgnoreAwait(this Task task)
#pragma warning restore RECS0154 // Parameter is never used
		{
			// Intentionaly left blank.
		}

		/// <summary>
		/// The purpose of this extension method is to ignore the exception thrown when a task is cancelled
		/// </summary>
		/// <param name="task">The task.</param>
		/// <returns>A task.</returns>
		public static async Task UntilCancelled(this Task task)
		{
			try
			{
				await task.ConfigureAwait(false);
			}
			catch (OperationCanceledException)
			{
				// Intentionally left blank.
			}
		}

		/// <summary>
		/// Converts a <see cref="TimeSpan"/> into a human readable format.
		/// </summary>
		/// <param name="timeSpan">The time span.</param>
		/// <returns>A human readable string</returns>
		public static string ToDurationString(this TimeSpan timeSpan)
		{
			// In case the TimeSpan is extremely short
			if (timeSpan.TotalMilliseconds < 1) return "1 millisecond";

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

		#endregion
	}
}
