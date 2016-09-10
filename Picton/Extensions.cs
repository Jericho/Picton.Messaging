using System;
using System.Text;
using System.Threading.Tasks;

namespace Picton
{
	public static class Extensions
	{
		#region PUBLIC EXTENSION METHODS

#pragma warning disable RECS0154 // Parameter is never used
		public static void IgnoreAwait(this Task task)
#pragma warning restore RECS0154 // Parameter is never used
		{
			// Intentionaly left blank. The purpose of this extension method is
			// avoid a Visual Studio warning about async calls that are not awaited
		}

		public static async Task UntilCancelled(this Task task)
		{
			try
			{
				await task.ConfigureAwait(false);
			}
			catch (OperationCanceledException)
			{
				// Intentionally left blank. 
				// We want to ignore the exception thrown when a task is cancelled
			}
		}

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
