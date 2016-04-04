using System;
using System.Threading.Tasks;

namespace Picton
{
	public static class Extensions
	{
		#region PUBLIC EXTENSION METHODS

		public static void IgnoreAwait(this Task task)
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

		#endregion
	}
}
