using System;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.Utilities
{
	/// <summary>
	/// Recurrent Cancellable Task.
	/// </summary>
	/// <remarks>
	/// From a <a href="https://stackoverflow.com/a/35308832/153084">StackOverflow discussion</a>.
	/// </remarks>
	internal static class RecurrentCancellableTask
	{
		/// <summary>
		/// Starts a new task in a recurrent manner repeating it according to the polling interval.
		/// Whoever use this method should protect himself by surrounding critical code in the task
		/// in a Try-Catch block.
		/// </summary>
		/// <param name="action">The action.</param>
		/// <param name="pollInterval">The poll interval.</param>
		/// <param name="token">The token.</param>
		/// <param name="taskCreationOptions">The task creation options.</param>
		public static void StartNew(Func<Task> action, TimeSpan pollInterval, CancellationToken token, TaskCreationOptions taskCreationOptions = TaskCreationOptions.None)
		{
			Task.Factory.StartNew(
				async () =>
				{
					do
					{
						try
						{
							await action().ConfigureAwait(false);
							if (token.WaitHandle.WaitOne(pollInterval)) break;
						}
						catch
						{
							return;
						}
					}
					while (true);
				},
				token,
				taskCreationOptions,
				TaskScheduler.Default);
		}
	}
}
