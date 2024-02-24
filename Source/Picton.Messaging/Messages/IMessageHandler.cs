using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.Messages
{
	/// <summary>
	/// Message handler interface.
	/// </summary>
	/// <typeparam name="T">The type of message.</typeparam>
	public interface IMessageHandler<T>
		where T : IMessage
	{
		/// <summary>
		/// Handles the specified message asynchronously.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns>The async task.</returns>
		Task HandleAsync(T message, CancellationToken cancellationToken);
	}
}
