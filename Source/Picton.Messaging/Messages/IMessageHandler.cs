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
		/// Handles the specified message.
		/// </summary>
		/// <param name="message">The message.</param>
		void Handle(T message);
	}
}
