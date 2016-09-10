namespace Picton.Messaging.Messages
{
	public interface IMessageHandler<T> where T : IMessage
	{
		void Handle(T message);
	}
}
