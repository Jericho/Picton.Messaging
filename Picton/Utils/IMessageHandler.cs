namespace Picton.Utils
{
	public interface IMessageHandler<T>
	{
		void Handle(T message);
	}
}
