using Picton.Messaging.Messages;

namespace Picton.Messaging.IntegrationTests
{
	public class MyMessage : IMessage
	{
		public string MessageContent { get; set; }
	}
}
