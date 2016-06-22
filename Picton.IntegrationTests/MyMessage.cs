using Picton.Messages;

namespace Picton.IntegrationTests
{
	public class MyMessage : IMessage
	{
		public string MessageContent { get; set; }
	}
}
