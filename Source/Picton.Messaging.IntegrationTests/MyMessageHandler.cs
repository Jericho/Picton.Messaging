using Microsoft.Extensions.Logging;
using Picton.Messaging.Messages;

namespace Picton.Messaging.IntegrationTests
{
	public class MyMessageHandler : IMessageHandler<MyMessage>
	{
		private readonly ILogger _log;

		public MyMessageHandler(ILogger log)
		{
			_log = log;
		}
		public void Handle(MyMessage message)
		{
			_log.LogInformation(message.MessageContent);
		}
	}
}
