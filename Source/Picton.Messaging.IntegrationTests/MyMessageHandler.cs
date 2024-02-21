using Microsoft.Extensions.Logging;
using Picton.Messaging.Messages;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.IntegrationTests
{
	public class MyMessageHandler : IMessageHandler<MyMessage>
	{
		private readonly ILogger _log;

		public MyMessageHandler(ILogger log)
		{
			_log = log;
		}

		public Task HandleAsync(MyMessage message, CancellationToken cancellationToken)
		{
			_log?.LogInformation(message.MessageContent);

			return Task.CompletedTask;
		}
	}
}
