using Picton.Logging;
using Picton.Utils;

namespace Picton.IntegrationTests
{
	public class MyMessageHandler : IMessageHandler<MyMessage>
	{
		private readonly Logger _logger;

		public MyMessageHandler()
		{
			var logProvider = new ColoredConsoleLogProvider(Logging.LogLevel.Debug);
			_logger = logProvider.GetLogger("MyMessageHandler");
		}
		public void Handle(MyMessage message)
		{
			_logger(Logging.LogLevel.Info, () => message.MessageContent);
		}
	}
}
