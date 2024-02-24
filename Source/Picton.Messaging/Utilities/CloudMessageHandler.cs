using Picton.Messaging.Messages;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.Utilities
{
	internal class CloudMessageHandler
	{
		private readonly IServiceProvider _serviceProvider;

		public CloudMessageHandler(IServiceProvider serviceProvider)
		{
			_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		}

		public async Task HandleMessageAsync(CloudMessage message, CancellationToken cancellationToken)
		{
			// Get the message handler from the DI service provider
			var contentType = message.Content.GetType();
			var handlerType = typeof(IMessageHandler<>).MakeGenericType([contentType]);
			var handler = _serviceProvider.GetService(handlerType);

			// Invoke the "HandleAsync" method asynchronously
			var handlerMethod = handlerType.GetMethod("HandleAsync", [contentType, typeof(CancellationToken)]);
			var result = (Task)handlerMethod.Invoke(handler, [message.Content, cancellationToken]);
			await result.ConfigureAwait(false);
		}
	}
}
