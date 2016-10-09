using Microsoft.WindowsAzure.Storage;
using Picton.Interfaces;
using Picton.Messaging.Logging;
using Picton.Messaging.Messages;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Picton.Messaging
{
	public class AsyncMessagePumpWithHandlers
	{
		#region FIELDS

		private readonly AsyncMessagePump _messagePump;
		private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();
		private static readonly IDictionary<Type, Type[]> _messageHandlers = GetMessageHandlers();

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets or sets the logic to execute when an error occurs.
		/// </summary>
		/// <example>
		/// OnError = (message, exception, isPoison) => Trace.TraceError("An error occured: {0}", exception);
		/// </example>
		/// <remarks>
		/// When isPoison is set to true, you should copy this message to a poison queue because it will be deleted from the original queue.
		/// </remarks>
		public Action<CloudMessage, Exception, bool> OnError
		{
			get { return _messagePump.OnError; }
			set { _messagePump.OnError = value; }
		}

		/// <summary>
		/// Gets or sets the logic to execute when queue is empty.
		/// </summary>
		/// <example>
		/// Here's an example:
		/// OnQueueEmpty = cancellationToken => Task.Delay(2500, cancellationToken).Wait();
		/// </example>
		/// <remarks>
		/// If this property is not set, the default logic is to pause for 1 second.
		/// </remarks>
		public Action<CancellationToken> OnQueueEmpty
		{
			get { return _messagePump.OnQueueEmpty; }
			set { _messagePump.OnQueueEmpty = value; }
		}

		#endregion

		#region CONSTRUCTOR

		public AsyncMessagePumpWithHandlers(string queueName, CloudStorageAccount cloudStorageAccount, int minConcurrentTasks = 1, int maxConcurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3) :
			this(queueName, StorageAccount.FromCloudStorageAccount(cloudStorageAccount), minConcurrentTasks, maxConcurrentTasks, visibilityTimeout, maxDequeueCount)
		{
		}

		/// <summary>
		/// High performance message processor (also known as a message "pump") for Azure storage queues. Designed to monitor an Azure storage queue and process the message as quickly and efficiently as possible.
		/// When messages are present in the queue, this message pump will increase the number of tasks that can concurrently process messages.
		/// Conversly, this message pump will reduce the number of tasks that can concurrently process messages when the queue is empty.
		/// </summary>
		/// <param name="minConcurrentTasks">The minimum number of concurrent tasks. The message pump will not scale down below this value</param>
		/// <param name="maxConcurrentTasks">The maximum number of concurrent tasks. The message pump will not scale up above this value</param>
		/// <param name="visibilityTimeout">The queue visibility timeout</param>
		/// <param name="maxDequeueCount">The number of times to try processing a given message before giving up</param>
		public AsyncMessagePumpWithHandlers(string queueName, IStorageAccount storageAccount, int minConcurrentTasks = 1, int maxConcurrentTasks = 25, TimeSpan? visibilityTimeout = null, int maxDequeueCount = 3)
		{
			_messagePump = new AsyncMessagePump(queueName, storageAccount, minConcurrentTasks, maxConcurrentTasks, visibilityTimeout, maxDequeueCount);
			_messagePump.OnMessage = (message, cancellationToken) =>
			{
				Type[] handlers = null;
				if (!_messageHandlers.TryGetValue(message.ContentType, out handlers))
				{
					throw new Exception($"Received a message of type {message.ContentType.FullName} but could not find a class implementing IMessageHandler<{message.ContentType.FullName}>");
				}

				foreach (var handlerType in handlers)
				{
					var handler = Activator.CreateInstance(handlerType);
					var handlerMethod = handlerType.GetMethod("Handle", new[] { message.ContentType });
					handlerMethod.Invoke(handler, new[] { message.Content });
				}
			};
		}

		#endregion

		#region PUBLIC METHODS

		public void Start()
		{
			_messagePump.Start();
		}

		public void Stop()
		{
			_messagePump.Stop();
		}

		#endregion

		#region PRIVATE METHODS

		private static IDictionary<Type, Type[]> GetMessageHandlers()
		{
			var assemblies = GetLocalAssemblies();

			var typesWithMessageHandlerInterfaces = assemblies
				.SelectMany(x => x.GetTypes())
				.Where(t => !t.IsInterface)
				.Select(type => new
				{
					Type = type,
					MessageTypes = type
									.GetInterfaces()
										.Where(i => i.IsGenericType)
										.Where(t => t.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
										.SelectMany(i => i.GetGenericArguments())
				})
				.Where(t => t.MessageTypes != null && t.MessageTypes.Any())
				.ToArray();

			var oneTypePerMessageHandler = typesWithMessageHandlerInterfaces
				.SelectMany(t => t.MessageTypes, (t, messageType) =>
				new
				{
					t.Type,
					MessageType = messageType
				}).ToArray();

			var messageHandlers = oneTypePerMessageHandler
				.GroupBy(h => h.MessageType)
				.ToDictionary(group => group.Key, group => group.Select(t => t.Type).ToArray());
			return messageHandlers;
		}

		private static IEnumerable<Assembly> GetLocalAssemblies()
		{
			var callingAssembly = Assembly.GetCallingAssembly();
			var path = new Uri(Path.GetDirectoryName(callingAssembly.Location)).AbsolutePath;

			return AppDomain.CurrentDomain.GetAssemblies()
				.Where(x => !x.IsDynamic && new Uri(x.CodeBase).AbsolutePath.Contains(path)).ToList();
		}

		#endregion
	}
}
