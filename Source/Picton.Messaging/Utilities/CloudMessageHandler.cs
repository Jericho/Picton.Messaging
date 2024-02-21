using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.Logging;
using Picton.Messaging.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.Messaging.Utilities
{
	internal class CloudMessageHandler
	{
		private static ConcurrentDictionary<Type, Type[]> _messageHandlerTypes = GetMessageHandlers(null);
		private static ConcurrentDictionary<Type, Lazy<object>> _messageHandlerInstances = new();

		private readonly IServiceProvider _serviceProvider;
		private readonly ILogger _logger;

		public CloudMessageHandler(IServiceProvider serviceProvider = null)
		{
			_serviceProvider = serviceProvider;
		}

		public async Task HandleMessageAsync(CloudMessage message, CancellationToken cancellationToken)
		{
			var contentType = message.Content.GetType();

			if (!_messageHandlerTypes.TryGetValue(contentType, out Type[] handlerTypes))
			{
				throw new Exception($"Received a message of type {contentType.FullName} but could not find a class implementing IMessageHandler<{contentType.FullName}>");
			}

			foreach (var handlerType in handlerTypes)
			{
				// Get the message handler from cache or instantiate a new one
				var handler = _messageHandlerInstances.GetOrAdd(handlerType, type => new Lazy<object>(() =>
				{
					// Let the DI service provider resolve the dependencies expected by the type constructor OR
					// invoke the parameterless constructor when the DI service provider was not provided
					var newHandlerInstance = _serviceProvider != null ?
						ActivatorUtilities.CreateInstance(_serviceProvider, type) :
						Activator.CreateInstance(type);

					// Return the newly created instance
					return newHandlerInstance;
				})).Value;

				// Invoke the "HandleAsync" method asynchronously
				var handlerMethod = handlerType.GetMethod("HandleAsync", [contentType, typeof(CancellationToken)]);
				var result = (Task)handlerMethod.Invoke(handler, [message.Content, cancellationToken]);
				await result.ConfigureAwait(false);
			}
		}

		private static ConcurrentDictionary<Type, Type[]> GetMessageHandlers(ILogger logger)
		{
			logger?.LogTrace("Discovering message handlers.");

			var assemblies = GetLocalAssemblies();

			var assembliesCount = assemblies.Length;
			if (assembliesCount == 0) logger?.LogTrace($"Did not find any local assembly.");
			else if (assembliesCount == 1) logger?.LogTrace("Found 1 local assembly.");
			else logger?.LogTrace($"Found {assemblies.Count()} local assemblies.");

			var typesWithMessageHandlerInterfaces = assemblies
				.SelectMany(x => x.GetTypes())
				.Where(t => !t.GetTypeInfo().IsInterface)
				.Select(type => new
				{
					Type = type,
					MessageTypes = type
						.GetInterfaces()
							.Where(i => i.GetTypeInfo().IsGenericType)
							.Where(i => i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
							.SelectMany(i => i.GetGenericArguments())
				})
				.Where(t => t.MessageTypes != null && t.MessageTypes.Any())
				.ToArray();

			var classesCount = typesWithMessageHandlerInterfaces.Length;
			if (classesCount == 0) logger?.LogTrace("Did not find any class implementing the 'IMessageHandler' interface.");
			else if (classesCount == 1) logger?.LogTrace("Found 1 class implementing the 'IMessageHandler' interface.");
			else logger?.LogTrace($"Found {typesWithMessageHandlerInterfaces.Count()} classes implementing the 'IMessageHandler' interface.");

			var oneTypePerMessageHandler = typesWithMessageHandlerInterfaces
				.SelectMany(t => t.MessageTypes, (t, messageType) =>
				new
				{
					t.Type,
					MessageType = messageType
				})
				.ToArray();

			var messageHandlers = oneTypePerMessageHandler
				.GroupBy(h => h.MessageType)
				.ToDictionary(
					group => group.Key,
					group => group.Select(t => t.Type).ToArray());

			return new ConcurrentDictionary<Type, Type[]>(messageHandlers);
		}

		private static Assembly[] GetLocalAssemblies()
		{
			var dependencies = DependencyContext.Default.RuntimeLibraries;

			var assemblies = new List<Assembly>();
			foreach (var library in dependencies)
			{
				if (IsCandidateLibrary(library))
				{
					var assembly = Assembly.Load(new AssemblyName(library.Name));
					assemblies.Add(assembly);
				}
			}

			return [.. assemblies];
		}

		private static bool IsCandidateLibrary(RuntimeLibrary library)
		{
			return !library.Name.StartsWith("Microsoft.", StringComparison.OrdinalIgnoreCase) &&
				!library.Name.StartsWith("System.", StringComparison.OrdinalIgnoreCase) &&
				!library.Name.StartsWith("NetStandard.", StringComparison.OrdinalIgnoreCase) &&
				!string.Equals(library.Type, "package", StringComparison.OrdinalIgnoreCase) &&
				!string.Equals(library.Type, "referenceassembly", StringComparison.OrdinalIgnoreCase);
		}
	}
}
