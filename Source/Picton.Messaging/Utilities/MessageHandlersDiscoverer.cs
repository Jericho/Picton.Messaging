using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.Logging;
using Picton.Messaging.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Picton.Messaging.Utilities
{
	internal static class MessageHandlersDiscoverer
	{
		public static IDictionary<Type, Type[]> GetMessageHandlers(ILogger logger)
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
				.ToDictionary(group => group.Key, group => group.Select(t => t.Type)
				.ToArray());

			return messageHandlers;
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

			return assemblies.ToArray();
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
