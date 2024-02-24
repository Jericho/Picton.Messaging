using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyModel;
using Picton.Messaging.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Picton.Messaging
{
	/// <summary>
	/// Public extension methods.
	/// </summary>
	public static class Public
	{
		public static IServiceCollection AddPictonMessageHandlers(this IServiceCollection services)
		{
			if (services == null)
			{
				throw new ArgumentNullException("services");
			}

			var assemblies = GetLocalAssemblies();

			var typesWithMessageHandlerInterfaces = assemblies
				.SelectMany(x => x.GetTypes())
				.Where(t => !t.GetTypeInfo().IsInterface)
				.Select(type => new
				{
					HandlerType = type,
					InterfaceTypes = type
						.GetInterfaces()
							.Where(i => i.GetTypeInfo().IsGenericType)
							.Where(i => i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
							.ToArray()
				})
				.Where(t => t.InterfaceTypes != null && t.InterfaceTypes.Any())
				.ToArray();

			foreach (var handlerType in typesWithMessageHandlerInterfaces)
			{
				foreach (var interfaceType in handlerType.InterfaceTypes)
				{
					services.AddSingleton(interfaceType, handlerType.HandlerType);
				}
			}

			return services;
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
