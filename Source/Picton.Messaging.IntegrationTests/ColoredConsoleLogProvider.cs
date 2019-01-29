namespace Picton.Messaging.IntegrationTests
{
	using Logging;
	using Picton.Messaging.Logging.LogProviders;
	using System;
	using System.Collections.Generic;
	using System.Globalization;

	public class ColoredConsoleLogProvider : LogProviderBase
	{
		private static readonly Dictionary<LogLevel, ConsoleColor> Colors = new Dictionary<LogLevel, ConsoleColor>
		{
			{LogLevel.Fatal, ConsoleColor.Red},
			{LogLevel.Error, ConsoleColor.Yellow},
			{LogLevel.Warn, ConsoleColor.Magenta},
			{LogLevel.Info, ConsoleColor.White},
			{LogLevel.Debug, ConsoleColor.Gray},
			{LogLevel.Trace, ConsoleColor.DarkGray}
		};
		private readonly LogLevel _minLevel = LogLevel.Trace;

		public ColoredConsoleLogProvider(LogLevel minLevel = LogLevel.Trace)
		{
			_minLevel = minLevel;
		}

		/// <summary>
		/// Gets the specified named logger.
		/// </summary>
		/// <param name="name">Name of the logger.</param>
		/// <returns>The logger reference.</returns>
		public override Logger GetLogger(string name)
		{
			return (logLevel, messageFunc, exception, formatParameters) =>
			{
				// messageFunc is null when checking if logLevel is enabled
				if (messageFunc == null) return (logLevel >= _minLevel);

				if (logLevel >= _minLevel)
				{
					// Please note: locking is important to ensure that multiple threads 
					// don't attempt to change the foreground color at the same time
					lock (this)
					{
						if (Colors.TryGetValue(logLevel, out ConsoleColor consoleColor))
						{
							var originalForground = Console.ForegroundColor;
							try
							{
								Console.ForegroundColor = consoleColor;
								WriteMessage(logLevel, name, messageFunc, formatParameters, exception);
							}
							finally
							{
								Console.ForegroundColor = originalForground;
							}
						}
						else
						{
							WriteMessage(logLevel, name, messageFunc, formatParameters, exception);
						}
					}
				}

				return true;
			};
		}

		private static void WriteMessage(
			LogLevel logLevel,
			string name,
			Func<string> messageFunc,
			object[] formatParameters,
			Exception exception)
		{
			var message = messageFunc();
			if (formatParameters?.Length > 0) message = string.Format(CultureInfo.InvariantCulture, message, formatParameters);
			if (exception != null)
			{
				message = message + "|" + exception;
			}
			Console.WriteLine("{0} | {1} | {2} | {3}", DateTime.UtcNow, logLevel, name, message);
		}
	}
}
