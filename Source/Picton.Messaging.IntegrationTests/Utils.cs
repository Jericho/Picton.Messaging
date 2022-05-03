using System;

namespace Picton.Messaging.IntegrationTests
{
	internal static class Utils
	{
		public static void CenterConsole()
		{
			var hWin = NativeMethods.GetConsoleWindow();
			if (hWin == IntPtr.Zero) return;

			var monitor = NativeMethods.MonitorFromWindow(hWin, NativeMethods.MONITOR_DEFAULT_TO_NEAREST);
			if (monitor == IntPtr.Zero) return;

			var monitorInfo = new NativeMethods.NativeMonitorInfo();
			NativeMethods.GetMonitorInfo(monitor, monitorInfo);

			NativeMethods.GetWindowRect(hWin, out NativeMethods.NativeRectangle consoleInfo);

			var monitorWidth = monitorInfo.Monitor.Right - monitorInfo.Monitor.Left;
			var monitorHeight = monitorInfo.Monitor.Bottom - monitorInfo.Monitor.Top;

			var consoleWidth = consoleInfo.Right - consoleInfo.Left;
			var consoleHeight = consoleInfo.Bottom - consoleInfo.Top;

			var left = monitorInfo.Monitor.Left + ((monitorWidth - consoleWidth) / 2);
			var top = monitorInfo.Monitor.Top + ((monitorHeight - consoleHeight) / 2);

			NativeMethods.MoveWindow(hWin, left, top, consoleWidth, consoleHeight, false);
		}

		public static char Prompt(string prompt)
		{
			while (Console.KeyAvailable)
			{
				Console.ReadKey(false);
			}
			Console.Out.WriteLine(prompt);
			var result = Console.ReadKey();
			return result.KeyChar;
		}
	}
}
