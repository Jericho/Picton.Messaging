using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Picton.IntegrationTests
{
	/// <summary>
	/// This class attempts to detect which version of the Azure emulator is installed on your machine.
	/// One thing that is quite confusing is that the version of the emulator does not match the Azure
	/// SDK version. 
	/// 
	/// For instance:
	///		- emulator 2.0 was released with SDK 2.2 in October 2013
	///		- emulator 3.0 was released with SDK 2.3 in April 2014
	///		- emulator 3.3 was released with SDK 2.4 in August 2014
	///		- emulator 3.4 was released with SDK 2.5 in November 2014
	///		- emulator 4.0 was released with SDK 2.6 in April 2015
	///		- emulator 4.1 was released with SDK 2.7 (2.7.1 was released in August 2015 but I'm not sure when 2.7 was released)
	///		- emulator 4.2 was released with SDK 2.8 (2.8.1 was released in November 2015 but I'm not sure when 2.8 was released)
	///		- emulator 4.3 was released with SDK 2.9 in March 2016
	/// </summary>
	public class AzureStorageEmulatorManager
	{
		private class EmulatorVersionInfo
		{
			public int Version { get; private set; }
			public string[] ProcessNames { get; private set; }	// the process name is not always the same on different platforms. For instance, "WAStorageEmulator" is named "WASTOR~1" on Windows 8.
			public string ExecutablePath { get; private set; }
			public string Parameters { get; private set; }

			public EmulatorVersionInfo(int version, IEnumerable<string> processNames, string executablePath, string parameters)
			{
				Version = version;
				ProcessNames = processNames.ToArray();
				ExecutablePath = executablePath;
				Parameters = parameters;
			}
		}

		#region FIELDS

		private static IList<EmulatorVersionInfo> _emulatorVersions = new List<EmulatorVersionInfo>();

		#endregion

		#region CONSTRUCTOR

		static AzureStorageEmulatorManager()
		{
			_emulatorVersions.Add(new EmulatorVersionInfo(2, new[] { "DSService" }, @"C:\Program Files\Microsoft SDKs\Windows Azure\Emulator\csrun.exe", "/devstore:start"));
			_emulatorVersions.Add(new EmulatorVersionInfo(3, new[] { "WAStorageEmulator", "WASTOR~1" }, @"C:\Program Files (x86)\Microsoft SDKs\Windows Azure\Storage Emulator\WAStorageEmulator.exe", "start"));
			_emulatorVersions.Add(new EmulatorVersionInfo(4, new[] { "AzureStorageEmulator" }, @"C:\Program Files (x86)\Microsoft SDKs\Azure\Storage Emulator\AzureStorageEmulator.exe", "start"));
		}

		#endregion

		#region PUBLIC METHODS

		public static void StartStorageEmulator()
		{
			var found = false;

			// Ordering emulators in reverse order is important to ensure we start the most recent version, even if an older version is available
			foreach (var emulatorVersion in _emulatorVersions.OrderByDescending(x => x.Version))
			{
				if (File.Exists(emulatorVersion.ExecutablePath))
				{
					var count = 0;
					Parallel.ForEach(emulatorVersion.ProcessNames, processName => Interlocked.Add(ref count, Process.GetProcessesByName(processName).Length));
					if (count == 0) ExecuteStorageEmulator(emulatorVersion.Parameters, emulatorVersion.ExecutablePath);
					found = true;
					break;
				}
			}

			if (!found)
			{
				throw new FileNotFoundException("Unable to find the Azure emulator on this computer");
			}
		}

		public static void StopStorageEmulator()
		{
			Parallel.ForEach(_emulatorVersions.SelectMany(x => x.ProcessNames), processName =>
			{
				var process = Process.GetProcessesByName(processName).FirstOrDefault();
				if (process != null) process.Kill();
			});
		}

		#endregion

		#region PRIVATE METHODS

		private static void ExecuteStorageEmulator(string argument, string fileName)
		{
			var start = new ProcessStartInfo
			{
				UseShellExecute = false,
				CreateNoWindow = true,
				Arguments = argument,
				FileName = fileName
			};
			var exitCode = ExecuteProcess(start);
			if (exitCode != 0)
			{
				var message = $"Error {exitCode} executing {start.FileName} {start.Arguments}";
				throw new InvalidOperationException(message);
			}
		}

		private static int ExecuteProcess(ProcessStartInfo start)
		{
			int exitCode;
			using (var proc = new Process { StartInfo = start })
			{
				proc.Start();
				proc.WaitForExit();
				exitCode = proc.ExitCode;
			}
			return exitCode;
		}

		#endregion
	}
}
