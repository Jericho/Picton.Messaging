using System;

namespace Picton.Messaging.IntegrationTests
{
	/// <remarks>
	/// From the https://github.com/Lapiniot/OOs.Common project.
	/// </remarks>
	public class MetricsCollectorOptions
	{
		public TimeSpan RecordInterval { get; set; } = TimeSpan.FromSeconds(5);
	}
}
