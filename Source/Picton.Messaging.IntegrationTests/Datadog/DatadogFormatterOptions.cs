
namespace Picton.Messaging.IntegrationTests.Datadog
{
	/// <summary>
	/// Options for data reported to Datadog
	/// </summary>
	public class DatadogFormatterOptions
	{
		/// <summary>
		/// The Hostname that is reported. Usually Environment.MachineName
		/// </summary>
		public string Hostname { get; set; }
	}
}
