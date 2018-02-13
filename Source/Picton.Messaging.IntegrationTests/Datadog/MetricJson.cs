namespace Picton.Messaging.IntegrationTests.Datadog
{
	/// <summary>
	/// For serializing http://docs.datadoghq.com/api/?lang=console#metrics
	/// </summary>
	class MetricJson
	{
		public string Metric { get; set; }
		public object[][] Points { get; set; }
		public string Host { get; set; }
		public string[] Tags { get; set; }
	}
}
