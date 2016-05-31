using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;

namespace Picton.Utils
{
	public class CloudMessageEnvelope
	{
		public Type MessageType { get; set; }
		public string Payload { get; set; }

		public static CloudMessageEnvelope FromObject<T>(T payload)
		{
			return new CloudMessageEnvelope
			{
				MessageType = typeof(T),
				Payload = JsonConvert.SerializeObject(payload)
			};
		}

		public static CloudMessageEnvelope FromCloudQueueMessage(CloudQueueMessage message)
		{
			return JsonConvert.DeserializeObject<CloudMessageEnvelope>(message.AsString);
		}
	}
}
