# Picton

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](http://jericho.mit-license.org/)
[![Build status](https://ci.appveyor.com/api/projects/status/2tl8wuancvf3awap?svg=true)](https://ci.appveyor.com/project/Jericho/picton)
[![Coverage Status](https://coveralls.io/repos/github/Jericho/Picton/badge.svg?branch=master)](https://coveralls.io/github/Jericho/Picton?branch=master)

## About

Picton is a C# library client containing a high performance worker role designed to process messages in an Azure storage queue as efficiently as possible.

## Nuget

Picton is available as a Nuget package.

[![NuGet Version](http://img.shields.io/nuget/v/Picton.svg)](https://www.nuget.org/packages/Picton/)
[![AppVeyor](https://img.shields.io/appveyor/ci/Jericho/picton.svg)](https://ci.appveyor.com/project/Jericho/picton)

## Release Notes

+ **1.0.0**
	- Initial release

## Installation

The easiest way to include Picton in your C# project is by grabing the nuget package:

```
PM> Install-Package Picton.Azure
```

Once you have the Picton library properly referenced in your project, add a new class like tis example:

```csharp
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Picton.Azure.WorkerRoles;

namespace MyNamespace
{
	public class MyQueueWorker : AsyncQueueWorker
	{
		public override CloudQueue GetQueue()
		{
			var storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
			var cloudQueueClient = storageAccount.CreateCloudQueueClient();
			cloudQueueClient.DefaultRequestOptions.RetryPolicy = new NoRetry();
			var cloudQueue = cloudQueueClient.GetQueueReference("myqueue");
			cloudQueue.CreateIfNotExists();
			return cloudQueue;
		}
		
		public override void OnMessage(CloudQueueMessage message, CancellationToken cancellationToken = default(CancellationToken))
		{
			Debug.WriteLine(msg.AsString);
		}
		
		public override void OnError(CloudQueueMessage message, Exception exception, bool isPoison)
		{
			Trace.TraceInformation("OnError: {0}", exception);
			
			if (isPoison)
			{
				// Copy message to a poison queue otherwise it will be lost forever
			}
		}

	}
}
```
