# Picton

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://jericho.mit-license.org/)
[![Build status](https://ci.appveyor.com/api/projects/status/2tl8wuancvf3awap?svg=true)](https://ci.appveyor.com/project/Jericho/picton.messaging)
[![Coverage Status](https://coveralls.io/repos/github/Jericho/Picton.Messaging/badge.svg?branch=master)](https://coveralls.io/github/Jericho/Picton.Messaging?branch=master)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2FJericho%2FPicton.Messaging.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2FJericho%2FPicton.Messaging?ref=badge_shield)


## About

Picton.Messaging is a C# library containing a high performance message processor (also known as a message "pump") designed to process messages from Azure storage queues as efficiently as possible.

I created Picton.Mesaging because I needed a way to process a large volume of messages from Azure storage queues as quickly and efficiently as possible. I searched for a long time, but I could never find a solution that met all my requirements.

In March 2016 I attended three webinars by [Daniel Marbach](https://github.com/danielmarbach) on "Async/Await & Task Parallel Library" ([part 1](https://github.com/danielmarbach/02-25-2016-AsyncWebinar), [part 2](https://github.com/danielmarbach/03-03-2016-AsyncWebinar) and [part 3](https://github.com/danielmarbach/03-10-2016-AsyncWebinar)).
Part 2 was particularly interresting to me because Daniel presented a generic message pump that meets most (but not all) of my requirements. Specifically, Daniel's message pump meets the following criteria:

- Concurrent message handling. This means that multiple messages can be processed at the same time. This is critical, especially if your message processing logic is I/O bound.
- Limiting concurrency. This means that we can decide the maximum number of messages that can be processed at the same time.
- Cancelling and graceful shutdown. This means that our message pump can be notified that we want to stop processing additional messages and also we can decide what to do with messages that are being processed at the moment when we decide to stop.

The sample code that Daniel shared during his webinars was very generic and not specific to Azure so I made the following enhancements:
- The messages are fetched from an Azure storage queue. We can specify which queue and it can even be a queue in the storage emulator.
- Backoff. When no messages are found in the queue, it's important to scale back and query the queue less often. As soon as new messages are available in the queue, we want to scale back up in order to process these messages as quickly as possible. I made two improvements to implemented this logic: 
  - first, introduce a pause when no message is found in the queue in order to reduce the number of queries to the storage queue. By default we pause for one second, but it's configurable. You could even eliminate the pause altogether but I do not recommend it. 
  - second, reduce the number of concurrent message processing tasks. This is the most important improvement introduced in the Picton library (in my humble opinion!). Daniel's sample code uses ``SemaphoreSlim`` to act as a "gatekeeper" and to limit the number of tasks that be be executed concurently. However, the number of "slots" permitted by the semaphore must be predetermined and is fixed. Picton eliminates this restriction and allows this number to be dynamically increased and decreased based on the presence or abscence of messages in the queue.

In December 2017 version 2.0 was released with a much more efficient method of fetching messages from the Azure queue: there is now a dedicated task for this pupose instead of allowing each individual concurent task to fetch their own messages. This means that the logic to increase/decrease the number of available slots in the SemaphoreSlim is no longer necessary and has ben removed.

In January 2024 version 9.0 was released with two major new features: the message pump is now able to monitor multiple queues and also a specialized version of the message pump was added to monitor queues that follow a naming convention. Additionaly, this specialized message pump queries the Azure storage at regular interval to detect if new queues have been created. This is, in my opinion, an ideal solution when you have a multi-tenant solution with one queue for each tenant.


## Nuget

Picton.Messaging is available as a Nuget package.

[![NuGet Version](https://img.shields.io/nuget/v/Picton.Messaging.svg)](https://www.nuget.org/packages/Picton.Messaging/)


## Installation

The easiest way to include Picton.Messaging in your C# project is by grabing the nuget package:

```
PM> Install-Package Picton.Messaging
```

## How to use

Once you have the Picton.Messaging library properly referenced in your project, you need to following two CSharp files:

### Program.cs

```csharp
using WorkerService1;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
```

### Worker.cs

```csharp
using Picton.Messaging;

namespace WorkerService1
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }
        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var connectionString = "<--  connection string for your Azure storage account -->";
            var concurrentTask = 10; // <-- this is the max number of messages that can be processed at a time

            // Configure the message pump
            var options = new MessagePumpOptions(connectionString, concurrentTasks);
            var messagePump = new AsyncMessagePump(options)
            {
                OnMessage = (queueName, message, cancellationToken) =>
                {
                    // This is where you insert your custom logic to process a message
                },
                OnError = (queueName, message, exception, isPoison) =>
                {
                    // Insert your custom error handling

                    // ==========================================================================
                    // Important note regarding "isPoison":
                    // --------------------------------------------------------------------------
                    // this parameter indicates whether this message has exceeded the maximum
                    // number of retries. 
                    //
                    // When you have configured the "poison queue name" for the given queue and 
                    // this parameter is "true", the message is automatically copied to the poison
                    // queue and removed from the original queue.
                    // 
                    // If you have not configured the "poison queue name" for the given queue and
                    // this parameter is "true", the message is automatically removed from the
                    // original queue and you are responsible for storing the message. If you don't,
                    // this mesage will be lost.
                    // ==========================================================================
                }
            };

            // Replace the following samples with the queues you want to monitor
            messagePump.AddQueue("queue01", "queue01-poison", TimeSpan.FromMinutes(1), 3, "queue01-oversize-messages");
            messagePump.AddQueue("queue02", "queue02-poison", TimeSpan.FromMinutes(1), 3, "queue02-oversize-messages");
            messagePump.AddQueue("queue03", "queue03-poison", TimeSpan.FromMinutes(1), 3, "queue03-oversize-messages");

            // Queues can share the same poison queue
            messagePump.AddQueue("queue04", "my-poison-queue", TimeSpan.FromMinutes(1), 3, "queue04-oversize-messages");
            messagePump.AddQueue("queue05", "my-poison-queue", TimeSpan.FromMinutes(1), 3, "queue05-oversize-messages");

            // Queues can also share the same blob storage for messages that exceed the max size
            messagePump.AddQueue("queue06", "my-poison-queue", TimeSpan.FromMinutes(1), 3, "large-messages-blob");
            messagePump.AddQueue("queue07", "my-poison-queue", TimeSpan.FromMinutes(1), 3, "large-messages-blob");

            // You can add all queues matching a given RegEx pattern
            await messagePump.AddQueuesByPatternAsync("myqueue*", "my-poison-queue", TimeSpan.FromMinutes(1), 3, "large-messages-blob", cancellationToken).ConfigureAwait(false);

            // Start the mesage pump (the token is particularly important because that's how the message pump will be notified when the worker is stopping)
            await messagePump.StartAsync(stoppingToken).ConfigureAwait(false);
        }
    }
}
```

## Message handlers

As demonstrated in the previous code sample, you can define your own `OnMessage` delegate which gets invoked by the message pump when each message is processed. This is perfect for simple scenarios where your C# logic is rather simple. However, your C# code can become complicated pretty quickly as the complexity of your logic increases.

The Picton.Messaging library includes a more advanced message pump that can simplify this situation for you: `AsyncMessagePumpWithHandlers`. You C# project must define so-called "handlers" for each of your message types. These handlers are simply c# classes that implement the `IMessageHandler<T>` interface, where `T` is the type of the message. For example, if you expect to process messages of type `MyMessage`, you must define a class with signature similar to this: `public class MyMessageHandler : IMessageHandler<MyMessage>`.

Once you have created all the handlers you need, you must register them with your solution's DI service collection like in this example:

### Program.cs

```csharp
using Picton.Messaging;
using WorkerService1;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

/*
    You can either register your message handlers one by one like this:
    
    builder.Services.AddSingleton<IMessageHandler<MyMessage>, MyMessageHandler>()
    builder.Services.AddSingleton<IMessageHandler<MyOtherMessage>, MyOtherMessageHandler>()
    builder.Services.AddSingleton<IMessageHandler<AnotherMessage>, AnotherMessageHandler>()
*/

// Or you can allow Picton.Messaging to scan your assemblies and to register all message handlers like this:
builder.Services.AddPictonMessageHandlers()

var host = builder.Build();
host.Run();
```

### Worker.cs

```csharp
using Picton.Messaging;

namespace WorkerService1
{
    public class Worker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<Worker> _logger;

        public Worker(IServiceProvider serviceProvider, ILogger<Worker> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var connectionString = "<--  connection string for your Azure storage account -->";
            var concurrentTask = 10; // <-- this is the max number of messages that can be processed at a time

            var options = new MessagePumpOptions(connectionString, concurrentTasks, null, null);
            var messagePump = new AsyncMessagePumpWithHandlers(options, _serviceProvider, _logger)
            {
                OnError = (queueName, message, exception, isPoison) =>
                {
                    // Insert your custom error handling
                }
            };

            messagePump.AddQueue("myqueue", null, TimeSpan.FromMinutes(1), 3);

            await messagePump.StartAsync(stoppingToken).ConfigureAwait(false);
        }
    }
}
```


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2FJericho%2FPicton.Messaging.svg?type=large)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2FJericho%2FPicton.Messaging?ref=badge_large)
