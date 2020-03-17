# CMP.ServiceFabricReceivers
Utils for consuming event hubs on service fabric.

## Introduction

This project contains two util projects for consuming eventhubs in service fabric.
One using Service Fabric's state for checkpointing (stateful) and one using Azure Storare for checkpointing (stateless).

Both use EventProcessor libraries, stateful uses the [Service fabric vesion 
in preview](https://www.nuget.org/packages/Microsoft.Azure.EventHubs.ServiceFabricProcessor/)

(in preview) and the stateful uses the  [EventProcessor package](https://www.nuget.org/packages/Microsoft.Azure.EventHubs.Processor/).

The approch and signature is the same in both cases, it only differs in servicetype, configuration and the option type.

### Stateless Sample

          var pipeline = Composition.Combine(
                                 CMP.ServiceFabricReceiver.Common.Features.PartitionLogging(),
                                 CMP.ServiceFabricReceiver.Common.Features.OperationLogging(telemetryClient),
                                 CMP.ServiceFabricReceiver.Common.Features.Logging(),
                                 CMP.ServiceFabricReceiver.Common.Features.Retry(),
                                 CMP.ServiceFabricReceiver.Common.Features.Handling(x => EventHandler.Handle("Sample", table, x.Events)),
                                 CMP.ServiceFabricReceiver.Common.Features.Checkpointing()
                                 );



                ServiceRuntime.RegisterServiceAsync(
                    "ReceiverServiceType2",
                    context =>
                        new SampleService(
                         context,
                         logger,
                         settings,
                         ServiceEventSource.Current.Message,
                         ct => Task.CompletedTask,
                         partitionId => ctx => pipeline(ctx),
                         options)
                        ).GetAwaiter().GetResult();

#### Setup (stateless sample)

The sample requires that you have an eventhub, a local storage and an application insights instance.

To run the (stateless1) sample you simple run;

```sh
dotnet run "eventhubconnectionstring" "applicationinsightsinstrumentationkey"
```

*Note* don't forget to start the local storage emulator.

You could also run this in a local cluster, using the "sample" service fabric project.

The is also a publisher that sends test events to a event hub. To run the publisher use the following;

```sh
dotnet run "eventhubconnectionstringwithentitypath" 
```
That will send 1000 test events to the given entity(hub).

#### Other samples

The samples folder includes a publisher that just pushes test event to a given hub.

There is also an service fabric application using the stateful approch.

*Note* - the sample shows a sample service that inherits the util service. It seems like service fabric wants registered service in the same project that runs register.

##### Event Handler
Note that the `Func<string, Func<IReadOnlyCollection<EventData>, CancellationToken, Task>>` is a way of passing your event handler into the service and it relies on a functional programming concept known as Partial Application. This gives you access to the partitionId, events and cancellation token in your event handler.

The real event handling function that is returned in the end, e.g. `(events, ct) => EventHandler.Handle(events.ToArray())`, will be created every time a batch of events comes in from EventHubs. This means that **no state** is maintained between the differnet executions of the function.






