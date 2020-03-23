# CMP.ServiceFabricReceivers
Utils for consuming event hubs on service fabric.

## Introduction

This project contains two util projects for consuming eventhubs in service fabric.
One using Service Fabric's state for checkpointing (stateful) and one using Azure Storare for checkpointing (stateless).

Both use EventProcessor libraries, stateful uses the [Service fabric vesion 
in preview](https://www.nuget.org/packages/Microsoft.Azure.EventHubs.ServiceFabricProcessor/)

(in preview) and the stateful uses the  [EventProcessor package](https://www.nuget.org/packages/Microsoft.Azure.EventHubs.Processor/).

The approch and signature is the same in both cases, it only differs in servicetype, configuration and the option type.

### Sample

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

The samples folder includes a publisher that just pushes test event to a given hub.

There is also an service fabric application using the stateful approch.

*Note* - the sample shows a sample service that inherits the util service. It seems like service fabric wants registered service in the same project that runs register.

##### Event Handler
Note that the `Func<string, Func<IReadOnlyCollection<EventData>, CancellationToken, Task>>` is a way of passing your event handler into the service and it relies on a functional programming concept known as Partial Application. This gives you access to the partitionId, events and cancellation token in your event handler.

The real event handling function that is returned in the end, e.g. `(events, ct) => EventHandler.Handle(events.ToArray())`, will be created every time a batch of events comes in from EventHubs. This means that **no state** is maintained between the differnet executions of the function.


### Monitoring

With the feature "OperationLogging" that enablas a custom request log to application insights, monitoring could be done using the following AI query.

```sql
requests
| where cloud_RoleName == 'name of receiver'
| where success == true
| summarize sum(todouble(customDimensions.EventCount)) by bin(timestamp, 10m), tostring(customDimensions.EventHubPartitionId)
| render timechart"
```

If the request log is found to "noicy", traces could also be used.

```sql
traces
| where cloud_RoleName == 'name of receiver'
| summarize sum(todouble(customDimensions.EventCount)) by bin(timestamp, 10m), tostring(customDimensions.EventHubPartitionId)
| render timechart
```

