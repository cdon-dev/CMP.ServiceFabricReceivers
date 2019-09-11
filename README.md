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

                ServiceRuntime.RegisterServiceAsync(
                    "ReceiverServiceType",
                    context =>
                        new SampleService(
                         context,
                         logger,
                         new TelemetryClient(TelemetryConfiguration.Active),
                         new ReceiverOptions()
                         {
                             ConnectionString = "",
                             ConsumerGroup = "sf"
                         },
                         ServiceEventSource.Current.Message,
                         (events, ct) => EventHandler.Handle(events.ToArray()),
                            ct => Task.CompletedTask
                   )).GetAwaiter().GetResult();

The samples folder includes a publisher that just pushes test event to a given hub.

There is also an service fabric application using the stateful approch.

*Note* - the sample shows a sample service that inhrits the util service. It seems like service fabric wants registered service in the same project that runs register.



