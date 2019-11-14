using CMP.ServiceFabricReceiver.Stateful;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace Stateful1
{
    public class SampleService : ReceiverService
    {
        public SampleService(StatefulServiceContext context, ILogger logger, TelemetryClient telemetryClient, CMP.ServiceFabricReceiver.Stateful.ReceiverOptions options, Action<string, object[]> serviceEventSource, Func<string, Func<IReadOnlyCollection<EventData>, CancellationToken, Task>> handleEvents, Func<CancellationToken, Task> @switch) : base(context, logger, telemetryClient, options, serviceEventSource, handleEvents, @switch)
        {
        }
    }
}
