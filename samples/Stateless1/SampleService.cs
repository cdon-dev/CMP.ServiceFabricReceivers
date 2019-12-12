using CMP.ServiceFabricRecevier.Stateless;
using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace Stateless1
{
    public class SampleService : ReceiverService
    {
        public SampleService(
            StatelessServiceContext context, 
            ILogger logger, 
            TelemetryClient telemetryClient, 
            ReceiverSettings settings, 
            Action<string, object[]> serviceEventSource, 
            Func<string, Func<IReadOnlyCollection<EventData>, CancellationToken, Task>> handleEvents,
            Func<CancellationToken, Task> @switch, 
            EventProcessorOptions options)
            : base(context, logger, telemetryClient, settings, serviceEventSource, handleEvents, @switch, options)
        {
        }
    }
}
