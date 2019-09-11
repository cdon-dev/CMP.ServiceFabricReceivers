using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using ServiceFabricReceiver;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Stateful1
{
    public class SampleService : ReceiverService
    {
        public SampleService(StatefulServiceContext context, ILogger logger, TelemetryClient telemetryClient, ServiceFabricReceiver.ReceiverOptions options, Action<string, object[]> serviceEventSource, Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents, Func<CancellationToken, Task> @switch) : base(context, logger, telemetryClient, options, serviceEventSource, handleEvents, @switch)
        {
        }
    }
}
