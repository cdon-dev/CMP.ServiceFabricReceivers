using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceFabricStatelessReceiver
{
    public class EventProcessorFactory : IEventProcessorFactory
    {
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;
        private readonly TelemetryClient _telemetryClient;
        private readonly ILogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Action<string, object[]> _serviceEventSource;

        public EventProcessorFactory(
            TelemetryClient telemetryClient,
            ILogger logger,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents)
        {
            _handleEvents = handleEvents;
            _telemetryClient = telemetryClient;
            _logger = logger;
            _cancellationToken = cancellationToken;
            _serviceEventSource = serviceEventSource;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
         => new EventProcessor(_telemetryClient, _logger, _cancellationToken, _serviceEventSource, _handleEvents);
    }
}
