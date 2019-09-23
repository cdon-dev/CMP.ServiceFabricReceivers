using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public class EventProcessorFactory : IEventProcessorFactory
    {
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;
        private readonly Func<IDisposable> _operationLogger;
        private readonly ILogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Action<string, object[]> _serviceEventSource;

        public EventProcessorFactory(
            Func<IDisposable> operationLogger,
            ILogger logger,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents)
        {
            _handleEvents = handleEvents;
            _operationLogger = operationLogger;
            _logger = logger;
            _cancellationToken = cancellationToken;
            _serviceEventSource = serviceEventSource;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
         => new EventProcessor(_operationLogger, _logger, _cancellationToken, _serviceEventSource, _handleEvents);
    }
}
