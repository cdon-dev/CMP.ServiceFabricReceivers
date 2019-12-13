using Microsoft.ApplicationInsights;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static CMP.ServiceFabricRecevier.Stateless.ReceiverService;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public class EventProcessorFactory : IEventProcessorFactory
    {
        private readonly EventHandlerCreator _eventHandlerCreator;
        private readonly Func<IReadOnlyCollection<EventData>, string, Func<Task>, Task> _operationLogger;
        private readonly ILogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Action<string, object[]> _serviceEventSource;

        public EventProcessorFactory(
            Func<IReadOnlyCollection<EventData>, string, Func<Task>, Task> operationLogger,
            ILogger logger,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            EventHandlerCreator eventHandlerCreator)
        {
            _eventHandlerCreator = eventHandlerCreator;
            _operationLogger = operationLogger;
            _logger = logger;
            _cancellationToken = cancellationToken;
            _serviceEventSource = serviceEventSource;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
         => new EventProcessor((events, f) => _operationLogger(events, context.PartitionId, f), _logger, _cancellationToken, _serviceEventSource, _eventHandlerCreator);
    }
}
