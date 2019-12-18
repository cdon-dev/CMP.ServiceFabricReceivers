using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.ServiceFabricProcessor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CMP.ServiceFabricReceiver.Common;
using static CMP.ServiceFabricReceiver.Stateful.ReceiverService;

namespace CMP.ServiceFabricReceiver.Stateful
{
    public class EventProcessor : IEventProcessor
    {
        private readonly Func<IReadOnlyCollection<EventData>, string, Func<Task>, Task> _operationLogger;
        private readonly ILogger _logger;
        private readonly Action<string, object[]> _serviceEventSource;
        private readonly EventHandlerCreator _eventHandlerCreator;
        private readonly int exceptionDelaySeconds;

        public EventProcessor(
            Func<IReadOnlyCollection<EventData>, string, Func<Task>, Task> operationLogger,
            ILogger logger,
            Action<string, object[]> serviceEventSource,
            EventHandlerCreator eventHandlerCreator,
            int exceptionDelaySeconds = 1
            )
        {
            _operationLogger = operationLogger;
            _logger = logger;
            _serviceEventSource = serviceEventSource;
            _eventHandlerCreator = eventHandlerCreator;
            this.exceptionDelaySeconds = exceptionDelaySeconds;
        }

        public override Task OpenAsync(CancellationToken cancellationToken, PartitionContext context)
        {
            _logger.LogInformation("EventProcessor.OpenAsync for {PartitionId}", context.PartitionId);
            _serviceEventSource("EventProcessor.OpenAsync for {0}", new[] { context.PartitionId });
            return Task.CompletedTask;
        }

        public override Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            _logger.LogInformation("EventProcessor.CloseAsync for {PartitionId} reason {1}", context.PartitionId, reason);
            _serviceEventSource("EventProcessor.CloseAsync for {0} reason {1}", new object[] { context.PartitionId, reason });
            return Task.CompletedTask;
        }

        public override Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            _logger.LogError(error, "EventProcessor.ProcessErrorAsync for {PartitionId}", context.PartitionId);
            _serviceEventSource("EventProcessor.ProcessErrorAsync for {0} error {1}", new object[] { context.PartitionId, error });
            return Task.CompletedTask;
        }

        public override Task ProcessEventsAsync(CancellationToken cancellationToken, PartitionContext context, IEnumerable<EventData> eventDatas)
         => eventDatas.ProcessAsync(
             cancellationToken,
             (events, f) => _operationLogger(events, context.PartitionId, f),
             context.PartitionId,
             context.CheckpointAsync,
             _eventHandlerCreator(context.PartitionId),
             _logger.LogDebug,
             Logging.Combine(_logger.LogInformation, _serviceEventSource),
             Logging.Combine(_logger.LogError, (ex, m, p) => _serviceEventSource(m, p)),
             exceptionDelaySeconds
             );
    }
}
