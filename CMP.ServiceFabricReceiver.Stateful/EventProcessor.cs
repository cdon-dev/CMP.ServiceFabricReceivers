using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.ServiceFabricProcessor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CMP.ServiceFabricReceiver.Common;


namespace CMP.ServiceFabricReceiver.Stateful
{
    public class EventProcessor : IEventProcessor
    {
        private readonly Func<IDisposable> _operationLogger;
        private readonly ILogger _logger;
        private readonly Action<string, object[]> _serviceEventSource;
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;

        public EventProcessor(
            Func<IDisposable> operationLogger,
            ILogger logger,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents
            )
        {
            _operationLogger = operationLogger;
            _logger = logger;
            _serviceEventSource = serviceEventSource;
            _handleEvents = handleEvents;
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
             _operationLogger,
             context.PartitionId,
             context.CheckpointAsync,
             _handleEvents,
             Logging.Combine(_logger.LogDebug, _serviceEventSource),
             Logging.Combine(_logger.LogInformation, _serviceEventSource),
             Logging.Combine(_logger.LogError, (ex, m, p) => _serviceEventSource(m, p))
             );
    }
}
