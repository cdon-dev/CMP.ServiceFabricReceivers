using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public class EventProcessor : IEventProcessor
    {
        private readonly ILogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Func<string, Func<EventContext, Task>> _f;
        //private readonly Action<string, object[]> _serviceEventSource;

        public EventProcessor(
            ILogger logger,
            CancellationToken cancellationToken,
            Func<string, Func<EventContext, Task>> f)
        {
            _logger = logger;
            _cancellationToken = cancellationToken;
            _f = f;
        }


        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            _logger.LogInformation("EventProcessor.CloseAsync for {PartitionId} reason {1}", context.PartitionId, reason);
            //_serviceEventSource("EventProcessor.CloseAsync for {0} reason {1}", new object[] { context.PartitionId, reason });
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            _logger.LogInformation("EventProcessor.OpenAsync for {PartitionId}", context.PartitionId);
            //_serviceEventSource("EventProcessor.OpenAsync for {0}", new[] { context.PartitionId });
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            if (error is ReceiverDisconnectedException)
            {
                _logger.LogInformation(
                    "Receiver disconnected on partition {PartitionId}. Exception: {@Exception}",
                    context.PartitionId, error);
                return Task.CompletedTask;
            }
            if (error is LeaseLostException)
            {
                _logger.LogInformation(
                    "Lease lost on partition {PartitionId}. Exception: {@Exception}",
                    context.PartitionId, error);
                return Task.CompletedTask;
            }
            _logger.LogError(error, "EventProcessor.ProcessErrorAsync for {PartitionId}", context.PartitionId);
            //_serviceEventSource("EventProcessor.ProcessErrorAsync for {0} error {1}", new object[] { context.PartitionId, error });
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
            => _f(context.PartitionId)(new EventContext
            {
                CancellationToken = _cancellationToken,
                Events = messages.ToArray(),
                PartitionId = context.PartitionId,
                Checkpoint = context.CheckpointAsync,
                Logger = _logger
            }); //TODO join tokens ?
    }
}
