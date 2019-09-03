using CMP.ServiceFabricReceiver.Common;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceFabricStatelessReceiver
{
    public class EventProcessor : IEventProcessor
    {
        private static readonly TimeSpan[] FailedDelaySteps =
{
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(10),
            TimeSpan.FromHours(1)
        };

        private readonly TelemetryClient _telemetryClient;
        private readonly ILogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Action<string, object[]> _serviceEventSource;
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;

        public EventProcessor(
            TelemetryClient telemetryClient,
            ILogger logger,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents)
        {
            this._telemetryClient = telemetryClient;
            this._logger = logger;
            this._cancellationToken = cancellationToken;
            this._serviceEventSource = serviceEventSource;
            this._handleEvents = handleEvents;
        }


        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            _logger.LogInformation("EventProcessor.CloseAsync for {PartitionId} reason {1}", context.PartitionId, reason);
            _serviceEventSource("EventProcessor.CloseAsync for {0} reason {1}", new object[] { context.PartitionId, reason });
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            _logger.LogInformation("EventProcessor.OpenAsync for {PartitionId}", context.PartitionId);
            _serviceEventSource("EventProcessor.OpenAsync for {0}", new[] { context.PartitionId });
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            _logger.LogError(error, "EventProcessor.ProcessErrorAsync for {PartitionId}", context.PartitionId);
            _serviceEventSource("EventProcessor.ProcessErrorAsync for {0} error {1}", new object[] { context.PartitionId, error });
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
            => messages.ProcessAsync(
                _cancellationToken,
                _telemetryClient,
                context.PartitionId,
                context.CheckpointAsync,
                _handleEvents,
                Logging.Combine(_logger.LogDebug, _serviceEventSource),
                Logging.Combine(_logger.LogInformation, _serviceEventSource),
                Logging.Combine(_logger.LogError, (ex, m, p) => _serviceEventSource(m, p))
            );

  
    }
}
