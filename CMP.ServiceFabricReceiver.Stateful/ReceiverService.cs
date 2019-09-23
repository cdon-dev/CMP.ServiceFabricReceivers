using CMP.ServiceFabricReceiver.Common;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.ServiceFabricProcessor;
using Microsoft.Extensions.Logging;
using Microsoft.ServiceFabric.Services.Runtime;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricReceiver.Stateful
{
    public class ReceiverService : StatefulService
    {
        private const int MaxMessageCount = 1000;
        private readonly Action<string, object[]> _serviceEventSource;

        /// <summary>
        ///     Names of the dictionaries that hold the current offset value and partition epoch.
        /// </summary>
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;
        private readonly Func<CancellationToken, Task> _switch;
        private readonly ILogger _logger;
        private readonly TelemetryClient _telemetryClient;
        private readonly ReceiverOptions _options;

        public ReceiverService(
            StatefulServiceContext context,
            ILogger logger,
            TelemetryClient telemetryClient,
            ReceiverOptions options,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents,
            Func<CancellationToken, Task> @switch) : base(context)
        {
            _telemetryClient = telemetryClient;
            _options = options;
            _logger = logger;
            _serviceEventSource = serviceEventSource;
            _handleEvents = handleEvents;
            _switch = @switch;
        }

        private void OnShutdown(Exception e)
        {
            if (e == null)
            {
                _logger.LogInformation("OnShutdown");
            }
            else
            {
                _logger.LogError(e, "OnShutdown got {ErrorMessage}", e.Message);
            }

            _serviceEventSource("OnShutdown got {0}", new object[] { e == null ? "NO ERROR" : e.ToString() });
        }

        protected override Task OnOpenAsync(ReplicaOpenMode openMode, CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnOpenAsync));
            return base.OnOpenAsync(openMode, cancellationToken);
        }

        protected override Task OnCloseAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnCloseAsync));
            return base.OnCloseAsync(cancellationToken);
        }

        protected override Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"{nameof(OnChangeRoleAsync)} - New role : {newRole}");
            return base.OnChangeRoleAsync(newRole, cancellationToken);
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _switch(cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();

                var options = new EventProcessorOptions
                {
                    OnShutdown = OnShutdown,
                    MaxBatchSize = MaxMessageCount,
                    PrefetchCount = MaxMessageCount,
                    InitialPositionProvider = s => EventPosition.FromStart()
                };

                _logger.LogInformation("Create ServiceFabricProcessor with {ConsumerGroup}", _options.ConsumerGroup);

                var processorService = new ServiceFabricProcessor(
                    Context.ServiceName,
                    Context.PartitionId,
                    StateManager,
                    Partition,
                    new EventProcessor(
                        () => _options.UseOperationLogging ? 
                         (IDisposable)_telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents") :
                         DisposableAction.Empty,
                        _logger, _serviceEventSource, _handleEvents),
                    _options.ConnectionString,
                    _options.ConsumerGroup,
                    options);

                await processorService.RunAsync(cancellationToken);
            }
            catch (Exception e) when (cancellationToken.IsCancellationRequested)
            {
                if (e is OperationCanceledException)
                {
                    _logger.LogError(e, nameof(ReceiverService) + "RunAsync canceled. RunAsync for {PartitionId}", Context.PartitionId);
                    _serviceEventSource($"{nameof(ReceiverService)}.RunAsync for {Context.PartitionId} error {e}", new object[0]);
                    throw;
                }

                _logger.LogError(e, nameof(ReceiverService) + "Exception during shutdown. Exception of unexpected type .RunAsync for {PartitionId}", Context.PartitionId);
                _serviceEventSource($"{nameof(ReceiverService)}.RunAsync for {Context.PartitionId} error {e}", new object[0]);
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (Exception e)
            {
                _logger.LogError(e, nameof(ReceiverService) + ".RunAsync for {PartitionId}", Context.PartitionId);
                _serviceEventSource($"{nameof(ReceiverService)}.RunAsync for {Context.PartitionId} error {e}", new object[0]);
                throw;
            }
        }
    }
}
