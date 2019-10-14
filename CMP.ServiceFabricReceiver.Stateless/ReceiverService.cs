using CMP.ServiceFabricReceiver.Common;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using Microsoft.ServiceFabric.Services.Runtime;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public class ReceiverService : StatelessService
    {
        private readonly ILogger _logger;
        private readonly TelemetryClient _telemetryClient;
        private readonly ReceiverOptions _options;
        private readonly Action<string, object[]> _serviceEventSource;
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;
        private readonly Func<CancellationToken, Task> _switch;
        private EventProcessorHost _host;

        public ReceiverService(
            StatelessServiceContext serviceContext,
            ILogger logger,
            TelemetryClient telemetryClient,
            ReceiverOptions options,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents,
            Func<CancellationToken, Task> @switch)
             : base(serviceContext)
        {
            _logger = logger;
            _telemetryClient = telemetryClient;
            _options = options;
            _serviceEventSource = serviceEventSource;
            _handleEvents = handleEvents;
            _switch = @switch;
        }

        protected override Task OnOpenAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnOpenAsync));
            _host = new EventProcessorHost(
                _options.EventHubPath,
                _options.ConsumerGroup,
                _options.EventHubConnectionString,
                _options.StorageConnectionString,
                _options.LeaseContainerName);

            return base.OnOpenAsync(cancellationToken);
        }

        protected override void OnAbort()
        {
            _logger.LogInformation(nameof(OnAbort));
            base.OnAbort();
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                await Execution.ExecuteAsync(cancellationToken, 
                    _logger, _serviceEventSource, 
                    nameof(ReceiverService), Context.PartitionId.ToString(),
                    async ct =>
                    {
                        await _switch(cancellationToken);
                        await _host.RegisterEventProcessorFactoryAsync(
                            new EventProcessorFactory(
                                 () => _options.UseOperationLogging ? //capture option :! ?
                                 (IDisposable)_telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents") :
                                 DisposableAction.Empty,
                            _logger, cancellationToken, _serviceEventSource, _handleEvents));
                    });
            }
            catch (FabricTransientException e)
            {
                _logger.LogError(e, nameof(ReceiverService) + "Exception .RunAsync for {PartitionId}", Context.PartitionId);
            }
        }
        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnCloseAsync));
            await _host.UnregisterEventProcessorAsync();
            await base.OnCloseAsync(cancellationToken);
        }

    }
}
