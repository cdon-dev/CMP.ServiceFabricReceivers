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
        private readonly ReceiverSettings _settings;
        private readonly Action<string, object[]> _serviceEventSource;
        private readonly Func<IReadOnlyCollection<EventData>, CancellationToken, Task> _handleEvents;
        private readonly Func<CancellationToken, Task> _switch;
        private readonly EventProcessorOptions _options;
        private readonly EventProcessorHost _host;

        public ReceiverService(
            StatelessServiceContext serviceContext,
            ILogger logger,
            TelemetryClient telemetryClient,
            ReceiverSettings settings,
            Action<string, object[]> serviceEventSource,
            Func<IReadOnlyCollection<EventData>, CancellationToken, Task> handleEvents,
            Func<CancellationToken, Task> @switch,
            EventProcessorOptions options)
             : base(serviceContext)
        {
            _logger = logger;
            _telemetryClient = telemetryClient;
            _settings = settings;
            _serviceEventSource = serviceEventSource;
            _handleEvents = handleEvents;
            _switch = @switch;
            _options = options;

            _host = new EventProcessorHost(
                $"{_settings.HostName ?? serviceContext.ServiceTypeName}-{serviceContext.NodeContext.NodeName}",
                _settings.EventHubPath,
                _settings.ConsumerGroup,
                _settings.EventHubConnectionString,
                _settings.StorageConnectionString,
                _settings.LeaseContainerName);
        }

        protected override Task OnOpenAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnOpenAsync));
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
                    ct => ReceiverExceptions.ExecuteAsync(ct, _logger, Context.PartitionId.ToString(),
                    async token =>
                    {
                        await _switch(token);
                        await _host.RegisterEventProcessorFactoryAsync(
                            new EventProcessorFactory(
                                 () => _settings.UseOperationLogging ? //capture option :! ?
                                 (IDisposable)_telemetryClient.StartOperation<RequestTelemetry>("ProcessEvents") :
                                 DisposableAction.Empty,
                            _logger, token, _serviceEventSource, partitionId => _handleEvents), _options);
                    }));
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
