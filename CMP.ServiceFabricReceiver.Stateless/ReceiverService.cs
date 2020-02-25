using CMP.ServiceFabricReceiver.Common;
using Microsoft.ApplicationInsights;
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
        private readonly ReceiverSettings _settings;
        private readonly Action<string, object[]> _serviceEventSource;
        private readonly Func<string, Func<EventContext, Task>> _f;
        private readonly Func<CancellationToken, Task> _switch;
        private readonly EventProcessorOptions _options;
        private readonly EventProcessorHost _host;

        public ReceiverService(
            StatelessServiceContext serviceContext,
            ILogger logger,
            ReceiverSettings settings,
            Action<string, object[]> serviceEventSource,
            Func<CancellationToken, Task> @switch,
            Func<string, Func<EventContext, Task>> f,
            EventProcessorOptions options)
             : base(serviceContext)
        {
            _logger = logger;
            _settings = settings;
            _serviceEventSource = serviceEventSource;
            _f = f;
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
                await _switch(cancellationToken);
                await RunAsync(_host, _logger, _options, cancellationToken, _serviceEventSource, Context.PartitionId.ToString() ,_f);
            }
            catch (FabricTransientException e)
            {
                _logger.LogError(e, nameof(ReceiverService) + "Exception .RunAsync for {PartitionId}", Context.PartitionId);
            }
        }

        public static Task RunAsync(
            EventProcessorHost host,
            ILogger logger,
            EventProcessorOptions options,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            string partition,
            Func<string, Func<EventContext, Task>> f)
            => Execution.ExecuteAsync(
                cancellationToken,
                logger,
                serviceEventSource,
                nameof(ReceiverService),
                partition,
                ct => ReceiverExceptions.ExecuteAsync(ct, logger, "none", t =>
                            host.RegisterEventProcessorFactoryAsync(new EventProcessorFactory(logger, t, f), options)
                        )
                );

        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnCloseAsync));
            await _host.UnregisterEventProcessorAsync();
            await base.OnCloseAsync(cancellationToken);
        }

    }
}
