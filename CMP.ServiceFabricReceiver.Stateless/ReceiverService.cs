using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using Microsoft.ServiceFabric.Services.Runtime;
using System;
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
                await RunAsync(_host, _logger, _options, cancellationToken, _serviceEventSource, Context.PartitionId.ToString(), _f);
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
            => Composition.Combine(
                    Features.Execution(logger, serviceEventSource, nameof(ReceiverService), partition),
                    Features.ReceiverExceptions(logger, partition),
                    Features.Run(ct => host.RegisterEventProcessorFactoryAsync(new EventProcessorFactory(logger, ct, f), options))
                    )(cancellationToken);

        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OnCloseAsync));
            await _host.UnregisterEventProcessorAsync();
            await base.OnCloseAsync(cancellationToken);
        }
    }

    public static class Features
    {
        public static Func<Func<CancellationToken, Task>, Func<CancellationToken, Task>> Execution(
            ILogger logger, Action<string, object[]> serviceEventSource,
            string serviceName,
            string partition)
            => f => ct => ServiceFabricReceiver.Common.Execution.ExecuteAsync(ct, logger, serviceEventSource, serviceName, partition, f);

        public static Func<Func<CancellationToken, Task>, Func<CancellationToken, Task>> ReceiverExceptions(
            ILogger logger, string partition)
            => f => ct => Stateless.ReceiverExceptions.ExecuteAsync(ct, logger, partition, f);

        public static Func<Func<CancellationToken, Task>, Func<CancellationToken, Task>> Run(Func<CancellationToken, Task> r)
            => f => async ct =>
            {
                await r(ct);
                await f(ct);
            };
    }
}
