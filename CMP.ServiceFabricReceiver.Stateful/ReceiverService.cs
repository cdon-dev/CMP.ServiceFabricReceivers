using CMP.ServiceFabricReceiver.Common;
using Microsoft.ApplicationInsights;
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

        private readonly EventHandlerCreator _eventHandlerCreator;
        private readonly Func<CancellationToken, Task> _switch;
        private readonly Func<string, EventPosition> _initialPositionProvider;
        private readonly ILogger _logger;
        private readonly TelemetryClient _telemetryClient;
        private readonly ReceiverOptions _options;

        public delegate Func<IReadOnlyCollection<EventData>, CancellationToken, Task> EventHandlerCreator(string partitionId);

        public ReceiverService(
          StatefulServiceContext context,
          ILogger logger,
          TelemetryClient telemetryClient,
          ReceiverOptions options,
          Action<string, object[]> serviceEventSource,
          EventHandlerCreator eventHandlerCreator)
          : this(context, logger, telemetryClient, options, serviceEventSource, eventHandlerCreator, ct => Task.CompletedTask)
        { }

        public ReceiverService(
            StatefulServiceContext context,
            ILogger logger,
            TelemetryClient telemetryClient,
            ReceiverOptions options,
            Action<string, object[]> serviceEventSource,
            EventHandlerCreator eventHandlerCreator,
            Func<CancellationToken, Task> @switch)
            : this(context, logger, telemetryClient, options, serviceEventSource, eventHandlerCreator, @switch,
              s => EventPosition.FromStart())
        { }

        public ReceiverService(
            StatefulServiceContext context,
            ILogger logger,
            TelemetryClient telemetryClient,
            ReceiverOptions options,
            Action<string, object[]> serviceEventSource,
            EventHandlerCreator eventHandlerCreator,
            Func<CancellationToken, Task> @switch,
            Func<string, EventPosition> initialPositionProvider)
             : base(context)
        {
            _telemetryClient = telemetryClient;
            _options = options;
            _logger = logger;
            _serviceEventSource = serviceEventSource;
            _eventHandlerCreator = eventHandlerCreator;
            _switch = @switch;
            _initialPositionProvider = initialPositionProvider;
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

        protected override void OnAbort()
        {
            _logger.LogInformation(nameof(OnAbort));
            base.OnAbort();
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            cancellationToken.Register(() => _logger.LogInformation($"{nameof(RunAsync)} is being cancelled"));

            try
            {
                await Execution
                    .ExecuteAsync(cancellationToken,
                    _logger, _serviceEventSource,
                    nameof(ReceiverService), Context.PartitionId.ToString(),
                    async ct =>
                    {
                        await _switch(cancellationToken);

                        cancellationToken.ThrowIfCancellationRequested();

                        var options = new EventProcessorOptions
                        {
                            OnShutdown = OnShutdown,
                            MaxBatchSize = MaxMessageCount,
                            PrefetchCount = MaxMessageCount,
                            InitialPositionProvider = s =>
                            {
                                _logger.LogInformation("Using InitialPositionProvider for {s}", s);
                                return _initialPositionProvider(s);
                            }
                        };

                        _logger.LogInformation("Create ServiceFabricProcessor with {ConsumerGroup}", _options.ConsumerGroup);

                        var processorService = new ServiceFabricProcessor(
                            Context.ServiceName,
                            Context.PartitionId,
                            StateManager,
                            Partition,
                            CreateProcessor(_options, _telemetryClient, _logger, _serviceEventSource, _eventHandlerCreator),
                            _options.ConnectionString,
                            _options.ConsumerGroup,
                            options);

                        await processorService.RunAsync(cancellationToken);
                    });
            }
            catch (FabricTransientException e)
            {
                _logger.LogError(e, nameof(ReceiverService) + "Exception .RunAsync for {PartitionId}", Context.PartitionId);
            }
        }

        public virtual EventProcessor CreateProcessor(
            ReceiverOptions options,
            TelemetryClient telemetryClient,
            ILogger logger,
            Action<string, object[]> serviceEventSource,
            EventHandlerCreator eventHandlerCreator)
            => new EventProcessor(_telemetryClient.UseOperationLogging(options.UseOperationLogging),
            logger, serviceEventSource, eventHandlerCreator, options.ExceptionDelaySeconds);
    }
}
