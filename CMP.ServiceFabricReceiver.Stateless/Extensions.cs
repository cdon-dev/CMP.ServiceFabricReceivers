using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public static class Extensions
    {
        public static Task RunAsync(
            this EventProcessorHost host,
            ILogger logger,
            EventProcessorOptions options,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            string serviceFabricPartition,
            Func<string, Func<EventContext, Task>> f)
             => RunAsync(host, _ => logger, options, cancellationToken, serviceEventSource, serviceFabricPartition, f);

        public static Task RunAsync(
            this EventProcessorHost host,
            Func<string, ILogger> loggerFactory,
            EventProcessorOptions options,
            CancellationToken cancellationToken,
            Action<string, object[]> serviceEventSource,
            string serviceFabricPartition,
            Func<string, Func<EventContext, Task>> f)
        {
            var logger = loggerFactory($"{host.HostName}.{nameof(RunAsync)}.{serviceFabricPartition}");
            return Composition.Combine(
                   Features.Execution(logger, serviceEventSource, nameof(ReceiverService), serviceFabricPartition),
                   Features.ReceiverExceptions(logger, serviceFabricPartition),
                   Features.Run(ct => host.RegisterEventProcessorFactoryAsync(new EventProcessorFactory(loggerFactory, ct, f), options))
                   )(cancellationToken);
        }
    }
}
