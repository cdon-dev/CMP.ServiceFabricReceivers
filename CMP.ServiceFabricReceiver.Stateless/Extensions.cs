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
            string partition,
            Func<string, Func<EventContext, Task>> f)
            => Composition.Combine(
               Features.Execution(logger, serviceEventSource, nameof(ReceiverService), partition),
               Features.ReceiverExceptions(logger, partition),
               Features.Run(ct => host.RegisterEventProcessorFactoryAsync(new EventProcessorFactory(logger, ct, f), options))
               )(cancellationToken);
    }
}
