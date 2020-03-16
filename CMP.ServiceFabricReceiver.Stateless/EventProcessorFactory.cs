using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public class EventProcessorFactory : IEventProcessorFactory
    {
        private readonly Func<string,ILogger> _loggerFactory;
        private readonly CancellationToken _cancellationToken;
        private readonly Func<string, Func<EventContext, Task>> f;

        public EventProcessorFactory(
            Func<string, ILogger> loggerFactory,
            CancellationToken cancellationToken,
            Func<string, Func<EventContext, Task>> f)
        {
            _loggerFactory = loggerFactory;
            _cancellationToken = cancellationToken;
            this.f = f;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
         => new EventProcessor(_loggerFactory($"Processor({context.PartitionId})") , _cancellationToken, f);
    }
}
