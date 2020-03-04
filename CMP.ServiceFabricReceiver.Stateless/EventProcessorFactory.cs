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
        private readonly ILogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Func<string, Func<EventContext, Task>> f;

        public EventProcessorFactory(
            ILogger logger,
            CancellationToken cancellationToken,
            Func<string, Func<EventContext, Task>> f)
        {
            _logger = logger;
            _cancellationToken = cancellationToken;
            this.f = f;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
         => new EventProcessor(_logger, _cancellationToken, f);
    }
}
