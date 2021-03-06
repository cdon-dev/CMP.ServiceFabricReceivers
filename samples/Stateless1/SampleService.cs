﻿using CMP.ServiceFabricReceiver.Common;
using CMP.ServiceFabricRecevier.Stateless;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace Stateless1
{
    public class SampleService : ReceiverService
    {
            public SampleService(StatelessServiceContext serviceContext, 
            ILogger logger, ReceiverSettings settings, 
            Action<string, object[]> serviceEventSource,
            Func<CancellationToken, Task> @switch,
            Func<string, Func<EventContext, Task>> f,
            EventProcessorOptions options) 
            : base(serviceContext, logger, settings, serviceEventSource, @switch, f, options)
        {
        }
    }
}
