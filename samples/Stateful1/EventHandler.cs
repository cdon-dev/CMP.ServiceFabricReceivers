using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Stateful1
{
    public static class EventHandler
    {
        public static Task Handle(params EventData[] events)
        {
            return Task.CompletedTask;
        }
    }
}
