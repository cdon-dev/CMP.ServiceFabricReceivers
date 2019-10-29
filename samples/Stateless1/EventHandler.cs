using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;

namespace Stateless1
{
    public static class EventHandler
    {
        public static Task Handle(params EventData[] events)
        {
            return Task.CompletedTask;
        }
    }
}
