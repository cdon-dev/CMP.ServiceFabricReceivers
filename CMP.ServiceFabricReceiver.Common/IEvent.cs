using System.Collections.Generic;

namespace CMP.ServiceFabricReceiver.Common
{
    public interface IEvent
    {
        string SourceId { get; }

        IDictionary<string, string> Meta { get; set; }
    }
}
