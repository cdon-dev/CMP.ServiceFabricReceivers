using System.Collections.Generic;

namespace CMP.ServiceFabricReceiver.Common.Testing
{
    public class TestEvent : IEvent
    {
        public TestEvent()
        { }

        public TestEvent(string sourceId)
        {
            SourceId = sourceId;
        }
        public string SourceId { get; set; }

        public string Message { get; set; }

        public int Count { get; set; }

        public IDictionary<string, string> Meta { get; set; } = new Dictionary<string, string>();
    }
}
