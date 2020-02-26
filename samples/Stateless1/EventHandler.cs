using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;
using CMP.ServiceFabricReceiver.Common;
using CMP.ServiceFabricReceiver.Common.Testing;
using System;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;

namespace Stateless1
{
    public static class EventHandler
    {
        public static async Task Handle(string node, CloudTable table, params EventData[] events)
        {
            foreach (var @event in events
                .FilteredOnEvents(Events.GetEventsFromTypes(typeof(TestEvent)))
                .Cast<TestEvent>())
            {
                var insertOrMergeOperation = TableOperation.InsertOrMerge(new TestEntity
                {
                    PartitionKey = @event.SourceId,
                    RowKey = @event.Meta.ContainsKey("eventid") ? @event.Meta["eventid"] : Guid.NewGuid().ToString(),
                    Partition = @event.Meta.ContainsKey("partitionkey") ? @event.Meta["partitionkey"] : Guid.NewGuid().ToString(),
                    Message = @event.Message,
                    Count = @event.Count
                });
                await table.ExecuteAsync(insertOrMergeOperation);
            }
        }
    }

    public class TestEntity : TableEntity
    {
        public string Message { get; set; }

        public string Partition { get; set; }

        public int Count { get; set; }

        public  string Node{ get; set; }
    }
}
