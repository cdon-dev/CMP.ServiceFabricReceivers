using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;
using CMP.ServiceFabricReceiver.Common;
using CMP.ServiceFabricReceiver.Common.Testing;
using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Stateless1
{
    public static class EventHandler
    {
        public static async Task Handle(string node, CloudTable table, params EventData[] events)
        {
            foreach (var item in events)
            {
                var @event = item.ToEvent(typeof(TestEvent)) as TestEvent;
                var insertOrMergeOperation = TableOperation.InsertOrMerge(new TestEntity
                {
                    PartitionKey = @event.SourceId,
                    RowKey = $"{@event.Count}-{Guid.NewGuid()}",
                    Partition = item.SystemProperties.PartitionKey,
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
