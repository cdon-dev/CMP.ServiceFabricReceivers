using Microsoft.Azure.EventHubs.Processor;

namespace CMP.ServiceFabricRecevier.Stateless
{
    public class ReceiverSettings
    {
        public string HostName { get; set; }
        public string EventHubPath { get; set; }
        public string ConsumerGroup { get; set; }
        public string EventHubConnectionString { get; set; }
        public string StorageConnectionString { get; set; }
        public string LeaseContainerName { get; set; }
        public bool UseOperationLogging { get; set; }

        public EventProcessorHost ToHost(string nodeName = "unknown") 
            => new EventProcessorHost(
                    $"{HostName}-{nodeName}",
                     EventHubPath,
                     ConsumerGroup,
                     EventHubConnectionString,
                     StorageConnectionString,
                     LeaseContainerName);
    }
}
