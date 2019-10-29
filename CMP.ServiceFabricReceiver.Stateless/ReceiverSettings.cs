namespace CMP.ServiceFabricRecevier.Stateless
{
    public class ReceiverSettings
    {
        public string EventHubPath { get; set; }
        public string ConsumerGroup { get; set; }
        public string EventHubConnectionString { get; set; }
        public string StorageConnectionString { get; set; }
        public string LeaseContainerName { get; set; }
        public bool UseOperationLogging { get; set; }
    }
}
