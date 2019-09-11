namespace CMP.ServiceFabricRecevier.Stateless
{
    public class ReceiverOptions
    {
        public string EventHubPath { get; set; }
        public string ConsumerGroup { get; set; }
        public string EventHubConnectionString { get; set; }
        public string StorageConnectionString { get; set; }
        public string LeaseContainerName { get; set; }
    }
}
