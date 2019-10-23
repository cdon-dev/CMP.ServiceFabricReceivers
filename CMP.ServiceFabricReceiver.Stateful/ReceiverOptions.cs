namespace CMP.ServiceFabricReceiver.Stateful
{
    public class ReceiverOptions
    {
        public string ConnectionString { get; set; }
        public string ConsumerGroup { get; set; }
        public bool UseOperationLogging { get; set; }
        public int ExceptionDelaySeconds { get; set; } = 1;
    }
}
