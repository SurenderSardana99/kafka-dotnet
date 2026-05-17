namespace KafkaDemo.Models;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "kafka-demo-group";
    public string TopicName { get; set; } = "demo-topic";
    public string AutoOffsetReset { get; set; } = "Earliest";
}
