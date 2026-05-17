namespace KafkaDemo.Models;

public class KafkaMessage
{
    public string Key { get; set; } = Guid.NewGuid().ToString();
    public string Value { get; set; } = string.Empty;
    public string Topic { get; set; } = "demo-topic";
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

public class PublishRequest
{
    public string Key { get; set; } = Guid.NewGuid().ToString();
    public string Value { get; set; } = string.Empty;
    public string? Topic { get; set; }
}
