using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDemo.Models;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Services;

public interface IKafkaAdminService
{
    Task<IEnumerable<string>> ListTopicsAsync();
    Task<bool> CreateTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1);
    Task<bool> DeleteTopicAsync(string topicName);
}

public class KafkaAdminService : IKafkaAdminService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaAdminService> _logger;

    public KafkaAdminService(IOptions<KafkaSettings> settings, ILogger<KafkaAdminService> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task<IEnumerable<string>> ListTopicsAsync()
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _settings.BootstrapServers }).Build();

        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        return metadata.Topics.Select(t => t.Topic);
    }

    public async Task<bool> CreateTopicAsync(string topicName, int partitions = 1, short replicationFactor = 1)
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _settings.BootstrapServers }).Build();
        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = replicationFactor
                }
            });
            _logger.LogInformation("Topic '{Topic}' created.", topicName);
            return true;
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            _logger.LogWarning("Topic '{Topic}' already exists.", topicName);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create topic '{Topic}'.", topicName);
            return false;
        }
    }

    public async Task<bool> DeleteTopicAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _settings.BootstrapServers }).Build();
        try
        {
            await adminClient.DeleteTopicsAsync(new[] { topicName });
            _logger.LogInformation("Topic '{Topic}' deleted.", topicName);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete topic '{Topic}'.", topicName);
            return false;
        }
    }
}
