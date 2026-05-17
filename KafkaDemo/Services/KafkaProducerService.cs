using Confluent.Kafka;
using KafkaDemo.Models;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Services;

// ─── Interface ───────────────────────────────────────────────────────────────

public interface IKafkaProducerService
{
    Task<bool> PublishAsync(string topic, string key, string value, CancellationToken ct = default);
    Task<bool> PublishAsync(KafkaMessage message, CancellationToken ct = default);
}

// ─── Implementation ──────────────────────────────────────────────────────────

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly KafkaSettings _settings;

    public KafkaProducerService(
        IOptions<KafkaSettings> settings,
        ILogger<KafkaProducerService> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            // Reliability settings
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
            // Performance settings
            LingerMs = 5,
            BatchSize = 16384,
            CompressionType = CompressionType.Snappy
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) =>
                _logger.LogError("Kafka producer error: {Reason} (IsFatal: {IsFatal})", e.Reason, e.IsFatal))
            .SetLogHandler((_, log) =>
                _logger.LogDebug("[Kafka Producer] {Message}", log.Message))
            .Build();
    }

    public async Task<bool> PublishAsync(string topic, string key, string value, CancellationToken ct = default)
    {
        try
        {
            var message = new Message<string, string> { Key = key, Value = value };
            var result = await _producer.ProduceAsync(topic, message, ct);

            _logger.LogInformation(
                "Message delivered to {Topic} [{Partition}] @ offset {Offset}",
                result.Topic, result.Partition.Value, result.Offset.Value);

            return result.Status == PersistenceStatus.Persisted;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to deliver message to {Topic}: {Reason}", topic, ex.Error.Reason);
            return false;
        }
    }

    public Task<bool> PublishAsync(KafkaMessage message, CancellationToken ct = default)
        => PublishAsync(message.Topic, message.Key, message.Value, ct);

    public void Dispose()
    {
        // Flush pending messages before shutdown (5s timeout)
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
