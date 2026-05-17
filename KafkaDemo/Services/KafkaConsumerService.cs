using Confluent.Kafka;
using KafkaDemo.Models;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Services;

/// <summary>
/// Long-running background consumer. Reads messages from Kafka and processes them.
/// Runs as an IHostedService — starts with the app, stops gracefully on shutdown.
/// </summary>
public class KafkaConsumerService : BackgroundService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IMessageStore _messageStore;

    public KafkaConsumerService(
        IOptions<KafkaSettings> settings,
        ILogger<KafkaConsumerService> logger,
        IMessageStore messageStore)
    {
        _settings = settings.Value;
        _logger = logger;
        _messageStore = messageStore;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run in a thread-pool thread so we don't block the host startup
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId,
            AutoOffsetReset = _settings.AutoOffsetReset.ToLower() switch
            {
                "latest" => AutoOffsetReset.Latest,
                _ => AutoOffsetReset.Earliest
            },
            // Manual commit for reliability
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            // Heartbeat & session
            HeartbeatIntervalMs = 3000,
            SessionTimeoutMs = 30000,
            MaxPollIntervalMs = 300000
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) =>
                _logger.LogError("Kafka consumer error: {Reason}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
                _logger.LogInformation("Assigned partitions: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .SetPartitionsRevokedHandler((c, partitions) =>
                _logger.LogInformation("Revoked partitions: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .Build();

        consumer.Subscribe(_settings.TopicName);
        _logger.LogInformation("Consumer started. Subscribed to topic: {Topic}", _settings.TopicName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Blocks up to 1s waiting for a message
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is null) continue;

                _logger.LogInformation(
                    "Consumed message | Topic: {Topic} | Partition: {Partition} | Offset: {Offset} | Key: {Key} | Value: {Value}",
                    result.Topic, result.Partition.Value, result.Offset.Value,
                    result.Message.Key, result.Message.Value);

                // Process the message
                ProcessMessage(result.Message.Key, result.Message.Value, result.Topic);

                // Manual commit after successful processing
                consumer.StoreOffset(result);
                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Consume error: {Reason}", ex.Error.Reason);
                // Decide: continue or throw based on severity
                if (ex.Error.IsFatal) break;
            }
            catch (OperationCanceledException)
            {
                // Shutdown requested
                break;
            }
        }

        _logger.LogInformation("Consumer shutting down...");
        consumer.Close(); // Commits offsets and leaves the consumer group cleanly
    }

    private void ProcessMessage(string key, string value, string topic)
    {
        // ── Add your business logic here ───────────────────────────────────
        // e.g., persist to DB, call another service, publish to another topic

        var msg = new KafkaMessage
        {
            Key = key,
            Value = value,
            Topic = topic,
            Timestamp = DateTime.UtcNow
        };

        _messageStore.Add(msg);
    }
}
