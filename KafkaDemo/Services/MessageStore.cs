using KafkaDemo.Models;
using System.Collections.Concurrent;

namespace KafkaDemo.Services;

/// <summary>
/// Thread-safe in-memory store for consumed messages.
/// In production, replace this with a database or cache.
/// </summary>
public interface IMessageStore
{
    void Add(KafkaMessage message);
    IReadOnlyList<KafkaMessage> GetAll();
    IReadOnlyList<KafkaMessage> GetByTopic(string topic);
    void Clear();
}

public class InMemoryMessageStore : IMessageStore
{
    private readonly ConcurrentQueue<KafkaMessage> _messages = new();
    private const int MaxMessages = 1000; // Ring-buffer style cap

    public void Add(KafkaMessage message)
    {
        _messages.Enqueue(message);

        // Trim if over limit
        while (_messages.Count > MaxMessages)
            _messages.TryDequeue(out _);
    }

    public IReadOnlyList<KafkaMessage> GetAll()
        => _messages.ToList().AsReadOnly();

    public IReadOnlyList<KafkaMessage> GetByTopic(string topic)
        => _messages.Where(m => m.Topic.Equals(topic, StringComparison.OrdinalIgnoreCase))
                    .ToList().AsReadOnly();

    public void Clear() => _messages.Clear();
}
