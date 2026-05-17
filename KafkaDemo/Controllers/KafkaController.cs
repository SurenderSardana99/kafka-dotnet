using KafkaDemo.Models;
using KafkaDemo.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaDemo.Controllers;

[ApiController]
[Route("api/kafka")]
[Produces("application/json")]
public class KafkaController : ControllerBase
{
    private readonly IKafkaProducerService _producer;
    private readonly IMessageStore _messageStore;
    private readonly IKafkaAdminService _adminService;
    private readonly ILogger<KafkaController> _logger;

    public KafkaController(
        IKafkaProducerService producer,
        IMessageStore messageStore,
        IKafkaAdminService adminService,
        ILogger<KafkaController> logger)
    {
        _producer = producer;
        _messageStore = messageStore;
        _adminService = adminService;
        _logger = logger;
    }

    // ─── Publish ─────────────────────────────────────────────────────────────

    /// <summary>Publish a message to a Kafka topic.</summary>
    [HttpPost("publish")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> Publish([FromBody] PublishRequest request, CancellationToken ct)
    {
        var topic = request.Topic ?? "demo-topic";
        var success = await _producer.PublishAsync(topic, request.Key, request.Value, ct);

        return success
            ? Ok(new { success = true, topic, key = request.Key, timestamp = DateTime.UtcNow })
            : StatusCode(500, new { success = false, error = "Message delivery failed" });
    }

    /// <summary>Batch publish multiple messages.</summary>
    [HttpPost("publish/batch")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public async Task<IActionResult> PublishBatch([FromBody] List<PublishRequest> requests, CancellationToken ct)
    {
        var results = new List<object>();
        foreach (var req in requests)
        {
            var topic = req.Topic ?? "demo-topic";
            var success = await _producer.PublishAsync(topic, req.Key, req.Value, ct);
            results.Add(new { key = req.Key, topic, success });
        }
        return Ok(new { total = requests.Count, results });
    }

    // ─── Consume / Messages ──────────────────────────────────────────────────

    /// <summary>Get all consumed messages (from in-memory store).</summary>
    [HttpGet("messages")]
    [ProducesResponseType(typeof(IReadOnlyList<KafkaMessage>), StatusCodes.Status200OK)]
    public IActionResult GetMessages([FromQuery] string? topic = null)
    {
        var messages = topic is not null
            ? _messageStore.GetByTopic(topic)
            : _messageStore.GetAll();

        return Ok(new { count = messages.Count, messages });
    }

    /// <summary>Clear consumed messages from the in-memory store.</summary>
    [HttpDelete("messages")]
    public IActionResult ClearMessages()
    {
        _messageStore.Clear();
        return Ok(new { cleared = true });
    }

    // ─── Admin ───────────────────────────────────────────────────────────────

    /// <summary>List all Kafka topics.</summary>
    [HttpGet("topics")]
    [ProducesResponseType(typeof(IEnumerable<string>), StatusCodes.Status200OK)]
    public async Task<IActionResult> ListTopics()
    {
        try
        {
            var topics = await _adminService.ListTopicsAsync();
            return Ok(new { topics });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list topics");
            return StatusCode(500, new { error = "Failed to connect to Kafka. Is it running?" });
        }
    }

    /// <summary>Create a new Kafka topic.</summary>
    [HttpPost("topics")]
    public async Task<IActionResult> CreateTopic(
        [FromQuery] string name,
        [FromQuery] int partitions = 1,
        [FromQuery] short replicationFactor = 1)
    {
        var created = await _adminService.CreateTopicAsync(name, partitions, replicationFactor);
        return created ? Ok(new { created = true, name }) : StatusCode(500, new { created = false });
    }

    /// <summary>Delete a Kafka topic.</summary>
    [HttpDelete("topics/{name}")]
    public async Task<IActionResult> DeleteTopic(string name)
    {
        var deleted = await _adminService.DeleteTopicAsync(name);
        return deleted ? Ok(new { deleted = true, name }) : StatusCode(500, new { deleted = false });
    }

    // ─── Health ──────────────────────────────────────────────────────────────

    /// <summary>Health check endpoint.</summary>
    [HttpGet("health")]
    public IActionResult Health() =>
        Ok(new { status = "healthy", timestamp = DateTime.UtcNow, service = "KafkaDemo API" });
}
