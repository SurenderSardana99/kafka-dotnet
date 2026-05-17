# Kafka + .NET 8 C# Demo

A complete Kafka integration with .NET 8 featuring a **Producer**, **Consumer** (background service), and a **REST API** — ready to run on GitHub Codespaces.

## Project Structure

```
KafkaDotNet/
├── .devcontainer/
│   └── devcontainer.json          # Codespaces config
├── docker-compose.yml             # Kafka + Zookeeper + Kafka UI
├── requests.http                  # REST Client test file
└── KafkaDemo/
    ├── KafkaDemo.csproj
    ├── Program.cs                 # DI wiring + app startup
    ├── appsettings.json           # Kafka config
    ├── Controllers/
    │   └── KafkaController.cs     # REST endpoints
    ├── Models/
    │   ├── KafkaSettings.cs
    │   └── KafkaMessage.cs
    └── Services/
        ├── KafkaProducerService.cs   # Confluent producer wrapper
        ├── KafkaConsumerService.cs   # BackgroundService consumer
        ├── KafkaAdminService.cs      # Topic management
        └── MessageStore.cs           # In-memory consumed message store
```

## Running on GitHub Codespaces

### 1. Open in Codespaces
Push this repo to GitHub, then click **Code → Codespaces → Create codespace**.

### 2. Start Kafka
```bash
docker compose up -d
```
Wait ~15 seconds for Kafka to be ready.

### 3. Run the API
```bash
cd KafkaDemo
dotnet run
```

### 4. Open Swagger UI
The Codespace will prompt you to open port **5000**. Navigate to the forwarded URL — Swagger UI loads at `/`.

### 5. Test with REST Client
Open `requests.http` in VS Code and click **Send Request** on any block.

---

## Running Locally

### Prerequisites
- [.NET 8 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)

```bash
# Start Kafka
docker compose up -d

# Run the app
cd KafkaDemo
dotnet run
```

Open http://localhost:5000 for Swagger UI.  
Open http://localhost:8080 for Kafka UI.

---

## Key Concepts

| Component | File | Description |
|---|---|---|
| **Producer** | `KafkaProducerService.cs` | Publishes messages. Idempotent, retries, snappy compression |
| **Consumer** | `KafkaConsumerService.cs` | `BackgroundService` — polls Kafka continuously, manual commit |
| **Admin** | `KafkaAdminService.cs` | Create/list/delete topics |
| **Store** | `MessageStore.cs` | Thread-safe in-memory store for consumed messages |
| **API** | `KafkaController.cs` | REST endpoints: publish, batch, messages, topics |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/kafka/publish` | Publish single message |
| `POST` | `/api/kafka/publish/batch` | Publish multiple messages |
| `GET` | `/api/kafka/messages` | Get consumed messages |
| `DELETE` | `/api/kafka/messages` | Clear message store |
| `GET` | `/api/kafka/topics` | List all topics |
| `POST` | `/api/kafka/topics` | Create topic |
| `DELETE` | `/api/kafka/topics/{name}` | Delete topic |
| `GET` | `/api/kafka/health` | Health check |

## Configuration (appsettings.json)

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "GroupId": "kafka-demo-group",
    "TopicName": "demo-topic",
    "AutoOffsetReset": "Earliest"
  }
}
```

## Production Considerations

- Replace `InMemoryMessageStore` with a real database (EF Core, Redis, etc.)
- Add authentication to the REST API
- Use Avro/Protobuf serialization instead of plain strings
- Set up Schema Registry for schema evolution
- Add dead-letter queue (DLQ) for failed messages
- Enable SSL/SASL for Kafka connections
