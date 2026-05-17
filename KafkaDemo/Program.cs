using KafkaDemo.Models;
using KafkaDemo.Services;

var builder = WebApplication.CreateBuilder(args);

// ─── Configuration ────────────────────────────────────────────────────────────
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("Kafka"));

// ─── Kafka Services ───────────────────────────────────────────────────────────
builder.Services.AddSingleton<IMessageStore, InMemoryMessageStore>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddSingleton<IKafkaAdminService, KafkaAdminService>();

// Consumer runs as a hosted background service
builder.Services.AddHostedService<KafkaConsumerService>();

// ─── Web / API ────────────────────────────────────────────────────────────────
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new()
    {
        Title = "Kafka Demo API",
        Version = "v1",
        Description = "Kafka + .NET 8 producer/consumer demo with REST API"
    });
});

// ─── CORS (for local dev / Codespaces) ───────────────────────────────────────
builder.Services.AddCors(options =>
    options.AddDefaultPolicy(policy =>
        policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();

// ─── Middleware ───────────────────────────────────────────────────────────────
app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "Kafka Demo API v1");
    c.RoutePrefix = string.Empty; // Swagger at root "/"
});

app.UseCors();
app.UseRouting();
app.MapControllers();

// ─── Minimal API endpoint (alternative to controller) ────────────────────────
app.MapGet("/ping", () => Results.Ok(new { pong = true, time = DateTime.UtcNow }))
   .WithTags("Health");

app.Run();
