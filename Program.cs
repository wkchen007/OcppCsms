using System.Collections.Concurrent;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

var connectorStates = new ConcurrentDictionary<string, ConnectorState>();
var chargePointConnections = new ConcurrentDictionary<string, bool>();
var transactions = new ConcurrentDictionary<int, Transaction>();
var transactionIdGenerator = new TransactionIdGenerator();

builder.Services.AddSingleton(connectorStates);
builder.Services.AddSingleton(chargePointConnections);
builder.Services.AddSingleton(transactions);
builder.Services.AddSingleton(transactionIdGenerator);

builder.Services.AddControllers().AddJsonOptions(options =>
{
    options.JsonSerializerOptions.Converters.Add(
        new JsonStringEnumConverter());
});

var app = builder.Build();
var heartbeatInterval = builder.Configuration.GetValue("Ocpp:HeartbeatInterval", 300);

app.UseWebSockets();

app.MapControllers();

app.MapGet("/", () => "Hello World!");

app.MapOcppWebSocket(connectorStates, chargePointConnections, transactions, transactionIdGenerator, heartbeatInterval);

app.Run();
