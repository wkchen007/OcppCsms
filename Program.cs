using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using static OcppMessageHandlers;

var builder = WebApplication.CreateBuilder(args);
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.Converters.Add(
        new JsonStringEnumConverter());
});
var app = builder.Build();
var connectorStates = new ConcurrentDictionary<string, ConnectorState>();
var chargePointConnections = new ConcurrentDictionary<string, bool>();
var transactions = new ConcurrentDictionary<int, Transaction>();
var transactionIdGenerator = new TransactionIdGenerator();

app.UseWebSockets();

app.MapGet("/", () => "Hello World!");

app.MapGet("/api/connectors/{chargePointId}/{connectorId}",
    (string chargePointId, int connectorId) =>
{
    var connectorKey = $"{chargePointId}/{connectorId}";

    if (connectorStates.TryGetValue(connectorKey, out var state))
    {
        return Results.Ok(state);
    }

    return Results.NotFound();
});

app.MapGet("/api/chargepoints/{chargePointId}/connectors",
    (string chargePointId) =>
{
    var prefix = $"{chargePointId}/";

    var connectors = connectorStates
        .Where(x => x.Key.StartsWith(prefix))
        .Select(x => new
        {
            ConnectorId = int.Parse(x.Key.Split('/')[1]),
            Status = x.Value.Status,
            ErrorCode = x.Value.ErrorCode,
            UpdatedAt = x.Value.UpdatedAt
        })
        .OrderBy(x => x.ConnectorId)
        .ToList();

    return Results.Ok(connectors);
});

app.MapGet("/api/chargepoints/{chargePointId}",
    (string chargePointId) =>
{
    if (chargePointConnections.TryGetValue(chargePointId, out var connected))
    {
        return Results.Ok(new
        {
            ChargePointId = chargePointId,
            Connected = connected
        });
    }

    return Results.NotFound();
});

app.MapGet("/api/transactions/{transactionId}",
    (int transactionId) =>
{
    if (transactions.TryGetValue(transactionId, out var transaction))
    {
        return Results.Ok(transaction);
    }

    return Results.NotFound();
});

app.Map("/ocpp/{chargePointId}", async context =>
{
    var chargePointId = context.Request.RouteValues["chargePointId"]?.ToString();

    if (string.IsNullOrWhiteSpace(chargePointId))
    {
        context.Response.StatusCode = 400;
        return;
    }

    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    var requestedProtocols = context.WebSockets.WebSocketRequestedProtocols;
    Console.WriteLine($"Requested protocols: " + $"{string.Join(", ", requestedProtocols)}");

    if (!requestedProtocols.Contains("ocpp1.6"))
    {
        context.Response.StatusCode = 400;
        return;
    }

    var socket = await context.WebSockets.AcceptWebSocketAsync("ocpp1.6");

    chargePointConnections[chargePointId] = true;

    Console.WriteLine($"Charge Point connected: {chargePointId}");


    try
    {
        while (socket.State == WebSocketState.Open)
        {
            var message = await ReceiveMessageAsync(socket, context.RequestAborted);

            if (message == null)
            {
                break;
            }

            Console.WriteLine($"收到完整訊息: {message}");

            // JSON String → JsonNode
            JsonNode? json;

            try
            {
                json = JsonNode.Parse(message);
            }
            catch
            {
                Console.WriteLine("JSON 格式錯誤");
                continue;
            }

            if (json is not JsonArray array)
            {
                Console.WriteLine("不是合法的 OCPP Message");
                continue;
            }

            if (array.Count == 0)
            {
                Console.WriteLine("OCPP Message 是空的");
                continue;
            }

            int messageTypeId;

            try
            {
                messageTypeId = array[0]!.GetValue<int>();
            }
            catch
            {
                Console.WriteLine("MessageTypeId 格式錯誤");
                continue;
            }

            // 判斷 OCPP Message 類型
            switch (messageTypeId)
            {
                case 2: // CALL
                    {
                        Console.WriteLine("這是 CALL");

                        if (array.Count != 4)
                        {
                            Console.WriteLine("CALL 格式錯誤");
                            break;
                        }

                        var uniqueId = array[1]?.GetValue<string>();
                        var action = array[2]?.GetValue<string>();
                        var payload = array[3];

                        Console.WriteLine($"UniqueId: {uniqueId}");
                        Console.WriteLine($"Action: {action}");
                        Console.WriteLine($"Payload: {payload}");

                        switch (action)
                        {
                            case "BootNotification":
                                await HandleBootNotificationAsync(socket, uniqueId, payload, context.RequestAborted);
                                break;
                            case "Heartbeat":
                                await HandleHeartbeatAsync(socket, uniqueId, context.RequestAborted);
                                break;
                            case "StatusNotification":
                                await HandleStatusNotificationAsync(socket, chargePointId, uniqueId, payload, connectorStates, context.RequestAborted);
                                break;
                            case "Authorize":
                                await HandleAuthorizeAsync(socket, uniqueId, payload, context.RequestAborted);
                                break;
                            case "StartTransaction":
                                await HandleStartTransactionAsync(socket, chargePointId, uniqueId, payload, transactionIdGenerator, transactions, context.RequestAborted);
                                break;
                            case "MeterValues":
                                await HandleMeterValuesAsync(socket, chargePointId, uniqueId, payload, transactions, context.RequestAborted);
                                break;
                            default:
                                await SendCallErrorAsync(socket, uniqueId, "NotSupported", "Action is not supported", context.RequestAborted);
                                break;
                        }
                        break;
                    }
                case 3: // CALLRESULT
                    Console.WriteLine("這是 CALLRESULT");
                    break;

                case 4: // CALLERROR
                    Console.WriteLine("這是 CALLERROR");
                    break;

                default:
                    Console.WriteLine(
                        $"未知的 MessageTypeId: {messageTypeId}");
                    break;
            }
        }

        if (socket.State == WebSocketState.CloseReceived)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, context.RequestAborted);
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"Charge Point connection aborted: {chargePointId}");
    }
    finally
    {
        chargePointConnections[chargePointId] = false;
        Console.WriteLine($"Charge Point disconnected: {chargePointId}");
    }
});

app.Run();
