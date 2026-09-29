using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

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

static async Task SendMessageAsync(WebSocket socket, JsonArray message, CancellationToken cancellationToken)
{
    var json = message.ToJsonString();

    var bytes = Encoding.UTF8.GetBytes(json);

    await socket.SendAsync(
        new ArraySegment<byte>(bytes),
        WebSocketMessageType.Text,
        true,
        cancellationToken);

    Console.WriteLine($"送出訊息: {json}");
}

static async Task<string?> ReceiveMessageAsync(WebSocket socket, CancellationToken cancellationToken)
{
    var buffer = new byte[4096];

    using var stream = new MemoryStream();

    WebSocketReceiveResult result;

    do
    {
        result = await socket.ReceiveAsync(
            new ArraySegment<byte>(buffer),
            cancellationToken);

        if (result.MessageType == WebSocketMessageType.Close)
        {
            return null;
        }

        if (result.MessageType != WebSocketMessageType.Text)
        {
            return null;
        }

        stream.Write(
            buffer,
            0,
            result.Count);

    } while (!result.EndOfMessage);

    return Encoding.UTF8.GetString(
        stream.ToArray());
}

static async Task SendCallResultAsync(WebSocket socket, string? uniqueId, JsonObject payload, CancellationToken cancellationToken)
{
    var response = new JsonArray
    {
        3,
        uniqueId,
        payload
    };

    await SendMessageAsync(socket, response, cancellationToken);
}

static async Task SendCallErrorAsync(WebSocket socket, string? uniqueId, string errorCode, string errorDescription, CancellationToken cancellationToken)
{
    var errorResponse = new JsonArray
    {
        4,
        uniqueId,
        errorCode,
        errorDescription,
        new JsonObject()
    };

    await SendMessageAsync(socket, errorResponse, cancellationToken);
}

static async Task HandleHeartbeatAsync(WebSocket socket, string? uniqueId, CancellationToken cancellationToken)
{
    var responsePayload = new JsonObject
    {
        ["currentTime"] = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'")
    };

    await SendCallResultAsync(socket, uniqueId, responsePayload, cancellationToken);
}

static async Task HandleBootNotificationAsync(WebSocket socket, string? uniqueId, JsonNode? payload, CancellationToken cancellationToken)
{
    if (payload is not JsonObject payloadObject)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "BootNotification payload must be an object", cancellationToken);
        return;
    }

    string? vendor;
    string? model;

    try
    {
        vendor = payloadObject["chargePointVendor"]?.GetValue<string>();
        model = payloadObject["chargePointModel"]?.GetValue<string>();
    }
    catch
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "BootNotification field type is invalid", cancellationToken);
        return;
    }

    if (string.IsNullOrWhiteSpace(vendor) || string.IsNullOrWhiteSpace(model))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "BootNotification required field is missing", cancellationToken);
        return;
    }

    Console.WriteLine($"Vendor: {vendor}");
    Console.WriteLine($"Model: {model}");

    var responsePayload = new JsonObject
    {
        ["status"] = "Accepted",
        ["currentTime"] = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ["interval"] = 300
    };

    await SendCallResultAsync(socket, uniqueId, responsePayload, cancellationToken);
}

static async Task HandleStatusNotificationAsync(WebSocket socket, string? chargePointId, string? uniqueId, JsonNode? payload,
                                                ConcurrentDictionary<string, ConnectorState> connectorStates, CancellationToken cancellationToken)
{
    if (payload is not JsonObject payloadObject)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StatusNotification payload must be an object", cancellationToken);
        return;
    }

    int connectorId;
    string? errorCode;
    string? status;

    try
    {
        connectorId = payloadObject["connectorId"]!.GetValue<int>();
        errorCode = payloadObject["errorCode"]?.GetValue<string>();
        status = payloadObject["status"]?.GetValue<string>();
    }
    catch
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StatusNotification field type is invalid", cancellationToken);
        return;
    }

    if (string.IsNullOrWhiteSpace(errorCode) || string.IsNullOrWhiteSpace(status))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StatusNotification required field is missing", cancellationToken);
        return;
    }

    if (connectorId < 0)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StatusNotification connectorId is invalid", cancellationToken);
        return;
    }

    if (!Enum.TryParse<ConnectorStatus>(status, out var connectorStatus))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StatusNotification status is invalid", cancellationToken);
        return;
    }

    if (!Enum.TryParse<ChargePointErrorCode>(errorCode, out var chargePointErrorCode))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StatusNotification errorCode is invalid", cancellationToken);
        return;
    }

    Console.WriteLine($"ChargePointId: {chargePointId}");
    Console.WriteLine($"ConnectorId: {connectorId}");
    Console.WriteLine($"ErrorCode: {chargePointErrorCode}");
    Console.WriteLine($"Status: {connectorStatus}");

    var connectorKey = $"{chargePointId}/{connectorId}";
    var state = new ConnectorState
    {
        Status = connectorStatus,
        ErrorCode = chargePointErrorCode,
        UpdatedAt = DateTime.UtcNow
    };
    connectorStates[connectorKey] = state;

    Console.WriteLine(
        $"{connectorKey} → " +
        $"{state.Status}, " +
        $"{state.ErrorCode}, " +
        $"{state.UpdatedAt}");

    await SendCallResultAsync(socket, uniqueId, new JsonObject(), cancellationToken);
}

static async Task HandleAuthorizeAsync(WebSocket socket, string? uniqueId, JsonNode? payload, CancellationToken cancellationToken)
{
    if (payload is not JsonObject payloadObject)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "Authorize payload must be an object", cancellationToken);
        return;
    }

    string? idTag;

    try
    {
        idTag = payloadObject["idTag"]?.GetValue<string>();
    }
    catch
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "Authorize idTag type is invalid", cancellationToken);
        return;
    }

    if (string.IsNullOrWhiteSpace(idTag))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "Authorize idTag is required", cancellationToken);
        return;
    }

    Console.WriteLine($"Authorize idTag: {idTag}");

    var responsePayload = new JsonObject
    {
        ["idTagInfo"] = new JsonObject
        {
            ["status"] = "Accepted"
        }
    };

    await SendCallResultAsync(socket, uniqueId, responsePayload, cancellationToken);
}

static async Task HandleStartTransactionAsync(WebSocket socket, string chargePointId, string? uniqueId, JsonNode? payload, TransactionIdGenerator transactionIdGenerator, ConcurrentDictionary<int, Transaction> transactions, CancellationToken cancellationToken)
{
    if (payload is not JsonObject payloadObject)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StartTransaction payload must be an object", cancellationToken);
        return;
    }

    int connectorId;
    string? idTag;
    int meterStart;
    string? timestamp;

    try
    {
        connectorId = payloadObject["connectorId"]!.GetValue<int>();
        idTag = payloadObject["idTag"]?.GetValue<string>();
        meterStart = payloadObject["meterStart"]!.GetValue<int>();
        timestamp = payloadObject["timestamp"]?.GetValue<string>();
    }
    catch
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StartTransaction field type is invalid", cancellationToken);
        return;
    }

    if (string.IsNullOrWhiteSpace(idTag) || string.IsNullOrWhiteSpace(timestamp))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StartTransaction required field is missing", cancellationToken);
        return;
    }

    if (connectorId <= 0)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StartTransaction connectorId is invalid", cancellationToken);
        return;
    }

    if (!DateTime.TryParse(timestamp, out var startedAt))
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "StartTransaction timestamp is invalid", cancellationToken);
        return;
    }

    var transactionId = transactionIdGenerator.Next();

    var transaction = new Transaction
    {
        TransactionId = transactionId,
        ChargePointId = chargePointId,
        ConnectorId = connectorId,
        IdTag = idTag,
        MeterStart = meterStart,
        StartedAt = startedAt
    };
    transactions[transactionId] = transaction;

    Console.WriteLine(
        $"Transaction started: " +
        $"TransactionId={transactionId}, " +
        $"ChargePoint={chargePointId}, " +
        $"Connector={connectorId}");

    var responsePayload = new JsonObject
    {
        ["transactionId"] = transactionId,
        ["idTagInfo"] = new JsonObject
        {
            ["status"] = "Accepted"
        }
    };

    await SendCallResultAsync(socket, uniqueId, responsePayload, cancellationToken);
}

static async Task HandleMeterValuesAsync(WebSocket socket, string chargePointId, string? uniqueId, JsonNode? payload, ConcurrentDictionary<int, Transaction> transactions, CancellationToken cancellationToken)
{
    if (payload is not JsonObject payloadObject)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues payload must be an object", cancellationToken);
        return;
    }

    int connectorId;

    try
    {
        connectorId = payloadObject["connectorId"]!.GetValue<int>();
    }
    catch
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues connectorId is invalid", cancellationToken);
        return;
    }

    if (connectorId <= 0)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues connectorId is invalid", cancellationToken);
        return;
    }

    int? transactionId = null;
    if (payloadObject["transactionId"] is not null)
    {
        try
        {
            transactionId = payloadObject["transactionId"]!.GetValue<int>();
        }
        catch
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues transactionId is invalid", cancellationToken);
            return;
        }
    }

    if (transactionId.HasValue)
    {
        if (!transactions.TryGetValue(transactionId.Value, out var transaction))
        {
            await SendCallErrorAsync(socket, uniqueId, "PropertyConstraintViolation", "MeterValues transactionId does not exist", cancellationToken);
            return;
        }

        if (transaction.ChargePointId != chargePointId || transaction.ConnectorId != connectorId)
        {
            await SendCallErrorAsync(socket, uniqueId, "PropertyConstraintViolation", "MeterValues transaction does not match connector", cancellationToken);
            return;
        }
    }

    Console.WriteLine(
        $"MeterValues received: " +
        $"ChargePoint={chargePointId}, " +
        $"Connector={connectorId}, " +
        $"TransactionId={transactionId?.ToString() ?? "none"}");

    if (payloadObject["meterValue"] is not JsonArray meterValues)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues meterValue must be an array", cancellationToken);
        return;
    }

    if (meterValues.Count == 0)
    {
        await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues meterValue cannot be empty", cancellationToken);
        return;
    }

    foreach (var meterValueNode in meterValues)
    {
        if (meterValueNode is not JsonObject meterValueObject)
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues meterValue item must be an object", cancellationToken);
            return;
        }

        string? timestamp;

        try
        {
            timestamp = meterValueObject["timestamp"]?.GetValue<string>();
        }
        catch
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues timestamp is invalid", cancellationToken);
            return;
        }

        if (string.IsNullOrWhiteSpace(timestamp))
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues timestamp is required", cancellationToken);
            return;
        }

        if (!DateTime.TryParse(timestamp, out var measuredAt))
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues timestamp format is invalid", cancellationToken);
            return;
        }

        Console.WriteLine($"MeterValue timestamp: {measuredAt}");

        if (meterValueObject["sampledValue"] is not JsonArray sampledValues)
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues sampledValue must be an array", cancellationToken);
            return;
        }

        if (sampledValues.Count == 0)
        {
            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues sampledValue cannot be empty", cancellationToken);
            return;
        }

        foreach (var sampledValueNode in sampledValues)
        {
            if (sampledValueNode is not JsonObject sampledValueObject)
            {
                await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues sampledValue item must be an object", cancellationToken);
                return;
            }

            string? value;

            try
            {
                value = sampledValueObject["value"]?.GetValue<string>();
            }
            catch
            {
                await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues sampledValue value is invalid", cancellationToken);
                return;
            }

            if (string.IsNullOrWhiteSpace(value))
            {
                await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues sampledValue value is required", cancellationToken);
                return;
            }

            string? measurand = null;
            string? unit = null;

            try
            {
                if (sampledValueObject["measurand"] is not null)
                {
                    measurand = sampledValueObject["measurand"]!.GetValue<string>();
                }

                if (sampledValueObject["unit"] is not null)
                {
                    unit = sampledValueObject["unit"]!.GetValue<string>();
                }
            }
            catch
            {
                await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "MeterValues sampledValue field type is invalid", cancellationToken);
                return;
            }

            Console.WriteLine(
                $"MeterValue: " +
                $"Time={measuredAt}, " +
                $"Value={value}, " +
                $"Measurand={measurand ?? "default"}, " +
                $"Unit={unit ?? "default"}");
        }
    }

    await SendCallResultAsync(
        socket,
        uniqueId,
        new JsonObject(),
        cancellationToken);
}

enum ConnectorStatus
{
    Available,
    Preparing,
    Charging,
    SuspendedEVSE,
    SuspendedEV,
    Finishing,
    Reserved,
    Unavailable,
    Faulted
}

enum ChargePointErrorCode
{
    ConnectorLockFailure,
    EVCommunicationError,
    GroundFailure,
    HighTemperature,
    InternalError,
    LocalListConflict,
    NoError,
    OtherError,
    OverCurrentFailure,
    OverVoltage,
    PowerMeterFailure,
    PowerSwitchFailure,
    ReaderFailure,
    ResetFailure,
    UnderVoltage,
    WeakSignal
}

class ConnectorState
{
    public ConnectorStatus Status { get; set; }

    public ChargePointErrorCode ErrorCode { get; set; }

    public DateTime UpdatedAt { get; set; }
}

class Transaction
{
    public int TransactionId { get; set; }

    public string ChargePointId { get; set; } = "";

    public int ConnectorId { get; set; }

    public string IdTag { get; set; } = "";

    public int MeterStart { get; set; }

    public DateTime StartedAt { get; set; }
}

class TransactionIdGenerator
{
    private int _current = 0;

    public int Next()
    {
        return Interlocked.Increment(ref _current);
    }
}