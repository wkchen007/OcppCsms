using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;

static class OcppMessageHandlers
{
    public static async Task SendMessageAsync(WebSocket socket, JsonArray message, CancellationToken cancellationToken)
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

    public static async Task<string?> ReceiveMessageAsync(WebSocket socket, CancellationToken cancellationToken)
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

    public static async Task SendCallResultAsync(WebSocket socket, string? uniqueId, JsonObject payload, CancellationToken cancellationToken)
    {
        var response = new JsonArray
        {
            3,
            uniqueId,
            payload
        };

        await SendMessageAsync(socket, response, cancellationToken);
    }

    public static async Task SendCallErrorAsync(WebSocket socket, string? uniqueId, string errorCode, string errorDescription, CancellationToken cancellationToken)
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

    public static async Task HandleHeartbeatAsync(WebSocket socket, string? uniqueId, CancellationToken cancellationToken)
    {
        var responsePayload = new JsonObject
        {
            ["currentTime"] = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'")
        };

        await SendCallResultAsync(socket, uniqueId, responsePayload, cancellationToken);
    }

    public static async Task HandleBootNotificationAsync(WebSocket socket, string? uniqueId, JsonNode? payload, CancellationToken cancellationToken)
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

    public static async Task HandleStatusNotificationAsync(WebSocket socket, string? chargePointId, string? uniqueId, JsonNode? payload,
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

    public static async Task HandleAuthorizeAsync(WebSocket socket, string? uniqueId, JsonNode? payload, CancellationToken cancellationToken)
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

    public static async Task HandleStartTransactionAsync(WebSocket socket, string chargePointId, string? uniqueId, JsonNode? payload, TransactionIdGenerator transactionIdGenerator, ConcurrentDictionary<int, Transaction> transactions, CancellationToken cancellationToken)
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

    public static async Task HandleMeterValuesAsync(WebSocket socket, string chargePointId, string? uniqueId, JsonNode? payload, ConcurrentDictionary<int, Transaction> transactions, CancellationToken cancellationToken)
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
}
