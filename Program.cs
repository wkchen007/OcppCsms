using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseWebSockets();

app.MapGet("/", () => "Hello World!");

app.Map("/ocpp", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    var socket = await context.WebSockets.AcceptWebSocketAsync();

    var buffer = new byte[4096];

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

                    if (action == "BootNotification")
                    {
                        if (payload is not JsonObject payloadObject)
                        {
                            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "BootNotification payload must be an object", context.RequestAborted);
                            break;
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
                            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "BootNotification field type is invalid", context.RequestAborted);
                            break;
                        }

                        if (string.IsNullOrWhiteSpace(vendor) || string.IsNullOrWhiteSpace(model))
                        {
                            await SendCallErrorAsync(socket, uniqueId, "FormationViolation", "BootNotification required field is missing", context.RequestAborted);
                            break;
                        }

                        Console.WriteLine($"Vendor: {vendor}");
                        Console.WriteLine($"Model: {model}");

                        var responsePayload = new JsonObject
                        {
                            ["status"] = "Accepted",
                            ["currentTime"] = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'"),
                            ["interval"] = 300
                        };

                        await SendCallResultAsync(socket, uniqueId, responsePayload, context.RequestAborted);
                    }
                    else
                    {
                        await SendCallErrorAsync(socket, uniqueId, "NotSupported", "Action is not supported", context.RequestAborted);
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
        Console.WriteLine("WebSocket 已關閉");
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