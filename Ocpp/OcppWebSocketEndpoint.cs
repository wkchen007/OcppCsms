using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json.Nodes;
using static OcppMessageHandlers;

static class OcppWebSocketEndpoint
{
    public static void MapOcppWebSocket(
        this WebApplication app,
        ConcurrentDictionary<string, ConnectorState> connectorStates,
        ConcurrentDictionary<string, ChargePoint> chargePoints,
        ConcurrentDictionary<string, bool> chargePointConnections,
        ConcurrentDictionary<int, Transaction> transactions,
        TransactionIdGenerator transactionIdGenerator,
        int heartbeatInterval)
    {
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
                                        await HandleBootNotificationAsync(socket, chargePointId, uniqueId, payload, heartbeatInterval, chargePoints, context.RequestAborted);
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
    }
}
