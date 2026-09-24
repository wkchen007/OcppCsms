using System.Net.WebSockets;
using System.Text;

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
        using var stream = new MemoryStream();

        WebSocketReceiveResult result;

        do
        {
            result = await socket.ReceiveAsync(
                new ArraySegment<byte>(buffer),
                context.RequestAborted);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                await socket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    null,
                    context.RequestAborted);

                return;
            }

            if (result.MessageType != WebSocketMessageType.Text)
            {
                break;
            }

            stream.Write(buffer, 0, result.Count);

        } while (!result.EndOfMessage);

        if (result.MessageType != WebSocketMessageType.Text)
        {
            continue;
        }

        var message = Encoding.UTF8.GetString(stream.ToArray());

        Console.WriteLine($"收到完整訊息: {message}");
    }
});

app.Run();
