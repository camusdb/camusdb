
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

using NUnit.Framework;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

using CamusDB.App.Services;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// A client-facing listener must set TCP keepalive on the sockets it accepts. Without it, the node never
/// learns that the host of a client died while a row stream waits for that client, and the statement of
/// that stream keeps its transaction away from the idle reaper. The test reads the options back from the
/// accepted socket of a real web server.
/// </summary>
[TestFixture]
// Serial: opens a listening port.
[NonParallelizable]
internal sealed class TestClientConnectionKeepAlive
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task AcceptedSocket_HasKeepAlive_OnlyWithTheHook(bool withHook)
    {
        TaskCompletionSource<(int On, int Time, int Interval, int Count)> accepted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel => kestrel.Listen(IPAddress.Loopback, 0, listen =>
        {
            if (withHook)
                ClientConnectionKeepAlive.Use(listen);

            listen.Use(next => connection =>
            {
                Socket socket = connection.Features.Get<IConnectionSocketFeature>()!.Socket;
                accepted.TrySetResult((
                    (int)socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive)!,
                    (int)socket.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime)!,
                    (int)socket.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval)!,
                    (int)socket.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount)!));
                return next(connection);
            });
        }));

        await using WebApplication app = builder.Build();
        await app.StartAsync();

        Uri address = new(System.Linq.Enumerable.First(app.Urls));
        using TcpClient client = new();
        await client.ConnectAsync(address.Host, address.Port);

        (int on, int time, int interval, int count) = await accepted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await app.StopAsync();

        if (!withHook)
        {
            Assert.AreEqual(0, on, "the web server sets no keepalive of its own, so the hook is what sets it");
            return;
        }

        Assert.AreNotEqual(0, on);
        Assert.AreEqual(ClientConnectionKeepAlive.IdleSeconds, time);
        Assert.AreEqual(ClientConnectionKeepAlive.IntervalSeconds, interval);
        Assert.AreEqual(ClientConnectionKeepAlive.ProbeCount, count);
    }
}
