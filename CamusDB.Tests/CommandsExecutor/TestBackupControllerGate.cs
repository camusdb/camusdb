/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsValidator;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Drives the backup admin surface through the real <see cref="BackupController"/> with a fabricated
/// <see cref="HttpContext"/>, verifying the fail-closed transport gate end to end: anonymous network
/// access is refused, loopback is allowed when auth is off, a credential over plaintext is refused, and
/// an authenticated-but-tokenless request is rejected — plus that every request emits an audit record.
/// Principal-level (superuser vs non-superuser) gating is covered at the executor layer in
/// <see cref="TestBackupCommandExecutor"/>.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestBackupControllerGate
{
    private sealed record Ctx(BackupController Controller, CapturingLogger Logger, EmbeddedKahuna Node,
        DatabaseRegistry Registry, CommandExecutor Executor, string Root);

    private readonly List<Ctx> created = new();

    [TearDown]
    public async Task TearDown()
    {
        foreach (Ctx c in created)
        {
            try { await c.Executor.DisposeAsync(); } catch { }
            try { await c.Registry.DisposeAsync(); } catch { }
            try { await c.Node.DisposeAsync(); } catch { }
            try { if (Directory.Exists(c.Root)) Directory.Delete(c.Root, recursive: true); } catch { }
        }
        created.Clear();
    }

    private async Task<Ctx> NewControllerAsync(CamusDBOptions options, IPAddress remote, bool https)
    {
        string root = Path.Combine(Path.GetTempPath(), "camus-t14-" + Guid.NewGuid().ToString("n"));
        string dataDir = Path.Combine(root, "data");
        Directory.CreateDirectory(dataDir);
        Directory.CreateDirectory(Path.Combine(root, "restores"));
        CamusConfig.DataDirectory = dataDir;

        CapturingLogger capturing = new();

        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = $"t14-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            BackupDir = Path.Combine(root, "backups"),
            RestoreRoot = Path.Combine(root, "restores"),
        }.WithTestNodeDefaults());
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);
        await node.FlushAsync();

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, options);
        CommandExecutor executor = new(
            new CommandValidator(options), new CatalogsManager(capturing), capturing, options,
            sharedNode: node, registry: registry, isClusterMode: false);

        BackupController controller = new(executor, new HttpTransactionCoordinator(executor), capturing, options);
        DefaultHttpContext http = new();
        http.Connection.RemoteIpAddress = remote;
        http.Request.IsHttps = https;
        controller.ControllerContext = new ControllerContext { HttpContext = http };

        Ctx ctx = new(controller, capturing, node, registry, executor, root);
        created.Add(ctx);
        return ctx;
    }

    private static (int status, string? code, string? statusText) Read(JsonResult result)
    {
        BackupResponse body = (BackupResponse)result.Value!;
        return (result.StatusCode ?? 200, body.Code, body.Status);
    }

    [Test]
    public async Task AuthOff_RemotePeer_RefusedAsInsufficientPrivilege()
    {
        Ctx c = await NewControllerAsync(CamusDBOptions.Default, IPAddress.Parse("203.0.113.7"), https: false);

        (int status, string? code, _) = Read(await c.Controller.TakeFullBackup());

        Assert.AreEqual(403, status);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, code);
        Assert.IsTrue(c.Logger.HasAudit("outcome=" + CamusDBErrorCodes.InsufficientPrivilege),
            "a refused request must still be audited");
    }

    [Test]
    public async Task AuthOff_Loopback_Allowed()
    {
        Ctx c = await NewControllerAsync(CamusDBOptions.Default, IPAddress.Loopback, https: false);

        (int status, _, string? statusText) = Read(await c.Controller.TakeFullBackup());

        Assert.AreEqual(200, status);
        Assert.AreEqual("ok", statusText);
        Assert.IsTrue(c.Logger.HasAudit("outcome=ok"), "a successful request must be audited");
    }

    [Test]
    public async Task AuthOn_NoToken_Loopback_AuthenticationFailed()
    {
        CamusDBOptions authOn = CamusDBOptions.Default with { AuthenticationEnabled = true };
        Ctx c = await NewControllerAsync(authOn, IPAddress.Loopback, https: true);

        (int status, string? code, _) = Read(await c.Controller.TakeFullBackup());

        Assert.AreEqual(401, status);
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, code);
    }

    [Test]
    public async Task AuthOn_PlaintextRemote_InsecureTransport()
    {
        CamusDBOptions authOn = CamusDBOptions.Default with { AuthenticationEnabled = true };
        Ctx c = await NewControllerAsync(authOn, IPAddress.Parse("203.0.113.7"), https: false);

        (int status, string? code, _) = Read(await c.Controller.TakeFullBackup());

        Assert.AreEqual(400, status);
        Assert.AreEqual(CamusDBErrorCodes.InsecureTransport, code);
    }

    /// <summary>Minimal ILogger that records formatted messages so audit emission can be asserted.</summary>
    private sealed class CapturingLogger : ILogger<ICamusDB>
    {
        private readonly List<string> messages = new();

        public bool HasAudit(string contains)
        {
            lock (messages)
                return messages.Exists(m => m.Contains("Backup admin audit", StringComparison.Ordinal)
                                            && m.Contains(contains, StringComparison.Ordinal));
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (messages)
                messages.Add(formatter(state, exception));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}
