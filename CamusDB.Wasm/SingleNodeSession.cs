/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text;
using System.Text.Json;

using Kahuna;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Wasm;

/// <summary>
/// One CamusDB engine in the tab: the embedded node, the database registry and the executor over
/// them. This is the playground's default mode, because it starts in well under a second and costs
/// one node's memory. <see cref="ClusterSession"/> is the other mode.
/// </summary>
internal sealed class SingleNodeSession : IAsyncDisposable
{
    /// <summary>The database every statement runs in. It is created at start.</summary>
    public const string DatabaseName = "playground";

    private readonly EmbeddedKahuna node;

    private readonly DatabaseRegistry registry;

    private readonly CommandExecutor executor;

    private readonly StatementRunner runner;

    private SingleNodeSession(EmbeddedKahuna node, DatabaseRegistry registry, CommandExecutor executor, StatementRunner runner)
    {
        this.node = node;
        this.registry = registry;
        this.executor = executor;
        this.runner = runner;
    }

    public static async Task<SingleNodeSession> StartAsync()
    {
        // The in-memory backends are the only ones the browser build of Kahuna has; its host-pumped
        // scheduling is on by default there. One partition, because one node serves everything.
        //
        // The timings are the single-node ones the test suite uses (TestNodeDefaults). With the
        // cluster defaults the first election takes about 4 s, which the visitor waits through
        // on every page load and every reset; these bring it to well under 1 s. They are safe
        // only because a single node wins its own election uncontested. Kommander requires the
        // heartbeat and the leader check to stay at most a fifth of the election timeout.
        EmbeddedKahunaOptions nodeOptions = new()
        {
            NodeName = "camusdb-playground",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            TimerInitialDelay = TimeSpan.FromMilliseconds(100),
            StartElectionTimeout = 150,
            EndElectionTimeout = 300,
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(300),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(250),
        };

        // The free-disk write guard is off: the browser's in-memory file system reports 0 bytes
        // free, so every write would be refused. Nothing here is written to a disk anyway.
        CamusDBOptions options = CamusDBOptions.Default with
        {
            DataDirectory = "/camusdb",
            MinFreeDiskBytes = 0,
        };

        EmbeddedKahuna node = new(nodeOptions, NullLoggerFactory.Instance);
        await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
        await node.WaitForLeaderAsync("playground", CancellationToken.None).ConfigureAwait(false);

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, options).ConfigureAwait(false);
        CommandExecutor executor = new(
            new CommandValidator(options),
            new CatalogsManager(NullLogger<ICamusDB>.Instance),
            NullLogger<ICamusDB>.Instance,
            options,
            sharedNode: node,
            registry: registry,
            isClusterMode: false);

        await executor.CreateDatabase(new CreateDatabaseTicket(name: DatabaseName, ifNotExists: true)).ConfigureAwait(false);

        return new SingleNodeSession(node, registry, executor, new StatementRunner(executor, options));
    }

    /// <summary>
    /// Runs every statement of <paramref name="script"/> in order and returns the JSON array of
    /// outcomes. A failed statement ends the script.
    /// </summary>
    public async Task<string> ExecuteAsync(string script, string databaseName)
    {
        ArrayBufferWriter<byte> buffer = new();
        using (Utf8JsonWriter writer = new(buffer))
        {
            writer.WriteStartArray();

            foreach (string statement in SqlScriptSplitter.Split(script))
            {
                if (!await runner.RunAsync(statement, databaseName, writer).ConfigureAwait(false))
                    break;
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    public async ValueTask DisposeAsync()
    {
        await executor.DisposeAsync().ConfigureAwait(false);
        await registry.DisposeAsync().ConfigureAwait(false);
        await node.DisposeAsync().ConfigureAwait(false);
    }
}
