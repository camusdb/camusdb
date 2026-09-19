/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;
using System.Text;
using System.Text.Json;
using Kahuna;
using Microsoft.Extensions.Logging.Abstractions;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Wasm;

/// <summary>
/// The JavaScript surface of the browser playground: one in-memory CamusDB engine per page, and a
/// call that runs a SQL script against it and returns every statement's outcome as JSON.
///
/// <para>The engine is the real one — the same parser, planner, executor and embedded Kahuna node
/// the server runs — on the browser build of Kahuna, which runs its Raft scheduling as async
/// continuations on the page's event loop instead of on worker threads. Storage and WAL are in
/// memory, so a reload starts empty. It is a single node: there is no cluster mode in a tab.</para>
///
/// <para>Calls are serialized through one gate. The runtime has one thread, so they could not run
/// in parallel anyway, but without the gate a second <c>executeAsync</c> issued before the first one
/// resolves would interleave its statements with the first script's at every await.</para>
///
/// <para>Each statement is autocommitted the way the REST transport does it: the database-scoped
/// statements and server-level queries run with no transaction, row-returning statements in a
/// read-only one, and everything else in a read-write one that is committed, or rolled back on
/// failure. Serializable conflicts are retried by <see cref="SerializableRetryHelper"/>. A script
/// stops at the first failed statement.</para>
/// </summary>
[SupportedOSPlatform("browser")]
public static partial class PlaygroundEngine
{
    /// <summary>The database every statement runs in. It is created at start.</summary>
    public const string DatabaseName = "playground";

    private static readonly SemaphoreSlim gate = new(1, 1);

    private static Session? session;

    /// <summary>
    /// Starts the engine and creates the <see cref="DatabaseName"/> database. Idempotent: a second
    /// call returns once the first engine is up.
    /// </summary>
    [JSExport]
    public static async Task InitAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            session ??= await Session.StartAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Discards every table and row by disposing the engine and starting a new one.
    /// </summary>
    [JSExport]
    public static async Task ResetAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (session is not null)
                await session.DisposeAsync().ConfigureAwait(false);

            session = null;
            session = await Session.StartAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Runs every statement of <paramref name="script"/> in order and returns a JSON array with one
    /// object per statement that ran. A statement that fails ends the script; its object carries the
    /// error, and the statements after it are not run.
    ///
    /// <para>Object shapes: <c>{"sql","ok":true,"columns":[{"name","type"}],"rows":[[…]],"ms"}</c>
    /// for a row-returning statement, <c>{"sql","ok":true,"affected","warning"?,"ms"}</c> for the
    /// rest, and <c>{"sql","ok":false,"code","message","ms"}</c> for a failure. A row is a positional
    /// array aligned to <c>columns</c>, encoded the way the REST API encodes it.</para>
    /// </summary>
    [JSExport]
    public static async Task<string> ExecuteAsync(string script)
        => await ExecuteInDatabaseAsync(script, DatabaseName).ConfigureAwait(false);

    /// <summary>
    /// Runs every statement of <paramref name="script"/> in order against
    /// <paramref name="databaseName"/>. Server-level statements such as <c>CREATE DATABASE</c>,
    /// <c>CREATE DATABASE ... BRANCH FROM</c>, and <c>SHOW DATABASES</c> still resolve their own
    /// target and do not open the context database.
    /// </summary>
    [JSExport]
    public static async Task<string> ExecuteInDatabaseAsync(string script, string? databaseName)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            session ??= await Session.StartAsync().ConfigureAwait(false);
            string contextDatabase = NormalizeDatabaseName(databaseName);

            ArrayBufferWriter<byte> buffer = new();
            using (Utf8JsonWriter writer = new(buffer))
            {
                writer.WriteStartArray();

                foreach (string statement in SqlScriptSplitter.Split(script))
                {
                    if (!await session.RunAsync(statement, contextDatabase, writer).ConfigureAwait(false))
                        break;
                }

                writer.WriteEndArray();
            }

            return Encoding.UTF8.GetString(buffer.WrittenSpan);
        }
        finally
        {
            gate.Release();
        }
    }

    private static string NormalizeDatabaseName(string? databaseName)
        => string.IsNullOrWhiteSpace(databaseName) ? DatabaseName : databaseName.Trim();

    /// <summary>One engine: the embedded node, the database registry and the executor over them.</summary>
    private sealed class Session : IAsyncDisposable
    {
        private readonly EmbeddedKahuna node;

        private readonly DatabaseRegistry registry;

        private readonly CommandExecutor executor;

        private readonly CamusDBOptions options;

        private Session(EmbeddedKahuna node, DatabaseRegistry registry, CommandExecutor executor, CamusDBOptions options)
        {
            this.node = node;
            this.registry = registry;
            this.executor = executor;
            this.options = options;
        }

        public static async Task<Session> StartAsync()
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

            return new Session(node, registry, executor, options);
        }

        /// <summary>
        /// Runs one statement and writes its outcome object. Returns false when the statement failed.
        /// </summary>
        public async Task<bool> RunAsync(string sql, string databaseName, Utf8JsonWriter writer)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            writer.WriteStartObject();
            writer.WriteString("sql", sql);
            writer.WriteString("database", databaseName);

            try
            {
                NodeType root = executor.ParseSql(sql).nodeType;

                if (StatementScope.ReturnsRows(root))
                    await QueryAsync(sql, databaseName, root, writer).ConfigureAwait(false);
                else
                    await NonQueryAsync(sql, databaseName, root, writer).ConfigureAwait(false);

                writer.WriteNumber("ms", stopwatch.Elapsed.TotalMilliseconds);
                writer.WriteEndObject();
                return true;
            }
            catch (Exception ex)
            {
                writer.WriteBoolean("ok", false);
                writer.WriteString("code", ex is CamusDBException camus ? camus.Code : ex.GetType().Name);
                writer.WriteString("message", ex.Message);
                writer.WriteNumber("ms", stopwatch.Elapsed.TotalMilliseconds);
                writer.WriteEndObject();
                return false;
            }
        }

        private async Task QueryAsync(string sql, string databaseName, NodeType root, Utf8JsonWriter writer)
        {
            QuerySchemaHolder schema = new();
            List<QueryResultRow> rows = [];

            if (StatementScope.IsServerLevelQuery(root))
            {
                // Reads the registry or this process's own state: no database, no transaction.
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(txnState: null!, database: databaseName, sql: sql, parameters: null),
                    schemaOut: schema).ConfigureAwait(false);

                await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
                    rows.Add(row);
            }
            else
            {
                await RetryAsync(async ct =>
                {
                    rows.Clear();

                    DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
                    KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote: true, cancellationToken: ct).ConfigureAwait(false);
                    try
                    {
                        (DatabaseDescriptor? db, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                            new ExecuteSQLTicket(txnState: tx, database: databaseName, sql: sql, parameters: null),
                            schemaOut: schema).ConfigureAwait(false);

                        // Buffer every row before the commit, so a retried attempt starts from nothing.
                        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
                            rows.Add(row);

                        await CommitOrReleaseAsync(db, database, tx, ct).ConfigureAwait(false);
                    }
                    catch
                    {
                        await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);
            }

            IReadOnlyList<DerivedColumnSchema> columns = schema.Schema;

            writer.WriteBoolean("ok", true);
            writer.WriteStartArray("columns");
            foreach (DerivedColumnSchema column in columns)
            {
                writer.WriteStartObject();
                writer.WriteString("name", column.Name);
                writer.WriteString("type", column.Type.ToString());
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteStartArray("rows");
            RowLayout? boundLayout = null;
            int[]? ordinals = null;
            foreach (QueryResultRow row in rows)
                CompactRowJsonWriter.WriteRow(writer, row, columns, ref boundLayout, ref ordinals);
            writer.WriteEndArray();
        }

        private async Task NonQueryAsync(string sql, string databaseName, NodeType root, Utf8JsonWriter writer)
        {
            int affected = 0;
            string? warning = null;

            if (StatementScope.IsDatabaseScopedMutation(root))
            {
                // Names its own target and writes the shared registry: no database, no transaction.
                ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                    new ExecuteSQLTicket(txnState: null!, database: databaseName, sql: sql, parameters: null)).ConfigureAwait(false);
                warning = result.Warning;
            }
            else
            {
                await RetryAsync(async ct =>
                {
                    DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
                    KvTransaction tx = await database.Transactions.BeginAsync(cancellationToken: ct).ConfigureAwait(false);
                    try
                    {
                        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                            new ExecuteSQLTicket(txnState: tx, database: databaseName, sql: sql, parameters: null)).ConfigureAwait(false);

                        await CommitOrReleaseAsync(result.Database, database, tx, ct).ConfigureAwait(false);

                        affected = result.ModifiedRows;
                        warning = result.Warning;
                    }
                    catch
                    {
                        await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                        throw;
                    }
                }).ConfigureAwait(false);
            }

            writer.WriteBoolean("ok", true);
            writer.WriteNumber("affected", affected);
            if (!string.IsNullOrEmpty(warning))
                writer.WriteString("warning", warning);
        }

        /// <summary>
        /// Commits through the descriptor the statement returned. A statement that returns none wrote
        /// nothing through <paramref name="tx"/>, so releasing it is correct — the same rule as the
        /// server's transaction coordinator.
        /// </summary>
        private static async Task CommitOrReleaseAsync(
            DatabaseDescriptor? resultDatabase, DatabaseDescriptor database, KvTransaction tx, CancellationToken ct)
        {
            if (resultDatabase is not null)
                await resultDatabase.Transactions.CommitAsync(tx, ct).ConfigureAwait(false);
            else
                await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
        }

        private Task RetryAsync(Func<CancellationToken, Task> attempt) =>
            options.DefaultIsolationLevel == CamusIsolationLevel.Serializable
                ? SerializableRetryHelper.ExecuteAutocommitAsync(attempt)
                : attempt(CancellationToken.None);

        public async ValueTask DisposeAsync()
        {
            await executor.DisposeAsync().ConfigureAwait(false);
            await registry.DisposeAsync().ConfigureAwait(false);
            await node.DisposeAsync().ConfigureAwait(false);
        }
    }
}
