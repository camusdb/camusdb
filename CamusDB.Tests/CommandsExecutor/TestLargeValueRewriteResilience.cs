/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>ALTER TABLE ... REWRITE STORAGE</c> under the conditions its rules exist for: a run that stops
/// between batches, a run continued by another engine from the persisted cursor, live update traffic on
/// the same rows, and batches that do not fit the mutation limit.
/// </summary>
[NonParallelizable]
internal sealed class TestLargeValueRewriteResilience : SharedNodeBaseTest
{
    private static readonly Random Rng = new(31337);

    private static string Incompressible(int length)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        char[] chars = new char[length];
        lock (Rng)
        {
            for (int i = 0; i < length; i++)
                chars[i] = alphabet[Rng.Next(alphabet.Length)];
        }
        return new string(chars);
    }

    /// <summary>
    /// Cancels a token when the rewriter logs its first committed batch. This stops a run between two
    /// batches at a known point, which stands in for a process that dies there.
    /// </summary>
    private sealed class StopAfterFirstBatchLogger : ILogger<ICamusDB>
    {
        private readonly CancellationTokenSource cancel;

        public StopAfterFirstBatchLogger(CancellationTokenSource cancel) => this.cancel = cancel;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            string message = formatter(state, exception);
            if (message.StartsWith("Storage rewrite of table", StringComparison.Ordinal) && message.Contains("rows scanned", StringComparison.Ordinal))
                cancel.Cancel();
        }
    }

    /// <summary>Counts the batches a rewrite commits, from its per-batch progress log line.</summary>
    private sealed class BatchCountingLogger : ILogger<ICamusDB>
    {
        private int batches;

        public int Batches => Volatile.Read(ref batches);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            string message = formatter(state, exception);
            if (message.StartsWith("Storage rewrite of table", StringComparison.Ordinal) && !message.Contains("finished", StringComparison.Ordinal))
                Interlocked.Increment(ref batches);
        }
    }

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
        => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db, sql, null));

    private static async Task Dml(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, string db, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? value) && value.Type != ColumnType.Null ? value.StrValue : null;

    /// <summary>Every row key of the table, in row-id order, with its revision and whether it is in the new form.</summary>
    private async Task<List<(string key, long revision, bool converted)>> RowStatesAsync(DatabaseDescriptor database)
    {
        string bucket = $"{database.Id}:{database.Schema.Tables["docs"].EffectiveStorageId}|r";
        List<(string, long, bool)> rows = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(bucket + "/", StringComparison.Ordinal) && entry.Value is { Length: > 1 } && entry.Value[0] == (byte)BranchKvKind.Value)
                rows.Add((key, entry.Revision, RowStorageForms.HasTrailer(entry.Value.AsSpan(1))));
        }
        return rows;
    }

    private async Task<string?> CursorAsync(DatabaseDescriptor database)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await SharedKahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{database.Id}/meta/storagerewrite:{database.Schema.Tables["docs"].EffectiveStorageId}", -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);
        return type == KeyValueResponseType.Get && entry?.Value is { } value ? Encoding.UTF8.GetString(value) : null;
    }

    /// <summary>A database whose "docs" rows were all written inline, by an engine with the feature off.</summary>
    private async Task<(string db, Dictionary<string, string> bodies)> SeedInlineAsync(int rows, int largeColumns = 1)
    {
        string db = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor inlineEngine = CreateCommandExecutor(Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false });
        TrackDatabase(db, inlineEngine);
        await inlineEngine.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));

        string extra = largeColumns > 1 ? ", extra string" : "";
        await Ddl(inlineEngine, db, $"CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string, n int64{extra})");

        Dictionary<string, string> bodies = [];
        for (int i = 0; i < rows; i++)
        {
            string body = Incompressible(3000);
            bodies["t" + i] = body;

            Dictionary<string, ColumnValue> parameters = new() { { "@body", new ColumnValue(ColumnType.String, body) } };
            string columns = "id, title, body, n";
            string values = $"gen_id(), 't{i}', @body, {i}";
            if (largeColumns > 1)
            {
                parameters["@extra"] = new ColumnValue(ColumnType.String, Incompressible(3000));
                columns += ", extra";
                values += ", @extra";
            }

            await Dml(inlineEngine, db, $"INSERT INTO docs ({columns}) VALUES ({values})", parameters);
        }

        return (db, bodies);
    }

    [Test]
    public async Task InterruptedRewrite_ResumesFromItsCursor_OnAnotherEngine_WithoutRewritingARow()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(12);

        // First engine: a run that stops right after its first committed batch of 4 rows.
        CommandExecutor first = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 4 });
        DatabaseDescriptor firstDb = await first.OpenDatabase(db);
        TableDescriptor firstTable = await first.OpenTable(new OpenTableTicket(db, "docs"));

        using CancellationTokenSource stop = new();
        StorageRewriter rewriter = new(new StopAfterFirstBatchLogger(stop));
        Assert.CatchAsync<OperationCanceledException>(() => rewriter.RewriteAsync(firstDb, firstTable, inline: false, stop.Token));

        List<(string key, long revision, bool converted)> afterStop = await RowStatesAsync(firstDb);
        Assert.AreEqual(12, afterStop.Count);
        CollectionAssert.AreEqual(Enumerable.Repeat(true, 4).Concat(Enumerable.Repeat(false, 8)), afterStop.Select(r => r.converted),
            "exactly the first committed batch, in row-id order, is converted");

        string? cursor = await CursorAsync(firstDb);
        Assert.IsNotNull(cursor, "the stopped run leaves its cursor");
        StringAssert.EndsWith(afterStop[3].key[^24..], cursor!, "the cursor names the last row of the committed batch");

        // Second engine: the statement continues after the cursor.
        CommandExecutor second = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 4 });
        await Ddl(second, db, "ALTER TABLE docs REWRITE STORAGE");

        DatabaseDescriptor secondDb = await second.OpenDatabase(db);
        List<(string key, long revision, bool converted)> afterResume = await RowStatesAsync(secondDb);

        Assert.IsTrue(afterResume.All(r => r.converted), "no row is skipped");
        for (int i = 0; i < 4; i++)
            Assert.AreEqual(afterStop[i].revision, afterResume[i].revision, "a row converted before the stop is not rewritten on resume");
        for (int i = 4; i < 12; i++)
            Assert.AreEqual(afterStop[i].revision + 1, afterResume[i].revision, "every other row is converted exactly once");

        Assert.IsNull(await CursorAsync(secondDb), "a completed run removes its cursor");

        foreach (QueryResultRow row in await Select(second, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }

    [Test]
    public async Task RunStoppedAfterADeferredBatch_ResumesBeforeTheDeferredRow()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(4);
        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 1 });
        DatabaseDescriptor database = await engine.OpenDatabase(db);
        TableDescriptor table = await engine.OpenTable(new OpenTableTicket(db, "docs"));

        List<(string key, long revision, bool converted)> before = await RowStatesAsync(database);
        ObjectIdValue firstRow = ObjectId.ToValue(before[0].key[^24..]);

        // A Serializable reader holds a shared lock on the first row, so the batch of that row fails at
        // commit and is deferred. The batch of the second row commits, and the run stops there.
        KvTransaction blocker = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            await table.Store.GetRow(blocker, firstRow);

            using CancellationTokenSource stop = new();
            StorageRewriter rewriter = new(new StopAfterFirstBatchLogger(stop));
            Assert.CatchAsync<OperationCanceledException>(() => rewriter.RewriteAsync(database, table, inline: false, stop.Token));

            List<(string key, long revision, bool converted)> stopped = await RowStatesAsync(database);
            Assert.IsFalse(stopped[0].converted, "sanity: the first row was deferred");
            Assert.IsTrue(stopped[1].converted, "sanity: the second row was converted before the stop");
            Assert.IsNull(await CursorAsync(database), "no cursor may pass the deferred first row");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(blocker);
        }

        await Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE");

        Assert.IsTrue((await RowStatesAsync(database)).All(r => r.converted), "the resumed run must convert the row deferred before the stop");
        foreach (QueryResultRow row in await Select(engine, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }

    [Test]
    public async Task RunStopped_ThenStorageStrategyChanged_ResumeAppliesTheNewStrategyToEveryRow()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(4);
        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 1 });
        DatabaseDescriptor database = await engine.OpenDatabase(db);
        TableDescriptor table = await engine.OpenTable(new OpenTableTicket(db, "docs"));

        using CancellationTokenSource stop = new();
        StorageRewriter rewriter = new(new StopAfterFirstBatchLogger(stop));
        Assert.CatchAsync<OperationCanceledException>(() => rewriter.RewriteAsync(database, table, inline: false, stop.Token));
        Assert.IsTrue((await RowStatesAsync(database))[0].converted, "sanity: the first row moved out of line under EXTENDED");

        await Ddl(engine, db, "ALTER TABLE docs ALTER COLUMN body SET STORAGE PLAIN");
        await Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE");

        Assert.IsTrue((await RowStatesAsync(database)).All(r => !r.converted),
            "the cursor of the EXTENDED run must not be resumed: PLAIN applies to the rows before it too");
        foreach (QueryResultRow row in await Select(engine, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }

    [Test]
    public async Task RunStopped_ThenLargeValueSettingsChanged_ResumeStartsFromTheBeginning()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(4);

        CommandExecutor first = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 1 });
        DatabaseDescriptor firstDb = await first.OpenDatabase(db);
        TableDescriptor firstTable = await first.OpenTable(new OpenTableTicket(db, "docs"));

        using CancellationTokenSource stop = new();
        StorageRewriter rewriter = new(new StopAfterFirstBatchLogger(stop));
        Assert.CatchAsync<OperationCanceledException>(() => rewriter.RewriteAsync(firstDb, firstTable, inline: false, stop.Token));
        Assert.IsNotNull(await CursorAsync(firstDb), "sanity: the stopped run left a cursor");

        // A second engine whose settings keep every value inline: its target differs from the cursor's.
        CommandExecutor second = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 1, LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false });
        await Ddl(second, db, "ALTER TABLE docs REWRITE STORAGE");

        DatabaseDescriptor secondDb = await second.OpenDatabase(db);
        Assert.IsTrue((await RowStatesAsync(secondDb)).All(r => !r.converted), "the row converted under the old settings must follow the new ones");
        foreach (QueryResultRow row in await Select(second, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }

    [Test]
    public async Task RewriteBatch_ClosesOnTheDecodedByteBound()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(12);

        // Move every body out of line, then convert back to inline under a bound that fits three 3000-byte
        // values per batch, although the row count would allow the whole table in one batch.
        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 50, LargeValueResolveBatchBytes = 10_000 });
        await Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE");

        DatabaseDescriptor database = await engine.OpenDatabase(db);
        TableDescriptor table = await engine.OpenTable(new OpenTableTicket(db, "docs"));
        Assert.IsTrue((await RowStatesAsync(database)).All(r => r.converted), "sanity: every body moved out of line");

        BatchCountingLogger logger = new();
        StorageRewriteReport report = await new StorageRewriter(logger).RewriteAsync(database, table, inline: true);

        Assert.AreEqual(12, report.RowsConverted);
        Assert.AreEqual(4, logger.Batches, "twelve rows of 3000 decoded bytes fit three to a batch under a 10,000-byte bound");
        Assert.IsTrue((await RowStatesAsync(database)).All(r => !r.converted));
        foreach (QueryResultRow row in await Select(engine, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }

    /// <summary>
    /// Runs a rewrite while two workers update the same rows. Each worker owns a disjoint set of titles,
    /// so any conflict a worker meets comes from the rewrite. Returns the failures the workers saw, and
    /// the last value each title was committed with.
    /// </summary>
    private async Task<(List<CamusDBException> conflicts, ConcurrentDictionary<string, (string body, long n)> lastWrite)> RunRewriteUnderTrafficAsync(
        CommandExecutor engine, string db, bool retryConflicts)
    {
        ConcurrentDictionary<string, (string body, long n)> lastWrite = new();
        ConcurrentBag<CamusDBException> conflicts = [];

        async Task TrafficAsync(int worker)
        {
            Random random = new(worker);
            for (int i = 0; i < 60; i++)
            {
                int index = random.Next(40);
                if (index % 2 != worker)
                    continue;

                string title = "t" + index;
                string body = random.Next(3) == 0 ? "small " + i : Incompressible(3000);
                long n = worker * 1000 + i;

                for (int attempt = 0; ; attempt++)
                {
                    try
                    {
                        await Dml(engine, db, $"UPDATE docs SET body = @body, n = {n} WHERE title = '{title}'",
                            new() { { "@body", new ColumnValue(ColumnType.String, body) } });
                        lastWrite[title] = (body, n);
                        break;
                    }
                    catch (CamusDBException ex)
                    {
                        conflicts.Add(ex);
                        if (!retryConflicts || !SerializableRetryHelper.IsRetryable(ex) || attempt >= 20)
                            break;

                        await Task.Delay(random.Next(2, 20));
                    }
                }
            }
        }

        await Task.WhenAll(TrafficAsync(0), TrafficAsync(1), Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE"));
        return ([.. conflicts], lastWrite);
    }

    private async Task AssertLastWritesAsync(CommandExecutor engine, string db, Dictionary<string, string> bodies, ConcurrentDictionary<string, (string body, long n)> lastWrite)
    {
        foreach (QueryResultRow row in await Select(engine, db, "SELECT title, body, n FROM docs"))
        {
            string title = Text(row, "title")!;
            if (lastWrite.TryGetValue(title, out (string body, long n) expected))
            {
                Assert.AreEqual(expected.body, Text(row, "body"), $"the last committed user write to {title} was lost");
                Assert.AreEqual(expected.n, row.Row["n"].LongValue);
            }
            else
            {
                Assert.AreEqual(bodies[title], Text(row, "body"));
            }
        }
    }

    [Test]
    public async Task RewriteUnderLiveUpdates_LosesNoUserWrite_AndUsersSeeOnlyRetryableConflicts()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(40);
        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 5 });

        (List<CamusDBException> conflicts, ConcurrentDictionary<string, (string body, long n)> lastWrite) =
            await RunRewriteUnderTrafficAsync(engine, db, retryConflicts: true);

        Assert.IsNotEmpty(lastWrite, "sanity: the traffic ran");
        Assert.IsTrue(conflicts.All(SerializableRetryHelper.IsRetryable),
            "a user may meet a conflict with a rewrite batch, but only a retryable one: " +
            string.Join("; ", conflicts.Where(c => !SerializableRetryHelper.IsRetryable(c)).Select(c => c.Code + " " + c.Message).Take(3)));

        await AssertLastWritesAsync(engine, db, bodies, lastWrite);

        // Rows the rewrite deferred while traffic changed them are converted by a later run. A row whose
        // last write stored a short body has nothing to convert and stays without a trailer.
        await Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE");
        DatabaseDescriptor database = await engine.OpenDatabase(db);
        List<QueryResultRow> rows = await Select(engine, db, "SELECT title, body FROM docs");
        int large = rows.Count(r => Text(r, "body")!.Length >= 3000);
        Assert.AreEqual(large, (await RowStatesAsync(database)).Count(r => r.converted),
            "every row with a large value is converted once the traffic stops");
    }

    [Test]
    [Ignore("Needs Kahuna support: a lock request that meets the live write intent of an undecided " +
            "transaction fails at once with AlreadyLocked, and no lock priority lets a maintenance " +
            "transaction give way. A user writer that arrives while a rewrite batch has staged its writes " +
            "therefore fails with a retryable conflict.")]
    public async Task RewriteUnderLiveUpdates_FailsNoUserTransaction()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(40);
        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 5 });

        (List<CamusDBException> conflicts, ConcurrentDictionary<string, (string body, long n)> lastWrite) =
            await RunRewriteUnderTrafficAsync(engine, db, retryConflicts: false);

        Assert.IsEmpty(conflicts, "the rewrite must never make a user transaction fail: " +
            string.Join("; ", conflicts.Select(e => e.Message).Take(3)));
        await AssertLastWritesAsync(engine, db, bodies, lastWrite);
    }

    [Test]
    public async Task RewriteBatch_ThatExceedsTheMutationLimit_IsSplit_AndTheRunCompletes()
    {
        // Each row gains two out-of-line values: 3 mutations for the row, plus 1 for the cursor per
        // batch. A batch of 10 rows needs 31, far over a limit of 12, so the run must split batches.
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(10, largeColumns: 2);

        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 10, MaxMutationsPerTransaction = 12 });
        await Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE");

        DatabaseDescriptor database = await engine.OpenDatabase(db);
        Assert.IsTrue((await RowStatesAsync(database)).All(r => r.converted), "every row is converted despite the limit");

        foreach (QueryResultRow row in await Select(engine, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }

    [Test]
    public async Task RewriteStorage_InsideAnExplicitTransaction_IsRefused()
    {
        (string db, Dictionary<string, string> bodies) = await SeedInlineAsync(4);
        CommandExecutor engine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 2 });
        DatabaseDescriptor database = await engine.OpenDatabase(db);

        // The default explicit transaction is Serializable: the rows this SELECT reads stay locked until
        // commit. Were the statement accepted, every batch over them would wait out the lock deadline and
        // be deferred, and the statement would report success having converted nothing.
        KvTransaction sessionTx = await database.Transactions.BeginAsync();
        sessionTx.MarkSessionOwned();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> read) = await engine.ExecuteSQLQuery(new ExecuteSQLTicket(sessionTx, db, "SELECT title FROM docs", null));
            Assert.AreEqual(4, (await read.ToListAsync()).Count);

            foreach (string sql in new[] { "ALTER TABLE docs REWRITE STORAGE", "ALTER TABLE docs REWRITE STORAGE INLINE" })
            {
                CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(() =>
                    engine.ExecuteDDLSQL(new ExecuteSQLTicket(sessionTx, db, sql, null)));
                Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, refused!.Code, sql);
                StringAssert.Contains("REWRITE STORAGE", refused.Message);
                StringAssert.Contains("Commit or roll back first", refused.Message, "the message names the fix");
            }

            Assert.IsTrue((await RowStatesAsync(database)).All(r => !r.converted), "the refusal writes no row");
            Assert.IsNull(await CursorAsync(database), "the refusal leaves no cursor");

            await database.Transactions.CommitAsync(sessionTx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(sessionTx);
        }

        // Outside an explicit transaction the same statement runs as before.
        await Ddl(engine, db, "ALTER TABLE docs REWRITE STORAGE");
        Assert.IsTrue((await RowStatesAsync(database)).All(r => r.converted));

        foreach (QueryResultRow row in await Select(engine, db, "SELECT title, body FROM docs"))
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
    }
}
