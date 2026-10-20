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
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An online backfill on a table that concurrent writers keep touching. <c>CREATE INDEX</c> and
/// <c>ALTER TABLE … ADD COLUMN</c> lock the rows of each batch before they read them, and a batch
/// that loses its locks is retried until it wins, so the schema change completes however long the
/// writes last, and the index and the column agree with every committed row afterwards.
///
/// <para>The scenarios run on a standalone engine and on a cluster-mode engine, and under each engine
/// default for the locking mode. The optimistic default is the one under which a backfill used to
/// take no lock at all and lose its batches at commit.</para>
/// </summary>
[NonParallelizable]
internal abstract class OnlineBackfillUnderWritesScenarios : SharedNodeBaseTest
{
    /// <summary>Rows the small tables hold; one backfill batch, so halving is exercised within it.</summary>
    private const int SmallRows = 40;

    /// <summary>
    /// Attempts a lock holder keeps a backfill batch losing before it commits. Above the eight attempts
    /// the column fill used to allow, and above the single attempt the index build used to allow.
    /// </summary>
    private const int LossesBeforeRelease = 12;

    protected abstract bool ClusterMode { get; }

    protected abstract KeyValueTransactionLocking WriterLocking { get; }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultTransactionLocking = WriterLocking };

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase));

            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<long>> IdsWhere(CommandExecutor executor, string dbname, string where)
    {
        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, $"SELECT id FROM items WHERE {where}");
        return rows.Select(r => r["id"].LongValue).OrderBy(v => v).ToList();
    }

    /// <summary>
    /// Whether the planner serves <paramref name="sql"/> from an index node. The lookups below are only
    /// evidence about the index when they are answered by it and not by a table scan.
    /// </summary>
    private static async Task<bool> UsedIndex(CommandExecutor executor, string dbname, string sql)
    {
        List<Dictionary<string, ColumnValue>> plan = await Query(executor, dbname, "EXPLAIN " + sql);
        return plan.Any(r => r.TryGetValue("node", out ColumnValue? node) && node.StrValue is "index-lookup" or "index-range-scan");
    }

    /// <summary>The number of entries the index named <paramref name="indexName"/> holds.</summary>
    private static async Task<int> CountIndexEntries(CommandExecutor executor, string dbname, DatabaseDescriptor database, string indexName, bool unique)
    {
        TableIndexSchema index = database.Schema.Tables["items"].Indexes!.First(ix => ix.Name == indexName);
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "items"));

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            int entries = 0;
            await foreach (var _ in table.Store.ScanIndex(tx, index.Id!, [ColumnType.Integer64], from: null, to: null, unique: unique))
                entries++;
            return entries;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>A database with table <c>items</c> holding <paramref name="rows"/> rows, <c>val = id</c>.</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable(int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, val INT64 NOT NULL)");

        const int rowsPerStatement = 200;
        for (int start = 1; start <= rows; start += rowsPerStatement)
        {
            int end = Math.Min(rows, start + rowsPerStatement - 1);
            string values = string.Join(", ", Enumerable.Range(start, end - start + 1).Select(i => $"({i}, {i})"));
            await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, val) VALUES " + values);
        }

        return (dbname, database, executor);
    }

    /// <summary>
    /// A writer that holds the exclusive lock of one row across many backfill attempts. It starts its
    /// transaction on the first attempt of the backfill, so its statement runs under the staged element
    /// and maintains it, and it commits on attempt <see cref="LossesBeforeRelease"/>. It is always
    /// pessimistic, so the lock it holds is a real one under either engine default.
    /// </summary>
    private sealed class HeldRowLock
    {
        private readonly CommandExecutor executor;
        private readonly string dbname;
        private readonly string sql;
        private KvTransaction? tx;
        private int attempts;

        public HeldRowLock(CommandExecutor executor, string dbname, string sql)
        {
            this.executor = executor;
            this.dbname = dbname;
            this.sql = sql;
        }

        public int Attempts => attempts;

        public async Task OnBackfillAttempt()
        {
            int attempt = Interlocked.Increment(ref attempts);

            if (attempt == 1)
            {
                DatabaseDescriptor database = await executor.OpenDatabase(dbname);
                tx = await database.Transactions.BeginAsync(locking: KeyValueTransactionLocking.Pessimistic);
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
                return;
            }

            if (attempt == LossesBeforeRelease && tx is { } held)
            {
                DatabaseDescriptor database = await executor.OpenDatabase(dbname);
                await database.Transactions.CommitAsync(held);
                tx = null;
            }
        }

        public async Task ReleaseAsync()
        {
            if (tx is { } held)
            {
                DatabaseDescriptor database = await executor.OpenDatabase(dbname);
                await database.Transactions.RollbackIfNotCompletedAsync(held);
                tx = null;
            }
        }
    }

    // -----------------------------------------------------------------------
    // A row held by an open transaction
    // -----------------------------------------------------------------------

    /// <summary>
    /// An open transaction holds one row of the batch across more attempts than any former attempt cap.
    /// The index build must keep retrying until the row is free, and must index the value that
    /// transaction committed, not the one the build first scanned.
    /// </summary>
    [Test]
    public async Task IndexBuild_WaitsForARowHeldByAnOpenTransaction_AndIndexesItsCommittedValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(SmallRows);

        HeldRowLock held = new(executor, dbname, "UPDATE items SET val = 1000 WHERE id = 7");
        IndexBackfiller.TestInterceptBeforeBatchLock = held.OnBackfillAttempt;
        try
        {
            await ExecuteDdl(executor, dbname, "CREATE INDEX val_idx ON items (val)");
        }
        finally
        {
            IndexBackfiller.TestInterceptBeforeBatchLock = null;
            await held.ReleaseAsync();
        }

        Assert.That(held.Attempts, Is.GreaterThanOrEqualTo(LossesBeforeRelease), "the build must retry the held row, not give up");

        Assert.That(await UsedIndex(executor, dbname, "SELECT id FROM items WHERE val = 1000"), Is.True, "the lookups must be served by the new index");
        Assert.That(await IdsWhere(executor, dbname, "val = 1000"), Is.EqualTo(new long[] { 7 }), "the committed update is indexed");
        Assert.That(await IdsWhere(executor, dbname, "val = 7"), Is.Empty, "no entry survives for the value the build first scanned");

        for (int id = 1; id <= SmallRows; id++)
        {
            if (id == 7)
                continue;
            Assert.That(await IdsWhere(executor, dbname, $"val = {id}"), Is.EqualTo(new long[] { id }), $"row {id} must be indexed");
        }

        Assert.That(await CountIndexEntries(executor, dbname, database, "val_idx", unique: false), Is.EqualTo(SmallRows), "one entry per row, no stale entry");
    }

    /// <summary>
    /// The same held row during the fill of an added column. The fill must keep retrying, must not
    /// overwrite the update that transaction committed, and must give every row its value.
    /// </summary>
    [Test]
    public async Task ColumnFill_WaitsForARowHeldByAnOpenTransaction_AndKeepsItsCommittedUpdate()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTable(SmallRows);

        HeldRowLock held = new(executor, dbname, "UPDATE items SET val = 1000 WHERE id = 7");
        executor.TestInterceptBeforeFillBatchLock = held.OnBackfillAttempt;
        try
        {
            await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 NOT NULL DEFAULT (7)");
        }
        finally
        {
            executor.TestInterceptBeforeFillBatchLock = null;
            await held.ReleaseAsync();
        }

        Assert.That(held.Attempts, Is.GreaterThanOrEqualTo(LossesBeforeRelease), "the fill must retry the held row, not take the column back");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT id, val, qty FROM items ORDER BY id");
        Assert.That(rows, Has.Count.EqualTo(SmallRows));
        Assert.That(rows.Select(r => r["qty"].LongValue), Is.All.EqualTo(7), "every row holds the default");
        Assert.That(rows.Single(r => r["id"].LongValue == 7)["val"].LongValue, Is.EqualTo(1000), "the committed update survives the fill");
        Assert.That(rows.Where(r => r["id"].LongValue != 7).Select(r => r["val"].LongValue), Is.EqualTo(Enumerable.Range(1, SmallRows).Where(i => i != 7).Select(i => (long)i)));
    }

    // -----------------------------------------------------------------------
    // A row updated while the index is WriteOnly
    // -----------------------------------------------------------------------

    /// <summary>
    /// An UPDATE commits between the unlocked id scan of a batch and the batch's locked read. The index
    /// is WriteOnly, so the UPDATE already wrote the row's entry. The unique build must read the row
    /// under its lock, find the entry the UPDATE wrote for the same row, and accept it; it must not
    /// write an entry for the value it scanned, and must not report a duplicate.
    /// </summary>
    [Test]
    public async Task UniqueIndexBuild_RowUpdatedBeforeItsBatchLocks_HoldsOneEntryForTheNewValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(SmallRows);

        IndexBackfiller.TestInterceptBeforeBatchLock = async () =>
        {
            IndexBackfiller.TestInterceptBeforeBatchLock = null;
            await ExecuteNonQuery(executor, dbname, "UPDATE items SET val = 1000 WHERE id = 7");
        };
        try
        {
            await ExecuteDdl(executor, dbname, "CREATE UNIQUE INDEX val_uniq ON items (val)");
        }
        finally
        {
            IndexBackfiller.TestInterceptBeforeBatchLock = null;
        }

        Assert.That(await IdsWhere(executor, dbname, "val = 1000"), Is.EqualTo(new long[] { 7 }));
        Assert.That(await IdsWhere(executor, dbname, "val = 7"), Is.Empty, "no stale entry for the scanned value");
        Assert.That(await CountIndexEntries(executor, dbname, database, "val_uniq", unique: true), Is.EqualTo(SmallRows));

        // The old value is free again, and the new one is taken.
        await ExecuteNonQuery(executor, dbname, $"INSERT INTO items (id, val) VALUES ({SmallRows + 1}, 7)");
        CamusDBException? duplicate = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, $"INSERT INTO items (id, val) VALUES ({SmallRows + 2}, 1000)"));
        Assert.That(duplicate!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }

    // -----------------------------------------------------------------------
    // Sustained writes
    // -----------------------------------------------------------------------

    /// <summary>
    /// Writers update random rows without pause while an index is built and a column is added over
    /// a table of several batches. Both must complete. Afterwards the index holds exactly one entry per
    /// row and finds every row by its current value, and the column fill lost none of the writers'
    /// updates.
    ///
    /// <para>The column is NOT NULL with a constant default, the shape of the production report. The
    /// writers' statements never name it and must be accepted while it is staged: the row they write
    /// inherits a NULL the fill replaces, and the NOT NULL rule applies to that column only for a value
    /// a statement assigns until the column is Public.</para>
    /// </summary>
    [Test]
    public async Task IndexBuildAndColumnFill_CompleteUnderSustainedWrites_AndAgreeWithEveryRow()
    {
        const int rows = 1_200;
        const int writers = 6;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows);

        ConcurrentDictionary<long, int> applied = new();
        ConcurrentDictionary<long, int> unresolved = new();
        writerErrors.Clear();
        lastWriterError = null;
        using CancellationTokenSource stop = new();

        Task[] writerTasks = new Task[writers];
        for (int w = 0; w < writers; w++)
            writerTasks[w] = Task.Run(() => WriteUntilStopped(executor, dbname, rows, applied, unresolved, stop.Token));

        int indexAttempts = 0, fillAttempts = 0, losses = 0;
        IndexBackfiller.TestInterceptBeforeBatchLock = () =>
        {
            Progress($"index batch attempt {Interlocked.Increment(ref indexAttempts)}; applied so far {applied.Values.Sum()}; writer errors {WriterErrors()}");
            return Task.CompletedTask;
        };
        executor.TestInterceptBeforeFillBatchLock = () =>
        {
            Progress($"fill batch attempt {Interlocked.Increment(ref fillAttempts)}; applied so far {applied.Values.Sum()}; writer errors {WriterErrors()}");
            return Task.CompletedTask;
        };
        BackfillBatchRunner.TestObserveLoss = (count, ex) =>
        {
            int n = Interlocked.Increment(ref losses);
            if (n <= 12 || n % 20 == 0)
                Progress($"loss {n} over {count} rows: {ex.Code} {ex.Message}");
        };

        try
        {
            Progress("CREATE INDEX start");
            await ExecuteDdl(executor, dbname, "CREATE INDEX val_idx ON items (val)");
            Progress("CREATE INDEX done; ADD COLUMN start");
            await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 NOT NULL DEFAULT (7)");
            Progress("ADD COLUMN done");
        }
        finally
        {
            IndexBackfiller.TestInterceptBeforeBatchLock = null;
            executor.TestInterceptBeforeFillBatchLock = null;
            BackfillBatchRunner.TestObserveLoss = null;
            stop.Cancel();
            await Task.WhenAll(writerTasks);
            Progress($"writers stopped; applied {applied.Values.Sum()}, unresolved {unresolved.Values.Sum()}; writer errors {WriterErrors()}");
        }

        Assert.That(applied.Values.Sum(), Is.GreaterThan(0), "the writers must have run");

        await DescribeStoredLayouts(executor, dbname, database, applied);

        List<Dictionary<string, ColumnValue>> table = await Query(executor, dbname, "SELECT id, val, qty FROM items ORDER BY id");
        Assert.That(table, Has.Count.EqualTo(rows));
        Assert.That(table.Select(r => r["qty"].LongValue), Is.All.EqualTo(7), "every row holds the default");

        foreach (Dictionary<string, ColumnValue> row in table)
        {
            long id = row["id"].LongValue;
            long expected = id + applied.GetValueOrDefault(id);
            long slack = unresolved.GetValueOrDefault(id);
            Assert.That(row["val"].LongValue, Is.InRange(expected, expected + slack), $"row {id} must hold every update the writers committed");
        }

        Assert.That(await UsedIndex(executor, dbname, "SELECT id FROM items WHERE val = 1"), Is.True, "the lookups must be served by the new index");

        foreach (IGrouping<long, long> byValue in table.GroupBy(r => r["val"].LongValue, r => r["id"].LongValue))
            Assert.That(await IdsWhere(executor, dbname, $"val = {byValue.Key}"), Is.EqualTo(byValue.OrderBy(v => v).ToList()), $"the index must find every row with val = {byValue.Key}");

        Assert.That(await CountIndexEntries(executor, dbname, database, "val_idx", unique: false), Is.EqualTo(rows), "one entry per row, no stale entry");
    }

    /// <summary>
    /// Increments <c>val</c> of random rows, one autocommit statement at a time, until stopped. A
    /// statement the engine refuses with a retryable code is run again; one whose commit outcome is
    /// unknown is counted as possibly applied.
    /// </summary>
    private static async Task WriteUntilStopped(
        CommandExecutor executor, string dbname, int rows,
        ConcurrentDictionary<long, int> applied, ConcurrentDictionary<long, int> unresolved, CancellationToken stop)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Random random = new();

        while (!stop.IsCancellationRequested)
        {
            long id = random.Next(1, rows + 1);

            KvTransaction tx = await database.Transactions.BeginAsync();
            try
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, $"UPDATE items SET val = val + 1 WHERE id = {id}", null));
                await database.Transactions.CommitAsync(tx);
                applied.AddOrUpdate(id, 1, static (_, n) => n + 1);
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
            {
                CountWriterError(ex);
                await Task.Delay(1);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved)
            {
                CountWriterError(ex);
                unresolved.AddOrUpdate(id, 1, static (_, n) => n + 1);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }
    }

    /// <summary>
    /// Reports, for the test log, the stored layout version of every row after the change and every
    /// row whose added column reads NULL: which writer last touched it and under which layout.
    /// </summary>
    private static async Task DescribeStoredLayouts(CommandExecutor executor, string dbname, DatabaseDescriptor database, ConcurrentDictionary<long, int> applied)
    {
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "items"));
        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly);
        try
        {
            SortedDictionary<int, int> versions = new();
            List<string> nullRows = [];

            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(tx, largeValues: CamusDB.Core.Storage.Kv.LargeValueFetch.Raw))
            {
                int stored = CamusDB.Core.Storage.Kv.RowEncoder.ReadStoredSchemaVersion(data.Span);
                versions[stored] = versions.GetValueOrDefault(stored) + 1;

                Dictionary<string, ColumnValue> row = await CamusDB.Core.Storage.Kv.RowEncoder.DecodeWritableAsync(
                    table.Schema, tx.TransactionId, rowId, data, visibilitySchemaVersion: table.Schema.Version);

                if (!row.TryGetValue("qty", out ColumnValue? qty) || qty.Type == ColumnType.Null)
                {
                    long id = row["id"].LongValue;
                    nullRows.Add($"id={id} stored={stored} val={row["val"].LongValue} applied={applied.GetValueOrDefault(id)}");
                }
            }

            Progress($"schema version {table.Schema.Version}; stored layouts {string.Join(", ", versions.Select(kv => $"v{kv.Key}x{kv.Value}"))}; null rows: {string.Join("; ", nullRows)}");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static readonly ConcurrentDictionary<string, int> writerErrors = new();

    private static string? lastWriterError;

    private static void CountWriterError(CamusDBException ex)
    {
        writerErrors.AddOrUpdate(ex.Code, 1, static (_, n) => n + 1);
        lastWriterError = ex.Message;
    }

    private static string WriterErrors()
        => string.Join(", ", writerErrors.Select(kv => $"{kv.Key}={kv.Value}")) + (lastWriterError is null ? "" : $" last: {lastWriterError}");

    private static void Progress(string message)
        => TestContext.Progress.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {message}");
}

/// <summary><see cref="OnlineBackfillUnderWritesScenarios"/> on a standalone engine with pessimistic writers.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOnlineBackfillUnderWritesStandalone : OnlineBackfillUnderWritesScenarios
{
    protected override bool ClusterMode => false;

    protected override KeyValueTransactionLocking WriterLocking => KeyValueTransactionLocking.Pessimistic;
}

/// <summary><see cref="OnlineBackfillUnderWritesScenarios"/> on a standalone engine with optimistic writers.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOnlineBackfillUnderWritesStandaloneOptimistic : OnlineBackfillUnderWritesScenarios
{
    protected override bool ClusterMode => false;

    protected override KeyValueTransactionLocking WriterLocking => KeyValueTransactionLocking.Optimistic;
}

/// <summary><see cref="OnlineBackfillUnderWritesScenarios"/> on a cluster-mode engine with pessimistic writers.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOnlineBackfillUnderWritesCluster : OnlineBackfillUnderWritesScenarios
{
    protected override bool ClusterMode => true;

    protected override KeyValueTransactionLocking WriterLocking => KeyValueTransactionLocking.Pessimistic;
}

/// <summary>
/// <see cref="OnlineBackfillUnderWritesScenarios"/> on a cluster-mode engine with optimistic writers: the
/// configuration under which the backfills gave up on a busy table.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOnlineBackfillUnderWritesClusterOptimistic : OnlineBackfillUnderWritesScenarios
{
    protected override bool ClusterMode => true;

    protected override KeyValueTransactionLocking WriterLocking => KeyValueTransactionLocking.Optimistic;
}
