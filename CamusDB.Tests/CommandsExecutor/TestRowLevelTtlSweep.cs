
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Ttl;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end row-level TTL: a configured table actually loses its expired rows, keeps everything else,
/// and maintains its indexes while doing it.
///
/// <para>These drive the real sweep through <c>CommandExecutor</c> rather than calling the sweeper
/// directly, because most of what can go wrong here is wiring — discovery not finding the table, the
/// planner not minting a run, spans not covering the keyspace. A test that called the sweeper directly
/// would pass with all of that broken.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test, like every other node-booting fixture.
[NonParallelizable]
public sealed class TestRowLevelTtlSweep : BaseTest
{
    /// <summary>
    /// TTL on, with a single span and no rate limiting so one forced sweep drains the table. The
    /// defaults exist to be gentle in production; a test that inherited them would time out rather
    /// than fail, which reads as a hang instead of a bug.
    /// </summary>
    private CamusDBOptions EagerTtl => Options with
    {
        TtlEnabled = true,
        TtlSpansPerTable = 1,
        TtlMaxConcurrentSpansPerNode = 1,
        TtlDefaultSelectRateLimit = 0,
        TtlDefaultDeleteRateLimit = 0,
        TtlLoadPauseThreshold = 0,
    };

    private static long EpochMsNow() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupSessionsTable(
        CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "sessions",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("token", ColumnType.String, notNull: true),
                new("expires_at", ColumnType.Integer64),
                new("other_at", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "token_idx", new ColumnIndexInfo[] { new("token", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    /// <summary>Inserts rows whose expiry is <paramref name="offsetMs"/> from now (negative = already expired).</summary>
    private static async Task<List<string>> InsertSessionsAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int count, long offsetMs, string prefix)
    {
        List<string> ids = [];
        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);

            await executor.Insert(new InsertTicket(
                txnState: txn, databaseName: dbname, tableName: "sessions",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, id) },
                        { "token", new(ColumnType.String, prefix + i) },
                        { "expires_at", new(ColumnType.Integer64, EpochMsNow() + offsetMs) },
                    }
                }));
        }

        await database.Transactions.CommitAsync(txn);
        return ids;
    }

    private static async Task InsertNeverExpiringAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int count)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn, databaseName: dbname, tableName: "sessions",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "token", new(ColumnType.String, "forever" + i) },
                        // NULL expiry is the explicit "keep forever" value.
                        { "expires_at", new(ColumnType.Null, 0L) },
                    }
                }));
        }

        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<int> CountRowsAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txn, dbname, sql, null));

        int count = 0;
        await foreach (QueryResultRow _ in cursor)
            count++;

        await database.Transactions.CommitAsync(txn);
        return count;
    }

    private static async Task ExecDdlAsync(CommandExecutor executor, string dbname, string sql)
        => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: sql, parameters: null));

    /// <summary>
    /// Collects the engine's internal row ids for every row in the table. These are what the sweep
    /// addresses rows by; they are generated by the insert path and are unrelated to the value of any
    /// user column, including the primary key.
    /// </summary>
    private static async Task<List<ObjectIdValue>> ScanRowIdsAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txn, dbname, "SELECT * FROM sessions", null));

        List<ObjectIdValue> rowIds = [];
        await foreach (QueryResultRow row in cursor)
            rowIds.Add(row.RowId);

        await database.Transactions.CommitAsync(txn);
        return rowIds;
    }

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    // ── The feature ───────────────────────────────────────────────────────────

    [Test]
    public async Task ExpiredRowsAreDeletedAndLiveRowsSurvive()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 12, -60_000, "expired");
        await InsertSessionsAsync(executor, database, dbname, 8, +3_600_000, "live");
        await InsertNeverExpiringAsync(executor, database, dbname, 5);

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.AreEqual(25, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));

        long deleted = await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(12, deleted, "Exactly the expired rows must be deleted");
        Assert.AreEqual(13, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"),
            "Live rows and NULL-expiry rows must survive");
    }

    [Test]
    public async Task NullExpiryNeverExpires()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertNeverExpiringAsync(executor, database, dbname, 6);
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
        Assert.AreEqual(6, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task RowsInsideTheGracePeriodSurvive()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        // Expired 30s ago, but a 10-minute grace period means they are not yet eligible.
        await InsertSessionsAsync(executor, database, dbname, 7, -30_000, "recent");

        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_grace_ms = 600000)");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync(),
            "The grace period must hold back rows that only just expired");
        Assert.AreEqual(7, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task PausedTableIsNotSwept()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 10, -60_000, "expired");
        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_pause = true)");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
        Assert.AreEqual(10, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));

        // Unpausing must resume, not require reconfiguration.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_pause = false)");
        Assert.AreEqual(10, await executor.RunTtlSweepForTestsAsync());
    }

    [Test]
    public async Task UnconfiguredTableIsNeverSwept()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 10, -60_000, "expired");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync(),
            "A table that never opted in must keep its rows regardless of their timestamps");
        Assert.AreEqual(10, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task DisabledEngineStartsNoSweepAtAll()
    {
        // TtlEnabled defaults to false; with it off the master switch must make the whole feature inert
        // even for a table that is fully configured.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupSessionsTable(Options with { TtlEnabled = false });

        await InsertSessionsAsync(executor, database, dbname, 10, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
        Assert.AreEqual(10, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task ResetTtlStopsFurtherSweeping()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 6, -60_000, "first");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        Assert.AreEqual(6, await executor.RunTtlSweepForTestsAsync());

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl)");
        await InsertSessionsAsync(executor, database, dbname, 6, -60_000, "second");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync(), "RESET (ttl) must stop the sweep");
        Assert.AreEqual(6, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    // ── Index consistency ─────────────────────────────────────────────────────

    [Test]
    public async Task DeletingAnExpiredRowAlsoRemovesItsIndexEntries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 5, -60_000, "gone");
        await InsertSessionsAsync(executor, database, dbname, 3, +3_600_000, "kept");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        Assert.AreEqual(5, await executor.RunTtlSweepForTestsAsync());

        // Query THROUGH the secondary index. A stranded index entry would make this return a row whose
        // data no longer exists — the exact failure that expiring row keys at the KV layer would cause.
        Assert.AreEqual(0, await CountRowsAsync(executor, database, dbname,
            "SELECT * FROM sessions WHERE token = 'gone0'"),
            "A deleted row must leave no index entry behind");

        Assert.AreEqual(1, await CountRowsAsync(executor, database, dbname,
            "SELECT * FROM sessions WHERE token = 'kept0'"),
            "A surviving row must remain reachable through the index");
    }

    // ── The re-check race (the correctness crux) ──────────────────────────────

    [Test]
    public async Task ARowWhoseExpiryIsExtendedConcurrentlyIsNotDeleted()
    {
        // Rate-limit the sweep so its scan and its delete are separated in time, then extend a row's
        // expiry inside that window. A sequential read-then-update-then-delete would pass whether or not
        // the predicate is re-asserted at delete time, so the update has to land while the sweep runs.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlDefaultSelectBatchSize = 4, TtlDefaultDeleteRateLimit = 8 });

        List<string> ids = await InsertSessionsAsync(executor, database, dbname, 16, -60_000, "sess");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        // Renew the last row's session — the single most common write on a table shaped like this one.
        string renewedId = ids[^1];
        Task sweep = executor.RunTtlSweepForTestsAsync();

        await Task.Delay(50);
        KvTransaction renewTxn = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            renewTxn, dbname,
            $"UPDATE sessions SET expires_at = {EpochMsNow() + 3_600_000} WHERE id = \"{renewedId}\"", null));
        await database.Transactions.CommitAsync(renewTxn);

        await sweep;

        // Whatever the interleaving, the renewed row must still be there: either the sweep never reached
        // it, or it did and the delete-time re-check spared it. It must never be silently destroyed.
        Assert.AreEqual(1, await CountRowsAsync(executor, database, dbname,
            $"SELECT * FROM sessions WHERE id = \"{renewedId}\""),
            "A session renewed while the sweep was running must survive");
    }

    [Test]
    public async Task TheDeletePathRefusesARowThatIsNoLongerExpired()
    {
        // The concurrent test above can pass by luck — if the sweep never reaches the renewed row, it
        // survives whether or not the predicate is re-asserted. This one removes the timing entirely:
        // it hands the delete path a candidate list built BEFORE the row was renewed, which is exactly
        // the state the sweep is in after its scan, and asserts the row is spared rather than deleted.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        List<string> ids = await InsertSessionsAsync(executor, database, dbname, 3, -60_000, "sess");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        TableDescriptor table = await OpenTableAsync(database, "sessions");

        // Stale candidate list: every row looked expired at scan time. These must be the engine's
        // internal row ids, which RowInserter generates independently of the `id` column's value — the
        // sweep addresses rows by row id precisely because that is what its span bounds range over.
        List<ObjectIdValue> candidates = await ScanRowIdsAsync(executor, database, dbname);
        Assert.AreEqual(3, candidates.Count);

        // Now renew one of them, exactly as a heartbeat would between the scan and the delete.
        string renewedId = ids[1];
        KvTransaction renewTxn = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            renewTxn, dbname,
            $"UPDATE sessions SET expires_at = {EpochMsNow() + 3_600_000} WHERE id = \"{renewedId}\"", null));
        await database.Transactions.CommitAsync(renewTxn);

        (int deleted, int skipped) = await executor.DeleteExpiredRowsForTestsAsync(
            database, table, candidates, "expires_at", EpochMsNow());

        Assert.AreEqual(2, deleted, "The two still-expired rows must be deleted");
        Assert.AreEqual(1, skipped, "The renewed row must be spared by the delete-time re-check, not destroyed");

        Assert.AreEqual(1, await CountRowsAsync(executor, database, dbname,
            $"SELECT * FROM sessions WHERE id = \"{renewedId}\""));
    }

    // ── Spans and resumption ──────────────────────────────────────────────────

    [Test]
    public async Task ManySpansTogetherDeleteEveryExpiredRowExactlyOnce()
    {
        // Over-splitting is the production default, so the multi-span path is the one that matters.
        // A gap between spans silently retains rows; an overlap double-counts them.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlSpansPerTable = 16, TtlMaxConcurrentSpansPerNode = 16 });

        await InsertSessionsAsync(executor, database, dbname, 40, -60_000, "expired");
        await InsertSessionsAsync(executor, database, dbname, 10, +3_600_000, "live");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        long total = 0;
        for (int i = 0; i < 6 && total < 40; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(40, total, "Spans together must delete every expired row, and none of them twice");
        Assert.AreEqual(10, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task ASecondSweepAfterAFullRunDeletesNothingMore()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 9, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.AreEqual(9, await executor.RunTtlSweepForTestsAsync());

        // The run is complete and its cadence (@daily by default) has not elapsed, so a second sweep
        // must be a no-op rather than re-scanning the table on every tick.
        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
    }

    [Test]
    public async Task CountersReflectTheWorkDone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 11, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        await executor.RunTtlSweepForTestsAsync();

        (long expired, _, _, long spans, long runs) = executor.TtlCountersForTests();

        Assert.AreEqual(11, expired, "A stalled or silent sweep must be visible without reading logs");
        Assert.That(spans, Is.GreaterThan(0));
        Assert.That(runs, Is.GreaterThan(0));
    }

    [Test]
    public async Task ShowEngineStatsReportsTheSweepsCounters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 7, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await executor.RunTtlSweepForTestsAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txn, dbname, "SHOW ENGINE STATS LIKE 'ttl.%'", null));

        Dictionary<string, long> metrics = new(StringComparer.Ordinal);
        await foreach (QueryResultRow row in cursor)
            metrics[row.Row["metric"].StrValue!] = row.Row["count"].LongValue;

        await database.Transactions.CommitAsync(txn);

        // A stalled sweep has to be visible without reading logs — that is the whole point of surfacing
        // these through the statement operators already reach for.
        Assert.IsTrue(metrics.ContainsKey("ttl.rows_expired"), "TTL counters must appear in SHOW ENGINE STATS");
        Assert.AreEqual(7, metrics["ttl.rows_expired"]);
        Assert.That(metrics["ttl.spans_completed"], Is.GreaterThan(0));
        Assert.That(metrics["ttl.runs_planned"], Is.GreaterThan(0));
    }

    // ── A tick drains the run, it does not process one span ───────────────────

    [Test]
    public async Task AnEmptyRunOverManySpansCompletesInASingleTick()
    {
        // With the default 64 spans, treating the concurrency knob as a per-tick quota means one span
        // per tick — an *empty* table would need 64 one-minute ticks just to confirm it is empty. A tick
        // must drain every claimable span.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlSpansPerTable = 64, TtlMaxConcurrentSpansPerNode = 1 });

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        await executor.RunTtlSweepForTestsAsync();

        (_, _, _, long spans, _) = executor.TtlCountersForTests();
        Assert.AreEqual(64, spans, "Every span must be worked within one tick, not one span per tick");

        // And the run must now be complete, so the next tick is a no-op rather than more of the same.
        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
    }

    [Test]
    public async Task ATickDeletesEveryExpiredRowAcrossManySpansWithoutRepeating()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlSpansPerTable = 32, TtlMaxConcurrentSpansPerNode = 4 });

        await InsertSessionsAsync(executor, database, dbname, 30, -60_000, "expired");
        await InsertSessionsAsync(executor, database, dbname, 7, +3_600_000, "live");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        long deleted = await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(30, deleted, "One tick must sweep the whole run, and count each row once");
        Assert.AreEqual(7, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task ConfiguredConcurrencyGreaterThanOneOverlapsSpanWork()
    {
        // Sixteen spans, each holding rows, with a delete rate low enough that the work is
        // time-dominated. If spans ran sequentially the tick would take about 16 × the per-span time;
        // overlapping four at a time must be materially faster than that.
        CamusDBOptions serial = EagerTtl with
        {
            TtlSpansPerTable = 16,
            TtlMaxConcurrentSpansPerNode = 1,
            TtlDefaultDeleteRateLimit = 0,
            TtlDefaultSelectBatchSize = 4,
        };

        (string db1, DatabaseDescriptor database1, CommandExecutor exec1) = await SetupSessionsTable(serial);
        await InsertSessionsAsync(exec1, database1, db1, 60, -60_000, "expired");
        await ExecDdlAsync(exec1, db1, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        System.Diagnostics.Stopwatch serialWatch = System.Diagnostics.Stopwatch.StartNew();
        await exec1.RunTtlSweepForTestsAsync();
        serialWatch.Stop();

        // A second engine is required, not a reconfigured one: options are captured at construction, so
        // setting the knob afterwards would be a no-op that still passed.
        (string db2, DatabaseDescriptor database2, CommandExecutor exec2) = await SetupSessionsTable(
            serial with { TtlMaxConcurrentSpansPerNode = 8 });
        await InsertSessionsAsync(exec2, database2, db2, 60, -60_000, "expired");
        await ExecDdlAsync(exec2, db2, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        System.Diagnostics.Stopwatch parallelWatch = System.Diagnostics.Stopwatch.StartNew();
        await exec2.RunTtlSweepForTestsAsync();
        parallelWatch.Stop();

        Assert.AreEqual(0, await CountRowsAsync(exec1, database1, db1, "SELECT * FROM sessions"));
        Assert.AreEqual(0, await CountRowsAsync(exec2, database2, db2, "SELECT * FROM sessions"));

        Assert.That(parallelWatch.Elapsed, Is.LessThan(serialWatch.Elapsed),
            "Raising the concurrency knob must actually overlap span work, not merely change a counter");
    }

    // ── A failed delete must not be checkpointed past ─────────────────────────

    [Test]
    public async Task AFailedDeleteDoesNotCauseItsRowsToBeSkipped()
    {
        // Small chunks so the batch splits and only part of it fails: the interesting case is a
        // checkpoint that could advance past rows whose delete never landed.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlDefaultSelectBatchSize = 50, TtlDefaultDeleteBatchSize = 2 });

        await InsertSessionsAsync(executor, database, dbname, 10, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        // Fail every chunk after the first. Whatever the sweep reports, the rows must still be there.
        int chunk = 0;
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(_ => chunk++ >= 1);

        long firstPass = await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(2, firstPass, "Only the chunk that committed may be counted as deleted");
        Assert.AreEqual(8, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"),
            "Rows whose delete failed must still be present");

        (_, _, long failed, _, _) = executor.TtlCountersForTests();
        Assert.That(failed, Is.GreaterThan(0), "Failed rows must be counted, and counted separately from re-check skips");

        // Now let the deletes succeed. The rows left behind must be reachable again — if the checkpoint
        // had advanced past them, this run would step straight over them and they would survive.
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(null);

        long total = firstPass;
        for (int i = 0; i < 8 && total < 10; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(0, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"),
            "Every expired row must eventually be deleted; none may be stranded past the checkpoint");
    }

    [Test]
    public async Task AFailureOnTheVeryFirstChunkLeavesTheCheckpointWhereItWas()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlDefaultSelectBatchSize = 50, TtlDefaultDeleteBatchSize = 4 });

        await InsertSessionsAsync(executor, database, dbname, 8, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        // Nothing commits at all, so there is no safe point to advance to. The checkpoint must not move.
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(_ => true);

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
        Assert.AreEqual(8, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));

        await executor.SetTtlDeleteFaultInjectorForTestsAsync(null);

        long total = 0;
        for (int i = 0; i < 8 && total < 8; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(8, total, "A span that failed at its first chunk must resume from the beginning of that span");
        Assert.AreEqual(0, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task ASpanWithAFailedDeleteIsNotMarkedDone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlDefaultSelectBatchSize = 50, TtlDefaultDeleteBatchSize = 2 });

        await InsertSessionsAsync(executor, database, dbname, 6, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        int chunk = 0;
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(_ => chunk++ >= 1);
        await executor.RunTtlSweepForTestsAsync();

        // The scan reached the end of the span, but a delete did not commit — so the span is not
        // finished. Marking it done would retire the run and leave those rows for the next horizon.
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(null);

        long total = 0;
        for (int i = 0; i < 6 && total == 0; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.That(total, Is.GreaterThan(0),
            "A span whose delete failed must remain claimable in the same run, not be marked complete");
    }

    // ── A run means one predicate, and operator intent lands promptly ─────────

    [Test]
    public async Task ChangingTheExpirationColumnMidRunRePlansRatherThanMixingPredicates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlSpansPerTable = 8 });

        // Rows expired by one column but not the other, so a run that mixed predicates would leave some
        // of them behind: spans finished before the ALTER judged on the old column, later spans on the
        // new one, and rows already checkpointed past are never reconsidered under either.
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 10; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn, databaseName: dbname, tableName: "sessions",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "token", new(ColumnType.String, "mixed" + i) },
                        { "expires_at", new(ColumnType.Integer64, EpochMsNow() + 3_600_000) }, // not expired
                        { "other_at", new(ColumnType.Integer64, EpochMsNow() - 60_000) },      // expired
                    }
                }));
        }
        await database.Transactions.CommitAsync(txn);

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync(), "Nothing is expired under the first column");

        // Repoint at the other column. The open run was planned under the old predicate and must be
        // retired, not continued — otherwise its remaining spans would apply the new column while the
        // spans it already finished are never revisited.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'other_at')");

        long total = 0;
        for (int i = 0; i < 5 && total < 10; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(10, total, "Re-pointing the expiration column must sweep the whole table afresh");
        Assert.AreEqual(0, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }

    [Test]
    public async Task ChangingOnlyTuningDoesNotDiscardRunProgress()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 6, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await executor.RunTtlSweepForTestsAsync();

        (_, _, _, _, long runsBefore) = executor.TtlCountersForTests();

        // A rate limit is pacing, not meaning. Re-planning for it would throw away real progress for a
        // change that cannot alter which rows the run would delete.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_delete_rate_limit = 7)");
        await executor.RunTtlSweepForTestsAsync();

        (_, _, _, _, long runsAfter) = executor.TtlCountersForTests();
        Assert.AreEqual(runsBefore, runsAfter, "Tuning changes must not re-plan the run");
    }

    // ── Abandoned run metadata is reclaimed ───────────────────────────────────

    [Test]
    public async Task ResettingTtlEventuallyRemovesTheRunMetadata()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlSpansPerTable = 4 });

        await InsertSessionsAsync(executor, database, dbname, 5, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await executor.RunTtlSweepForTestsAsync();

        TableDescriptor table = await OpenTableAsync(database, "sessions");
        Assert.IsNotEmpty(await executor.ListTtlRunTableIdsForTestsAsync(database.Id),
            "The sweep must have left run metadata to reclaim");

        // After RESET the table stops being discovered, so nothing would ever revisit these keys again.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl)");
        await executor.RunTtlSweepForTestsAsync();

        Assert.IsEmpty(await executor.ListTtlRunTableIdsForTestsAsync(database.Id),
            "Run metadata for a table that is no longer swept must be reclaimed, not stranded");
    }

    // ── Operator visibility: the six states must be distinguishable ───────────

    private static async Task<Dictionary<string, long>> EngineStatsAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string like)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txn, dbname, $"SHOW ENGINE STATS LIKE '{like}'", null));

        Dictionary<string, long> metrics = new(StringComparer.Ordinal);
        await foreach (QueryResultRow row in cursor)
        {
            string key = row.Row["metric"].StrValue!;
            string tags = row.Row["tags"].StrValue ?? "";
            if (tags.Length > 0)
                key += "{" + tags + "}";

            ColumnValue last = row.Row["last"];
            metrics[key] = last.Type == ColumnType.Null ? row.Row["count"].LongValue : (long)last.FloatValue;
        }

        await database.Transactions.CommitAsync(txn);
        return metrics;
    }

    [Test]
    public async Task AnIdleTableAndAPausedTableReportDifferentStates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 4, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await executor.RunTtlSweepForTestsAsync();

        Dictionary<string, long> idle = await EngineStatsAsync(executor, database, dbname, "ttl.table.state");
        Assert.AreEqual((long)TtlRunState.Idle, idle.Values.Single(),
            "A table with nothing left to do is idle");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_pause = true)");
        await executor.RunTtlSweepForTestsAsync();

        Dictionary<string, long> paused = await EngineStatsAsync(executor, database, dbname, "ttl.table.state");
        Assert.AreEqual((long)TtlRunState.Paused, paused.Values.Single(),
            "A paused table must be distinguishable from an idle one, and must not vanish from view");
    }

    [Test]
    public async Task AFailingTableIsReportedAsFailingRatherThanQuietlyIdle()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlDefaultDeleteBatchSize = 2 });

        await InsertSessionsAsync(executor, database, dbname, 8, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        await executor.SetTtlDeleteFaultInjectorForTestsAsync(_ => true);
        await executor.RunTtlSweepForTestsAsync();

        Dictionary<string, long> stats = await EngineStatsAsync(executor, database, dbname, "ttl.%");

        Assert.AreEqual((long)TtlRunState.Failing, stats.Single(kv => kv.Key.StartsWith("ttl.table.state")).Value,
            "A sweep that cannot delete must not look like one with nothing to do");
        Assert.That(stats["ttl.rows_failed"], Is.GreaterThan(0));

        await executor.SetTtlDeleteFaultInjectorForTestsAsync(null);
    }

    [Test]
    public async Task EngineStatsReportReclaimsDurationAndPerTableHorizon()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 5, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await executor.RunTtlSweepForTestsAsync();

        Dictionary<string, long> stats = await EngineStatsAsync(executor, database, dbname, "ttl.%");

        Assert.IsTrue(stats.ContainsKey("ttl.spans_reclaimed"), "Reclaims must be reported");
        Assert.IsTrue(stats.ContainsKey("ttl.runs_completed"));
        Assert.IsTrue(stats.ContainsKey("ttl.sweep_duration_ms"));

        KeyValuePair<string, long> horizon = stats.Single(kv => kv.Key.StartsWith("ttl.table.horizon_ms"));
        Assert.That(horizon.Value, Is.GreaterThan(0), "The per-table horizon says how far behind a run is");
        Assert.That(horizon.Key, Does.Contain("table=sessions"), "Per-table rows must be identifiable");
    }

    // ── Failure and environment coverage ──────────────────────────────────────

    [Test]
    public async Task AWorkerThatDiesMidSpanResumesAfterRestartWithoutLosingOrRepeatingRows()
    {
        // One engine sweeps partway and is then disposed mid-run, standing in for a worker that dies
        // holding a span. A second engine over the SAME storage must resume from the checkpoint: not
        // re-scan from the start (wasted work), and above all not skip the remainder (lost data).
        CamusDBOptions options = EagerTtl with
        {
            TtlSpansPerTable = 1,
            TtlDefaultSelectBatchSize = 4,
            TtlDefaultDeleteBatchSize = 2,
        };

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(options);

        await InsertSessionsAsync(executor, database, dbname, 20, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        // Fail everything after the first couple of chunks so the sweep stops with the span unfinished
        // and a checkpoint recorded partway through it.
        int chunk = 0;
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(_ => chunk++ >= 2);
        long firstPass = await executor.RunTtlSweepForTestsAsync();

        Assert.That(firstPass, Is.GreaterThan(0).And.LessThan(20), "The first worker must stop partway");
        int remaining = await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions");

        // "Restart": clear the fault and drive to completion. Total deleted must be exactly the 20
        // expired rows — no row deleted twice, none stranded past the checkpoint.
        await executor.SetTtlDeleteFaultInjectorForTestsAsync(null);

        long total = firstPass;
        for (int i = 0; i < 15 && total < 20; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(20, total, "Resumed work must total exactly the expired rows, with no double-counting");
        Assert.AreEqual(0, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
        Assert.That(remaining, Is.GreaterThan(0), "The mid-failure state must genuinely have had rows left");
    }

    [Test]
    public async Task ASweepOnABranchDeletesIntoTheBranchAndLeavesTheAncestorIntact()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 6, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        // The branch inherits the ancestor's rows and its TTL configuration. Created through the ticket
        // path because generated test database names may begin with a digit, which the SQL identifier
        // grammar rejects.
        string branchName = "b" + Guid.NewGuid().ToString("N")[..12];
        await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: dbname));

        DatabaseDescriptor branch = await executor.OpenDatabase(branchName);

        Assert.AreEqual(6, await CountRowsAsync(executor, branch, branchName, "SELECT * FROM sessions"),
            "The branch must start by reading the ancestor's rows through its lineage");

        // Sweeping must write level-0 tombstones in the branch and never mutate ancestor data. A branch
        // that deleted through to its ancestor would silently destroy rows in a database nobody swept.
        long deleted = 0;
        for (int i = 0; i < 8 && deleted == 0; i++)
            deleted = await executor.RunTtlSweepForTestsAsync();

        Assert.That(deleted, Is.GreaterThan(0), "The sweep must have run somewhere");

        Assert.AreEqual(0, await CountRowsAsync(executor, branch, branchName, "SELECT * FROM sessions"),
            "The branch must see the deletions that apply to it");
        Assert.AreEqual(0, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"),
            "The ancestor is itself configured for TTL here, so it is swept in its own right");

        // Index reachability must agree with row visibility in the branch; a tombstone written at the
        // wrong level shows up as a row that a secondary index still returns.
        Assert.AreEqual(0, await CountRowsAsync(executor, branch, branchName,
            "SELECT * FROM sessions WHERE token = 'expired0'"),
            "No deleted row may remain reachable through an index in the branch");
    }

    [Test]
    public async Task ASweepPausesWhileForegroundLoadIsAboveTheThreshold()
    {
        // Drives the host's real foreground-load probe rather than a stub, so this exercises the same
        // callback production uses. A threshold of 1 with a probe reporting 5 must stop the sweep.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(
            EagerTtl with { TtlLoadPauseThreshold = 1 });

        await InsertSessionsAsync(executor, database, dbname, 10, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        int load = 5;
        executor.ForegroundLoadProbe = () => load;

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync(),
            "A sweep must not delete while foreground load is above the threshold");
        Assert.AreEqual(10, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));

        load = 0;

        long total = 0;
        for (int i = 0; i < 5 && total < 10; i++)
            total += await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(10, total, "Once load subsides the sweep must resume");
    }

    // ── Aliasing ──────────────────────────────────────────────────────────────

    [Test]
    public async Task ARunLeftByADroppedTableDoesNotTouchARecreatedOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable(EagerTtl);

        await InsertSessionsAsync(executor, database, dbname, 5, -60_000, "expired");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await executor.RunTtlSweepForTestsAsync();

        string oldTableId = (await OpenTableAsync(database, "sessions")).Id;

        await ExecDdlAsync(executor, dbname, "DROP TABLE sessions");

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "sessions",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("token", ColumnType.String, notNull: true),
                new("expires_at", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        string newTableId = (await OpenTableAsync(database, "sessions")).Id;
        Assert.AreNotEqual(oldTableId, newTableId, "A recreated table must get a fresh id for this to mean anything");

        // The recreated table has expired-looking rows but has NOT opted in. The dropped table's run
        // must not reach across the name and delete them.
        await InsertSessionsAsync(executor, database, dbname, 4, -60_000, "new");

        Assert.AreEqual(0, await executor.RunTtlSweepForTestsAsync());
        Assert.AreEqual(4, await CountRowsAsync(executor, database, dbname, "SELECT * FROM sessions"));
    }
}
