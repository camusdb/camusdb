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

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the slow query log and <c>SHOW SLOW QUERIES</c>, driven through
/// <see cref="ExecuteSQLTicket"/> so every statement takes the path a console session takes.
///
/// <para><b>Every test builds its own engine with the configuration it needs.</b> An engine fixes
/// its settings when it is constructed, so enabling the log after building one would be a no-op that
/// still passes — coverage in appearance only. That is also why the disabled arm gets a second
/// engine rather than reusing the first.</para>
///
/// <para><b>The threshold is 0 ms in most tests</b>, which records every statement, including the
/// <c>SHOW SLOW QUERIES</c> that reads the log. A statement is recorded when its cursor ends, and
/// the snapshot the cursor is built from is taken before that, so a <c>SHOW</c> never sees itself —
/// but it does see the previous one. Assertions therefore narrow by kind or by text rather than
/// counting every row.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSlowQueryLogStatements : BaseTest
{
    private static CamusDBOptions Logging(CamusDBOptions options) =>
        options with { SlowQueryLogEnabled = true, SlowQueryLogThresholdMs = 0 };

    private static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor, string db, string sql, KvTransaction? txnState = null)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState ?? KvTransaction.CreateReadOnly(), database: db, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    /// <summary>Reads the log without a transaction, the way a server-level statement is reached.</summary>
    private static Task<List<QueryResultRow>> ShowSlowQueriesAsync(CommandExecutor executor, string db, string sql = "SHOW SLOW QUERIES")
        => QueryAsync(executor, db, sql, txnState: null!);

    private static string? Text(QueryResultRow row, string column) => row.Row[column].StrValue;

    private static long Number(QueryResultRow row, string column) => row.Row[column].LongValue;

    private static bool Flag(QueryResultRow row, string column) => row.Row[column].BoolValue;

    private static QueryResultRow? FirstWithSql(IEnumerable<QueryResultRow> rows, string fragment)
        => rows.FirstOrDefault(row => Text(row, "sql")?.Contains(fragment, StringComparison.OrdinalIgnoreCase) == true);

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots(
        CamusDBOptions options, int rows = 20)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddl, database: dbname,
            sql: "CREATE TABLE robots (id OID PRIMARY KEY, name STRING NOT NULL, year INT64 NOT NULL)",
            parameters: null));
        await database.Transactions.CommitAsync(ddl);

        for (int i = 0; i < rows; i++)
        {
            KvTransaction insert = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: insert, database: dbname,
                sql: $"INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'robot{i}', {2000 + i})",
                parameters: null));
            await database.Transactions.CommitAsync(insert);
        }

        return (dbname, database, executor);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // The gate
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Off by default. The statement still answers — no rows, never an error — so a script that
    /// polls a fleet gets the same shape from a node that has the log off.
    /// </summary>
    [Test]
    public async Task DisabledByDefault_ReturnsNoRowsWithoutError()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Options, rows: 3);

        await QueryAsync(executor, dbname, "SELECT * FROM robots");

        Assert.IsEmpty(await ShowSlowQueriesAsync(executor, dbname));
    }

    /// <summary>
    /// A statement below the threshold is not recorded. This is the arm that proves the threshold is
    /// consulted at all: with a 0 ms threshold every test below would pass even if the comparison
    /// were missing.
    /// </summary>
    [Test]
    public async Task StatementUnderTheThresholdIsNotRecorded()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(
            Options with { SlowQueryLogEnabled = true, SlowQueryLogThresholdMs = 600_000 }, rows: 3);

        await QueryAsync(executor, dbname, "SELECT * FROM robots");

        Assert.IsEmpty(await ShowSlowQueriesAsync(executor, dbname));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // What an entry carries
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task RecordsASelectWithItsTextKindAndRowCount()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 5);

        await QueryAsync(executor, dbname, "SELECT * FROM robots");

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "SELECT * FROM robots");

        Assert.IsNotNull(entry);
        Assert.AreEqual("select", Text(entry!.Value, "kind"));
        Assert.AreEqual(dbname, Text(entry!.Value, "database"));
        Assert.AreEqual(5, Number(entry!.Value, "rows_returned"));
        Assert.AreEqual("completed", Text(entry!.Value, "outcome"));
        Assert.IsFalse(Flag(entry!.Value, "truncated"));
        Assert.GreaterOrEqual(entry!.Value.Row["duration_ms"].FloatValue, 0d);
    }

    /// <summary>
    /// The duration must span draining the cursor, not returning it. <c>ExecuteSQLQuery</c> hands
    /// back a lazy cursor, so a clock stopped at its return would report microseconds for a scan
    /// that read every row — which is the failure this whole recording path exists to avoid.
    /// </summary>
    [Test]
    public async Task RowsReadCoversTheWholeScanNotJustThePlan()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 20);

        await QueryAsync(executor, dbname, "SELECT * FROM robots WHERE year > 2015");

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "year > 2015");

        Assert.IsNotNull(entry);
        Assert.IsTrue(Flag(entry!.Value, "full_scan"), "a predicate no index serves must be reported as a full scan");
        Assert.AreEqual(20, Number(entry!.Value, "rows_read"));
        Assert.Less(Number(entry!.Value, "rows_returned"), Number(entry!.Value, "rows_read"));
    }

    /// <summary>
    /// A primary-key lookup seeks rather than scans, so it must not be reported as a full scan. The
    /// negative case matters as much as the positive one: a flag that is always true says nothing.
    /// </summary>
    [Test]
    public async Task AnIndexedLookupIsNotReportedAsAFullScan()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 10);

        List<QueryResultRow> all = await QueryAsync(executor, dbname, "SELECT id FROM robots");
        string id = all[0].Row["id"].StrValue!;

        await QueryAsync(executor, dbname, $"SELECT * FROM robots WHERE id = \"{id}\"");

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), $"id = \"{id}\"");

        Assert.IsNotNull(entry);
        Assert.IsFalse(Flag(entry!.Value, "full_scan"));
    }

    /// <summary>
    /// A sort that outgrows its in-memory budget writes to disk, and the entry says so. The forced
    /// threshold is the only practical way to reach the spill path from a test — it exists for that.
    /// </summary>
    [Test]
    public async Task SpillIsReported()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(
            Logging(Options) with { SpillEnabled = true, ForceSpillThresholdRows = 1 }, rows: 10);

        await QueryAsync(executor, dbname, "SELECT * FROM robots ORDER BY year DESC");

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "ORDER BY year DESC");

        Assert.IsNotNull(entry);
        Assert.IsTrue(Flag(entry!.Value, "spilled"));
    }

    /// <summary>Without spill enabled the same statement reports no spill.</summary>
    [Test]
    public async Task NoSpillIsReportedWhenSpillIsOff()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 10);

        await QueryAsync(executor, dbname, "SELECT * FROM robots ORDER BY year DESC");

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "ORDER BY year DESC");

        Assert.IsNotNull(entry);
        Assert.IsFalse(Flag(entry!.Value, "spilled"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // How a statement ends
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A caller that stops reading — a client that disconnected, or one that took the first page —
    /// still produced load, so the statement is recorded and marked as abandoned.
    /// </summary>
    [Test]
    public async Task AnAbandonedCursorIsStillRecorded()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 20);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), dbname, "SELECT * FROM robots WHERE year > 1900", null));

        await foreach (QueryResultRow _ in cursor)
            break;

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "year > 1900");

        Assert.IsNotNull(entry);
        Assert.AreEqual("abandoned", Text(entry!.Value, "outcome"));
        Assert.AreEqual(1, Number(entry!.Value, "rows_returned"));
    }

    /// <summary>
    /// A slow failure is the case an operator most wants to see, so a statement that raises is
    /// recorded with its engine error code rather than dropped.
    /// </summary>
    [Test]
    public async Task AFailedStatementIsRecordedWithItsErrorCode()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 3);

        Assert.ThrowsAsync<CamusDBException>(
            async () => await QueryAsync(executor, dbname, "SELECT * FROM androids"));

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "FROM androids");

        Assert.IsNotNull(entry);
        Assert.AreEqual("failed", Text(entry!.Value, "outcome"));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, Text(entry!.Value, "error_code"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Statements that return no rows
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// An UPDATE finishes inside its own call, so the whole call is timed, and its affected-row count
    /// stands in for rows returned. Its locate scan carries the same probe, so the scan facts are
    /// reported for a mutation exactly as they are for a SELECT.
    /// </summary>
    [Test]
    public async Task AnUpdateIsRecordedWithItsAffectedRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobots(Logging(Options), rows: 10);

        KvTransaction update = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: update, database: dbname,
            sql: "UPDATE robots SET name = 'renamed' WHERE year > 2005", parameters: null));
        await database.Transactions.CommitAsync(update);

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "SET name = 'renamed'");

        Assert.IsNotNull(entry);
        Assert.AreEqual("update", Text(entry!.Value, "kind"));
        Assert.AreEqual(4, Number(entry!.Value, "rows_returned"));
        Assert.IsTrue(Flag(entry!.Value, "full_scan"));
        Assert.Greater(Number(entry!.Value, "rows_read"), 0);
    }

    /// <summary>An INSERT is recorded like any other statement, with one affected row.</summary>
    [Test]
    public async Task AnInsertIsRecorded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobots(Logging(Options), rows: 1);

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: insert, database: dbname,
            sql: "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'marker', 1999)", parameters: null));
        await database.Transactions.CommitAsync(insert);

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "'marker'");

        Assert.IsNotNull(entry);
        Assert.AreEqual("insert", Text(entry!.Value, "kind"));
        Assert.AreEqual(1, Number(entry!.Value, "rows_returned"));
    }

    /// <summary>DDL is timed the same way — it is often the slowest thing a node does.</summary>
    [Test]
    public async Task DdlIsRecorded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobots(Logging(Options), rows: 1);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddl, database: dbname,
            sql: "CREATE TABLE spares (id OID PRIMARY KEY, label STRING NOT NULL)", parameters: null));
        await database.Transactions.CommitAsync(ddl);

        QueryResultRow? entry = FirstWithSql(await ShowSlowQueriesAsync(executor, dbname), "CREATE TABLE spares");

        Assert.IsNotNull(entry);
        Assert.AreEqual("create_table", Text(entry!.Value, "kind"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Reading the log
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reading the log must not change it.
    ///
    /// <para>Without this the statement records itself, so every read evicts an entry — and anything
    /// that polls the log, which the operator dashboard does every few seconds, erases the history it
    /// exists to display. On a small ring it erases it completely, which is how this was found.</para>
    /// </summary>
    [Test]
    public async Task ReadingTheLogDoesNotRecordTheRead()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(
            Logging(Options) with { SlowQueryLogMaxEntries = 4 }, rows: 2);

        await QueryAsync(executor, dbname, "SELECT name FROM robots");

        // Poll the log far more often than the ring can hold. A self-recording read would push the
        // SELECT out well before this loop ends.
        for (int i = 0; i < 20; i++)
            await ShowSlowQueriesAsync(executor, dbname);

        List<QueryResultRow> entries = await ShowSlowQueriesAsync(executor, dbname);

        Assert.IsNotNull(
            FirstWithSql(entries, "SELECT name FROM robots"),
            "polling the log must not evict the statements it reports");

        Assert.IsEmpty(
            entries.Where(row => Text(row, "kind") == "show_slow_queries"),
            "the statement that reads the log must not appear in it");
    }

    [Test]
    public async Task RowsAreNewestFirst()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 3);

        await QueryAsync(executor, dbname, "SELECT name FROM robots");
        await QueryAsync(executor, dbname, "SELECT year FROM robots");

        List<QueryResultRow> entries = await ShowSlowQueriesAsync(executor, dbname);
        List<long> sequences = entries.Select(row => Number(row, "seq")).ToList();

        CollectionAssert.AreEqual(sequences.OrderByDescending(value => value).ToList(), sequences);

        int yearAt = entries.FindIndex(row => Text(row, "sql") == "SELECT year FROM robots");
        int nameAt = entries.FindIndex(row => Text(row, "sql") == "SELECT name FROM robots");

        Assert.GreaterOrEqual(yearAt, 0);
        Assert.GreaterOrEqual(nameAt, 0);
        Assert.Less(yearAt, nameAt);
    }

    [Test]
    public async Task LikeNarrowsOnTheSqlText()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 3);

        await QueryAsync(executor, dbname, "SELECT name FROM robots");
        await QueryAsync(executor, dbname, "SELECT year FROM robots");

        List<QueryResultRow> entries = await ShowSlowQueriesAsync(
            executor, dbname, "SHOW SLOW QUERIES LIKE '%SELECT year%'");

        Assert.AreEqual(1, entries.Count);
        Assert.AreEqual("SELECT year FROM robots", Text(entries[0], "sql"));
    }

    [Test]
    public async Task LikeWithNoMatchReturnsEmpty()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 3);

        await QueryAsync(executor, dbname, "SELECT name FROM robots");

        Assert.IsEmpty(await ShowSlowQueriesAsync(executor, dbname, "SHOW SLOW QUERIES LIKE '%nothing matches this%'"));
    }

    /// <summary>
    /// The ring is what makes the log safe to leave on, so the bound is asserted through the
    /// statement an operator actually runs, not only through the buffer's own unit tests.
    /// </summary>
    [Test]
    public async Task TheLogIsBoundedByItsConfiguredCapacity()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(
            Logging(Options) with { SlowQueryLogMaxEntries = 3 }, rows: 2);

        for (int i = 0; i < 12; i++)
            await QueryAsync(executor, dbname, $"SELECT name FROM robots WHERE year > {1000 + i}");

        Assert.AreEqual(3, (await ShowSlowQueriesAsync(executor, dbname)).Count);
    }

    /// <summary>
    /// Long statement text is truncated, and the entry says it was — otherwise a reader takes a cut
    /// WHERE clause for the whole predicate.
    /// </summary>
    [Test]
    public async Task LongSqlIsTruncatedAndFlagged()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(
            Logging(Options) with { SlowQueryLogMaxSqlLength = 12 }, rows: 2);

        await QueryAsync(executor, dbname, "SELECT name FROM robots WHERE year > 1000");

        List<QueryResultRow> entries = await ShowSlowQueriesAsync(executor, dbname);
        QueryResultRow entry = entries[0];

        Assert.AreEqual(12, Text(entry, "sql")!.Length);
        Assert.IsTrue(Flag(entry, "truncated"));
    }

    /// <summary>
    /// The log is per process, so the statement resolves without opening a database and without a
    /// transaction — the same contract <c>SHOW ENGINE STATS</c> has.
    /// </summary>
    [Test]
    public async Task NeedsNoDatabaseContext()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots(Logging(Options), rows: 2);

        await QueryAsync(executor, dbname, "SELECT name FROM robots");

        List<QueryResultRow> entries = await QueryAsync(executor, db: "", sql: "SHOW SLOW QUERIES", txnState: null!);

        Assert.IsNotEmpty(entries);
    }
}
