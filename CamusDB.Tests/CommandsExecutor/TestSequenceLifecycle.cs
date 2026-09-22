/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kahuna;
using Kahuna.Shared.Sequences;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The parts of the sequence feature that move a live counter or remove one: <c>setval</c>,
/// <c>ALTER SEQUENCE … RESTART</c>, <c>TRUNCATE … RESTART IDENTITY</c>, and the lifecycle paths
/// whose whole point is that a counter is deleted rather than leaked.
/// </summary>
/// <remarks>
/// <para>This fixture runs its own node with a short <c>SequencerBlockLease</c>. Moving a live
/// counter deliberately takes one lease period to answer — Kahuna withholds success until no
/// window reserved from the replaced incarnation can still be served anywhere — so at the default
/// five seconds every test here would pay five seconds. The shortened lease changes the timing,
/// not the semantics.</para>
///
/// <para>Every lifecycle assertion reads the counter out of Kahuna rather than the catalog. The
/// catalog losing a record is what a delta does; the counter surviving it is exactly the leak
/// these paths exist to prevent, and only a read of the sequencer can tell the two apart.</para>
/// </remarks>
[NonParallelizable]
internal sealed class TestSequenceLifecycle : BaseTest
{
    protected override void ConfigureNodeOptions(Kahuna.EmbeddedKahunaOptions options)
    {
        // Short enough to keep an update-bearing test fast, long enough to stay a real lease: the
        // update still waits one out, and the sequence still refuses allocations meanwhile.
        options.SequencerBlockLease = TimeSpan.FromMilliseconds(250);
    }

    private IKahuna Kahuna => TestNode!.Kahuna;

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

    private static async Task<int> ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(
        CommandExecutor executor, string dbname, string sql, KvTransaction? reuse = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = reuse ?? await database.Transactions.BeginAsync();
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
            if (reuse is null)
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<bool> CounterExistsAsync(string dbId, string sequenceId)
    {
        (SequenceResponseType type, ReadOnlySequenceEntry? entry) = await Kahuna.LocateAndGetSequence(
            MetaKeys.KahunaSequenceName(dbId, sequenceId), SequenceDurability.Persistent, CancellationToken.None);

        return type == SequenceResponseType.Success && entry is not null;
    }

    // -----------------------------------------------------------------------
    // Moving a live counter
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestSetValMovesTheCounter()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE moved");
        await Query(executor, dbname, "SELECT nextval('moved') AS v");

        // setval returns the value it was given, not the value that comes next — PostgreSQL's rule.
        List<Dictionary<string, ColumnValue>> set = await Query(executor, dbname, "SELECT setval('moved', 500) AS v");
        Assert.That(set[0]["v"].LongValue, Is.EqualTo(500));

        // is_called defaults to true, which treats 500 as already issued.
        List<Dictionary<string, ColumnValue>> next = await Query(executor, dbname, "SELECT nextval('moved') AS v");
        Assert.That(next[0]["v"].LongValue, Is.EqualTo(501));
    }

    /// <summary>
    /// <c>is_called = false</c> means the value given is the one the next <c>nextval</c> returns,
    /// rather than the one before it. Both spellings are tested because getting the off-by-one
    /// wrong is the classic <c>setval</c> mistake and each spelling hides the other's error.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestSetValIsCalledFalse()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE uncalled");
        await Query(executor, dbname, "SELECT setval('uncalled', 42, false) AS v");

        List<Dictionary<string, ColumnValue>> next = await Query(executor, dbname, "SELECT nextval('uncalled') AS v");
        Assert.That(next[0]["v"].LongValue, Is.EqualTo(42));
    }

    [Test]
    [NonParallelizable]
    public async Task TestAlterSequenceRestart()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE restarted START WITH 10");
        await Query(executor, dbname, "SELECT nextval('restarted') AS v");
        await Query(executor, dbname, "SELECT nextval('restarted') AS v");

        await ExecuteDdl(executor, dbname, "ALTER SEQUENCE restarted RESTART WITH 100");

        List<Dictionary<string, ColumnValue>> afterRestart = await Query(executor, dbname, "SELECT nextval('restarted') AS v");
        Assert.That(afterRestart[0]["v"].LongValue, Is.EqualTo(100));

        // A bare RESTART returns to the recorded start value, which is why the catalog keeps it.
        await ExecuteDdl(executor, dbname, "ALTER SEQUENCE restarted RESTART");

        List<Dictionary<string, ColumnValue>> afterBare = await Query(executor, dbname, "SELECT nextval('restarted') AS v");
        Assert.That(afterBare[0]["v"].LongValue, Is.EqualTo(10));
    }

    /// <summary>
    /// <c>CACHE 1</c> is not incidental here. With the node-wide cache a single draw reserves a
    /// whole block, so the counter's position after one <c>nextval</c> is already a thousand — and
    /// a maximum set below that would exhaust the sequence rather than retune it. That is correct
    /// behavior, and it is exactly the trap the reserved ceiling sets for a reader who expects the
    /// counter to sit on the last value issued.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestAlterSequenceIncrementAndMaxValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE retuned CACHE 1");
        await Query(executor, dbname, "SELECT nextval('retuned') AS v");

        await ExecuteDdl(executor, dbname, "ALTER SEQUENCE retuned INCREMENT BY 10 MAXVALUE 1000");

        Assert.That(database.Schema.Sequences["retuned"].Increment, Is.EqualTo(10));
        Assert.That(database.Schema.Sequences["retuned"].MaxValue, Is.EqualTo(1000));

        List<Dictionary<string, ColumnValue>> next = await Query(executor, dbname, "SELECT nextval('retuned') AS v");
        Assert.That(next[0]["v"].LongValue, Is.EqualTo(11));
    }

    /// <summary>
    /// A sequence that has issued everything its maximum allows reports exhaustion with its own
    /// code and a message naming the sequence — not "internal error".
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestExhaustedSequenceReportsItself()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE tiny START WITH 1 MAXVALUE 2");

        await Query(executor, dbname, "SELECT nextval('tiny') AS v");
        await Query(executor, dbname, "SELECT nextval('tiny') AS v");

        CamusDBException? exhausted = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT nextval('tiny') AS v"));

        Assert.That(exhausted!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceExhausted));
        Assert.That(exhausted.Message, Does.Contain("tiny"));
    }

    // -----------------------------------------------------------------------
    // currval and lastval
    // -----------------------------------------------------------------------

    /// <summary>
    /// <c>currval</c> and <c>lastval</c> are scoped to the transaction, which is the only bounded
    /// scope CamusDB has: the REST path is stateless, and defining "session" as the auth token
    /// would make two unrelated concurrent requests from one user share a value.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestCurrValAndLastValAreTransactionScoped()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE tracked");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            tx.MarkSessionOwned();

            await Query(executor, dbname, "SELECT nextval('tracked') AS v", tx);
            await Query(executor, dbname, "SELECT nextval('tracked') AS v", tx);

            List<Dictionary<string, ColumnValue>> current = await Query(executor, dbname, "SELECT currval('tracked') AS v", tx);
            Assert.That(current[0]["v"].LongValue, Is.EqualTo(2));

            List<Dictionary<string, ColumnValue>> last = await Query(executor, dbname, "SELECT lastval() AS v", tx);
            Assert.That(last[0]["v"].LongValue, Is.EqualTo(2));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        // A different transaction drew nothing, so it has nothing to report — and the sequence's
        // own reading is a reserved ceiling, not a value anyone was issued.
        CamusDBException? undefined = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT currval('tracked') AS v"));

        Assert.That(undefined!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceValueNotYetDefined));
    }

    // -----------------------------------------------------------------------
    // Transaction semantics
    // -----------------------------------------------------------------------

    /// <summary>
    /// A rolled-back statement consumes the values it drew and they are never reissued. That is
    /// PostgreSQL's behavior and the only workable one: rolling an advance back would mean either
    /// holding the sequence's partition under lock for the life of every writing transaction, or
    /// reissuing values and breaking the guarantee the whole feature rests on.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestRolledBackInsertStillConsumesValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE consumed");
        await ExecuteDdl(executor, dbname, "CREATE TABLE gapped (id oid PRIMARY KEY, n int64)");

        KvTransaction abandoned = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                abandoned, dbname,
                "INSERT INTO gapped (id, n) VALUES (gen_id(), nextval('consumed')), (gen_id(), nextval('consumed'))",
                null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(abandoned);
        }

        await ExecuteNonQuery(executor, dbname, "INSERT INTO gapped (id, n) VALUES (gen_id(), nextval('consumed'))");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT n FROM gapped");
        Assert.That(rows.Count, Is.EqualTo(1), "the rolled-back rows must not be present");
        Assert.That(rows[0]["n"].LongValue, Is.EqualTo(3), "the rolled-back values must not be reissued");
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    /// <summary>
    /// Many callers drawing from one sequence at once never receive the same value twice.
    /// Uniqueness is asserted, not monotonicity: values may be handed out in a different order
    /// than they were requested, which is a documented property rather than a defect.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestConcurrentDrawsNeverRepeatAValue()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE contended");

        const int callers = 8;
        const int drawsPerCaller = 10;

        Task<List<Dictionary<string, ColumnValue>>>[] draws = new Task<List<Dictionary<string, ColumnValue>>>[callers];

        for (int caller = 0; caller < callers; caller++)
        {
            draws[caller] = Task.Run(async () =>
            {
                List<Dictionary<string, ColumnValue>> drawn = [];

                for (int i = 0; i < drawsPerCaller; i++)
                    drawn.AddRange(await Query(executor, dbname, "SELECT nextval('contended') AS v"));

                return drawn;
            });
        }

        List<Dictionary<string, ColumnValue>>[] results = await Task.WhenAll(draws);

        List<long> values = [.. results.SelectMany(rows => rows).Select(row => row["v"].LongValue)];

        Assert.That(values.Count, Is.EqualTo(callers * drawsPerCaller));
        Assert.That(values.Distinct().Count(), Is.EqualTo(values.Count), "a value was issued twice");
    }

    // -----------------------------------------------------------------------
    // Lifecycle, asserted against the sequencer
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestDropTableTakesItsOwnedSequence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE owning (id oid PRIMARY KEY, n serial)");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO owning (id) VALUES (gen_id())");

        string sequenceId = database.Schema.Sequences["owning_n_seq"].Id!;
        Assert.That(await CounterExistsAsync(database.Id, sequenceId), Is.True);

        // FORCE takes the immediate path. A deferred drop deliberately keeps the sequence so a
        // RELINK brings back a table whose identity column can still issue.
        await ExecuteDdl(executor, dbname, "DROP TABLE owning FORCE");

        Assert.That(database.Schema.Sequences.ContainsKey("owning_n_seq"), Is.False);
        Assert.That(await CounterExistsAsync(database.Id, sequenceId), Is.False);
    }

    /// <summary>
    /// A deferred drop detaches the relation but keeps it recoverable, so its identity sequence
    /// has to survive: a relinked table whose identity column cannot issue is not the table that
    /// was dropped.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestDeferredDropKeepsTheOwnedSequence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE retained (id oid PRIMARY KEY, n serial)");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO retained (id) VALUES (gen_id())");

        string sequenceId = database.Schema.Sequences["retained_n_seq"].Id!;

        await ExecuteDdl(executor, dbname, "DROP TABLE retained");

        Assert.That(database.Schema.Sequences.ContainsKey("retained_n_seq"), Is.True);
        Assert.That(await CounterExistsAsync(database.Id, sequenceId), Is.True);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDropColumnTakesItsOwnedSequence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE shedding (id oid PRIMARY KEY, n serial, label string)");

        string sequenceId = database.Schema.Sequences["shedding_n_seq"].Id!;
        Assert.That(await CounterExistsAsync(database.Id, sequenceId), Is.True);

        await ExecuteDdl(executor, dbname, "ALTER TABLE shedding DROP COLUMN n");

        Assert.That(database.Schema.Sequences.ContainsKey("shedding_n_seq"), Is.False);
        Assert.That(await CounterExistsAsync(database.Id, sequenceId), Is.False);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDropDatabaseLeavesNoCounter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE swept_a");
        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE swept_b");

        string dbId = database.Id;
        string idA = database.Schema.Sequences["swept_a"].Id!;
        string idB = database.Schema.Sequences["swept_b"].Id!;

        await Query(executor, dbname, "SELECT nextval('swept_a') AS v");
        await Query(executor, dbname, "SELECT nextval('swept_b') AS v");

        Assert.That(await CounterExistsAsync(dbId, idA), Is.True);
        Assert.That(await CounterExistsAsync(dbId, idB), Is.True);

        await executor.DropDatabase(new DropDatabaseTicket(dbname, ifExists: false, force: true));

        // A prefix purge cannot see a counter — it lives outside every namespace the purge scans —
        // so this is the assertion that the dropper read the catalog before deleting it.
        Assert.That(await CounterExistsAsync(dbId, idA), Is.False);
        Assert.That(await CounterExistsAsync(dbId, idB), Is.False);
    }

    // -----------------------------------------------------------------------
    // TRUNCATE
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestTruncateRestartIdentity()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE numbered (id oid PRIMARY KEY, n serial)");
        await ExecuteNonQuery(executor, dbname,
            "INSERT INTO numbered (id) VALUES (gen_id()), (gen_id()), (gen_id())");

        await ExecuteDdl(executor, dbname, "TRUNCATE TABLE numbered RESTART IDENTITY");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO numbered (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT n FROM numbered");
        Assert.That(rows.Count, Is.EqualTo(1));
        Assert.That(rows[0]["n"].LongValue, Is.EqualTo(1));
    }

    /// <summary>
    /// <c>CONTINUE IDENTITY</c> is the default and leaves the sequence alone. The bare form means
    /// the same thing, so both are asserted: a default that silently restarted would be a
    /// data-shaped surprise.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestTruncateContinueIdentityLeavesTheSequenceAlone()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE kept (id oid PRIMARY KEY, n serial)");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO kept (id) VALUES (gen_id()), (gen_id())");

        await ExecuteDdl(executor, dbname, "TRUNCATE TABLE kept CONTINUE IDENTITY");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO kept (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> afterContinue = await Query(executor, dbname, "SELECT n FROM kept");
        Assert.That(afterContinue[0]["n"].LongValue, Is.GreaterThan(2));

        await ExecuteDdl(executor, dbname, "TRUNCATE TABLE kept");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO kept (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> afterBare = await Query(executor, dbname, "SELECT n FROM kept");
        Assert.That(afterBare[0]["n"].LongValue, Is.GreaterThan(afterContinue[0]["n"].LongValue));
    }

    /// <summary>
    /// A sequence a column merely defaults from is shared — other relations may draw from it — so
    /// truncating one of them must not reset it. PostgreSQL restarts owned sequences only.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestTruncateRestartIdentityLeavesAnUnownedSequenceAlone()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE shared_counter");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE borrower (id oid PRIMARY KEY, n int64 DEFAULT(nextval('shared_counter')))");

        await ExecuteNonQuery(executor, dbname, "INSERT INTO borrower (id) VALUES (gen_id()), (gen_id())");

        await ExecuteDdl(executor, dbname, "TRUNCATE TABLE borrower RESTART IDENTITY");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO borrower (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT n FROM borrower");
        Assert.That(rows.Count, Is.EqualTo(1));
        Assert.That(rows[0]["n"].LongValue, Is.GreaterThan(2), "a shared sequence must not be restarted");
    }

    /// <summary>
    /// The listed position is the sequence's <b>reserved ceiling</b>, not the last value issued.
    /// Under a cache of one the two are the same number; under a larger cache they differ by
    /// nearly a whole block. That is the whole reason the column is named <c>reserved_upto</c>
    /// and never <c>last_value</c>: a reader who takes it for the last issued number is wrong by
    /// up to the cache size.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestShowSequencesReportsTheCeilingNotTheLastValue()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        // CACHE 1 reserves each value durably before handing it out, so for this one sequence the
        // ceiling and the last issued value are the same number.
        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE exact CACHE 1");

        for (int i = 0; i < 4; i++)
            await Query(executor, dbname, "SELECT nextval('exact') AS v");

        List<Dictionary<string, ColumnValue>> exactRows = await Query(executor, dbname, "SHOW SEQUENCES LIKE 'exact'");
        Assert.That(exactRows[0]["reserved_upto"].LongValue, Is.EqualTo(4));

        // A larger cache reserves a block per commit, so four draws leave the ceiling at the top of
        // that block rather than at 4.
        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE blocked CACHE 1000");

        for (int i = 0; i < 4; i++)
            await Query(executor, dbname, "SELECT nextval('blocked') AS v");

        List<Dictionary<string, ColumnValue>> blockedRows = await Query(executor, dbname, "SHOW SEQUENCES LIKE 'blocked'");
        Assert.That(blockedRows[0]["reserved_upto"].LongValue, Is.GreaterThan(4),
            "the reported number is a ceiling; if it equalled the draw count the name would be a lie");
    }

    /// <summary>
    /// A <c>CREATE SEQUENCE</c> that writes no <c>CACHE</c> clause caches one value, as PostgreSQL
    /// does. Each value is therefore reserved durably before it is handed out, so a restart, a
    /// leadership change or an eviction abandons nothing and skips nothing.
    /// </summary>
    /// <remarks>
    /// <para>The recorded cache is what this test pins, because it is what decides the behaviour.
    /// A sequence that follows the node-wide block instead skips to the top of its block whenever
    /// the block is abandoned — correct, but a surprise to a reader whose statement never
    /// mentioned a cache. A caller who wants the throughput back asks for it by name;
    /// <see cref="TestShowSequencesReportsTheCeilingNotTheLastValue"/> covers that arm.</para>
    ///
    /// <para>The reopen below closes the database descriptor, not the Kahuna node that owns the
    /// counter, so it does not by itself force a block to be abandoned. It shows continuity over
    /// a reload of the catalog record. A true process restart is not exercised here.</para>
    /// </remarks>
    [Test]
    [NonParallelizable]
    public async Task TestPlainSequenceCachesOneValue()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE ticket_no");

        List<Dictionary<string, ColumnValue>> listed = await Query(executor, dbname, "SHOW SEQUENCES LIKE 'ticket_no'");
        Assert.That(listed[0]["cache"].LongValue, Is.EqualTo(1), "a plain CREATE SEQUENCE caches one value");

        for (int i = 0; i < 3; i++)
            await Query(executor, dbname, "SELECT nextval('ticket_no') AS v");

        // With a cache of one the reserved ceiling is the last value issued, with nothing held
        // back. Under the node-wide block this would read 1000 instead.
        List<Dictionary<string, ColumnValue>> position = await Query(executor, dbname, "SHOW SEQUENCES LIKE 'ticket_no'");
        Assert.That(position[0]["reserved_upto"].LongValue, Is.EqualTo(3));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        await executor.OpenDatabase(dbname);

        List<Dictionary<string, ColumnValue>> afterReopen = await Query(executor, dbname, "SELECT nextval('ticket_no') AS v");
        Assert.That(afterReopen[0]["v"].LongValue, Is.EqualTo(4), "nothing was held back, so nothing is skipped");
    }

    // -----------------------------------------------------------------------
    // Branch fork
    // -----------------------------------------------------------------------

    /// <summary>
    /// A branch's sequence issues values above every value its inherited rows already hold.
    /// </summary>
    /// <remarks>
    /// The catalog record is a meta key and rides the fork's metadata copy; the counter is not a
    /// key at all and does not. Left alone, the branch's first draw would find nothing, create a
    /// counter at the sequence's start value, and begin reissuing numbers the inherited rows
    /// already carry — so this asserts the values, not the presence of a record.
    /// </remarks>
    [Test]
    [NonParallelizable]
    public async Task TestBranchSequenceIssuesAboveInheritedRows()
    {
        // Both names are written into the CREATE DATABASE ... BRANCH FROM text below, so both need
        // a letter prefix: a raw GUID that starts with a digit lexes as a number.
        CommandExecutor executor = CreateCommandExecutor();

        string rootName = "r" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await ExecuteDdl(executor, rootName, "CREATE TABLE forked (id oid PRIMARY KEY, n serial)");
        await ExecuteNonQuery(executor, rootName,
            "INSERT INTO forked (id) VALUES (gen_id()), (gen_id()), (gen_id())");

        List<Dictionary<string, ColumnValue>> rootRows = await Query(executor, rootName, "SELECT n FROM forked");
        long highestInherited = rootRows.Max(row => row["n"].LongValue);

        string branchName = "b" + Guid.NewGuid().ToString("n");

        DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null))).Database!;

        TrackDatabase(branchName, executor);

        Assert.That(branch.Schema.Sequences.ContainsKey("forked_n_seq"), Is.True,
            "the branch inherited the catalog record");

        await ExecuteNonQuery(executor, branchName, "INSERT INTO forked (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> branchRows = await Query(executor, branchName, "SELECT n FROM forked");
        long issued = branchRows.Max(row => row["n"].LongValue);

        Assert.That(issued, Is.GreaterThan(highestInherited),
            "the branch reissued a value its inherited rows already hold");
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    /// <summary>
    /// A sequence and its column binding survive a close and reopen with every field intact —
    /// asserted by reading the reloaded schema and by drawing a value, not by inspecting the
    /// in-memory object the create left behind.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestSequenceSurvivesAReopen()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname,
            "CREATE SEQUENCE persisted START WITH 7 INCREMENT BY 3 MINVALUE 7 MAXVALUE 700 CACHE 2");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE persisted_rows (id oid PRIMARY KEY, n int64 DEFAULT(nextval('persisted')))");

        await ExecuteNonQuery(executor, dbname, "INSERT INTO persisted_rows (id) VALUES (gen_id())");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.That(reopened.Schema.Sequences.ContainsKey("persisted"), Is.True);

        SequenceSchema reloaded = reopened.Schema.Sequences["persisted"];
        Assert.That(reloaded.StartValue, Is.EqualTo(7));
        Assert.That(reloaded.Increment, Is.EqualTo(3));
        Assert.That(reloaded.MinValue, Is.EqualTo(7));
        Assert.That(reloaded.MaxValue, Is.EqualTo(700));
        Assert.That(reloaded.CacheSize, Is.EqualTo(2));

        // The column's binding survived too: the default still draws, and from the same counter.
        await ExecuteNonQuery(executor, dbname, "INSERT INTO persisted_rows (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT n FROM persisted_rows ORDER BY n");
        Assert.That(rows.Count, Is.EqualTo(2));
        Assert.That(rows[0]["n"].LongValue, Is.EqualTo(7));
        Assert.That(rows[1]["n"].LongValue, Is.GreaterThan(7));
    }

    // -----------------------------------------------------------------------
    // Operator surface
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestShowSequencesAndComment()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE listed START WITH 5 INCREMENT BY 2 MAXVALUE 99 CACHE 4");
        await ExecuteDdl(executor, dbname, "CREATE TABLE listed_owner (id oid PRIMARY KEY, n serial)");
        await ExecuteDdl(executor, dbname, "COMMENT ON SEQUENCE listed IS 'order numbers'");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SHOW SEQUENCES");

        Dictionary<string, ColumnValue> listed = rows.Single(row =>
            string.Equals(row["sequence"].StrValue, "listed", StringComparison.Ordinal));

        Assert.That(listed["start_value"].LongValue, Is.EqualTo(5));
        Assert.That(listed["increment"].LongValue, Is.EqualTo(2));
        Assert.That(listed["max_value"].LongValue, Is.EqualTo(99));
        Assert.That(listed["cache"].LongValue, Is.EqualTo(4));
        Assert.That(listed["owned_by"].Type, Is.EqualTo(ColumnType.Null));
        Assert.That(listed["comment"].StrValue, Is.EqualTo("order numbers"));

        Dictionary<string, ColumnValue> owned = rows.Single(row =>
            string.Equals(row["sequence"].StrValue, "listed_owner_n_seq", StringComparison.Ordinal));

        Assert.That(owned["owned_by"].StrValue, Is.EqualTo("listed_owner.n"));

        // Nothing has drawn from it, so it has no counter yet and its position is NULL rather
        // than a number invented from the recorded start.
        Assert.That(listed["reserved_upto"].Type, Is.EqualTo(ColumnType.Null));

        // The column is never called "last value". What it reports is the reserved ceiling, and
        // the gap below proves why that name would be wrong.
        Assert.That(listed.ContainsKey("last_value"), Is.False);

        List<Dictionary<string, ColumnValue>> rendered = await Query(executor, dbname, "SHOW CREATE SEQUENCE listed");
        Assert.That(rendered[0]["create sequence"].StrValue, Does.Contain("START WITH 5"));
        Assert.That(rendered[0]["create sequence"].StrValue, Does.Contain("INCREMENT BY 2"));
        Assert.That(rendered[0]["create sequence"].StrValue, Does.Contain("MAXVALUE 99"));
    }
}
