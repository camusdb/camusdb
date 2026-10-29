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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SELECT … FOR UPDATE [OF t] [NOWAIT | SKIP LOCKED]</c> end to end, through
/// <see cref="CommandExecutor.ExecuteSQLQuery"/>: each returned row holds the exclusive row lock that an
/// UPDATE of the row takes, until the transaction ends; the values come from a read made after the
/// lock; a row a concurrent commit moved out of the WHERE clause is dropped and the next row fills the
/// LIMIT. Runs with hash routing and with key-range routing, whose point locks route the same way.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[NonParallelizable]
internal sealed class TestSelectForUpdate : BaseTest
{
    private const int Jobs = 10;

    private readonly bool keyRange;

    public TestSelectForUpdate(bool keyRange) => this.keyRange = keyRange;

    protected override int NodeInitialPartitions => keyRange ? 2 : 1;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = keyRange };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    private static async Task<long[]> IdsAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => (await QueryAsync(executor, db, tx, sql)).Select(r => r.Row["id"].LongValue).ToArray();

    private static Task<KvTransaction> BeginAsync(
        DatabaseDescriptor database,
        CamusIsolationLevel level = CamusIsolationLevel.ReadCommitted,
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic)
        => database.Transactions.BeginAsync(level, CamusTransactionMode.ReadWrite, locking: locking);

    /// <summary>Runs <paramref name="sql"/> in its own committed Read Committed transaction.</summary>
    private static async Task CommitAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await BeginAsync(database);
        try
        {
            await ExecAsync(executor, db, tx, sql);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<CamusDBException> ExpectFailureAsync(Func<Task> action)
    {
        CamusDBException? failure = null;
        try
        {
            await action();
        }
        catch (CamusDBException e)
        {
            failure = e;
        }

        Assert.IsNotNull(failure, "the statement was expected to fail");
        return failure!;
    }

    /// <summary>
    /// Creates <c>jobs</c> with ids 1..10, each <c>ready</c> with an empty note, through
    /// <paramref name="engine"/> (a new engine with the fixture's options when null).
    /// </summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync(CommandExecutor? engine = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            engine is null ? await CreateDatabase() : await CreateDatabaseWith(engine);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE jobs (id INT64 PRIMARY KEY, state STRING, note STRING)", null));

        string values = string.Join(", ", Enumerable.Range(1, Jobs).Select(i => $"({i}, 'ready', '')"));
        await CommitAsync(executor, database, db, "INSERT INTO jobs (id, state, note) VALUES " + values);

        return (db, database, executor);
    }

    // ── parser ────────────────────────────────────────────────────────────────────────────────────

    [TestCase("SELECT * FROM jobs FOR UPDATE", RowLockingAst.Wait, null)]
    [TestCase("SELECT * FROM jobs WHERE id = 1 FOR NO KEY UPDATE", RowLockingAst.Wait, null)]
    [TestCase("SELECT id FROM jobs ORDER BY id LIMIT 2 FOR UPDATE NOWAIT", RowLockingAst.NoWait, null)]
    [TestCase("SELECT id FROM jobs FOR UPDATE SKIP LOCKED", RowLockingAst.SkipLocked, null)]
    [TestCase("SELECT id FROM jobs j FOR UPDATE OF j SKIP LOCKED", RowLockingAst.SkipLocked, "j")]
    [TestCase("select id from jobs for update of jobs nowait", RowLockingAst.NoWait, "jobs")]
    public void Parser_AcceptsEveryForm(string sql, string policy, string? of)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(NodeType.SelectForUpdate, ast.nodeType);
        Assert.AreEqual(NodeType.Select, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.LockingClause, ast.rightAst!.nodeType);
        Assert.AreEqual(policy, RowLockingAst.PolicyOf(ast.rightAst));
        Assert.AreEqual(of, ast.rightAst.leftAst?.yytext);
        Assert.IsTrue(StatementScope.BeginsWritableTransaction(ast));
        Assert.IsTrue(StatementScope.BeginsWritableTransaction(ast.nodeType));
        Assert.IsTrue(StatementScope.ReturnsRows(ast));
    }

    /// <summary>
    /// Every EXPLAIN form takes the clause, as in PostgreSQL. Only EXPLAIN (ANALYZE) runs the query and
    /// takes locks, so only its root type makes a transport begin a writable transaction.
    /// </summary>
    [TestCase("EXPLAIN SELECT * FROM jobs FOR UPDATE", NodeType.Explain, false)]
    [TestCase("EXPLAIN (LOGICAL) SELECT * FROM jobs FOR UPDATE NOWAIT", NodeType.ExplainLogical, false)]
    [TestCase("EXPLAIN (PHYSICAL) SELECT id FROM jobs ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED", NodeType.ExplainPhysical, false)]
    [TestCase("EXPLAIN (ANALYZE) SELECT * FROM jobs FOR UPDATE OF jobs", NodeType.ExplainAnalyzeForUpdate, true)]
    public void Parser_AcceptsTheClauseUnderEveryExplainForm(string sql, NodeType root, bool takesLocks)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(root, ast.nodeType);
        Assert.AreEqual(NodeType.SelectForUpdate, ast.leftAst!.nodeType);
        Assert.AreEqual(takesLocks, StatementScope.BeginsWritableTransaction(ast));
        Assert.AreEqual(takesLocks, StatementScope.BeginsWritableTransaction(ast.nodeType));
        Assert.IsTrue(StatementScope.ReturnsRows(ast));
    }

    [Test]
    public void Parser_ExplainAnalyzeWithoutTheClause_KeepsItsRootType()
    {
        NodeAst ast = SQLParserProcessor.Parse("EXPLAIN (ANALYZE) SELECT * FROM jobs");

        Assert.AreEqual(NodeType.ExplainAnalyze, ast.nodeType);
        Assert.IsFalse(StatementScope.BeginsWritableTransaction(ast.nodeType));
    }

    [TestCase("SELECT * FROM (SELECT * FROM jobs FOR UPDATE) d")]
    [TestCase("SELECT * FROM jobs WHERE id IN (SELECT id FROM jobs FOR UPDATE)")]
    [TestCase("INSERT INTO jobs (id, state, note) SELECT id, state, note FROM jobs FOR UPDATE")]
    [TestCase("CREATE TABLE copy AS SELECT * FROM jobs FOR UPDATE")]
    [TestCase("CREATE VIEW v AS SELECT * FROM jobs FOR UPDATE")]
    public void Parser_RejectsTheClauseOutsideATopLevelSelect(string sql)
    {
        Assert.Catch(() => SQLParserProcessor.Parse(sql));
    }

    [TestCase("SELECT * FROM jobs FOR SHARE", CamusDBErrorCodes.FeatureNotSupported)]
    [TestCase("SELECT * FROM jobs FOR KEY SHARE", CamusDBErrorCodes.FeatureNotSupported)]
    [TestCase("SELECT id FROM jobs UNION SELECT id FROM jobs FOR UPDATE", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT * FROM jobs FOR UPDATE WAIT", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT * FROM jobs FOR UPDATE SKIP", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT * FROM jobs FOR UPDATE NOWAIT SKIP LOCKED", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT * FROM jobs FOR UPDATE OF", CamusDBErrorCodes.InvalidInput)]
    public void Parser_RefusesUnsupportedStrengthsAndOptions(string sql, string code)
    {
        CamusDBException? e = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));
        Assert.AreEqual(code, e!.Code);
    }

    // ── locking ───────────────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ALockedRow_IsRefusedToEveryOtherWriter_UntilTheHolderCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        List<QueryResultRow> locked = await QueryAsync(executor, db, holder, "SELECT id, state FROM jobs WHERE id = 1 FOR UPDATE");
        Assert.AreEqual(1, locked.Count);
        Assert.AreEqual("ready", locked[0].Row["state"].StrValue);

        foreach (string sql in new[]
                 {
                     "UPDATE jobs SET state = 'taken' WHERE id = 1",
                     "DELETE FROM jobs WHERE id = 1",
                 })
        {
            KvTransaction other = await BeginAsync(database);
            CamusDBException e = await ExpectFailureAsync(() => ExecAsync(executor, db, other, sql));
            Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, e.Code, sql);
            await database.Transactions.RollbackIfNotCompletedAsync(other);
        }

        KvTransaction reader = await BeginAsync(database);
        CamusDBException readerConflict = await ExpectFailureAsync(
            () => QueryAsync(executor, db, reader, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE"));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, readerConflict.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(reader);

        // A plain read is not blocked.
        KvTransaction plain = await BeginAsync(database);
        Assert.AreEqual(new long[] { 1 }, await IdsAsync(executor, db, plain, "SELECT id FROM jobs WHERE id = 1"));
        await database.Transactions.CommitAsync(plain);

        await database.Transactions.CommitAsync(holder);

        await CommitAsync(executor, database, db, "UPDATE jobs SET state = 'taken' WHERE id = 1");
        KvTransaction check = await BeginAsync(database);
        List<QueryResultRow> after = await QueryAsync(executor, db, check, "SELECT state FROM jobs WHERE id = 1");
        Assert.AreEqual("taken", after[0].Row["state"].StrValue);
        await database.Transactions.CommitAsync(check);
    }

    [Test]
    public async Task ARollback_ReleasesTheLocks()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        Assert.AreEqual(new long[] { 2, 3 }, await IdsAsync(executor, db, holder, "SELECT id FROM jobs WHERE id IN (2, 3) ORDER BY id FOR UPDATE"));
        await database.Transactions.RollbackAsync(holder);

        KvTransaction next = await BeginAsync(database);
        Assert.AreEqual(new long[] { 2, 3 }, await IdsAsync(executor, db, next, "SELECT id FROM jobs WHERE id IN (2, 3) ORDER BY id FOR UPDATE NOWAIT"));
        await database.Transactions.CommitAsync(next);
    }

    [Test]
    public async Task NoWait_FailsWithLockNotAvailable_WhichIsNotRetried()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        await QueryAsync(executor, db, holder, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE");

        KvTransaction other = await BeginAsync(database);
        CamusDBException e = await ExpectFailureAsync(
            () => QueryAsync(executor, db, other, "SELECT id FROM jobs WHERE id <= 2 ORDER BY id FOR UPDATE NOWAIT"));
        await database.Transactions.RollbackIfNotCompletedAsync(other);

        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, e.Code);
        Assert.IsFalse(SerializableRetryHelper.IsRetryable(e));
        Assert.AreEqual(409, CamusDBErrorCodes.GetHttpStatus(e.Code));

        await database.Transactions.CommitAsync(holder);
    }

    [Test]
    public async Task SkipLocked_ReturnsTheNextFreeRows_AndLocksNoRowPastTheLimit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        Assert.AreEqual(new long[] { 1, 2, 3 }, await IdsAsync(executor, db, first, "SELECT id FROM jobs WHERE id <= 3 ORDER BY id FOR UPDATE"));

        KvTransaction second = await BeginAsync(database);
        Assert.AreEqual(new long[] { 4, 5 }, await IdsAsync(executor, db, second,
            "SELECT id FROM jobs WHERE state = 'ready' ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED"));

        // Row 6 comes right after the LIMIT and must be free; row 4 is held by the second transaction.
        KvTransaction third = await BeginAsync(database);
        Assert.AreEqual(new long[] { 6 }, await IdsAsync(executor, db, third, "SELECT id FROM jobs WHERE id = 6 FOR UPDATE NOWAIT"));
        CamusDBException held = await ExpectFailureAsync(
            () => QueryAsync(executor, db, third, "SELECT id FROM jobs WHERE id = 4 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, held.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(third);

        // Without ORDER BY, and with an OFFSET: the skipped rows do not count toward either.
        KvTransaction fourth = await BeginAsync(database);
        long[] page = await IdsAsync(executor, db, fourth, "SELECT id FROM jobs LIMIT 2 OFFSET 1 FOR UPDATE SKIP LOCKED");
        Assert.AreEqual(2, page.Length);
        Assert.That(page, Has.None.AnyOf(1L, 2L, 3L, 4L, 5L));
        await database.Transactions.RollbackIfNotCompletedAsync(fourth);

        await database.Transactions.CommitAsync(second);
        await database.Transactions.CommitAsync(first);
    }

    [Test]
    public async Task ARowAConcurrentCommitMovedOutOfTheWhere_IsDropped_AndTheNextRowFillsTheLimit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        QueryExecutor queryExecutor = executor.QueryExecutorForTests;
        bool hookRan = false;

        queryExecutor.TestBeforeRowLockHook = async () =>
        {
            queryExecutor.TestBeforeRowLockHook = null;
            hookRan = true;

            // After the scan chose rows 1 and 2 and before the lock: row 1 leaves the WHERE clause, and
            // row 2 changes a column the statement returns.
            await CommitAsync(executor, database, db, "UPDATE jobs SET state = 'done' WHERE id = 1");
            await CommitAsync(executor, database, db, "UPDATE jobs SET note = 'changed' WHERE id = 2");
        };

        try
        {
            KvTransaction tx = await BeginAsync(database);
            List<QueryResultRow> rows = await QueryAsync(executor, db, tx,
                "SELECT id, note FROM jobs WHERE state = 'ready' ORDER BY id LIMIT 2 FOR UPDATE");
            await database.Transactions.CommitAsync(tx);

            Assert.IsTrue(hookRan);
            Assert.AreEqual(new long[] { 2, 3 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
            Assert.AreEqual("changed", rows[0].Row["note"].StrValue, "the value comes from the read made under the lock");
        }
        finally
        {
            queryExecutor.TestBeforeRowLockHook = null;
        }
    }

    [Test]
    public async Task ARowAConcurrentCommitDeleted_IsDropped_AndTheNextRowFillsTheLimit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        QueryExecutor queryExecutor = executor.QueryExecutorForTests;
        bool hookRan = false;

        queryExecutor.TestBeforeRowLockHook = async () =>
        {
            queryExecutor.TestBeforeRowLockHook = null;
            hookRan = true;

            // After the scan chose rows 1 and 2 and before the lock, row 1 leaves the WHERE clause and
            // row 2 is deleted.
            await CommitAsync(executor, database, db, "UPDATE jobs SET state = 'done' WHERE id = 1");
            await CommitAsync(executor, database, db, "DELETE FROM jobs WHERE id = 2");
        };

        try
        {
            KvTransaction tx = await BeginAsync(database);
            long[] ids = await IdsAsync(executor, db, tx, "SELECT id FROM jobs WHERE state = 'ready' ORDER BY id LIMIT 2 FOR UPDATE");
            await database.Transactions.CommitAsync(tx);

            Assert.IsTrue(hookRan);
            Assert.AreEqual(new long[] { 3, 4 }, ids);
        }
        finally
        {
            queryExecutor.TestBeforeRowLockHook = null;
        }
    }

    [Test]
    public async Task AnUpdateAfterForUpdate_InTheSameTransaction_HasNoConflict()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await BeginAsync(database);
        await QueryAsync(executor, db, tx, "SELECT id FROM jobs WHERE id = 7 FOR UPDATE");
        await ExecAsync(executor, db, tx, "UPDATE jobs SET state = 'done' WHERE id = 7");
        await QueryAsync(executor, db, tx, "SELECT id FROM jobs WHERE id = 7 FOR UPDATE NOWAIT");
        await database.Transactions.CommitAsync(tx);

        KvTransaction check = await BeginAsync(database);
        List<QueryResultRow> rows = await QueryAsync(executor, db, check, "SELECT state FROM jobs WHERE id = 7");
        Assert.AreEqual("done", rows[0].Row["state"].StrValue);
        await database.Transactions.CommitAsync(check);
    }

    [Test]
    public async Task Serializable_LocksTheRows_AndRefusesSkipLocked()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction serializable = await BeginAsync(database, CamusIsolationLevel.Serializable);
        Assert.AreEqual(new long[] { 8 }, await IdsAsync(executor, db, serializable, "SELECT id FROM jobs WHERE id = 8 FOR UPDATE"));

        // A Read Committed FOR UPDATE of the same row sees the lock: the key lock is taken at every level.
        KvTransaction readCommitted = await BeginAsync(database);
        CamusDBException conflict = await ExpectFailureAsync(
            () => QueryAsync(executor, db, readCommitted, "SELECT id FROM jobs WHERE id = 8 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, conflict.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(readCommitted);

        await ExecAsync(executor, db, serializable, "UPDATE jobs SET state = 'done' WHERE id = 8");
        await database.Transactions.CommitAsync(serializable);

        KvTransaction skipper = await BeginAsync(database, CamusIsolationLevel.Serializable);
        CamusDBException refused = await ExpectFailureAsync(
            () => QueryAsync(executor, db, skipper, "SELECT id FROM jobs LIMIT 1 FOR UPDATE SKIP LOCKED"));
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, refused.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(skipper);

        // NOWAIT at Serializable meets a row a Read Committed transaction holds.
        KvTransaction holder = await BeginAsync(database);
        await QueryAsync(executor, db, holder, "SELECT id FROM jobs WHERE id = 9 FOR UPDATE");
        KvTransaction waiter = await BeginAsync(database, CamusIsolationLevel.Serializable);
        CamusDBException noWait = await ExpectFailureAsync(
            () => QueryAsync(executor, db, waiter, "SELECT id FROM jobs WHERE id = 9 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, noWait.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(waiter);
        await database.Transactions.CommitAsync(holder);
    }

    /// <summary>
    /// At Serializable a scan takes a shared predicate lock over the rows it reads before any row is
    /// locked, as every Serializable read-write SELECT does. A full scan whose range covers a row another
    /// transaction holds therefore fails there, under the wait-die rule, with the retryable
    /// <c>TransactionConflict</c>, even under NOWAIT: the lock step that maps a held row to
    /// <c>LockNotAvailable</c> is never reached. A point read reaches the lock step (see
    /// <see cref="Serializable_LocksTheRows_AndRefusesSkipLocked"/>).
    /// </summary>
    [Test]
    public async Task Serializable_AFullScanMeetsAHeldRowAtItsPredicateLock()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await BeginAsync(database);
        await QueryAsync(executor, db, holder, "SELECT id FROM jobs WHERE id = 5 FOR UPDATE");

        KvTransaction scanner = await BeginAsync(database, CamusIsolationLevel.Serializable);
        CamusDBException e = await ExpectFailureAsync(
            () => QueryAsync(executor, db, scanner, "SELECT id FROM jobs WHERE note = '' FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, e.Code, e.Message);
        await database.Transactions.RollbackIfNotCompletedAsync(scanner);

        await database.Transactions.CommitAsync(holder);
    }

    // ── refusals ──────────────────────────────────────────────────────────────────────────────────

    [TestCase("SELECT DISTINCT state FROM jobs FOR UPDATE")]
    [TestCase("SELECT state FROM jobs GROUP BY state FOR UPDATE")]
    [TestCase("SELECT count(*) FROM jobs FOR UPDATE")]
    [TestCase("SELECT a.id FROM jobs a JOIN jobs b ON a.id = b.id FOR UPDATE")]
    [TestCase("SELECT id FROM jobs_view FOR UPDATE")]
    [TestCase("SELECT id FROM jobs FOR UPDATE OF other")]
    [TestCase("SELECT 1 FOR UPDATE")]
    public async Task ShapesWithNoTableRowToLock_AreRefused(string sql)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // A view body is bound when the view is created, so the DDL needs a transaction.
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db, "CREATE VIEW jobs_view AS SELECT id FROM jobs", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction tx = await BeginAsync(database);
        CamusDBException e = await ExpectFailureAsync(() => QueryAsync(executor, db, tx, sql));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, e.Code, e.Message);
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
    }

    [Test]
    public async Task TransactionsThatCannotOwnALock_AreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction readOnly = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        CamusDBException readOnlyRefusal = await ExpectFailureAsync(
            () => QueryAsync(executor, db, readOnly, "SELECT id FROM jobs FOR UPDATE"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, readOnlyRefusal.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(readOnly);

        KvTransaction optimistic = await BeginAsync(database, locking: KeyValueTransactionLocking.Optimistic);
        CamusDBException optimisticRefusal = await ExpectFailureAsync(
            () => QueryAsync(executor, db, optimistic, "SELECT id FROM jobs FOR UPDATE"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, optimisticRefusal.Code);
        StringAssert.Contains("LOCKING PESSIMISTIC", optimisticRefusal.Message);
        await database.Transactions.RollbackIfNotCompletedAsync(optimistic);

        KvTransaction timeTravel = await BeginAsync(database);
        CamusDBException asOfRefusal = await ExpectFailureAsync(
            () => QueryAsync(executor, db, timeTravel, "SELECT id FROM jobs AS OF SYSTEM TIME '-1s' FOR UPDATE"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, asOfRefusal.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(timeTravel);
    }

    // ── EXPLAIN ───────────────────────────────────────────────────────────────────────────────────

    /// <summary>Runs an EXPLAIN form and returns its rows by node name, in plan order.</summary>
    private static async Task<List<(string Node, QueryResultRow Row)>> ExplainAsync(
        CommandExecutor executor, string db, KvTransaction tx, string sql)
        => (await QueryAsync(executor, db, tx, sql)).Select(r => (r.Row["node"].StrValue!, r)).ToList();

    private static QueryResultRow LockStep(List<(string Node, QueryResultRow Row)> plan)
        => plan.Single(p => p.Node == "lock_rows").Row;

    /// <summary>
    /// Holds rows <paramref name="ids"/> in a new open transaction the caller must roll back.
    /// </summary>
    private static async Task<KvTransaction> HoldAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string ids)
    {
        KvTransaction holder = await BeginAsync(database);
        await QueryAsync(executor, db, holder, $"SELECT id FROM jobs WHERE id IN ({ids}) FOR UPDATE");
        return holder;
    }

    /// <summary>
    /// Plain EXPLAIN shows the lock step where the statement runs it (below the LIMIT, above the sort)
    /// with its wait policy, and takes no lock: another transaction can lock every row with NOWAIT
    /// while the explaining transaction is still open.
    /// </summary>
    [Test]
    public async Task Explain_ShowsTheLockStep_AndTakesNoLock()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction explaining = await BeginAsync(database);
        try
        {
            List<(string Node, QueryResultRow Row)> plan = await ExplainAsync(executor, db, explaining,
                "EXPLAIN SELECT id FROM jobs WHERE state = 'ready' ORDER BY note, id LIMIT 3 FOR UPDATE SKIP LOCKED");

            List<string> nodes = plan.Select(p => p.Node).ToList();
            int lockAt = nodes.IndexOf("lock_rows");
            Assert.That(lockAt, Is.GreaterThanOrEqualTo(0), string.Join(", ", nodes));
            Assert.That(nodes.IndexOf("limit"), Is.LessThan(lockAt), "the LIMIT is above the lock step");
            Assert.That(nodes.IndexOf("sort"), Is.GreaterThan(lockAt), "the sort is below the lock step");
            Assert.AreEqual("for update, skip locked", LockStep(plan).Row["detail"].StrValue);

            KvTransaction other = await BeginAsync(database);
            Assert.AreEqual(Jobs, (await IdsAsync(executor, db, other, "SELECT id FROM jobs FOR UPDATE NOWAIT")).Length);
            await database.Transactions.CommitAsync(other);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(explaining);
        }
    }

    /// <summary>
    /// Plain EXPLAIN takes no lock, so, as in PostgreSQL, the transaction rules do not apply to it: it
    /// works in a read-only and in an optimistic transaction. EXPLAIN (ANALYZE) takes the locks, and is
    /// refused there as the statement is.
    /// </summary>
    [Test]
    public async Task Explain_WorksWhereTheLockCannotBeTaken_AndAnalyzeIsRefusedThere()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction readOnly = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            Assert.IsNotNull(LockStep(await ExplainAsync(executor, db, readOnly, "EXPLAIN SELECT id FROM jobs WHERE id = 1 FOR UPDATE")));
            CamusDBException refusal = await ExpectFailureAsync(
                () => QueryAsync(executor, db, readOnly, "EXPLAIN (ANALYZE) SELECT id FROM jobs WHERE id = 1 FOR UPDATE"));
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, refusal.Code);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(readOnly);
        }

        KvTransaction optimistic = await BeginAsync(database, locking: KeyValueTransactionLocking.Optimistic);
        try
        {
            Assert.IsNotNull(LockStep(await ExplainAsync(executor, db, optimistic, "EXPLAIN SELECT id FROM jobs WHERE id = 1 FOR UPDATE")));
            CamusDBException refusal = await ExpectFailureAsync(
                () => QueryAsync(executor, db, optimistic, "EXPLAIN (ANALYZE) SELECT id FROM jobs WHERE id = 1 FOR UPDATE"));
            StringAssert.Contains("LOCKING PESSIMISTIC", refusal.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(optimistic);
        }
    }

    /// <summary>The query checks of the statement apply to every EXPLAIN form.</summary>
    [TestCase("EXPLAIN SELECT COUNT(*) FROM jobs FOR UPDATE")]
    [TestCase("EXPLAIN SELECT DISTINCT state FROM jobs FOR UPDATE")]
    [TestCase("EXPLAIN SELECT 1 FOR UPDATE")]
    [TestCase("EXPLAIN (ANALYZE) SELECT id FROM jobs FOR UPDATE OF other")]
    public async Task Explain_RefusesWhatTheStatementRefuses(string sql)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await BeginAsync(database);
        CamusDBException refusal = await ExpectFailureAsync(() => QueryAsync(executor, db, tx, sql));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, refusal.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
    }

    /// <summary>
    /// EXPLAIN (ANALYZE) runs the statement: the rows it returns stay locked until its transaction
    /// ends, as in PostgreSQL, and NOWAIT fails it on a held row as the statement fails.
    /// </summary>
    [Test]
    public async Task ExplainAnalyze_HoldsTheLocksUntilTheTransactionEnds_AndNoWaitFails()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction analyzing = await BeginAsync(database);
        List<(string Node, QueryResultRow Row)> plan = await ExplainAsync(executor, db, analyzing,
            "EXPLAIN (ANALYZE) SELECT id FROM jobs WHERE id <= 3 FOR UPDATE");
        Assert.AreEqual(3, LockStep(plan).Row["actual_rows"].LongValue);

        KvTransaction probe = await BeginAsync(database);
        CamusDBException held = await ExpectFailureAsync(
            () => QueryAsync(executor, db, probe, "SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, held.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(probe);

        // The NOWAIT form of the same EXPLAIN, from another transaction, fails as the statement does.
        KvTransaction other = await BeginAsync(database);
        CamusDBException noWait = await ExpectFailureAsync(
            () => QueryAsync(executor, db, other, "EXPLAIN (ANALYZE) SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, noWait.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(other);

        await database.Transactions.CommitAsync(analyzing);

        KvTransaction after = await BeginAsync(database);
        Assert.AreEqual(new long[] { 2 }, await IdsAsync(executor, db, after, "SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT"));
        await database.Transactions.CommitAsync(after);
    }

    /// <summary>
    /// The lock step reports its three counters. Rows 2 and 3 are held, so a SKIP LOCKED page of
    /// LIMIT 3 sends rows 1, 2 and 3 (one locked, two skipped) and then rows 4 and 5:
    /// 3 locked, 2 skipped, 0 dropped, and 3 rows out.
    /// </summary>
    [Test]
    public async Task ExplainAnalyze_CountsTheRowsSkippedBecauseHeld()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction holder = await HoldAsync(executor, database, db, "2, 3");
        try
        {
            KvTransaction analyzing = await BeginAsync(database);
            List<(string Node, QueryResultRow Row)> plan = await ExplainAsync(executor, db, analyzing,
                "EXPLAIN (ANALYZE) SELECT id FROM jobs ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED");
            await database.Transactions.CommitAsync(analyzing);

            QueryResultRow lockStep = LockStep(plan);
            Assert.AreEqual(3, lockStep.Row["actual_rows"].LongValue);
            StringAssert.Contains("for update, skip locked, locked=3, skipped-held=2, recheck-dropped=0", lockStep.Row["detail"].StrValue);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(holder);
        }
    }

    /// <summary>
    /// After the scan chose rows 1 and 2 and before the lock, a concurrent commit moves row 1 out of
    /// the WHERE clause and deletes row 2. Both are locked and then dropped, and rows 3 and 4 fill
    /// the LIMIT: 4 locked, 0 skipped, 2 dropped, and 2 rows out.
    /// </summary>
    [Test]
    public async Task ExplainAnalyze_CountsTheRowsTheRecheckDropped()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        QueryExecutor queryExecutor = executor.QueryExecutorForTests;
        bool hookRan = false;

        queryExecutor.TestBeforeRowLockHook = async () =>
        {
            queryExecutor.TestBeforeRowLockHook = null;
            hookRan = true;

            await CommitAsync(executor, database, db, "UPDATE jobs SET state = 'done' WHERE id = 1");
            await CommitAsync(executor, database, db, "DELETE FROM jobs WHERE id = 2");
        };

        try
        {
            KvTransaction analyzing = await BeginAsync(database);
            List<(string Node, QueryResultRow Row)> plan = await ExplainAsync(executor, db, analyzing,
                "EXPLAIN (ANALYZE) SELECT id FROM jobs WHERE state = 'ready' ORDER BY id LIMIT 2 FOR UPDATE");
            await database.Transactions.CommitAsync(analyzing);

            Assert.IsTrue(hookRan);
            QueryResultRow lockStep = LockStep(plan);
            Assert.AreEqual(2, lockStep.Row["actual_rows"].LongValue);
            StringAssert.Contains("locked=4, skipped-held=0, recheck-dropped=2", lockStep.Row["detail"].StrValue);
        }
        finally
        {
            queryExecutor.TestBeforeRowLockHook = null;
        }
    }

    // ── result cache ──────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The statement runs the same SELECT, with the same cache hint, as a plain cached read. So its
    /// fingerprint can match a cached entry. It must still never be served from the cache, because a
    /// cached row is not locked, and it must never fill the cache. The cache gate admits read-only
    /// transactions only, and a locking read always runs in a read-write one: an autocommit
    /// statement gets its own short read-write transaction, and an explicit one is refused when
    /// read-only.
    /// </summary>
    [Test]
    public async Task TheResultCache_IsNeitherReadNorFilled()
    {
        // The option alone attaches no cache to a test engine; the host builds the cache and passes it in.
        CamusDBOptions cacheOn = Options with { QueryResultCacheEnabled = true };
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(
            CreateCommandExecutor(cacheOn, new CamusDB.Core.Config.CamusDBOptionsHolder(cacheOn),
                cache: new QueryResultCache(cacheOn, sweepIntervalMs: -1)));

        async Task<(long[] Ids, CacheMetadataHolder Meta)> RunAsync(KvTransaction tx, string sql)
        {
            CacheMetadataHolder meta = new();
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null), meta);
            long[] ids = (await cursor.ToListAsync()).Select(r => r.Row["id"].LongValue).ToArray();
            return (ids, meta);
        }

        const string Plain = "SELECT id FROM jobs{cache=fu} WHERE id = 1";
        const string Locking = "SELECT id FROM jobs{cache=fu} WHERE id = 1 FOR UPDATE";

        // The autocommit shape: one read-write transaction for the one statement. It bypasses the cache.
        KvTransaction single = await BeginAsync(database);
        (long[] first, CacheMetadataHolder firstMeta) = await RunAsync(single, Locking);
        await database.Transactions.CommitAsync(single);
        Assert.AreEqual(new long[] { 1 }, first);
        Assert.AreEqual(QueryCacheStatus.Bypass, firstMeta.Status);
        Assert.AreEqual(QueryCacheBypassReason.InTransaction, firstMeta.BypassReason);

        // It did not fill the cache: the first plain read is a miss, and the second one is a hit.
        CacheMetadataHolder missMeta = (await RunAsync(KvTransaction.CreateReadOnly(), Plain)).Meta;
        Assert.AreEqual(QueryCacheStatus.Miss, missMeta.Status, missMeta.BypassReason.ToString());
        CacheMetadataHolder hitMeta = (await RunAsync(KvTransaction.CreateReadOnly(), Plain)).Meta;
        Assert.AreEqual(QueryCacheStatus.Hit, hitMeta.Status, hitMeta.BypassReason.ToString());

        // An explicit transaction with an entry in the cache: the statement runs live and holds the row.
        KvTransaction holder = await BeginAsync(database);
        (long[] held, CacheMetadataHolder heldMeta) = await RunAsync(holder, Locking);
        Assert.AreEqual(new long[] { 1 }, held);
        Assert.AreEqual(QueryCacheStatus.Bypass, heldMeta.Status);
        Assert.AreEqual(QueryCacheBypassReason.InTransaction, heldMeta.BypassReason);

        KvTransaction probe = await BeginAsync(database);
        CamusDBException e = await ExpectFailureAsync(
            () => QueryAsync(executor, db, probe, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT"));
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, e.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(probe);

        await database.Transactions.CommitAsync(holder);
    }

    // ── job queue ─────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The job-queue shape SKIP LOCKED exists for: several workers, each in a loop of BEGIN, pick one
    /// ready job with SKIP LOCKED, mark it done, COMMIT. Every job is processed exactly once, no worker
    /// sees an error, and no lock is left behind.
    /// </summary>
    [Test]
    public async Task JobQueue_EveryJobIsProcessedExactlyOnce_WithNoErrors()
    {
        const int Workers = 4;
        const int QueueJobs = 40;

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE queue (id INT64 PRIMARY KEY, state STRING, worker INT64)", null));
        string values = string.Join(", ", Enumerable.Range(1, QueueJobs).Select(i => $"({i}, 'ready', -1)"));
        await CommitAsync(executor, database, db, "INSERT INTO queue (id, state, worker) VALUES " + values);

        List<long>[] picked = Enumerable.Range(0, Workers).Select(_ => new List<long>()).ToArray();

        async Task WorkAsync(int worker)
        {
            while (true)
            {
                KvTransaction tx = await BeginAsync(database);
                try
                {
                    long[] job = await IdsAsync(executor, db, tx,
                        "SELECT id FROM queue WHERE state = 'ready' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED");

                    if (job.Length == 0)
                    {
                        await database.Transactions.CommitAsync(tx);
                        return;
                    }

                    await ExecAsync(executor, db, tx, $"UPDATE queue SET state = 'done', worker = {worker} WHERE id = {job[0]}");
                    await database.Transactions.CommitAsync(tx);
                    picked[worker].Add(job[0]);
                }
                finally
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, Workers).Select(w => Task.Run(() => WorkAsync(w))));

        long[] all = picked.SelectMany(p => p).OrderBy(id => id).ToArray();
        Assert.AreEqual(Enumerable.Range(1, QueueJobs).Select(i => (long)i).ToArray(), all, "each job exactly once");

        KvTransaction check = await BeginAsync(database);
        List<QueryResultRow> rows = await QueryAsync(executor, db, check, "SELECT id, state, worker FROM queue ORDER BY id");
        await database.Transactions.CommitAsync(check);

        foreach (QueryResultRow row in rows)
        {
            long id = row.Row["id"].LongValue;
            Assert.AreEqual("done", row.Row["state"].StrValue);
            int worker = (int)row.Row["worker"].LongValue;
            Assert.That(picked[worker], Does.Contain(id), $"job {id} is marked with the worker that committed it");
        }

        // Every lock was released: one transaction can lock the whole table with NOWAIT.
        KvTransaction sweep = await BeginAsync(database);
        Assert.AreEqual(QueueJobs, (await IdsAsync(executor, db, sweep, "SELECT id FROM queue FOR UPDATE NOWAIT")).Length);
        await database.Transactions.CommitAsync(sweep);
    }
}
