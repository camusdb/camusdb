/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Extensions.Inline;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>statement_timeout_ms</c> on plain SQL: a statement still running at its limit fails with
/// <see cref="CamusDBErrorCodes.StatementTimeout"/>, a statement that ends in time is not affected, the
/// failure follows the rules of any failed statement, and the setting applies to the next statement
/// after a published change. The statements of inline code are in
/// <c>TestInlineFunctionStatementTimeout</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestStatementTimeout : BaseTest
{
    private const int TimeoutMs = 300;

    /// <summary>How long after the limit a stopped statement may return, on a slow runner under the load of the suite.</summary>
    private const int SlackMs = 3000;

    /// <summary>Twenty-five million pairs: many seconds of work that returns one row.</summary>
    private const string SlowRead = "SELECT count(*) AS c FROM generate_series(1, 5000) a, generate_series(1, 5000) b";

    internal static async Task<List<QueryResultRow>> Query(CommandExecutor executor, string db, string sql, KvTransaction? tx = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction transaction = tx ?? await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(transaction, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            if (tx is null)
                await database.Transactions.CommitAsync(transaction);

            return rows;
        }
        finally
        {
            if (tx is null)
                await database.Transactions.RollbackIfNotCompletedAsync(transaction);
        }
    }

    private static async Task<int> NonQuery(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction transaction = await database.Transactions.BeginAsync();

        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(transaction, db, sql, null));
            await database.Transactions.CommitAsync(transaction);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(transaction);
        }
    }

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction transaction = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(transaction, db, sql, null));
        await database.Transactions.CommitAsync(transaction);
    }

    private static async Task<CamusDBException> Stopped(Func<Task> statement)
    {
        Stopwatch watch = Stopwatch.StartNew();
        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () => await statement())!;
        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.StatementTimeout, stopped.Code, stopped.Message);
        StringAssert.Contains("statement_timeout_ms", stopped.Message);
        Assert.Less(watch.ElapsedMilliseconds, TimeoutMs + SlackMs, "the statement must end soon after its limit");

        return stopped;
    }

    [Test]
    public async Task APlainRead_StopsAtTheLimit_AndAFastOneIsNotAffected()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { StatementTimeoutMs = TimeoutMs });

        await Stopped(() => Query(executor, db, SlowRead));

        List<QueryResultRow> fast = await Query(executor, db, "SELECT count(*) AS c FROM generate_series(1, 100) i");
        Assert.AreEqual(100, fast.Single().Row["c"].LongValue);
    }

    [Test]
    public async Task APlainWrite_StopsAtTheLimit_AndKeepsNothing()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { StatementTimeoutMs = TimeoutMs });

        await Ddl(executor, db, "CREATE TABLE pairs (id INT64 PRIMARY KEY)");

        // The source reads twenty-five million pairs and keeps none: the write has nothing to write
        // before its limit, and only the checks of its reads can stop it.
        await Stopped(() => NonQuery(executor, db,
            "INSERT INTO pairs (id) SELECT a * 100000 + b FROM generate_series(1, 5000) a, generate_series(1, 5000) b WHERE a + b < 0"));

        Assert.AreEqual(0, (await Query(executor, db, "SELECT count(*) AS c FROM pairs")).Single().Row["c"].LongValue);
    }

    [Test]
    public async Task AStoppedReadInAnExplicitTransaction_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { StatementTimeoutMs = TimeoutMs });

        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            await Stopped(() => Query(executor, db, SlowRead, tx));

            // PostgreSQL's rule: a failed statement aborts the transaction, a timeout included.
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT 1 AS one", tx))!;
            Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, refused.Code, refused.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task SchemaDdl_IsNotBounded()
    {
        // A limit no statement can meet: anything it bounds fails.
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { StatementTimeoutMs = 1 });

        await Ddl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await Ddl(executor, db, "CREATE INDEX t_v ON t (v)");
        await Ddl(executor, db, "ALTER TABLE t ADD COLUMN w INT64");

        Assert.IsTrue((await executor.OpenDatabase(db)).Schema.Tables.ContainsKey("t"));
    }

    [Test]
    public void TheCodeMapsToBadRequest() =>
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.StatementTimeout));
}

/// <summary>
/// <c>statement_timeout_ms</c> is a runtime setting: the engine reads it at the start of each statement,
/// so a published change applies to the next statement without a restart.
/// </summary>
[NonParallelizable]
internal sealed class TestStatementTimeoutIsARuntimeSetting : BaseTest
{
    [Test]
    public async Task APublishedLimitAppliesToTheNextStatement()
    {
        CamusDBOptions initial = Options with { StatementTimeoutMs = 0 };
        CamusDBOptionsHolder holder = new(initial);

        (string db, _, CommandExecutor executor) = await CreateDatabaseWith(CreateCommandExecutor(initial, holder));

        const string sql = "SELECT count(*) AS c FROM generate_series(1, 1000) a, generate_series(1, 300) b";

        Assert.AreEqual(300_000, (await TestStatementTimeout.Query(executor, db, sql)).Single().Row["c"].LongValue);

        holder.Publish(initial with { StatementTimeoutMs = 1 });

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(() => TestStatementTimeout.Query(executor, db, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTimeout, stopped.Code, stopped.Message);
        StringAssert.Contains("1 ms", stopped.Message);

        holder.Publish(initial);
        Assert.AreEqual(300_000, (await TestStatementTimeout.Query(executor, db, sql)).Single().Row["c"].LongValue);
    }
}

/// <summary>
/// The rules of <see cref="StatementDeadline"/> itself: who owns a deadline, what a statement inside a
/// statement gets, what a flow sees after the statement ended, and the two bounds that hold without
/// the ambient value (a peer's fragment and a trigger fire).
/// </summary>
[TestFixture]
internal sealed class TestStatementDeadline
{
    [Test]
    public void WithNoLimit_ThereIsNoDeadline()
    {
        Assert.IsNull(StatementDeadline.Begin(0, out bool owned));
        Assert.IsFalse(owned);
        Assert.IsNull(StatementDeadline.Current);
    }

    [Test]
    public async Task AStatementInsideAStatement_InheritsTheDeadline_AndEndsNothing()
    {
        await Task.Yield();

        StatementDeadline outer = StatementDeadline.Begin(60_000, out bool ownsOuter)!;
        Assert.IsTrue(ownsOuter);
        Assert.AreSame(outer, StatementDeadline.Current);

        // A nested statement with a different, or no, limit of its own still runs under the outer one.
        Assert.AreSame(outer, StatementDeadline.Begin(5, out bool ownsInner));
        Assert.IsFalse(ownsInner);
        Assert.AreSame(outer, StatementDeadline.Begin(0, out _));

        outer.End();

        // A flow that still holds the value after the end (a background task the statement started) sees none.
        Assert.IsNull(StatementDeadline.Current);
        Assert.IsFalse(outer.IsExpired);
    }

    [Test]
    public async Task AStatementInsideAStatement_FailsAtItsStart_WhenTheDeadlineHasPassed()
    {
        await Task.Yield();

        StatementDeadline outer = StatementDeadline.Begin(1, out _)!;
        await Task.Delay(20);

        CamusDBException late = Assert.Throws<CamusDBException>(() => StatementDeadline.Begin(60_000, out _))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTimeout, late.Code);

        outer.End();
    }

    [Test]
    public async Task SchemaDdl_RunsWithoutTheDeadline_AndTheCallerGetsItBack()
    {
        await Task.Yield();

        StatementDeadline outer = StatementDeadline.Begin(60_000, out _)!;

        await RunsWithoutADeadline();

        Assert.AreSame(outer, StatementDeadline.Current, "taking the deadline off is local to the async method that does it");
        outer.End();

        static async Task RunsWithoutADeadline()
        {
            StatementDeadline.Suspend();
            await Task.Yield();
            Assert.IsNull(StatementDeadline.Current);
        }
    }

    [Test]
    public void APeersFragment_StopsAfterTheTimeItWasSent()
    {
        Stopwatch watch = Stopwatch.StartNew();

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await foreach (int _ in StatementDeadline.BoundFragment(100, CancellationToken.None, Endless))
            {
            }
        })!;

        Assert.AreEqual(CamusDBErrorCodes.StatementTimeout, stopped.Code);
        Assert.Less(watch.ElapsedMilliseconds, 5000);
    }

    [Test]
    public void APeersFragment_WhoseCoordinatorWentAway_EndsWithTheBareCancellation()
    {
        using CancellationTokenSource request = new();
        request.CancelAfter(50);

        Assert.That(async () =>
        {
            await foreach (int _ in StatementDeadline.BoundFragment(60_000, request.Token, Endless))
            {
            }
        }, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void ATriggerFire_NeverRunsPastTheStatementDeadline_WhateverTimeItsSqlTook()
    {
        long statementDeadline = Environment.TickCount64 + 100;
        TriggerBudget budget = new(60_000, 0, 0, statementDeadline);

        // Time spent in the body's SQL is added to the body's own deadline, never past the statement's.
        budget.Suspend();
        Thread.Sleep(150);
        budget.Resume();

        CallBudgetExceededException late = Assert.Throws<CallBudgetExceededException>(budget.Tick)!;
        Assert.AreEqual(CallBudgetKind.Time, late.Kind);
    }

    [Test]
    public void ATriggerFire_WithNoStatementDeadline_KeepsItsOwnBudget()
    {
        TriggerBudget budget = new(60_000, 0, 0);

        budget.Suspend();
        Thread.Sleep(20);
        budget.Resume();

        Assert.DoesNotThrow(budget.Tick);
    }

    private static async IAsyncEnumerable<int> Endless([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (int i = 0; ; i++)
        {
            await Task.Delay(10, cancellationToken);
            yield return i;
        }
    }
}
