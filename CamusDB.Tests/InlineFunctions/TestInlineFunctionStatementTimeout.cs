/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// <c>statement_timeout_ms</c> bounds the statements that run inline C# code. The budgets of an inline
/// function apply to one call, and a statement calls a function once per row, so without a limit per
/// statement <c>SELECT burn(v) FROM t</c> runs for as long as the table is large, with every call inside
/// its budget. Each case here is a statement that would run for many seconds: it must fail with
/// <see cref="CamusDBErrorCodes.StatementTimeout"/> soon after the limit, and a write must leave nothing
/// behind.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInlineFunctionStatementTimeout : InlineFunctionTestBase
{
    private const int StatementTimeoutMs = 400;

    /// <summary>How long after the limit a stopped statement may return, on a slow runner under the load of the suite.</summary>
    private const int SlackMs = 3000;

    private const int Rows = 60;

    /// <summary>
    /// Each call is well inside the call budget of one second (a few tens to a few hundreds of
    /// milliseconds), and sixty of them take several seconds: only a limit per statement stops it.
    /// </summary>
    private const string Burn =
        "CREATE FUNCTION burn(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ " +
        "long x = 0; for (long i = 0; i < 3000000; i++) { x += i % 7; } return n + (x % 2); $$";

    private const string Spin =
        "CREATE FUNCTION spin(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ while (true) { } $$";

    private async Task<(string Db, CommandExecutor Executor)> Start(int statementTimeoutMs = StatementTimeoutMs, int callTimeoutMs = 1000)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with
        {
            StatementTimeoutMs = statementTimeoutMs,
            InlineFunctionCallTimeoutMs = callTimeoutMs,
        });

        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await ExecNonQuery(executor, db, $"INSERT INTO t (id, v) SELECT i, i FROM generate_series(1, {Rows}) i");
        await ExecDdl(executor, db, Burn);
        await ExecDdl(executor, db, Spin);
        return (db, executor);
    }

    private static async Task<CamusDBException> Stopped(System.Func<Task> statement, int withinMs)
    {
        Stopwatch watch = Stopwatch.StartNew();
        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () => await statement())!;
        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.StatementTimeout, stopped.Code, stopped.Message);
        StringAssert.Contains("statement_timeout_ms", stopped.Message);
        Assert.Less(watch.ElapsedMilliseconds, withinMs + SlackMs, "the statement must end soon after its limit");

        return stopped;
    }

    /// <summary>
    /// A DDL statement in its own transaction, rolled back when it fails as a transport rolls it back.
    /// <c>ExecDdl</c> leaves a failed statement's transaction open, and the locks of its reads with it.
    /// </summary>
    private static async Task DdlRolledBackOnFailure(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static Task<long> Sum(CommandExecutor executor, string db) =>
        Number(executor, db, "SELECT sum(v) FROM t");

    [Test]
    public async Task ASelectThatCallsAFunctionPerRow_StopsAtTheStatementLimit()
    {
        (string db, CommandExecutor executor) = await Start();

        await Stopped(() => ExecQuery(executor, db, "SELECT burn(v) FROM t"), StatementTimeoutMs);
        await Stopped(() => ExecQuery(executor, db, "SELECT id FROM t WHERE burn(v) > 0"), StatementTimeoutMs);
        await Stopped(() => ExecQuery(executor, db, "SELECT sum(burn(v)) FROM t"), StatementTimeoutMs);

        // The limit is per statement: a cheap statement afterwards runs as before.
        Assert.AreEqual(Rows * (Rows + 1) / 2, await Sum(executor, db));
    }

    [Test]
    public async Task ACallThatRunsWhenTheTimeIsUp_StopsAtTheStatementDeadline_NotAtItsOwnBudget()
    {
        // The call budget alone would hold the statement for 30 seconds.
        (string db, CommandExecutor executor) = await Start(callTimeoutMs: 30_000);

        CamusDBException stopped = await Stopped(() => ExecQuery(executor, db, "SELECT spin(1)"), StatementTimeoutMs);
        StringAssert.DoesNotContain("inline_function_call_timeout_ms", stopped.Message);
    }

    [Test]
    public async Task ACallThatRunsOutOfItsOwnBudgetFirst_StillReportsTheCallBudget()
    {
        (string db, CommandExecutor executor) = await Start(statementTimeoutMs: 60_000, callTimeoutMs: 100);

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT spin(1)"))!;
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped.Code, stopped.Message);
        StringAssert.Contains("inline_function_call_timeout_ms", stopped.Message);
    }

    [Test]
    public async Task AWriteThatCallsAFunctionPerRow_StopsAndKeepsNothing()
    {
        (string db, CommandExecutor executor) = await Start();
        long before = await Sum(executor, db);

        // In the SET list: evaluated in the write phase, once per written row.
        await Stopped(() => ExecNonQuery(executor, db, "UPDATE t SET v = burn(v) + 1 WHERE TRUE"), StatementTimeoutMs);

        // In the WHERE: evaluated by the locate scan, before the first write.
        await Stopped(() => ExecNonQuery(executor, db, "DELETE FROM t WHERE burn(v) > 0"), StatementTimeoutMs);

        // In the source of an INSERT ... SELECT.
        await ExecDdl(executor, db, "CREATE TABLE copy (id INT64 PRIMARY KEY, v INT64)");
        await Stopped(() => ExecNonQuery(executor, db, "INSERT INTO copy (id, v) SELECT id, burn(v) FROM t"), StatementTimeoutMs);

        // In the query of a CREATE TABLE ... AS, which drops the table it created when the load fails.
        await Stopped(() => DdlRolledBackOnFailure(executor, db, "CREATE TABLE made AS SELECT id, burn(v) AS v FROM t"), StatementTimeoutMs);

        Assert.AreEqual(before, await Sum(executor, db), "no row of a stopped UPDATE or DELETE is kept");
        Assert.AreEqual(0, await Number(executor, db, "SELECT count(*) FROM copy"), "no row of a stopped INSERT ... SELECT is kept");
        Assert.IsFalse((await executor.OpenDatabase(db)).Schema.Tables.ContainsKey("made"), "a stopped CREATE TABLE ... AS drops its table");
    }

    [Test]
    public async Task ATriggerThatFiresPerRow_StopsAtTheStatementLimit()
    {
        (string db, CommandExecutor executor) = await Start();
        long before = await Sum(executor, db);

        await ExecDdl(executor, db,
            "CREATE TRIGGER slow ON t BEFORE UPDATE LANGUAGE csharp AS $$\n" +
            "    long x = 0;\n" +
            "    for (long i = 0; i < 3000000; i++) { x += i % 7; }\n" +
            "    if (x < 0) throw new ArgumentException(\"x\");\n" +
            "$$");

        await Stopped(() => ExecNonQuery(executor, db, "UPDATE t SET v = v + 1 WHERE TRUE"), StatementTimeoutMs);

        Assert.AreEqual(before, await Sum(executor, db), "no row of a stopped UPDATE is kept");
    }

    [Test]
    public async Task AnEndlessTrigger_StopsAtTheStatementDeadline_NotAtItsOwnBudget()
    {
        (string db, CommandExecutor executor) = await Start(callTimeoutMs: 30_000);

        await ExecDdl(executor, db, "CREATE TRIGGER endless ON t BEFORE UPDATE LANGUAGE csharp AS $$ while (true) { } $$");

        await Stopped(() => ExecNonQuery(executor, db, "UPDATE t SET v = v + 1 WHERE id = 1"), StatementTimeoutMs);
    }

    [Test]
    public async Task TheSqlATriggerRuns_HasTheDeadlineOfTheStatementThatFiredIt()
    {
        (string db, CommandExecutor executor) = await Start();

        // Each statement of the loop is fast, and the body's own work is small: the time goes into the
        // SQL, which the trigger's own budget does not count. Only the outer statement's deadline,
        // inherited by every statement the body runs, ends the loop.
        await ExecDdl(executor, db,
            "CREATE TRIGGER chatty ON t AFTER UPDATE LANGUAGE csharp AS $$\n" +
            "    for (int i = 0; i < 1000000; i++)\n" +
            "        await Sql.QueryAsync(\"SELECT count(*) FROM t\");\n" +
            "$$");

        await Stopped(() => ExecNonQuery(executor, db, "UPDATE t SET v = v + 1 WHERE id = 1"), StatementTimeoutMs);
        Assert.AreEqual(1, await Number(executor, db, "SELECT v FROM t WHERE id = 1"), "the stopped UPDATE is not kept");
    }

    [Test]
    public async Task WithNoLimit_TheSameStatementsRunToTheirEnd()
    {
        (string db, CommandExecutor executor) = await Start(statementTimeoutMs: 0);

        // A smaller table, so the unbounded run stays short.
        await ExecNonQuery(executor, db, "DELETE FROM t WHERE id > 3");

        Assert.AreEqual(3, (await ExecQuery(executor, db, "SELECT burn(v) FROM t")).Count);
        Assert.AreEqual(3, await ExecNonQuery(executor, db, "UPDATE t SET v = burn(v) WHERE TRUE"));
    }
}
