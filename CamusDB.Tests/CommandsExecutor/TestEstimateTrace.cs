/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The estimate trace of <c>EXPLAIN (ANALYZE)</c>: every estimated node names the rule that made
/// its estimate and the inputs the rule read. The trace appears on <c>ANALYZE</c> rows only, so a
/// statement and a plain <c>EXPLAIN</c> pay nothing for it.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestEstimateTrace : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    // 1,000 rows over 200 keys, five rows each: every value has the same count, so the
    // most-common-value list stays empty (nothing stands out) and an equality prices at 1 / NDV.
    private const int Rows = 1000;
    private const int Keys = 200;

    [Test]
    public void TraceJson_RendersEveryField()
    {
        EstimateTrace trace = new();
        trace.Factors.Add(new EstimateFactor
        {
            Rule = EstimateRule.EqualityNdv,
            Stage = EstimateStage.Comparison,
            Column = "k",
            Selectivity = 0.005,
            Raw = 0.005,
            Alternative = 0.004,
            Ndv = 200,
            Rows = 1000,
            Buckets = 100,
            Divided = true,
        });
        trace.Complete(EstimateFormula.Product, 1000, 5);

        using JsonDocument json = JsonDocument.Parse(EstimateTraceJson.Render(trace));
        JsonElement root = json.RootElement;
        Assert.That(root.GetProperty("formula").GetString(), Is.EqualTo("product"));
        Assert.That(root.GetProperty("rule").GetString(), Is.EqualTo("equality-ndv"), "the dominant rule comes from the smallest factor");
        Assert.That(root.GetProperty("base").GetDouble(), Is.EqualTo(1000));
        Assert.That(root.GetProperty("estimate").GetDouble(), Is.EqualTo(5));
        JsonElement factor = root.GetProperty("factors")[0];
        Assert.That(factor.GetProperty("column").GetString(), Is.EqualTo("k"));
        Assert.That(factor.GetProperty("sel").GetDouble(), Is.EqualTo(0.005));
        Assert.That(factor.GetProperty("alternative").GetDouble(), Is.EqualTo(0.004));
        Assert.That(factor.GetProperty("ndv").GetInt64(), Is.EqualTo(200));
        Assert.That(factor.GetProperty("divided").GetBoolean(), Is.True);
    }

    [Test]
    public void RuleNames_AreKebabCaseAndRoundTrip()
    {
        Assert.That(EstimateRuleNames.Name(EstimateRule.IndexEqualityKeyNdv), Is.EqualTo("index-equality-key-ndv"));
        Assert.That(EstimateRuleNames.Name(EstimateRule.JoinSkewPair), Is.EqualTo("join-skew-pair"));
        foreach (EstimateRule rule in Enum.GetValues<EstimateRule>())
        {
            Assert.That(EstimateRuleNames.TryParse(EstimateRuleNames.Name(rule), out EstimateRule parsed), Is.True, rule.ToString());
            Assert.That(parsed, Is.EqualTo(rule));
        }
        Assert.That(EstimateRuleNames.TryParse("no-such-rule", out _), Is.False);
    }

    [Test]
    public async Task ExplainAnalyze_CarriesAnEstimateTrace_AndPlainExplainDoesNot()
    {
        Db db = await CreateTable(Options);

        List<QueryResultRow> analyzed = await Query(db, "EXPLAIN (ANALYZE) SELECT id FROM t WHERE k = 7");
        QueryResultRow filter = analyzed.First(r => r.Row["node"].StrValue == "filter");
        Assert.That(filter.Row.ContainsKey("estimate_trace"), Is.True);
        string trace = filter.Row["estimate_trace"].StrValue ?? "";
        Assert.That(trace, Does.Contain("\"rule\":\"equality-ndv\""), trace);
        Assert.That(trace, Does.Contain("\"formula\":\"product\""), trace);

        using JsonDocument json = JsonDocument.Parse(trace);
        Assert.That(json.RootElement.GetProperty("estimate").GetDouble(), Is.EqualTo(filter.Row["estimated_rows"].LongValue), "the trace carries the engine's own estimate");
        Assert.That(json.RootElement.GetProperty("factors")[0].GetProperty("ndv").GetInt64(), Is.EqualTo(Keys), "the rule's input");

        foreach (QueryResultRow row in analyzed.Where(r => r.Row["node"].StrValue is "filter" or "table-scan" or "scan"))
            Assert.That(row.Row["estimate_trace"].Type, Is.Not.EqualTo(ColumnType.Null), $"every estimated node is traced: {row.Row["node"].StrValue}");

        List<QueryResultRow> plain = await Query(db, "EXPLAIN SELECT id FROM t WHERE k = 7");
        Assert.That(plain.All(r => !r.Row.ContainsKey("estimate_trace")), Is.True, "plain EXPLAIN has no trace column");
    }

    [Test]
    public async Task NormalPlanning_LeavesNoTraceOnAnyNode_AndPaysNothingForIt()
    {
        Db db = await CreateTable(Options with { CostBasedJoinOrderEnabled = true });
        await Ddl(db, "CREATE TABLE u (id int64 primary key, k int64)");
        await Exec(db, "INSERT INTO u (id, k) VALUES " + string.Join(", ", Enumerable.Range(0, 200).Select(i => $"({i}, {i})")));
        await Query(db, "ANALYZE u");

        (BoundSelectQuery bound, QueryTicket ticket) = await Bind(db, "SELECT t.id FROM t, u WHERE t.k = u.k AND u.k < 50 AND t.id > 10");

        JoinQueryPlanner normal = new(db.Executor.Options, db.Executor.Statistics);
        JoinQueryPlanner tracing = new(db.Executor.Options, db.Executor.Statistics, traceEstimates: true);

        // One plan of each first, so the measurement below sees neither lazy initialization.
        normal.GetPlan(db.Database, bound, ticket);
        tracing.GetPlan(db.Database, bound, ticket);

        long before = GC.GetAllocatedBytesForCurrentThread();
        QueryPlan plainPlan = normal.GetPlan(db.Database, bound, ticket);
        long plainBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        QueryPlan tracedPlan = tracing.GetPlan(db.Database, bound, ticket);
        long tracedBytes = GC.GetAllocatedBytesForCurrentThread() - before;

        List<PhysicalPlanNode> plainNodes = Nodes(plainPlan.Root);
        List<PhysicalPlanNode> tracedNodes = Nodes(tracedPlan.Root);
        Assert.That(plainNodes.All(n => n.EstimateTrace is null), Is.True, "a statement's planner records no trace");
        Assert.That(tracedNodes.All(n => n.EstimateTrace is not null), Is.True, "the ANALYZE planner records one on every node");
        Assert.That(tracedNodes.Any(n => n.EstimateTrace is { Formula: EstimateFormula.JoinSearch }), Is.True, "the join carries the search's trace");
        Assert.That(tracedBytes, Is.GreaterThan(plainBytes), $"the trace is the extra allocation: plain {plainBytes} bytes, traced {tracedBytes} bytes");
    }

    private static List<PhysicalPlanNode> Nodes(PhysicalPlanNode root)
    {
        List<PhysicalPlanNode> nodes = [];
        for (PhysicalPlanNode? node = root; node is not null; node = node.Input)
            nodes.Add(node);
        return nodes;
    }

    private async Task<(BoundSelectQuery Bound, QueryTicket Ticket)> Bind(Db db, string sql)
    {
        ExecuteSQLTicket executeTicket = new(txnState: await db.Database.Transactions.BeginAsync(), database: db.Name, sql: sql, parameters: null);
        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(db.Executor.Catalogs, logger)).BindAsync(db.Database, selectQuery);
        return (bound, QueryTicketAdapter.ToQueryTicket(bound, executeTicket));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateTable(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(options with { PlanCacheEnabled = false });
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE t (id int64 primary key, k int64)");

        List<string> rows = new(Rows);
        for (int i = 0; i < Rows; i++)
            rows.Add($"({i}, {i % Keys})");

        for (int start = 0; start < Rows; start += 250)
            await Exec(db, "INSERT INTO t (id, k) VALUES " + string.Join(", ", rows.Skip(start).Take(250)));

        await Query(db, "ANALYZE t");
        return db;
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task<List<QueryResultRow>> Query(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }
}
