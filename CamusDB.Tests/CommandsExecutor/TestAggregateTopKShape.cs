/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The planner's recognition of <c>GROUP BY ... ORDER BY aggregate LIMIT n</c>
/// (<see cref="AggregateTopK"/> on the <see cref="AggregateNode"/>): which forms it accepts, which it
/// refuses, how it computes k, and how EXPLAIN shows it. Planner-level only: the fixture skips the
/// binder, so a column the robots table does not have (<c>role</c>) still plans. The fixture table
/// has <c>name</c> NOT NULL and <c>year</c> nullable, which is what the MIN rule depends on.
/// </summary>
public sealed class TestAggregateTopKShape
{
    private static QueryPlannerTestContext? context;

    private readonly QueryPlanner queryPlanner = new(CamusDBOptions.Default);

    [OneTimeSetUp]
    public void OneTimeSetUp() => context = QueryPlannerTestContext.Create();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (context is not null)
        {
            await context.DisposeAsync().ConfigureAwait(false);
            context = null;
        }
    }

    private QueryPlan Plan(string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(ast);

        ExecuteSQLTicket executeTicket = new(
            txnState: context!.Txn,
            database: QueryPlannerTestContext.DatabaseName,
            sql: sql,
            parameters: parameters);

        return queryPlanner.GetPlan(context.Database, context.Table, QueryTicketAdapter.ToQueryTicket(query, executeTicket));
    }

    private AggregateTopK? TopKOf(string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        PhysicalPlanNode? node = Plan(sql, parameters).Root;

        while (node is not null and not AggregateNode)
            node = node.Input;

        Assert.IsNotNull(node, "The plan must have an aggregate node: " + sql);
        return ((AggregateNode)node!).TopK;
    }

    [Test]
    public void CountDescending_ByAlias_IsRecognized()
    {
        AggregateTopK? topK = TopKOf("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC LIMIT 10");

        Assert.IsNotNull(topK);
        Assert.AreEqual("c", topK!.OutputName);
        Assert.AreEqual(AggregateTopKOrder.CountDescending, topK.Order);
        Assert.AreEqual(10, topK.K);
        Assert.IsTrue(topK.Descending);
        Assert.IsFalse(topK.OtherAggregatesProjected);
        Assert.IsFalse(topK.SpanLocalTopKIsExact, "A COUNT adds up over the spans, so a span-local top k is not exact");
    }

    [Test]
    public void CountOfColumn_IsRecognized()
    {
        AggregateTopK? topK = TopKOf("SELECT role, COUNT(year) AS c FROM robots GROUP BY role ORDER BY c DESC LIMIT 3");

        Assert.AreEqual(AggregateTopKOrder.CountDescending, topK?.Order);
    }

    [Test]
    public void MaxDescending_IsRecognized_AndSpanLocalTopKIsExact()
    {
        AggregateTopK? topK = TopKOf("SELECT role, MAX(year) AS m FROM robots GROUP BY role ORDER BY m DESC LIMIT 5");

        Assert.IsNotNull(topK);
        Assert.AreEqual(AggregateTopKOrder.MaxDescending, topK!.Order);
        Assert.AreEqual(5, topK.K);
        Assert.IsTrue(topK.SpanLocalTopKIsExact, "MAX DESC over a nullable column is exact: NULL is the smallest value");
    }

    [Test]
    public void MinAscending_OnNotNullColumn_IsRecognized()
    {
        AggregateTopK? topK = TopKOf("SELECT role, MIN(name) AS lo FROM robots GROUP BY role ORDER BY lo ASC LIMIT 5");

        Assert.IsNotNull(topK);
        Assert.AreEqual(AggregateTopKOrder.MinAscending, topK!.Order);
        Assert.IsFalse(topK.Descending);
        Assert.IsTrue(topK.SpanLocalTopKIsExact);
    }

    [Test]
    public void MinAscending_OnNullableColumn_IsRefused()
    {
        // MIN skips NULL but the sort puts NULL first, so a group whose values are all NULL would be
        // ranked last on each span and could be lost.
        Assert.IsNull(TopKOf("SELECT role, MIN(year) AS lo FROM robots GROUP BY role ORDER BY lo ASC LIMIT 5"));
    }

    [Test]
    public void OffsetAddsToK()
    {
        AggregateTopK? topK = TopKOf("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC LIMIT 10 OFFSET 7");

        Assert.AreEqual(17, topK?.K);
    }

    [Test]
    public void ParameterLimitAndOffset_AreEvaluatedForTheStatement()
    {
        AggregateTopK? topK = TopKOf(
            "SELECT role, MAX(year) AS m FROM robots GROUP BY role ORDER BY m DESC LIMIT @limit OFFSET @offset",
            new()
            {
                { "@limit", new ColumnValue(ColumnType.Integer64, 4) },
                { "@offset", new ColumnValue(ColumnType.Integer64, 2) },
            });

        Assert.AreEqual(6, topK?.K);
    }

    [Test]
    public void LimitZero_IsRecognizedWithKZero()
    {
        Assert.AreEqual(0, TopKOf("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC LIMIT 0")?.K);
    }

    [Test]
    public void OtherAggregate_ClearsSpanLocalTopK()
    {
        AggregateTopK? topK = TopKOf("SELECT role, MAX(year) AS m, COUNT(*) AS c FROM robots GROUP BY role ORDER BY m DESC LIMIT 5");

        Assert.IsNotNull(topK);
        Assert.IsTrue(topK!.OtherAggregatesProjected);
        Assert.IsFalse(topK.SpanLocalTopKIsExact, "The COUNT next to the MAX needs every span");
    }

    [Test]
    public void ComputedOutput_ClearsSpanLocalTopK()
    {
        AggregateTopK? topK = TopKOf("SELECT role, MAX(year) AS m, year + 1 AS y FROM robots GROUP BY role ORDER BY m DESC LIMIT 5");

        Assert.IsTrue(topK?.OtherAggregatesProjected);
    }

    [TestCase("SELECT role, SUM(year) AS s FROM robots GROUP BY role ORDER BY s DESC LIMIT 10", TestName = "Refused_Sum")]
    [TestCase("SELECT role, AVG(year) AS a FROM robots GROUP BY role ORDER BY a DESC LIMIT 10", TestName = "Refused_Avg")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c ASC LIMIT 10", TestName = "Refused_CountAscending")]
    [TestCase("SELECT role, MAX(year) AS m FROM robots GROUP BY role ORDER BY m ASC LIMIT 10", TestName = "Refused_MaxAscending")]
    [TestCase("SELECT role, MIN(name) AS lo FROM robots GROUP BY role ORDER BY lo DESC LIMIT 10", TestName = "Refused_MinDescending")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC, role LIMIT 10", TestName = "Refused_TwoOrderKeys")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role HAVING COUNT(*) > 1 ORDER BY c DESC LIMIT 10", TestName = "Refused_Having")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC", TestName = "Refused_NoLimit")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY role LIMIT 10", TestName = "Refused_OrderByGroupKey")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC LIMIT 2147483647 OFFSET 1", TestName = "Refused_KPastInt32")]
    [TestCase("SELECT role, COUNT(*) AS c FROM robots GROUP BY role ORDER BY c DESC LIMIT 9223372036854775807 OFFSET 9223372036854775807", TestName = "Refused_KOverflow")]
    public void Refused(string sql)
    {
        Assert.IsNull(TopKOf(sql));
    }

    [Test]
    public void Explain_ShowsTheShapeOnTheAggregateRow()
    {
        string rendered = PlanRenderer.Render(Plan("SELECT role, MAX(year) AS m FROM robots GROUP BY role ORDER BY m DESC LIMIT 5 OFFSET 1"));

        Assert.That(rendered, Does.Contain("aggregate(group=[role], aggs=[max(year)], top_k=[m DESC, k: 6])"));
    }

    [Test]
    public void Explain_IsUnchangedWithoutTheShape()
    {
        string rendered = PlanRenderer.Render(Plan("SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role ORDER BY role"));

        Assert.That(rendered, Does.Contain("aggregate(group=[role], aggs=[count(*)])"));
        Assert.That(rendered, Does.Not.Contain("top_k"));
    }
}
