/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An Integer64 overflow as a client sees it: the HTTP SQL endpoints answer 400 with
/// <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>, and the gRPC service answers
/// <c>OUT_OF_RANGE</c> with the same code in the <c>camus-error-code</c> trailer. Before, the
/// overflowing query answered 200 with a wrapped value, and <c>min / -1</c> answered an unclassified
/// server error. The controllers and the service run in-process, with no HTTP pipeline.
///
/// <para>Also covers the partial split of an aggregate below a gather, which ships the SUM and AVG
/// of an Integer64 column as exact NUMERIC span totals. The split is built directly, as no
/// single-node fixture reaches a gather; <c>TestClusterInt64Gather</c> runs it across nodes.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInt64OverflowTransports : BaseTest
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private async Task<(string db, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE t (id int64 PRIMARY KEY, v int64)", null));
        await database.Transactions.CommitAsync(tx);

        tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t (id, v) VALUES (1, 9223372036854775807)", null));
        await database.Transactions.CommitAsync(tx);

        return (db, executor);
    }

    private ExecuteSQLController Sql(CommandExecutor executor, string db, string sql) =>
        new(executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), logger, Options)
        {
            ControllerContext = Context(new { databaseName = db, sql }),
        };

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, CamelCase));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    /// <summary>Reads Status, Code and Message from any of the response DTOs, which share those names.</summary>
    private static void AssertOutOfRange(JsonResult result, string what)
    {
        object value = result.Value!;
        Type type = value.GetType();
        string? status = (string?)type.GetProperty("Status")!.GetValue(value);
        string? code = (string?)type.GetProperty("Code")!.GetValue(value);
        string? message = (string?)type.GetProperty("Message")!.GetValue(value);

        Assert.AreEqual("failed", status, $"{what}: {message}");
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, code, $"{what}: {message}");
        Assert.AreEqual(400, result.StatusCode, $"{what}: {message}");
    }

    private static async Task AssertRpcOutOfRange(Func<Task> call, string what)
    {
        RpcException exception = Assert.ThrowsAsync<RpcException>(async () => await call(), what)!;
        Assert.AreEqual(StatusCode.OutOfRange, exception.StatusCode, $"{what}: {exception.Status.Detail}");
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, exception.Trailers.GetValue("camus-error-code"), $"{what}: {exception.Status.Detail}");
    }

    [Test]
    public void ErrorCode_MapsToHttp400AndGrpcOutOfRange()
    {
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.IntegerValueOutOfRange));
        Assert.AreEqual(StatusCode.OutOfRange, GrpcErrorMapper.GetGrpcStatus(CamusDBErrorCodes.IntegerValueOutOfRange));
    }

    [Test]
    public async Task OverHttp_AnOverflowIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        AssertOutOfRange(await Sql(executor, db, "SELECT v + 1 AS x FROM t").ExecuteSQLQuery(), "query");
        AssertOutOfRange(await Sql(executor, db, "SELECT -9223372036854775808 / -1 AS x").ExecuteSQLQuery(), "min / -1");
        AssertOutOfRange(await Sql(executor, db, "UPDATE t SET v = v * 2 WHERE id = 1").ExecuteNonSQLQuery(), "UPDATE");
    }

    [Test]
    public async Task OverGrpc_AnOverflowIsOutOfRangeWithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();
        CamusSqlService service = new(executor, new HttpTransactionCoordinator(executor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

        await AssertRpcOutOfRange(() => service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT SUM(v) + 1 AS x FROM t" },
            new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()), "query");
        await AssertRpcOutOfRange(() => service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT -9223372036854775808 / -1 AS x" },
            new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()), "min / -1");
        await AssertRpcOutOfRange(() => service.ExecuteNonQuery(new SqlRequest { Database = db, Sql = "UPDATE t SET v = v + 1 WHERE id = 1" },
            new TestServerCallContext()), "UPDATE");
    }

    // ── Partial aggregation below a gather ──────────────────────────────────

    private static NodeAst Identifier(string name) => new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst Aliased(string function, string column, string alias) =>
        new(NodeType.ExprAlias,
            new NodeAst(NodeType.ExprFuncCall, Identifier(function), Identifier(column), null, null, null, null, null, null),
            Identifier(alias), null, null, null, null, null, null);

    /// <summary>The planner's split for one projection over <c>nums (id, n int64, f float64, g int64)</c>, grouped by g.</summary>
    private async Task<PartialAggregatePlan> SplitAsync(string function, string column)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db, "CREATE TABLE nums (id int64 PRIMARY KEY, n int64, f float64, g int64)", null));
        await database.Transactions.CommitAsync(ddl);

        MethodInfo split = typeof(QueryPlanner).GetMethod("TryBuildPartialAggregatePlan", BindingFlags.NonPublic | BindingFlags.Static)!;
        GatherNode gather = new(new TablePlacement("ks", false, [new PlacementSpan(null, null, 1, 1, null, [], true, true)], 0));
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "nums"));
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            QueryTicket ticket = new(tx, db, "nums", null, [Identifier("g"), Aliased(function, column, "v")], null, null, null, null, null, null,
                groupBy: [Identifier("g")]);
            return (PartialAggregatePlan)split.Invoke(null, [ticket, table, gather])!;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static bool IsNumericCast(NodeAst? node) =>
        node is { nodeType: NodeType.ExprCast, rightAst.nodeType: NodeType.TypeNumeric };

    [Test]
    public async Task PartialSplit_OfAnInteger64Sum_ShipsExactNumericSpanTotals()
    {
        PartialAggregatePlan plan = await SplitAsync("sum", "n");

        NodeAst shipped = plan.ShipProjections.Single(p => p.nodeType == NodeType.ExprAlias).leftAst!;
        Assert.IsTrue(IsNumericCast(shipped.rightAst), "the span sums CAST(n AS numeric)");
        CollectionAssert.AreEqual(new[] { "v" }, plan.IntegerSumOutputs.ToArray());
        Assert.IsTrue(plan.NeedsFinalization);
    }

    [Test]
    public async Task PartialSplit_OfAnInteger64Avg_ShipsExactNumericSpanTotals()
    {
        PartialAggregatePlan plan = await SplitAsync("avg", "n");

        NodeAst shippedSum = plan.ShipProjections.Single(p => p.rightAst?.yytext == "__partial_v_sum").leftAst!;
        Assert.IsTrue(IsNumericCast(shippedSum.rightAst), "the span sums CAST(n AS numeric)");
        Assert.IsTrue(plan.AvgFinalizers.Single().IntegerArgument);
        CollectionAssert.IsEmpty(plan.IntegerSumOutputs);
    }

    [Test]
    public async Task PartialSplit_OfAFloatSum_ShipsTheArgumentUnchanged()
    {
        PartialAggregatePlan sum = await SplitAsync("sum", "f");
        Assert.AreEqual(NodeType.Identifier, sum.ShipProjections.Single(p => p.nodeType == NodeType.ExprAlias).leftAst!.rightAst!.nodeType);
        CollectionAssert.IsEmpty(sum.IntegerSumOutputs);
        Assert.IsFalse(sum.NeedsFinalization);

        PartialAggregatePlan average = await SplitAsync("avg", "f");
        Assert.IsFalse(average.AvgFinalizers.Single().IntegerArgument);
    }

    [Test]
    public void FinalizePartials_ChecksOnlyTheMergedIntegerTotal()
    {
        PartialAggregatePlan plan = new()
        {
            ShipProjections = [],
            MergeProjections = [],
            IntegerSumOutputs = ["s"],
            AvgFinalizers = [new PartialAvgFinalizer("a", "a_sum", "a_cnt", IntegerArgument: true)],
        };

        Int128 Units(Int128 whole) => whole * NumericMath.ScaleFactor;

        QueryResultRow Merged(Int128 sum, Int128 avgSum, long count) => new(default, new Dictionary<string, ColumnValue>
        {
            ["s"] = ColumnValue.FromNumeric(Units(sum)),
            ["a_sum"] = ColumnValue.FromNumeric(Units(avgSum)),
            ["a_cnt"] = new(ColumnType.Integer64, count),
        });

        // Two spans of max each: the SUM is out of range, the AVG is max.
        Int128 twoMax = 2 * (Int128)long.MaxValue;
        CamusDBException ex = Assert.Throws<CamusDBException>(() => QueryExecutor.FinalizePartials(Merged(twoMax, twoMax, 2), plan))!;
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, ex.Code, ex.Message);

        // A total at the limit converts back to Integer64, and the AVG is the exact total over the count.
        QueryResultRow finalized = QueryExecutor.FinalizePartials(Merged(long.MinValue, twoMax, 2), plan);
        Assert.AreEqual(ColumnType.Integer64, finalized.Row["s"].Type);
        Assert.AreEqual(long.MinValue, finalized.Row["s"].LongValue);
        Assert.AreEqual(ColumnType.Float64, finalized.Row["a"].Type);
        Assert.AreEqual((double)long.MaxValue, finalized.Row["a"].FloatValue);
        Assert.IsFalse(finalized.Row.ContainsKey("a_sum"));

        // A group with no non-NULL value keeps its NULL SUM.
        QueryResultRow empty = QueryExecutor.FinalizePartials(new QueryResultRow(default, new Dictionary<string, ColumnValue>
        {
            ["s"] = ColumnValue.Null,
            ["a_sum"] = ColumnValue.Null,
            ["a_cnt"] = new(ColumnType.Integer64, 0L),
        }), plan);
        Assert.AreEqual(ColumnType.Null, empty.Row["s"].Type);
        Assert.AreEqual(ColumnType.Null, empty.Row["a"].Type);
    }
}
