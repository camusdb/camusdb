/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Aggregate extension functions: global and grouped use, HAVING, derived tables, INSERT … SELECT,
/// compound projections, the NULL and empty-group rules, the array result type, errors, the CHECK,
/// DEFAULT and WHERE refusals, a spilling GROUP BY against a non-spilling one, and the rule that the
/// partial-aggregation split never ships an extension aggregate.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionAggregates : ExtensionTestBase
{
    private static async Task Seed(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE nums (id INT64 PRIMARY KEY, grp STRING, x INT64, name STRING)");
        await ExecNonQuery(executor, db,
            "INSERT INTO nums (id, grp, x, name) VALUES " +
            "(1, 'a', 2, 'p'), (2, 'a', 8, 'q'), (3, 'a', NULL, 'r'), " +
            "(4, 'b', 3, 's'), (5, 'b', 3, 't'), (6, 'b', 3, 'u'), " +
            "(7, 'c', NULL, 'v')");
    }

    [Test]
    public async Task Global_Grouped_Having_AndCompound()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await Seed(executor, db);

        Assert.AreEqual(Math.Pow(2.0 * 8 * 3 * 3 * 3, 1.0 / 5), (await Scalar(executor, db, "SELECT rx_geomean(x) AS g FROM nums")).FloatValue, 1e-9,
            "a global aggregate; NULL rows are skipped for a non-nullable parameter");

        List<Dictionary<string, ColumnValue>> grouped = await ExecQuery(executor, db,
            "SELECT grp, rx_geomean(x) AS g, SUM(x) AS s, rx_count_nulls(x) AS n FROM nums GROUP BY grp ORDER BY grp");

        Assert.AreEqual(3, grouped.Count);
        Assert.AreEqual(4.0, grouped[0]["g"].FloatValue, 1e-9);
        Assert.AreEqual(10, grouped[0]["s"].LongValue, "a built-in beside an extension aggregate keeps its own result");
        Assert.AreEqual("1/3", grouped[0]["n"].StrValue, "a nullable parameter receives the NULL rows");
        Assert.AreEqual(3.0, grouped[1]["g"].FloatValue, 1e-9);
        Assert.AreEqual(ColumnType.Null, grouped[2]["g"].Type, "a group whose rows are all skipped finishes an empty state");
        Assert.AreEqual("1/1", grouped[2]["n"].StrValue);

        List<Dictionary<string, ColumnValue>> having = await ExecQuery(executor, db,
            "SELECT grp FROM nums GROUP BY grp HAVING rx_geomean(x) > 3.5");
        CollectionAssert.AreEqual(new[] { "a" }, having.Select(r => r["grp"].StrValue));

        Assert.AreEqual(8.0, (await Scalar(executor, db, "SELECT rx_geomean(x) * 2 AS g2 FROM nums WHERE grp = 'a'")).FloatValue, 1e-9,
            "a compound projection over an extension aggregate");

        Assert.AreEqual("p-q-r", (await Scalar(executor, db, "SELECT rx_concat(name, '-') AS c FROM nums WHERE grp = 'a'")).StrValue,
            "a two-argument aggregate");
    }

    [Test]
    public async Task DerivedTable_InsertSelect_AndArrayResult()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await Seed(executor, db);

        List<Dictionary<string, ColumnValue>> derived = await ExecQuery(executor, db,
            "SELECT d.grp AS grp FROM (SELECT grp, rx_geomean(x) AS g FROM nums GROUP BY grp) AS d WHERE d.g > 3.5");
        CollectionAssert.AreEqual(new[] { "a" }, derived.Select(r => r["grp"].StrValue));

        await ExecDdl(executor, db, "CREATE TABLE summary (grp STRING PRIMARY KEY, joined STRING, g FLOAT64)");
        await ExecNonQuery(executor, db, "INSERT INTO summary (grp, joined, g) SELECT grp, rx_concat(name, '+'), rx_geomean(x) FROM nums GROUP BY grp");

        List<Dictionary<string, ColumnValue>> summary = await ExecQuery(executor, db, "SELECT grp, joined, g FROM summary ORDER BY grp");
        CollectionAssert.AreEqual(new[] { "p+q+r", "s+t+u", "v" }, summary.Select(r => r["joined"].StrValue));
        Assert.AreEqual(4.0, summary[0]["g"].FloatValue, 1e-9);

        // An array result: rx_collect returns IReadOnlyList<long>.
        List<Dictionary<string, ColumnValue>> collected = await ExecQuery(executor, db, "SELECT grp, rx_collect(x) AS c FROM nums GROUP BY grp ORDER BY grp");
        CollectionAssert.AreEqual(new long[] { 2, 8 }, collected[0]["c"].ArrayValues!.Select(v => v.LongValue));
        Assert.AreEqual(0, collected[2]["c"].ArrayValues!.Count);

        await ExecDdl(executor, db, "CREATE TABLE collected AS SELECT grp, rx_collect(x) AS c FROM nums GROUP BY grp");
        TableColumnSchema column = (await executor.OpenDatabase(db)).Schema.Tables["collected"].Columns!.Single(c => c.Name == "c");
        Assert.AreEqual(ColumnType.Array, column.Type);
        Assert.AreEqual(ColumnType.Integer64, column.ArrayElementType, "the Finish result's element type types the column");

        TableColumnSchema geo = await CtasColumn(executor, db, "CREATE TABLE geo AS SELECT grp, rx_geomean(x) AS g FROM nums GROUP BY grp", "geo", "g");
        Assert.AreEqual(ColumnType.Float64, geo.Type, "the Finish return type types the column");
    }

    [Test]
    public async Task OrderBy_Join_AndSubquery()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await Seed(executor, db);

        List<Dictionary<string, ColumnValue>> ordered = await ExecQuery(executor, db,
            "SELECT grp, rx_geomean(x) AS g FROM nums WHERE x IS NOT NULL GROUP BY grp ORDER BY rx_geomean(x) DESC");
        CollectionAssert.AreEqual(new[] { "a", "b" }, ordered.Select(r => r["grp"].StrValue), "ORDER BY the aggregate itself");

        List<Dictionary<string, ColumnValue>> byAlias = await ExecQuery(executor, db,
            "SELECT grp, rx_geomean(x) AS g FROM nums WHERE x IS NOT NULL GROUP BY grp ORDER BY g");
        CollectionAssert.AreEqual(new[] { "b", "a" }, byAlias.Select(r => r["grp"].StrValue), "ORDER BY the alias of the aggregate");

        List<Dictionary<string, ColumnValue>> joined = await ExecQuery(executor, db,
            "SELECT n.grp AS grp, rx_concat(m.name, ',') AS c FROM nums n JOIN nums m ON m.id = n.id GROUP BY n.grp ORDER BY n.grp");
        CollectionAssert.AreEqual(new[] { "p,q,r", "s,t,u", "v" }, joined.Select(r => r["c"].StrValue), "an aggregate over a join");

        Assert.AreEqual(4.0, (await Scalar(executor, db, "SELECT (SELECT rx_geomean(x) FROM nums WHERE grp = 'a') AS g")).FloatValue, 1e-9,
            "an aggregate in a scalar subquery");
    }

    private static async Task<TableColumnSchema> CtasColumn(CommandExecutor executor, string db, string sql, string table, string column)
    {
        await ExecDdl(executor, db, sql);
        return (await executor.OpenDatabase(db)).Schema.Tables[table].Columns!.Single(c => c.Name == column);
    }

    [Test]
    public async Task ZeroRows_CreateThenFinish()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await Seed(executor, db);

        Assert.AreEqual("rows:0", (await Scalar(executor, db, "SELECT rx_rows(x) AS r FROM nums WHERE id > 100")).StrValue,
            "a global aggregate over zero rows creates a state and finishes it");
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT rx_geomean(x) AS g FROM nums WHERE id > 100")).Type);
        Assert.AreEqual(0, (await ExecQuery(executor, db, "SELECT grp, rx_rows(x) AS r FROM nums WHERE id > 100 GROUP BY grp")).Count,
            "GROUP BY over zero rows has no group");

        Assert.AreEqual("rows:5", (await Scalar(executor, db, "SELECT rx_rows(x) AS r FROM nums")).StrValue,
            "a NULL argument for a non-nullable parameter skips the row");
    }

    [Test]
    public async Task Errors_NameTheAggregate()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await Seed(executor, db);
        await ExecNonQuery(executor, db, "INSERT INTO nums (id, grp, x, name) VALUES (8, 'd', 13, 'w')");

        CamusDBException failed = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT grp, rx_fail_on13(x) AS f FROM nums GROUP BY grp"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, failed.Code);
        StringAssert.Contains("rx_fail_on13", failed.Message);
        StringAssert.Contains("unlucky 13", failed.Message);

        CamusDBException arity = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT rx_concat(name) AS c FROM nums"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, arity.Code);
        StringAssert.Contains("rx_concat", arity.Message);

        CamusDBException star = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT rx_rows(*) AS c FROM nums"))!;
        StringAssert.Contains("does not accept *", star.Message);

        CamusDBException where = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT id FROM nums WHERE rx_geomean(x) > 1"))!;
        StringAssert.Contains("rx_geomean", where.Message);
        StringAssert.Contains("not allowed", where.Message);
    }

    [Test]
    public async Task CheckAndDefault_RefuseAnExtensionAggregate()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        CamusDBException check = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, db, "CREATE TABLE c1 (id INT64 PRIMARY KEY, x FLOAT64, CHECK (rx_geomean(x) > 0))"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, check.Code);
        StringAssert.Contains("aggregate", check.Message);

        CamusDBException defaulted = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, db, "CREATE TABLE c2 (id INT64 PRIMARY KEY, x FLOAT64 DEFAULT (rx_geomean(1)))"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, defaulted.Code);
        StringAssert.Contains("aggregate", defaulted.Message);
    }

    [Test]
    public async Task SpillingGroupBy_GivesTheSameResult()
    {
        const string query = "SELECT grp, rx_collect(x) AS c, rx_count_nulls(x) AS n, rx_geomean(x) AS g FROM wide GROUP BY grp";

        async Task<Dictionary<string, (string Collected, string Nulls, double Geo)>> Run(CamusDBOptions options)
        {
            (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture(options);

            await ExecDdl(executor, db, "CREATE TABLE wide (id INT64 PRIMARY KEY, grp STRING, x INT64)");

            List<string> values = [];

            for (int i = 1; i <= 240; i++)
                values.Add($"({i}, 'g{i % 40}', {(i % 7 == 0 ? "NULL" : (i % 13 + 1).ToString())})");

            await ExecNonQuery(executor, db, "INSERT INTO wide (id, grp, x) VALUES " + string.Join(", ", values));

            return (await ExecQuery(executor, db, query)).ToDictionary(
                r => r["grp"].StrValue!,
                r => (string.Join(",", r["c"].ArrayValues!.Select(v => v.LongValue)), r["n"].StrValue!, r["g"].FloatValue));
        }

        // Two engines: the spill decision is fixed when an engine is built.
        Dictionary<string, (string, string, double)> inMemory = await Run(WithExtensions("Fixture.Rich"));
        Dictionary<string, (string, string, double)> spilled = await Run(WithExtensions("Fixture.Rich") with { SpillEnabled = true, ForceSpillThresholdRows = 4 });

        Assert.AreEqual(40, inMemory.Count);
        CollectionAssert.AreEquivalent(inMemory.Keys, spilled.Keys);

        foreach ((string group, (string collected, string nulls, double geo)) in inMemory)
        {
            Assert.AreEqual(collected, spilled[group].Item1, group);
            Assert.AreEqual(nulls, spilled[group].Item2, group);
            Assert.AreEqual(geo, spilled[group].Item3, 1e-9, group);
        }
    }

    [Test]
    public async Task Scope_View_AndShowExtensions()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Rich"));
        await Seed(executor, db);

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT rx_geomean(x) AS g FROM nums"))!;
        StringAssert.Contains("rx_geomean", missing.Message, "a loaded but not installed aggregate does not resolve");

        await ExecDdl(executor, db, "CREATE EXTENSION rich");
        Assert.AreEqual(4.0, (await Scalar(executor, db, "SELECT rich.rx_geomean(x) AS g FROM nums WHERE grp = 'a'")).FloatValue, 1e-9,
            "the qualified form resolves too");

        Dictionary<string, ColumnValue> rich = (await ExecQuery(executor, db, "SHOW EXTENSIONS")).Single(r => r["name"].StrValue == "rich");
        StringAssert.Contains("rx_geomean", rich["aggregates"].StrValue);
        StringAssert.Contains("rx_geomean(FLOAT64) -> FLOAT64", rich["signatures"].StrValue);
        StringAssert.Contains("rx_concat(STRING, STRING) -> STRING", rich["signatures"].StrValue);
        StringAssert.Contains("rx_collect(INT64) -> ARRAY(INT64)", rich["signatures"].StrValue);

        Assert.IsTrue((await executor.OpenDatabase(db)).Schema.Extensions["rich"].HasMember(ExtensionMemberKind.Aggregate, "rx_geomean"));

        await ExecDdl(executor, db, "CREATE VIEW geo AS SELECT grp, rx_geomean(x) AS g FROM nums GROUP BY grp");
        Assert.AreEqual(3, (await ExecQuery(executor, db, "SELECT grp, g FROM geo")).Count);

        CamusDBException drop = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "DROP EXTENSION rich"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionHasDependents, drop.Code);
        StringAssert.Contains("view geo", drop.Message);
    }

    [Test]
    public void Boot_RefusesAFunctionAndAnAggregateWithOneName()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => CreateCommandExecutor(WithExtensions("Fixture.CrossKindDuplicate")))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionDuplicateMember, ex.Code);
        StringAssert.Contains("ck_same", ex.Message);
    }

    /// <summary>
    /// The partial-aggregation split is what would ship an aggregate below a gather. It is called here
    /// directly, over a gather, for a built-in (which it splits) and for an extension aggregate (which it
    /// must not), so the test does not depend on a cluster choosing to fragment.
    /// </summary>
    [Test]
    public async Task PartialAggregation_NeverShipsAnExtensionAggregate()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithRichFixture();
        await Seed(executor, db);

        MethodInfo split = typeof(QueryPlanner).GetMethod("TryBuildPartialAggregatePlan", BindingFlags.NonPublic | BindingFlags.Static)!;
        GatherNode gather = new(new TablePlacement("ks", false, [new PlacementSpan(null, null, 1, 1, null, [], true, true)], 0));
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "nums"));
        KvTransaction tx = await database.Transactions.BeginAsync();

        object? Split(string function)
        {
            NodeAst call = new(NodeType.ExprFuncCall, Identifier(function), Identifier("x"), null, null, null, null, null, null);
            NodeAst projection = new(NodeType.ExprAlias, call, Identifier("v"), null, null, null, null, null, null);
            QueryTicket ticket = new(tx, db, "nums", null, [projection], null, null, null, null, null, null);
            return split.Invoke(null, [ticket, table, gather]);
        }

        Assert.IsNotNull(Split("max"), "the control: a built-in over a gather is split");
        Assert.IsNull(Split("rx_geomean"), "an extension aggregate runs above the gather");

        AggregateNode node = new(gather)
        {
            AggregateProjections = [new NodeAst(NodeType.ExprFuncCall, Identifier("rx_geomean"), Identifier("x"), null, null, null, null, null, null)],
        };
        Assert.IsFalse(node.IsDecomposableAggregate, "an extension aggregate is never decomposed into local plus merge");
        Assert.IsFalse(AggregateFunctionCatalog.IsPartialMergeable("rx_geomean"));
        Assert.IsFalse(AggregateFunctionCatalog.IsDecomposable("rx_geomean"));

        await database.Transactions.RollbackAsync(tx);
    }

    private static NodeAst Identifier(string name) => new(NodeType.Identifier, null, null, null, null, null, null, null, name);
}
