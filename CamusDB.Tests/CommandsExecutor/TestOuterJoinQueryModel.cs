/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// How each parsed join form becomes a <see cref="JoinSource"/>: a left join keeps its operands
/// with <see cref="JoinKind.LeftOuter"/>, a right join is the same kind with the operands swapped,
/// and a cross join is the comma form. The query shape and the predicate pushdown both read the
/// kind, so they are asserted here as well, without a node.
/// </summary>
[TestFixture]
public sealed class TestOuterJoinQueryModel
{
    private static SelectQuery Create(string sql) => new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));

    [Test]
    public void LeftJoin_ProducesLeftOuterSource_WithOperandsInPlace()
    {
        SelectQuery query = Create("SELECT u.email, p.title FROM app_users u LEFT JOIN posts p ON p.user_id = u.id");

        JoinSource join = (JoinSource)query.Source;
        Assert.AreEqual(JoinKind.LeftOuter, join.Kind);
        Assert.AreEqual("app_users", ((TableSource)join.Left).TableName);
        Assert.AreEqual("posts", ((TableSource)join.Right).TableName);
        Assert.AreEqual(NodeType.ExprEquals, join.OnPredicate.nodeType);
    }

    [Test]
    public void PlainJoin_StaysInner()
    {
        SelectQuery query = Create("SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id");
        Assert.AreEqual(JoinKind.Inner, ((JoinSource)query.Source).Kind);
    }

    [Test]
    public void RightJoin_IsLeftOuterWithOperandsSwapped()
    {
        SelectQuery query = Create("SELECT u.email, p.title FROM posts p RIGHT JOIN app_users u ON p.user_id = u.id");

        JoinSource join = (JoinSource)query.Source;
        Assert.AreEqual(JoinKind.LeftOuter, join.Kind);
        Assert.AreEqual("app_users", ((TableSource)join.Left).TableName, "the preserved (right-hand) table becomes the left input");
        Assert.AreEqual("posts", ((TableSource)join.Right).TableName);
    }

    [Test]
    public void RightJoin_WithDerivedLeftOperand_Swaps()
    {
        SelectQuery query = Create(
            "SELECT u.email, d.n FROM (SELECT user_id, COUNT(*) AS n FROM posts GROUP BY user_id) d RIGHT JOIN app_users u ON d.user_id = u.id");

        JoinSource join = (JoinSource)query.Source;
        Assert.AreEqual(JoinKind.LeftOuter, join.Kind);
        Assert.IsInstanceOf<TableSource>(join.Left);
        Assert.IsInstanceOf<DerivedTableSource>(join.Right);
    }

    [Test]
    public void RightJoin_AfterAnotherJoin_IsRefused()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Create(
            "SELECT a.id FROM a JOIN b ON b.a_id = a.id RIGHT JOIN c ON c.b_id = b.id"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code);
        StringAssert.Contains("LEFT JOIN", ex.Message);
    }

    [Test]
    public void CrossJoinOnlyTree_EqualsTheCommaForm()
    {
        const string where = " WHERE a.x = b.x AND b.y = c.y AND a.z = 1";
        SelectQuery cross = Create("SELECT a.id FROM a CROSS JOIN b CROSS JOIN c" + where);
        SelectQuery comma = Create("SELECT a.id FROM a, b, c" + where);

        Assert.AreEqual(QueryShapeComputer.Compute(comma), QueryShapeComputer.Compute(cross),
            "a FROM made of cross joins only must take the comma-join hoist and get the same shape");

        // The hoist moved the equi-join conjuncts into ON and left only the single-table conjunct.
        JoinSource top = (JoinSource)cross.Source;
        Assert.AreEqual(JoinKind.Inner, top.Kind);
        Assert.AreEqual("c", ((TableSource)top.Right).TableName);
        Assert.AreEqual(JoinKind.Inner, ((JoinSource)top.Left).Kind);
        Assert.AreEqual(NodeType.ExprEquals, top.OnPredicate.nodeType, "b.y = c.y hoisted into ON");
        Assert.IsNotNull(cross.Where);
        Assert.AreEqual(NodeType.ExprEquals, cross.Where!.Expression.nodeType, "only a.z = 1 remains in WHERE");
    }

    [Test]
    public void CrossJoinMixedWithOnJoins_IsInnerOnLiteralTrue()
    {
        SelectQuery query = Create("SELECT a.id FROM a JOIN b ON b.a_id = a.id CROSS JOIN c");

        JoinSource top = (JoinSource)query.Source;
        Assert.AreEqual(JoinKind.Inner, top.Kind);
        Assert.AreEqual("c", ((TableSource)top.Right).TableName);
        Assert.AreSame(NodeAst.True, top.OnPredicate, "the cross join carries the shared literal true as its ON");
        Assert.AreEqual(NodeType.ExprEquals, ((JoinSource)top.Left).OnPredicate.nodeType);
    }

    [Test]
    public void Shape_DiffersBetweenInnerAndLeftOuter()
    {
        string inner = QueryShapeComputer.Compute(Create("SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id"));
        string left = QueryShapeComputer.Compute(Create("SELECT u.email FROM app_users u LEFT JOIN posts p ON p.user_id = u.id"));

        Assert.AreNotEqual(inner, left, "a left and an inner variant of the same text must never share a plan-cache entry");
    }

    // ── predicate pushdown by preserved side ───────────────────────────────────

    [Test]
    public void Pushdown_PreservedSideConjunct_IsPushed_NullExtendedSideConjunct_StaysPostJoin()
    {
        BoundSelectQuery bound = Bind(
            "SELECT e.id, g.studio_id FROM environments e LEFT JOIN games g ON g.id = e.game_id",
            ("environments", "e", [("id", ColumnType.Integer64), ("game_id", ColumnType.Integer64)]),
            ("games", "g", [("id", ColumnType.Integer64), ("studio_id", ColumnType.Integer64)]));

        JoinPredicatePushdown.Result pushed = JoinPredicatePushdown.Analyze(bound, Where("e.id = 7"));
        Assert.IsNotNull(pushed.ScanFiltersByAlias["e"], "a conjunct on the preserved side is pushed to its scan");
        Assert.IsNull(pushed.ScanFiltersByAlias["g"]);
        Assert.IsNull(pushed.PostJoinFilter);

        JoinPredicatePushdown.Result kept = JoinPredicatePushdown.Analyze(bound, Where("g.studio_id = 5"));
        Assert.IsNull(kept.ScanFiltersByAlias["e"]);
        Assert.IsNull(kept.ScanFiltersByAlias["g"], "a conjunct on the null-extended side must not be pushed below the outer join");
        Assert.IsNotNull(kept.PostJoinFilter);
    }

    [Test]
    public void Pushdown_MixedChain_OnlyNullExtendedAliasStaysPostJoin()
    {
        BoundSelectQuery bound = Bind(
            "SELECT a.id FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.b_id = b.id JOIN d ON d.a_id = a.id",
            ("a", "a", [("id", ColumnType.Integer64), ("x", ColumnType.Integer64)]),
            ("b", "b", [("id", ColumnType.Integer64), ("a_id", ColumnType.Integer64)]),
            ("c", "c", [("id", ColumnType.Integer64), ("b_id", ColumnType.Integer64), ("y", ColumnType.Integer64)]),
            ("d", "d", [("id", ColumnType.Integer64), ("a_id", ColumnType.Integer64), ("z", ColumnType.Integer64)]));

        JoinPredicatePushdown.Result result = JoinPredicatePushdown.Analyze(bound, Where("a.x = 1 AND c.y = 2 AND d.z = 3"));

        Assert.IsNotNull(result.ScanFiltersByAlias["a"], "a sits on the preserved side of the only outer join");
        Assert.IsNull(result.ScanFiltersByAlias["b"]);
        Assert.IsNull(result.ScanFiltersByAlias["c"], "c is null-extended: its conjunct runs after the join");
        Assert.IsNotNull(result.ScanFiltersByAlias["d"], "d joins above the outer join on the preserved side");
        Assert.IsNotNull(result.PostJoinFilter);
    }

    [Test]
    public void Pushdown_InnerJoin_IsUnchanged()
    {
        BoundSelectQuery bound = Bind(
            "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id",
            ("app_users", "u", [("id", ColumnType.Id), ("role", ColumnType.String)]),
            ("posts", "p", [("id", ColumnType.Id), ("user_id", ColumnType.Id), ("published", ColumnType.Bool)]));

        JoinPredicatePushdown.Result result = JoinPredicatePushdown.Analyze(bound, Where("u.role = \"admin\" AND p.published = true"));

        Assert.IsNotNull(result.ScanFiltersByAlias["u"]);
        Assert.IsNotNull(result.ScanFiltersByAlias["p"]);
        Assert.IsNull(result.PostJoinFilter);
    }

    private static NodeAst Where(string predicate) =>
        SQLParserProcessor.Parse("SELECT 1 FROM t WHERE " + predicate).extendedOne!;

    private static BoundSelectQuery Bind(
        string sql,
        params (string table, string alias, (string name, ColumnType type)[] columns)[] sources)
    {
        List<BoundTableSource> boundSources = new(sources.Length);

        foreach ((string table, string alias, (string name, ColumnType type)[] columns) in sources)
        {
            List<TableColumnSchema> columnSchemas = new(columns.Length);
            for (int i = 0; i < columns.Length; i++)
                columnSchemas.Add(new TableColumnSchema($"col{i}", columns[i].name, columns[i].type, false, null));

            TableSchema schema = new() { Id = table, Name = table, Columns = columnSchemas, Version = 0 };
            TableDescriptor descriptor = new(schema.Id!, schema.Name!, schema, store: null!);
            boundSources.Add(new BoundTableSource(new TableSource(table, alias), descriptor, alias));
        }

        SelectQuery query = Create(sql);
        return new BoundSelectQuery(query, boundSources, new QueryRowNameResolver(boundSources));
    }
}
