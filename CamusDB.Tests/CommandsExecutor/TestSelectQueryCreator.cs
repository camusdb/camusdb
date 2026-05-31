
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

public class TestSelectQueryCreator
{
    private static SelectQuery ParseSelectQuery(string sql)
    {
        SelectQueryCreator selectQueryCreator = new();
        return selectQueryCreator.CreateSelectQuery(SQLParserProcessor.Parse(sql));
    }

    [Test]
    public void CreateSelectQuery_SimpleProjectionAndTable()
    {
        SelectQuery query = ParseSelectQuery("SELECT id FROM robots");

        TableSource source = AssertTableSource(query);
        Assert.AreEqual("robots", source.TableName);
        Assert.IsNull(source.Alias);
        Assert.IsNull(source.ForcedIndexName);

        Assert.AreEqual(1, query.Projections.Count);
        Assert.AreEqual(NodeType.Identifier, query.Projections[0].Expression.nodeType);
        Assert.AreEqual("id", query.Projections[0].Expression.yytext);
        Assert.AreEqual("id", query.Projections[0].OutputName);

        Assert.IsNull(query.Where);
        Assert.IsNull(query.GroupBy);
        Assert.IsNull(query.OrderBy);
        Assert.IsNull(query.Limit);
        Assert.IsNull(query.Offset);
    }

    [Test]
    public void CreateSelectQuery_MultiColumnProjection()
    {
        SelectQuery query = ParseSelectQuery("SELECT id, name FROM robots");

        Assert.AreEqual(2, query.Projections.Count);
        Assert.AreEqual("id", query.Projections[0].OutputName);
        Assert.AreEqual("name", query.Projections[1].OutputName);
    }

    [Test]
    public void CreateSelectQuery_StarProjection()
    {
        SelectQuery query = ParseSelectQuery("SELECT * FROM robots");

        Assert.AreEqual(1, query.Projections.Count);
        Assert.AreEqual(NodeType.ExprAllFields, query.Projections[0].Expression.nodeType);
        Assert.IsNull(query.Projections[0].OutputName);
    }

    [Test]
    public void CreateSelectQuery_WhereClause()
    {
        SelectQuery query = ParseSelectQuery("SELECT * FROM robots WHERE year = 2000");

        Assert.IsNotNull(query.Where);
        Assert.AreEqual(NodeType.ExprEquals, query.Where!.Expression.nodeType);
    }

    [Test]
    public void CreateSelectQuery_OrderBySingleColumn()
    {
        SelectQuery query = ParseSelectQuery("SELECT * FROM robots ORDER BY year");

        Assert.IsNotNull(query.OrderBy);
        Assert.AreEqual(1, query.OrderBy!.Count);
        Assert.AreEqual(OrderType.Ascending, query.OrderBy[0].Direction);
        Assert.AreEqual("year", query.OrderBy[0].Expression.yytext);
    }

    [Test]
    public void CreateSelectQuery_OrderByMultipleColumnsWithDirections()
    {
        SelectQuery query = ParseSelectQuery("SELECT * FROM robots ORDER BY year ASC, name DESC");

        Assert.IsNotNull(query.OrderBy);
        Assert.AreEqual(2, query.OrderBy!.Count);
        Assert.AreEqual(OrderType.Ascending, query.OrderBy[0].Direction);
        Assert.AreEqual("year", query.OrderBy[0].Expression.yytext);
        Assert.AreEqual(OrderType.Descending, query.OrderBy[1].Direction);
        Assert.AreEqual("name", query.OrderBy[1].Expression.yytext);
    }

    [Test]
    public void CreateSelectQuery_Limit()
    {
        SelectQuery query = ParseSelectQuery("SELECT * FROM robots LIMIT 5");

        Assert.IsNotNull(query.Limit);
        Assert.IsNull(query.Offset);
    }

    [Test]
    public void CreateSelectQuery_LimitOffset()
    {
        SelectQuery query = ParseSelectQuery("SELECT * FROM robots LIMIT 5 OFFSET 2");

        Assert.IsNotNull(query.Limit);
        Assert.IsNotNull(query.Offset);
    }

    [Test]
    public void CreateSelectQuery_FullClauseCombination()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT id, name FROM robots WHERE year >= 2020 ORDER BY year DESC LIMIT 5 OFFSET 1");

        Assert.AreEqual(2, query.Projections.Count);
        Assert.IsNotNull(query.Where);
        Assert.IsNotNull(query.OrderBy);
        Assert.AreEqual(1, query.OrderBy!.Count);
        Assert.AreEqual(OrderType.Descending, query.OrderBy[0].Direction);
        Assert.IsNotNull(query.Limit);
        Assert.IsNotNull(query.Offset);
    }

    [Test]
    public void CreateSelectQuery_ForcedIndexHint()
    {
        SelectQuery query = ParseSelectQuery("SELECT id FROM robots@{FORCE_INDEX=year_idx}");

        TableSource source = AssertTableSource(query);
        Assert.AreEqual("year_idx", source.ForcedIndexName);
    }

    [Test]
    public void CreateSelectQuery_ForcedPrimaryKeyIndexHint()
    {
        SelectQuery query = ParseSelectQuery("SELECT id FROM robots@{FORCE_INDEX=pk}");

        TableSource source = AssertTableSource(query);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, source.ForcedIndexName);
    }

    [Test]
    public void CreateSelectQuery_ProjectionAlias()
    {
        SelectQuery query = ParseSelectQuery("SELECT SUM(year) AS totalyear FROM robots");

        Assert.AreEqual(1, query.Projections.Count);
        Assert.AreEqual(NodeType.ExprAlias, query.Projections[0].Expression.nodeType);
        Assert.AreEqual("totalyear", query.Projections[0].OutputName);
    }

    [Test]
    public void CreateSelectQuery_GroupBySingleColumn()
    {
        SelectQuery query = ParseSelectQuery("SELECT name, COUNT(*) FROM robots GROUP BY name");

        Assert.IsNotNull(query.GroupBy);
        Assert.AreEqual(1, query.GroupBy!.Count);
        Assert.AreEqual(NodeType.Identifier, query.GroupBy[0].nodeType);
        Assert.AreEqual("name", query.GroupBy[0].yytext);
    }

    [Test]
    public void CreateSelectQuery_GroupByMultipleColumns()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT name, enabled, COUNT(*) FROM robots GROUP BY name, enabled");

        Assert.IsNotNull(query.GroupBy);
        Assert.AreEqual(2, query.GroupBy!.Count);
        Assert.AreEqual("name", query.GroupBy[0].yytext);
        Assert.AreEqual("enabled", query.GroupBy[1].yytext);
    }

    [Test]
    public void CreateSelectQuery_GroupByExpression()
    {
        SelectQuery query = ParseSelectQuery("SELECT year + 100 AS y FROM robots GROUP BY year + 100");

        Assert.IsNotNull(query.GroupBy);
        Assert.AreEqual(1, query.GroupBy!.Count);
        Assert.AreEqual(NodeType.ExprAdd, query.GroupBy[0].nodeType);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsGroupBy()
    {
        SelectQuery query = ParseSelectQuery("SELECT name, COUNT(*) FROM robots GROUP BY name");

        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "select-query-creator-test");
        ExecuteSQLTicket ticket = new(txnState: txn, database: "db", sql: "", parameters: null);

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.IsNotNull(legacyTicket.GroupBy);
        Assert.AreEqual(1, legacyTicket.GroupBy!.Count);
        Assert.AreEqual("name", legacyTicket.GroupBy[0].yytext);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsGroupByOrderByAggregateAlias()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY cnt");

        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "select-query-creator-test");
        ExecuteSQLTicket ticket = new(txnState: txn, database: "db", sql: "", parameters: null);

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.IsNotNull(legacyTicket.OrderBy);
        Assert.AreEqual(1, legacyTicket.OrderBy!.Count);
        Assert.AreEqual("cnt", legacyTicket.OrderBy[0].ColumnName);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsGroupByOrderByAggregateExpression()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY COUNT(*) DESC");

        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "select-query-creator-test");
        ExecuteSQLTicket ticket = new(txnState: txn, database: "db", sql: "", parameters: null);

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.IsNotNull(legacyTicket.OrderBy);
        Assert.AreEqual(1, legacyTicket.OrderBy!.Count);
        Assert.AreEqual("cnt", legacyTicket.OrderBy[0].ColumnName);
        Assert.AreEqual(OrderType.Descending, legacyTicket.OrderBy[0].Type);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsGroupByOrderByGroupColumnNotInSelect()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY name");

        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "select-query-creator-test");
        ExecuteSQLTicket ticket = new(txnState: txn, database: "db", sql: "", parameters: null);

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.IsNotNull(legacyTicket.OrderBy);
        Assert.AreEqual(1, legacyTicket.OrderBy!.Count);
        Assert.AreEqual("name", legacyTicket.OrderBy[0].ColumnName);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsLegacyShape()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT id, name FROM robots WHERE year = 2000 ORDER BY year LIMIT 1 OFFSET 0");

        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "select-query-creator-test");
        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: "db",
            sql: "",
            parameters: new() { { "@year", new ColumnValue(ColumnType.Integer64, 2000) } });

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.AreEqual("db", legacyTicket.DatabaseName);
        Assert.AreEqual("robots", legacyTicket.TableName);
        Assert.IsNull(legacyTicket.IndexName);
        Assert.AreEqual(2, legacyTicket.Projection!.Count);
        Assert.IsNull(legacyTicket.Filters);
        Assert.AreEqual(NodeType.ExprEquals, legacyTicket.Where!.nodeType);
        Assert.AreEqual(1, legacyTicket.OrderBy!.Count);
        Assert.AreEqual("year", legacyTicket.OrderBy[0].ColumnName);
        Assert.IsNotNull(legacyTicket.Limit);
        Assert.IsNotNull(legacyTicket.Offset);
        Assert.AreSame(ticket.Parameters, legacyTicket.Parameters);
    }

    [Test]
    public void CreateSelectQuery_TableAliasWithAs()
    {
        SelectQuery query = ParseSelectQuery("SELECT id FROM robots AS r");

        TableSource source = AssertTableSource(query);
        Assert.AreEqual("robots", source.TableName);
        Assert.AreEqual("r", source.Alias);
    }

    [Test]
    public void CreateSelectQuery_TableAliasWithoutAs()
    {
        SelectQuery query = ParseSelectQuery("SELECT id FROM robots r");

        TableSource source = AssertTableSource(query);
        Assert.AreEqual("robots", source.TableName);
        Assert.AreEqual("r", source.Alias);
    }

    [Test]
    public void CreateSelectQuery_InnerJoin()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        Assert.IsInstanceOf<JoinSource>(query.Source);
        JoinSource join = (JoinSource)query.Source;

        Assert.AreEqual(JoinKind.Inner, join.Kind);
        Assert.AreEqual(NodeType.ExprEquals, join.OnPredicate.nodeType);
        Assert.AreEqual("p.user_id", join.OnPredicate.leftAst!.yytext);
        Assert.AreEqual("u.id", join.OnPredicate.rightAst!.yytext);

        Assert.IsInstanceOf<TableSource>(join.Left);
        Assert.IsInstanceOf<TableSource>(join.Right);
        TableSource left = (TableSource)join.Left;
        TableSource right = (TableSource)join.Right;

        Assert.AreEqual("app_users", left.TableName);
        Assert.AreEqual("u", left.Alias);
        Assert.AreEqual("posts", right.TableName);
        Assert.AreEqual("p", right.Alias);

        Assert.AreEqual("u.email", query.Projections[0].Expression.yytext);
        Assert.AreEqual("p.title", query.Projections[1].Expression.yytext);
    }

    [Test]
    public void CreateSelectQuery_JoinAggregatedDerivedTable()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT u.email, d.post_count FROM app_users u "
            + "JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "ON d.user_id = u.id ORDER BY u.email");

        Assert.IsInstanceOf<JoinSource>(query.Source);
        JoinSource join = (JoinSource)query.Source;

        Assert.IsInstanceOf<TableSource>(join.Left);
        Assert.IsInstanceOf<DerivedTableSource>(join.Right);

        DerivedTableSource derived = (DerivedTableSource)join.Right;
        Assert.AreEqual("d", derived.Alias);
        Assert.IsInstanceOf<TableSource>(derived.Query.Source);
        Assert.AreEqual("posts", ((TableSource)derived.Query.Source).TableName);
        Assert.AreEqual(2, derived.Query.Projections.Count);
        Assert.IsNotNull(derived.Query.GroupBy);
        Assert.AreEqual(1, derived.Query.GroupBy!.Count);
    }

    [Test]
    public void CreateSelectQuery_CommaJoinNormalizesToLeftDeepJoinTree()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT r.id, u.amount FROM robots r, user_robots u WHERE r.id = u.robots_id");

        Assert.IsInstanceOf<JoinSource>(query.Source);
        JoinSource join = (JoinSource)query.Source;

        Assert.IsInstanceOf<TableSource>(join.Left);
        Assert.IsInstanceOf<TableSource>(join.Right);
        Assert.AreEqual("r", ((TableSource)join.Left).Alias);
        Assert.AreEqual("u", ((TableSource)join.Right).Alias);
        Assert.AreEqual(NodeType.ExprEquals, join.OnPredicate.nodeType);
        Assert.AreEqual("r.id", join.OnPredicate.leftAst!.yytext);
        Assert.AreEqual("u.robots_id", join.OnPredicate.rightAst!.yytext);
        Assert.IsNull(query.Where);
    }

    [Test]
    public void CreateSelectQuery_CommaJoinLeavesResidualSingleSourceFilterInWhere()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT r.id, u.amount FROM robots r, user_robots u "
            + "WHERE r.id = u.robots_id AND u.amount > 10");

        JoinSource join = (JoinSource)query.Source;
        Assert.AreEqual(NodeType.ExprEquals, join.OnPredicate.nodeType);
        Assert.IsNotNull(query.Where);
        Assert.AreEqual(NodeType.ExprGreaterThan, query.Where!.Expression.nodeType);
    }

    [Test]
    public void CreateSelectQuery_CommaJoinThreeSourcesBuildsNestedJoinTree()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT u.email, p.title, u2.email FROM app_users u, posts p, app_users u2 "
            + "WHERE p.user_id = u.id AND u2.id = p.user_id");

        JoinSource outerJoin = (JoinSource)query.Source;
        Assert.IsInstanceOf<JoinSource>(outerJoin.Left);
        JoinSource innerJoin = (JoinSource)outerJoin.Left!;
        Assert.AreEqual("u2", ((TableSource)outerJoin.Right).Alias);
        Assert.AreEqual("p", ((TableSource)innerJoin.Right).Alias);
        Assert.AreEqual("u", ((TableSource)innerJoin.Left).Alias);
        Assert.AreEqual("p.user_id", innerJoin.OnPredicate.leftAst!.yytext);
        Assert.AreEqual("u.id", innerJoin.OnPredicate.rightAst!.yytext);
        Assert.AreEqual("u2.id", outerJoin.OnPredicate.leftAst!.yytext);
        Assert.AreEqual("p.user_id", outerJoin.OnPredicate.rightAst!.yytext);
        Assert.IsNull(query.Where);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsDerivedTableOnlySource()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d");

        ExecuteSQLTicket ticket = new(
            txnState: null!,
            database: "db",
            sql: "",
            parameters: null);

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.AreEqual("posts", legacyTicket.TableName);
        Assert.AreEqual(1, legacyTicket.Projection!.Count);
    }

    [Test]
    public void QueryTicketAdapter_RoundTripsDerivedTableJoinSource()
    {
        SelectQuery query = ParseSelectQuery(
            "SELECT u.email, d.post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "JOIN app_users u ON d.user_id = u.id");

        ExecuteSQLTicket ticket = new(
            txnState: null!,
            database: "db",
            sql: "",
            parameters: null);

        QueryTicket legacyTicket = QueryTicketAdapter.ToQueryTicket(query, ticket);

        Assert.AreEqual("posts", legacyTicket.TableName);
    }

    private static TableSource AssertTableSource(SelectQuery query)
    {
        Assert.IsInstanceOf<TableSource>(query.Source);
        return (TableSource)query.Source;
    }
}
