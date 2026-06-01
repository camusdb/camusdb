
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

public class TestQueryBinder : BaseTest
{
    [Test]
    public void ResolveRowLookupKey_SingleTableUnqualified_returnsBareColumnName()
    {
        BoundTableSource users = MakeBoundSource("users", "users", ("id", ColumnType.Id), ("name", ColumnType.String));
        QueryRowNameResolver resolver = new([users]);

        Assert.AreEqual("id", resolver.ResolveRowLookupKey("id"));
    }

    [Test]
    public void ResolveRowLookupKey_SingleTableQualified_returnsBareColumnName()
    {
        BoundTableSource users = MakeBoundSource("users", "u", ("id", ColumnType.Id));
        QueryRowNameResolver resolver = new([users]);

        Assert.AreEqual("id", resolver.ResolveRowLookupKey("u.id"));
    }

    [Test]
    public void ResolveRowLookupKey_MultiSourceQualified_returnsQualifiedKey()
    {
        BoundTableSource users = MakeBoundSource("users", "u", ("id", ColumnType.Id), ("name", ColumnType.String));
        BoundTableSource posts = MakeBoundSource("posts", "p", ("id", ColumnType.Id), ("title", ColumnType.String));
        QueryRowNameResolver resolver = new([users, posts]);

        Assert.AreEqual("u.name", resolver.ResolveRowLookupKey("u.name"));
    }

    [Test]
    public void ValidateColumnReference_UnknownColumn_throws()
    {
        BoundTableSource users = MakeBoundSource("users", "users", ("id", ColumnType.Id));
        QueryRowNameResolver resolver = new([users]);

        CamusDBException exception = Assert.Throws<CamusDBException>(() => resolver.ValidateColumnReference("missing"))!;

        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, exception.Code);
        StringAssert.Contains("Unknown column", exception.Message);
    }

    [Test]
    public void ValidateColumnReference_UnknownAlias_throws()
    {
        BoundTableSource users = MakeBoundSource("users", "u", ("id", ColumnType.Id), ("email", ColumnType.String));
        BoundTableSource posts = MakeBoundSource("posts", "p", ("id", ColumnType.Id));
        QueryRowNameResolver resolver = new([users, posts]);

        CamusDBException exception = Assert.Throws<CamusDBException>(() => resolver.ValidateColumnReference("missing.email"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Unknown alias", exception.Message);
    }

    [Test]
    public void ValidateColumnReference_AmbiguousColumn_throws()
    {
        BoundTableSource users = MakeBoundSource("users", "u", ("id", ColumnType.Id));
        BoundTableSource posts = MakeBoundSource("posts", "p", ("id", ColumnType.Id));
        QueryRowNameResolver resolver = new([users, posts]);

        CamusDBException exception = Assert.Throws<CamusDBException>(() => resolver.ValidateColumnReference("id"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Ambiguous column", exception.Message);
    }

    [Test]
    public void EvalExpr_UsesRowNameResolverForAliasedSingleTableColumn()
    {
        BoundTableSource users = MakeBoundSource("users", "u", ("score", ColumnType.Integer64));
        QueryRowNameResolver resolver = new([users]);

        Dictionary<string, ColumnValue> row = new()
        {
            ["score"] = new(ColumnType.Integer64, 42)
        };

        NodeAst identifier = new(
            NodeType.Identifier,
            leftAst: null,
            rightAst: null,
            extendedOne: null,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: "u.score");

        ColumnValue value = SqlExecutor.EvalExpr(identifier, row, parameters: null, rowNameResolver: resolver);

        Assert.AreEqual(42, value.LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_UnknownColumnInProjection_throws()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQueryCreator creator = new();
        SelectQuery query = creator.CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT no_such FROM robots"));

        QueryBinder binder = new(new TableOpener(catalogs, logger));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await binder.BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, exception.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_ValidSingleTableQuery_opensTable()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQueryCreator creator = new();
        SelectQuery query = creator.CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT id, name FROM robots WHERE year > 2000"));

        QueryBinder binder = new(new TableOpener(catalogs, logger));
        BoundSelectQuery bound = await binder.BindAsync(database, query);

        Assert.AreEqual(1, bound.Sources.Count);
        Assert.AreEqual("robots", bound.Sources[0].Table.Name);
        Assert.AreEqual("robots", bound.Sources[0].Alias);
        Assert.AreEqual("id", bound.RowNames.ResolveRowLookupKey("id"));
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByWithAggregate_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name"));

        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, query);

        Assert.AreEqual(1, bound.Query.GroupBy!.Count);
        Assert.AreEqual("name", bound.Query.GroupBy[0].yytext);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByMultipleAggregates_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) AS cnt, SUM(year) AS total FROM robots GROUP BY name"));

        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, query);

        Assert.AreEqual(3, bound.Query.Projections.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByExpressionMatch_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT year + 100 AS y FROM robots GROUP BY year + 100"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByNonGroupedProjection_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT name, year FROM robots GROUP BY name"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("GROUP BY", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByUnknownColumn_throwsUnknownColumn()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT name FROM robots GROUP BY missing_col"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, exception.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GlobalAggregateWithoutGroupBy_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT COUNT(*) FROM robots"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByOrderByAggregateAlias_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY cnt"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByOrderByAggregateExpression_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY COUNT(*) DESC"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByOrderByUnknownAlias_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY missing_alias"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("ORDER BY", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByOrderByGroupColumnNotInSelect_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY name"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByOrderByUnlistedColumn_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT COUNT(*) AS cnt FROM robots GROUP BY name ORDER BY year"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("ORDER BY", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_MixedAggregateWithoutGroupBy_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT name, COUNT(*) FROM robots"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Aggregations cannot be accompanied", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByHavingAggregateAlias_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name HAVING cnt > 0"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByHavingGroupKey_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name FROM robots GROUP BY name HAVING name = 'R2D2'"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_AggregateOnlyHavingAlias_bindsSuccessfully()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT COUNT(*) AS x FROM robots HAVING x > 0"));

        await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_SelectDistinctWithGroupBy_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT DISTINCT name FROM robots GROUP BY name"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("DISTINCT", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_SelectDistinctWithAggregateProjection_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT DISTINCT COUNT(*) FROM robots"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("DISTINCT", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_HavingWithoutGroupByOrAggregate_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse("SELECT name FROM robots HAVING name = 'R2D2'"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("HAVING", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByHavingNonGroupedColumn_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) FROM robots GROUP BY name HAVING year = 2000"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("HAVING", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_GroupByHavingUnknownAlias_throwsInvalidInput()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, _) = await SetupExecutorWithRobotsTable();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT name, COUNT(*) AS cnt FROM robots GROUP BY name HAVING missing_alias > 0"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("HAVING", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinWithAliases_bindsSuccessfully()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id"));

        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, query);

        Assert.AreEqual(2, bound.Sources.Count);
        Assert.AreEqual("app_users", bound.Sources[0].Table.Name);
        Assert.AreEqual("u", bound.Sources[0].Alias);
        Assert.AreEqual("posts", bound.Sources[1].Table.Name);
        Assert.AreEqual("p", bound.Sources[1].Alias);
        Assert.AreEqual("u.email", bound.RowNames.ResolveRowLookupKey("u.email"));
        Assert.AreEqual("p.title", bound.RowNames.ResolveRowLookupKey("p.title"));
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinPreservesSourceOrder()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id JOIN app_users u2 ON u2.id = p.user_id"));

        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, query);

        Assert.AreEqual(3, bound.Sources.Count);
        Assert.AreEqual("u", bound.Sources[0].Alias);
        Assert.AreEqual("p", bound.Sources[1].Alias);
        Assert.AreEqual("u2", bound.Sources[2].Alias);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinDuplicateAlias_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts u ON p.user_id = u.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Duplicate alias", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinAmbiguousColumn_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT id FROM app_users u JOIN posts p ON p.user_id = u.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Ambiguous column", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinUnknownAlias_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT missing.email FROM app_users u JOIN posts p ON p.user_id = u.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Unknown alias", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinOnReferencesFutureTable_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = c.id JOIN app_users c ON c.id = p.user_id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Unknown alias 'c'", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_InnerJoinUnknownTable_throws()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN missing_table p ON p.user_id = u.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, exception.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_CommaJoinWithAliases_bindsSuccessfully()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email, p.title FROM app_users u, posts p WHERE p.user_id = u.id"));

        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, query);

        Assert.AreEqual(2, bound.Sources.Count);
        Assert.AreEqual("u.email", bound.RowNames.ResolveRowLookupKey("u.email"));
        Assert.IsNull(query.Where);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_CommaJoinWithoutRequiredAlias_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users, posts WHERE posts.user_id = app_users.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Unknown alias", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_CommaJoinDuplicateAlias_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u, posts u WHERE p.user_id = u.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Duplicate alias", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BindAsync_CommaJoinAmbiguousColumn_throwsInvalidInput()
    {
        (DatabaseDescriptor database, CatalogsManager catalogs) = await SetupUsersAndPostsTables();

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT id FROM app_users u, posts p WHERE p.user_id = u.id"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await new QueryBinder(new TableOpener(catalogs, logger)).BindAsync(database, query))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("Ambiguous column", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteSQLQuery_UnknownColumn_throwsAtBindTime()
    {
        (string dbname, DatabaseDescriptor database, _, CommandExecutor executor) = await SetupExecutorWithRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: "SELECT no_such FROM robots",
            parameters: null);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.ExecuteSQLQuery(ticket))!;

        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, exception.Code);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CatalogsManager catalogs, CommandExecutor executor)> SetupExecutorWithRobotsTable()
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        CommandExecutor executor = new(validator, catalogs, logger);

        string dbname = Guid.NewGuid().ToString("n");
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, catalogs, executor);
    }

    private async Task<(DatabaseDescriptor database, CatalogsManager catalogs)> SetupUsersAndPostsTables()
    {
        (_, DatabaseDescriptor database, CatalogsManager catalogs, CommandExecutor executor) = await SetupExecutorWithRobotsTable();

        string dbname = database.Name;
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "app_users",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
                new("role", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "posts",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("user_id", ColumnType.Id, notNull: true),
                new("title", ColumnType.String, notNull: true),
                new("published", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        return (database, catalogs);
    }

    private static BoundTableSource MakeBoundSource(
        string tableName,
        string alias,
        params (string name, ColumnType type)[] columns)
    {
        List<TableColumnSchema> columnSchemas = new(columns.Length);

        for (int i = 0; i < columns.Length; i++)
            columnSchemas.Add(new TableColumnSchema($"col{i}", columns[i].name, columns[i].type, false, null));

        TableSchema schema = new()
        {
            Id = tableName,
            Name = tableName,
            Columns = columnSchemas,
            Version = 0
        };

        TableDescriptor table = new(schema.Id!, schema.Name!, schema, store: null!);
        return new BoundTableSource(new TableSource(tableName, alias), table, alias);
    }
}
