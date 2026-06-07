
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
public sealed class TestJoinPredicatePushdown
{
    [Test]
    public void Analyze_PushesSingleTablePredicateToOneAlias()
    {
        BoundSelectQuery bound = MakeBoundQuery(
            ("app_users", "u", new[] { ("id", ColumnType.Id), ("role", ColumnType.String) }),
            ("posts", "p", new[] { ("id", ColumnType.Id), ("user_id", ColumnType.Id) }));

        NodeAst where = SQLParserProcessor.Parse(
            "SELECT 1 FROM app_users WHERE u.role = \"admin\"").extendedOne!;

        JoinPredicatePushdown.Result result = JoinPredicatePushdown.Analyze(bound, where);

        Assert.IsNotNull(result.ScanFiltersByAlias["u"]);
        Assert.IsNull(result.ScanFiltersByAlias["p"]);
        Assert.IsNull(result.PostJoinFilter);
    }

    [Test]
    public void Analyze_KeepsCrossTablePredicateAsPostJoinFilter()
    {
        BoundSelectQuery bound = MakeBoundQuery(
            ("app_users", "u", new[] { ("id", ColumnType.Id), ("role", ColumnType.String) }),
            ("posts", "p", new[] { ("id", ColumnType.Id), ("user_id", ColumnType.Id) }));

        NodeAst where = SQLParserProcessor.Parse(
            "SELECT 1 FROM app_users WHERE p.user_id = u.id").extendedOne!;

        JoinPredicatePushdown.Result result = JoinPredicatePushdown.Analyze(bound, where);

        Assert.IsNull(result.ScanFiltersByAlias["u"]);
        Assert.IsNull(result.ScanFiltersByAlias["p"]);
        Assert.IsNotNull(result.PostJoinFilter);
    }

    [Test]
    public void Analyze_SplitsMixedSingleTableAndCrossTablePredicates()
    {
        BoundSelectQuery bound = MakeBoundQuery(
            ("app_users", "u", new[] { ("id", ColumnType.Id), ("role", ColumnType.String) }),
            ("posts", "p", new[] { ("id", ColumnType.Id), ("user_id", ColumnType.Id), ("published", ColumnType.Bool) }));

        NodeAst where = SQLParserProcessor.Parse(
            "SELECT 1 FROM app_users WHERE u.role = \"admin\" AND p.published = true AND p.user_id = u.id").extendedOne!;

        JoinPredicatePushdown.Result result = JoinPredicatePushdown.Analyze(bound, where);

        Assert.IsNotNull(result.ScanFiltersByAlias["u"]);
        Assert.IsNotNull(result.ScanFiltersByAlias["p"]);
        Assert.IsNotNull(result.PostJoinFilter);
    }

    private static BoundSelectQuery MakeBoundQuery(
        params (string table, string alias, (string name, ColumnType type)[] columns)[] sources)
    {
        List<BoundTableSource> boundSources = new(sources.Length);

        foreach ((string table, string alias, (string name, ColumnType type)[] columns) in sources)
            boundSources.Add(MakeBoundSource(table, alias, columns));

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id"));

        return new BoundSelectQuery(query, boundSources, new QueryRowNameResolver(boundSources));
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
