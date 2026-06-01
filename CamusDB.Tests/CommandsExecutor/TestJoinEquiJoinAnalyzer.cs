
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
[NonParallelizable]
public sealed class TestJoinEquiJoinAnalyzer
{
    [Test]
    public void TryMatch_FindsMultiIndexOnRightJoinColumn()
    {
        BoundSelectQuery bound = MakeBoundQuery(
            ("app_users", "u", new[] { ("id", ColumnType.Id) }),
            ("posts", "p", new[] { ("id", ColumnType.Id), ("user_id", ColumnType.Id) }, IndexType.Multi, "user_id"));

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id"));

        Assert.IsTrue(
            JoinEquiJoinAnalyzer.TryMatch(
                bound.Sources[1],
                ((JoinSource)query.Source).OnPredicate,
                bound,
                out JoinEquiJoinIndexMatch match));

        Assert.AreEqual("user_id", match.RightIndexColumn);
        Assert.AreEqual("u.id", match.LeftLookupColumn);
        Assert.AreEqual(IndexType.Multi, match.Index.Type);
        Assert.IsFalse(match.UseUniqueLookup);
    }

    [Test]
    public void TryMatch_FindsUniqueIndexOnRightJoinColumn()
    {
        BoundSelectQuery bound = MakeBoundQuery(
            ("app_users", "u", new[] { ("id", ColumnType.Id) }),
            ("posts", "p", new[] { ("id", ColumnType.Id), ("user_id", ColumnType.Id) }, IndexType.Unique, "user_id"));

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON u.id = p.user_id"));

        Assert.IsTrue(
            JoinEquiJoinAnalyzer.TryMatch(
                bound.Sources[1],
                ((JoinSource)query.Source).OnPredicate,
                bound,
                out JoinEquiJoinIndexMatch match));

        Assert.AreEqual("user_id", match.RightIndexColumn);
        Assert.AreEqual("u.id", match.LeftLookupColumn);
        Assert.IsTrue(match.UseUniqueLookup);
    }

    [Test]
    public void TryMatch_ReturnsFalseWhenRightJoinColumnIsNotIndexed()
    {
        BoundSelectQuery bound = MakeBoundQuery(
            ("app_users", "u", new[] { ("id", ColumnType.Id) }),
            ("posts", "p", new[] { ("id", ColumnType.Id), ("user_id", ColumnType.Id) }));

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id"));

        Assert.IsFalse(
            JoinEquiJoinAnalyzer.TryMatch(
                bound.Sources[1],
                ((JoinSource)query.Source).OnPredicate,
                bound,
                out _));
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

    private static BoundSelectQuery MakeBoundQuery(
        (string table, string alias, (string name, ColumnType type)[] columns) left,
        (string table, string alias, (string name, ColumnType type)[] columns, IndexType indexType, string indexColumn) right)
    {
        List<BoundTableSource> boundSources =
        [
            MakeBoundSource(left.table, left.alias, left.columns),
            MakeBoundSource(right.table, right.alias, right.columns, right.indexType, right.indexColumn),
        ];

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(
            SQLParserProcessor.Parse(
                "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id"));

        return new BoundSelectQuery(query, boundSources, new QueryRowNameResolver(boundSources));
    }

    private static BoundTableSource MakeBoundSource(
        string tableName,
        string alias,
        (string name, ColumnType type)[] columns,
        IndexType? indexType = null,
        string? indexColumn = null)
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

        if (indexType is not null && indexColumn is not null)
        {
            table.Indexes["posts_user_id_idx"] = new TableIndexSchema(
                "posts_user_id_idx",
                [indexColumn],
                indexType.Value);
        }

        return new BoundTableSource(new TableSource(tableName, alias), table, alias);
    }
}
