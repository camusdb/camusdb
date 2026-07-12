/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Ascending/descending ordered indexes. Descending is honored for fixed-width column types: the
/// key is encoded with the column's direction so a forward-only scan yields descending order, and
/// the planner streams a matching <c>ORDER BY … DESC</c> without a sort. Variable-width types
/// (String/Id/Bytes) reject descending at DDL. These tests pin the persisted format, the DDL gate,
/// end-to-end query behavior, and the sort-elision plan shape.
/// </summary>
[NonParallelizable]
internal sealed class TestDescendingIndexOrder : BaseTest
{
    private const string TableName = "robots";

    // ── Persistence format ────────────────────────────────────────────────────────────────

    [Test]
    public void SerializationRoundTrip_PreservesColumnDirections()
    {
        TableIndexSchema index = new(
            id: "idx-1",
            name: "name_year_idx",
            columnIds: ["col-name", "col-year"],
            type: IndexType.Multi,
            state: SchemaElementState.Public,
            startOffset: null,
            columnDirections: [OrderType.Ascending, OrderType.Descending]
        );

        byte[] bytes = MetaJsonSerializer.Serialize(index, MetaJsonContext.Default.TableIndexSchema);
        TableIndexSchema decoded = MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.TableIndexSchema);

        Assert.IsNotNull(decoded.ColumnDirections);
        Assert.AreEqual(2, decoded.ColumnDirections!.Length);
        Assert.AreEqual(OrderType.Ascending, decoded.DirectionAt(0));
        Assert.AreEqual(OrderType.Descending, decoded.DirectionAt(1));
    }

    [Test]
    public void NullColumnDirections_DeserializeAsAllAscending()
    {
        TableIndexSchema index = new(
            id: "idx-1",
            name: "name_idx",
            columnIds: ["col-name"],
            type: IndexType.Multi,
            state: SchemaElementState.Public,
            startOffset: null
        );

        byte[] bytes = MetaJsonSerializer.Serialize(index, MetaJsonContext.Default.TableIndexSchema);
        TableIndexSchema decoded = MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.TableIndexSchema);

        Assert.IsNull(decoded.ColumnDirections);
        Assert.AreEqual(OrderType.Ascending, decoded.DirectionAt(0));
        Assert.AreEqual(OrderType.Ascending, decoded.DirectionAt(5));
    }

    // ── DDL gate: fixed-width DESC accepted, variable-width DESC rejected ────────────────────

    [Test]
    [NonParallelizable]
    public async Task FixedWidthDescendingIndex_IsAcceptedAndPersists()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRobotsTable();
        await ExecDDL(executor, dbname, "CREATE INDEX year_desc_idx ON robots (year DESC)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        TableIndexSchema? idx = db2.Schema.Tables[TableName].Indexes!.FirstOrDefault(ix => ix.Name == "year_desc_idx");
        Assert.IsNotNull(idx, "descending index on a fixed-width column must be created and persisted");
        Assert.IsNotNull(idx!.ColumnDirections, "descending direction must persist");
        Assert.AreEqual(OrderType.Descending, idx.DirectionAt(0));
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingStringIndex_ViaCreateIndexSql_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateRobotsTable();

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, "CREATE INDEX name_desc_idx ON robots (name DESC)", null))
        )!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("Descending"));
        Assert.That(ex.Message, Does.Contain("String"));
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingStringColumn_InInlineConstraint_Rejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        // A descending String index column is still unsupported (String is terminator-delimited);
        // Id, by contrast, is now fixed-width and accepts descending (see TestNativeIdEncoding).
        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "descidx",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "name_desc", new ColumnIndexInfo[] { new("name", OrderType.Descending) })
            },
            ifNotExists: false
        );

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(createTicket))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("Descending"));
    }

    // ── End-to-end query behavior ───────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task OrderByDescending_ReturnsRowsInDescendingOrder()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRobotsTable();
        await ExecDDL(executor, dbname, "CREATE INDEX year_desc_idx ON robots (year DESC)");
        await InsertYears(executor, dbname, new[] { 2001, 2010, 2005, 1999, 2020, 2008 });

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT year FROM robots ORDER BY year DESC");

        List<long> years = rows.Select(r => r.Row["year"].LongValue).ToList();
        CollectionAssert.AreEqual(new long[] { 2020, 2010, 2008, 2005, 2001, 1999 }, years);
    }

    /// <summary>
    /// The whole point of a descending index: a matching <c>ORDER BY … DESC</c> streams from the
    /// index with no in-memory sort node in the plan.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task OrderByDescending_ElidesSortWhenDescendingIndexMatches()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRobotsTable();
        await ExecDDL(executor, dbname, "CREATE INDEX year_desc_idx ON robots (year DESC)");
        await InsertYears(executor, dbname, new[] { 2001, 2010, 2005 });

        List<string> nodes = await ExplainNodes(executor, dbname, "SELECT year FROM robots ORDER BY year DESC");

        Assert.IsFalse(nodes.Contains("sort"),
            "ORDER BY year DESC must stream from the descending index with no sort node; plan nodes: "
                + string.Join(", ", nodes));
    }

    /// <summary>An ascending ORDER BY must NOT be served by a descending index (it would reverse the
    /// result); the planner keeps the sort, so the answer is still correctly ascending.</summary>
    [Test]
    [NonParallelizable]
    public async Task OrderByAscending_WithOnlyDescendingIndex_StillReturnsAscending()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRobotsTable();
        await ExecDDL(executor, dbname, "CREATE INDEX year_desc_idx ON robots (year DESC)");
        await InsertYears(executor, dbname, new[] { 2001, 2010, 2005 });

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT year FROM robots ORDER BY year ASC");
        List<long> years = rows.Select(r => r.Row["year"].LongValue).ToList();

        CollectionAssert.AreEqual(new long[] { 2001, 2005, 2010 }, years);
    }

    [Test]
    [NonParallelizable]
    public async Task EqualityLookup_OnDescendingIndex_ReturnsMatchingRow()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRobotsTable();
        await ExecDDL(executor, dbname, "CREATE INDEX year_desc_idx ON robots (year DESC)");
        await InsertYears(executor, dbname, new[] { 2001, 2010, 2005 });

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT year FROM robots WHERE year = 2005");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2005, rows[0].Row["year"].LongValue);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateRobotsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, name string(64) not null, year int64 not null)");
        return (dbname, database, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task InsertYears(CommandExecutor executor, string dbname, int[] years)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        foreach (int year in years)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                $"INSERT INTO {TableName} (id, name, year) VALUES (gen_id(), 'r{year}', {year})", null));
            await db.Transactions.CommitAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> ExecSelect(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<List<string>> ExplainNodes(CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "EXPLAIN " + sql);
        List<string> nodes = new();
        foreach (QueryResultRow r in rows)
            if (r.Row.TryGetValue("node", out ColumnValue? node) && node.StrValue is not null)
                nodes.Add(node.StrValue);
        return nodes;
    }
}
