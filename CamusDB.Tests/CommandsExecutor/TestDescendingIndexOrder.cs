/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Phase 1 of ascending/descending ordered indexes: the per-column direction is threaded through
/// the schema model and persists, but descending columns are rejected at every DDL entry point
/// until the key encoder and planner honor them. These tests pin both halves:
///   - the persisted <see cref="TableIndexSchema.ColumnDirections"/> round-trips (and null means
///     all-ascending, so pre-existing indexes load unchanged);
///   - a descending index column is rejected via SQL, via an ALTER ticket, and via an inline
///     CREATE TABLE constraint.
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
        Assert.AreEqual(OrderType.Ascending, decoded.ColumnDirections[0]);
        Assert.AreEqual(OrderType.Descending, decoded.ColumnDirections[1]);
        Assert.AreEqual(OrderType.Ascending, decoded.DirectionAt(0));
        Assert.AreEqual(OrderType.Descending, decoded.DirectionAt(1));
    }

    [Test]
    public void NullColumnDirections_DeserializeAsAllAscending()
    {
        // An index persisted before mixed-direction indexes existed has no ColumnDirections field.
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
        // DirectionAt must default to Ascending for the null (compact) form and for out-of-range.
        Assert.AreEqual(OrderType.Ascending, decoded.DirectionAt(0));
        Assert.AreEqual(OrderType.Ascending, decoded.DirectionAt(5));
    }

    /// <summary>
    /// An all-ascending index created through the real DDL path stays in the compact
    /// null-directions form after a close/reopen cycle (Extract collapses all-ascending to null),
    /// proving the plumbing carries the field without inflating existing indexes.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task AscendingIndex_ReopenRoundTrip_DirectionsStayNull()
    {
        (string dbname, _, CommandExecutor executor) = await CreateTableWithAscendingIndex();

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        Assert.IsTrue(db2.Schema.Tables.TryGetValue(TableName, out TableSchema? schema));
        TableIndexSchema? nameIdx = schema!.Indexes!.FirstOrDefault(ix => ix.Name == "name_idx");
        Assert.IsNotNull(nameIdx, "name_idx must survive reopen");
        Assert.IsNull(nameIdx!.ColumnDirections, "an all-ascending index stays in the compact null form");
        Assert.AreEqual(OrderType.Ascending, nameIdx.DirectionAt(0));
    }

    // ── Descending rejected at every DDL entry point (Phase 1) ─────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task DescendingIndex_ViaCreateIndexSql_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTableWithAscendingIndex();

        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, "CREATE INDEX year_desc_idx ON robots (year DESC)", null))
        )!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("Descending"));
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingColumn_InAlterIndexTicket_Rejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateTableWithAscendingIndex();

        AlterIndexTicket alterTicket = new(
            databaseName: dbname,
            tableName: TableName,
            indexName: "year_desc_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Descending) },
            operation: AlterIndexOperation.AddIndex
        );

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterIndex(alterTicket))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("year"));
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingColumn_InInlineConstraint_Rejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "descpk",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Descending) })
            },
            ifNotExists: false
        );

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(createTicket))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("Descending"));
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTableWithAscendingIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: TableName,
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(createTicket);
        await database.Transactions.CommitAsync(tx);

        AlterIndexTicket alterTicket = new(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );
        Assert.IsTrue(await executor.AlterIndex(alterTicket));

        return (dbname, database, executor);
    }
}
