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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the right-side residual-filter paths of the join executor after they were switched
/// from qualifying each right row into a fresh <c>Dictionary&lt;string,ColumnValue&gt;</c>
/// (<see cref="QueryRowMerger.QualifyRow"/>) to re-wrapping the decoded <see cref="QueryRow"/>
/// with a single cached alias-qualified <see cref="RowLayout"/>
/// (<see cref="QueryRowMerger.QualifyRowAsQueryRow"/>) — the allocation-free path.
///
/// <para>Two behavioural risks are guarded here:</para>
/// <list type="number">
///   <item><description>
///     <b>Cross-path parity.</b> The residual predicate on a right-table non-key column must
///     produce identical results whether the right side is reached through the index-nested-loop
///     row fetch (<c>LoadRightRow</c>) or the full nested-loop scan (<c>ScanBoundTable</c>).
///   </description></item>
///   <item><description>
///     <b>Mixed schema versions.</b> The single cached qualified layout is built once from the
///     first right row. Rows written before and after an <c>ALTER TABLE ... ADD COLUMN</c> decode
///     through different schema-history column lists, so this proves the cached layout still reads
///     the correct per-row value for both the defaulted old rows and the explicit new rows.
///   </description></item>
/// </list>
/// </summary>
public sealed class TestJoinRightSideResidual : SharedNodeBaseTest
{
    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// Creates orders / line_items. <paramref name="indexOrderId"/> chooses whether the join key
    /// on the right table is indexed, which (with cost-based planning off) selects
    /// index-nested-loop vs plain nested-loop — i.e. the <c>LoadRightRow</c> vs <c>ScanBoundTable</c>
    /// right-side path.
    /// </summary>
    private async Task<Fixture> Setup(bool indexOrderId)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        ConstraintInfo[] itemConstraints = indexOrderId
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_id_idx", [new("order_id", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "line_items",
            columns:
            [
                new("id", ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("product", ColumnType.String, notNull: true),
            ],
            constraints: itemConstraints,
            ifNotExists: false));

        string orderAId = ObjectIdGenerator.Generate().ToString();
        string orderBId = ObjectIdGenerator.Generate().ToString();
        string orderDId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                new() { { "id", new(ColumnType.Id, orderAId) }, { "name", new(ColumnType.String, "Order-A") } },
                new() { { "id", new(ColumnType.Id, orderBId) }, { "name", new(ColumnType.String, "Order-B") } },
                new() { { "id", new(ColumnType.Id, orderDId) }, { "name", new(ColumnType.String, "Order-D") } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "line_items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderAId) }, { "product", new(ColumnType.String, "Widget") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderBId) }, { "product", new(ColumnType.String, "Gadget") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderBId) }, { "product", new(ColumnType.String, "Doohickey") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderDId) }, { "product", new(ColumnType.String, "Sprocket") } },
            ]));

        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await f.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    /// <summary>
    /// A residual predicate on a right-table non-key column (<c>li.product &lt;&gt; "Doohickey"</c>)
    /// must produce identical rows through both right-side paths: index-nested-loop (indexed
    /// <c>order_id</c> → <c>LoadRightRow</c>) and nested-loop (unindexed → <c>ScanBoundTable</c>).
    /// Also asserts the residual actually removed a row (3 of the 4 join pairs survive), so the
    /// test would fail if the filter were silently dropped rather than merely evaluated identically.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RightSideResidualFilter_IndexAndNestedLoop_ProduceIdenticalRows()
    {
        const string sql =
            "SELECT o.name, li.product " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE li.product <> \"Doohickey\" " +
            "ORDER BY o.name, li.product";

        Fixture indexed = await Setup(indexOrderId: true);
        List<QueryResultRow> indexedRows = await Run(indexed, sql);

        Fixture nestedLoop = await Setup(indexOrderId: false);
        List<QueryResultRow> nlRows = await Run(nestedLoop, sql);

        // Residual removed exactly the "Doohickey" pair: A/Widget, B/Gadget, D/Sprocket remain.
        Assert.AreEqual(3, indexedRows.Count, "index-nested-loop: residual must exclude the Doohickey row");
        Assert.AreEqual(3, nlRows.Count, "nested-loop: residual must exclude the Doohickey row");

        for (int i = 0; i < indexedRows.Count; i++)
        {
            Assert.AreEqual(indexedRows[i].Row["name"].StrValue, nlRows[i].Row["name"].StrValue, $"row[{i}].name");
            Assert.AreEqual(indexedRows[i].Row["product"].StrValue, nlRows[i].Row["product"].StrValue, $"row[{i}].product");
            Assert.AreNotEqual("Doohickey", indexedRows[i].Row["product"].StrValue, "residual row leaked");
        }

        Assert.AreEqual("Order-A", indexedRows[0].Row["name"].StrValue);
        Assert.AreEqual("Widget", indexedRows[0].Row["product"].StrValue);
        Assert.AreEqual("Order-B", indexedRows[1].Row["name"].StrValue);
        Assert.AreEqual("Gadget", indexedRows[1].Row["product"].StrValue);
        Assert.AreEqual("Order-D", indexedRows[2].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", indexedRows[2].Row["product"].StrValue);
    }

    /// <summary>
    /// Adds a column to the right table between two inserts so the scan sees rows from two schema
    /// versions, then joins with a residual predicate on the new column. The single cached qualified
    /// layout must read the correct per-row value for both the defaulted old-version rows and the
    /// explicit new-version rows: the residual must exclude the defaulted rows, and a projection of
    /// the new column must show the default for old rows and the stored value for new rows.
    /// Exercises the index-nested-loop right path (<c>LoadRightRow</c>), whose cached qualified
    /// layout persists across every left row of the join.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RightSideResidualFilter_AcrossMixedSchemaVersions_ReadsCorrectPerRowValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "line_items",
            columns: [new("id", ColumnType.Id), new("order_id", ColumnType.Id), new("product", ColumnType.String, notNull: true)],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_id_idx", [new("order_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        string orderAId = ObjectIdGenerator.Generate().ToString();
        string orderBId = ObjectIdGenerator.Generate().ToString();
        string orderDId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                new() { { "id", new(ColumnType.Id, orderAId) }, { "name", new(ColumnType.String, "Order-A") } },
                new() { { "id", new(ColumnType.Id, orderBId) }, { "name", new(ColumnType.String, "Order-B") } },
                new() { { "id", new(ColumnType.Id, orderDId) }, { "name", new(ColumnType.String, "Order-D") } },
            ]));

        // Old-version rows: written before the note column exists → note is injected as its default.
        await executor.Insert(new InsertTicket(txn, dbname, "line_items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderAId) }, { "product", new(ColumnType.String, "Widget") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderBId) }, { "product", new(ColumnType.String, "Gadget") } },
            ]));

        await database.Transactions.CommitAsync(txn);

        // Schema change bumps the table version; existing rows stay at their stored version.
        KvTransaction txnDdl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txnDdl, database: dbname,
            sql: "ALTER TABLE line_items ADD COLUMN note string DEFAULT('none')", parameters: null));
        await database.Transactions.CommitAsync(txnDdl);

        // New-version rows: carry an explicit note value.
        KvTransaction txn2 = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txn2, dbname, "line_items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderBId) }, { "product", new(ColumnType.String, "Doohickey") }, { "note", new(ColumnType.String, "special") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, orderDId) }, { "product", new(ColumnType.String, "Sprocket") }, { "note", new(ColumnType.String, "special") } },
            ]));
        await database.Transactions.CommitAsync(txn2);

        Fixture f = new(dbname, database, executor);

        // Residual on the new column: only the new-version rows (note = "special") survive; the
        // old-version rows carry the injected default "none" and must be excluded.
        List<QueryResultRow> filtered = await Run(f,
            "SELECT o.name, li.product, li.note " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "WHERE li.note = \"special\" " +
            "ORDER BY o.name, li.product");

        Assert.AreEqual(2, filtered.Count, "only the two new-version rows carry note = 'special'");
        Assert.AreEqual("Order-B", filtered[0].Row["name"].StrValue);
        Assert.AreEqual("Doohickey", filtered[0].Row["product"].StrValue);
        Assert.AreEqual("special", filtered[0].Row["note"].StrValue);
        Assert.AreEqual("Order-D", filtered[1].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", filtered[1].Row["product"].StrValue);
        Assert.AreEqual("special", filtered[1].Row["note"].StrValue);

        // No residual: every join pair appears, and the projected note must be the default for the
        // old-version rows and the stored value for the new-version rows — proving the cached
        // qualified layout reads the correct per-row value across both versions.
        List<QueryResultRow> all = await Run(f,
            "SELECT o.name, li.product, li.note " +
            "FROM orders o JOIN line_items li ON li.order_id = o.id " +
            "ORDER BY o.name, li.product");

        Assert.AreEqual(4, all.Count);
        Assert.AreEqual("Widget", all[0].Row["product"].StrValue);
        Assert.AreEqual("none", all[0].Row["note"].StrValue, "old-version row must read the injected default");
        Assert.AreEqual("Doohickey", all[1].Row["product"].StrValue);
        Assert.AreEqual("special", all[1].Row["note"].StrValue, "new-version row must read its stored value");
        Assert.AreEqual("Gadget", all[2].Row["product"].StrValue);
        Assert.AreEqual("none", all[2].Row["note"].StrValue, "old-version row must read the injected default");
        Assert.AreEqual("Sprocket", all[3].Row["product"].StrValue);
        Assert.AreEqual("special", all[3].Row["note"].StrValue, "new-version row must read its stored value");
    }

    /// <summary>
    /// The allocation win itself: <see cref="QueryRowMerger.QualifyRowAsQueryRow"/> must re-expose
    /// the source row's values through the alias-qualified layout <b>without copying</b> the
    /// <c>Values</c> array — that shared reference is what removes the per-row dictionary allocation
    /// on the right-side scan paths. Also confirms the qualified names resolve to the same values.
    /// </summary>
    [Test]
    public void QualifyRowAsQueryRow_SharesValuesArray_NoPerRowCopy()
    {
        RowLayout layout = RowLayout.ForColumns(["id", "product"]);
        ColumnValue[] values =
        [
            new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
            new(ColumnType.String, "Widget"),
        ];
        QueryRow row = new(ObjectIdGenerator.Generate(), layout, values);

        RowLayout qualifiedLayout = QueryRowMerger.BuildQualifiedLayout(row.Layout, "li");
        QueryRow qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

        Assert.IsTrue(ReferenceEquals(row.Values, qualified.Values), "Values array must be shared, not copied");
        Assert.AreEqual("Widget", qualified["li.product"].StrValue, "qualified key must resolve to the source value");
        Assert.AreEqual(values[0].StrValue, qualified["li.id"].StrValue);
    }
}
