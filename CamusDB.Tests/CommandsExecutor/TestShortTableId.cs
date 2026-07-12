
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies the short base-62 table-id scheme: format, monotonicity, never-reused after DROP,
/// keyspace disjointness on recreate, key-length shrinkage, and branch-lineage safety.
/// </summary>
[NonParallelizable]
internal sealed class TestShortTableId : BaseTest
{
    private static readonly string Base62Chars =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private static bool IsBase62(string s) => s.All(c => Base62Chars.Contains(c));

    // -----------------------------------------------------------------------
    // Format — already covered in TestTableCreator; repeated here as a baseline
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task NewTable_HasShortBase62Id_WithNoKeySeparators()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "t",
            new ColumnInfo[] { new("id", ColumnType.Id), new("v", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        CatalogsManager catalogs = new(logger);
        string tableId = catalogs.GetTableSchema(db, "t").Id!;

        Assert.IsNotEmpty(tableId);
        Assert.IsTrue(IsBase62(tableId), $"id '{tableId}' contains non-base62 chars");
        Assert.IsFalse(tableId.Contains('/'), $"id '{tableId}' must not contain '/'");
        Assert.IsFalse(tableId.Contains(':'), $"id '{tableId}' must not contain ':'");
        Assert.IsFalse(tableId.Contains('~'), $"id '{tableId}' must not contain '~'");
        Assert.Less(tableId.Length, 24,
            $"Short id '{tableId}' must be shorter than the 24-char ObjectId it replaces");
    }

    // -----------------------------------------------------------------------
    // Never reused after DROP + recreate
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TableId_IsNeverReusedAfterDropAndRecreate()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        CatalogsManager catalogs = new(logger);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "tbl",
            new ColumnInfo[] { new("id", ColumnType.Id) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        string idBefore = catalogs.GetTableSchema(db, "tbl").Id!;

        await executor.DropTable(new DropTableTicket(
            databaseName: dbname, tableName: "tbl", ifExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "tbl",
            new ColumnInfo[] { new("id", ColumnType.Id), new("v", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        string idAfter = catalogs.GetTableSchema(db, "tbl").Id!;

        Assert.AreNotEqual(idBefore, idAfter, "id must not be reused after DROP + recreate");

        bool afterIsHigher = idAfter.Length > idBefore.Length ||
                             (idAfter.Length == idBefore.Length && string.CompareOrdinal(idAfter, idBefore) > 0);
        Assert.IsTrue(afterIsHigher,
            $"Recreated id='{idAfter}' must encode a strictly higher counter than dropped id='{idBefore}'");
    }

    // -----------------------------------------------------------------------
    // Keyspace disjointness: a recreated table does not see dropped rows
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task DropRecreate_NewTableDoesNotSeeOldRows()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        // Create original table and insert a row.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "tbl",
            new ColumnInfo[] { new("id", ColumnType.Id), new("v", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        KvTransaction tx1 = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx1, databaseName: dbname, tableName: "tbl",
            values: new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "v", new(ColumnType.String, "original") } } }));
        await db.Transactions.CommitAsync(tx1);

        // Drop and recreate with the same name — gets a fresh id.
        await executor.DropTable(new DropTableTicket(databaseName: dbname, tableName: "tbl", ifExists: false));
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "tbl",
            new ColumnInfo[] { new("id", ColumnType.Id), new("v", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        // The recreated table must be empty — its new id points to a disjoint keyspace.
        KvTransaction tx2 = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx2, dbname, "SELECT * FROM tbl", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx2);

        Assert.AreEqual(0, rows.Count,
            "Recreated table must be empty — its new id addresses a disjoint KV keyspace");
    }

    // -----------------------------------------------------------------------
    // Key-length shrinkage: row/index key for a short-id table is shorter
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TableId_ProducesKeysShortherThanObjectIdEquivalent()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "shrink",
            new ColumnInfo[] { new("id", ColumnType.Id), new("v", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        CatalogsManager catalogs = new(logger);
        string tableId = catalogs.GetTableSchema(db, "shrink").Id!;

        // The row bucket prefix is "{dbId}:{tableId}:r".
        // With a short base-62 tableId (e.g. "1") this is much shorter than with a 24-hex ObjectId.
        // A 24-hex ObjectId prefix would be "{dbId}:{24-hex-chars}:r" = at least 28 more chars.
        string shortPrefix = $"{db.Id}:{tableId}:r";
        string objectIdEquivalent = $"{db.Id}:{ObjectIdGenerator.Generate()}:r";

        Assert.Less(shortPrefix.Length, objectIdEquivalent.Length,
            $"Short-id prefix '{shortPrefix}' must be shorter than ObjectId prefix '{objectIdEquivalent}'");

        // Quantify: should be at least 20 chars shorter (spec says ~20 chars off every row key)
        int savings = objectIdEquivalent.Length - shortPrefix.Length;
        Assert.GreaterOrEqual(savings, 20,
            $"Expected ≥20 chars of savings, got {savings} (short='{shortPrefix}', oid='{objectIdEquivalent}')");
    }

    // -----------------------------------------------------------------------
    // Monotonic across successive creates
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TableIds_AreStrictlyMonotonicAcrossCreates()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        CatalogsManager catalogs = new(logger);

        List<string> ids = new();
        for (int i = 0; i < 5; i++)
        {
            string name = $"t{i}";
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: name,
                new ColumnInfo[] { new("id", ColumnType.Id) },
                constraints: new ConstraintInfo[]
                {
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
                },
                ifNotExists: false));
            ids.Add(catalogs.GetTableSchema(db, name).Id!);
        }

        for (int i = 1; i < ids.Count; i++)
        {
            bool aLess = ids[i - 1].Length < ids[i].Length ||
                         (ids[i - 1].Length == ids[i].Length && string.CompareOrdinal(ids[i - 1], ids[i]) < 0);
            Assert.IsTrue(aLess, $"ids[{i-1}]='{ids[i-1]}' must be < ids[{i}]='{ids[i]}'");
        }
    }

    // -----------------------------------------------------------------------
    // Branch: new table in branch does not collide with inherited table id
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Branch_NewTableId_DoesNotCollideWithInheritedTableId()
    {
        // Set up a root database with a table.
        string rootName = NewName();
        CommandExecutor rootExecutor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await rootExecutor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, rootExecutor);

        await rootExecutor.CreateTable(new CreateTableTicket(
            databaseName: rootName, tableName: "items",
            new ColumnInfo[] { new("id", ColumnType.Id), new("v", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        CatalogsManager catalogs = new(logger);
        string inheritedId = catalogs.GetTableSchema(rootDb, "items").Id!;

        // Fork the root into a branch.
        string branchName = NewName();
        DatabaseDescriptor branchDb = await rootExecutor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, rootExecutor);

        // Create a new table in the branch.
        await rootExecutor.CreateTable(new CreateTableTicket(
            databaseName: branchName, tableName: "branch_only",
            new ColumnInfo[] { new("id", ColumnType.Id), new("note", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        string branchNewId = catalogs.GetTableSchema(branchDb, "branch_only").Id!;

        // The branch's new table id must differ from the inherited ancestor table id.
        Assert.AreNotEqual(inheritedId, branchNewId,
            $"Branch new table id '{branchNewId}' must not collide with inherited id '{inheritedId}'");
    }

    // -----------------------------------------------------------------------
    // Branch: ancestor rows are visible via the inherited table id
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Branch_ReadsAncestorRowsViaInheritedTableId()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: rootName, tableName: "data",
            new ColumnInfo[] { new("id", ColumnType.Id), new("val", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        // Insert two rows into the root.
        KvTransaction tx1 = await rootDb.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx1, rootName, "data",
            new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "val", new(ColumnType.String, "alpha") } } }));
        await executor.Insert(new InsertTicket(tx1, rootName, "data",
            new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "val", new(ColumnType.String, "beta") } } }));
        await rootDb.Transactions.CommitAsync(tx1);

        // Create a branch.
        string branchName = NewName();
        DatabaseDescriptor branchDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // The branch must see the root's rows through the inherited table id.
        KvTransaction tx2 = await branchDb.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx2, branchName, "SELECT val FROM data ORDER BY val", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await branchDb.Transactions.CommitAsync(tx2);

        Assert.AreEqual(2, rows.Count, "Branch must see both ancestor rows via the inherited table id");
        List<string> vals = rows.Select(r => r.Row["val"].StrValue!).OrderBy(v => v).ToList();
        CollectionAssert.AreEqual(new[] { "alpha", "beta" }, vals);
    }

    // -----------------------------------------------------------------------
    // Branch: DROP + recreate in branch gets a fresh id (disjoint from inherited)
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Branch_DropAndRecreate_GetsFreshIdDisjointFromAncestor()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: rootName, tableName: "shared",
            new ColumnInfo[] { new("id", ColumnType.Id) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        CatalogsManager catalogs = new(logger);
        string ancestorId = catalogs.GetTableSchema(rootDb, "shared").Id!;

        // Branch from root.
        string branchName = NewName();
        DatabaseDescriptor branchDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Drop and recreate in the branch — should get a fresh, globally-unique id.
        await executor.DropTable(new DropTableTicket(databaseName: branchName, tableName: "shared", ifExists: false));
        await executor.CreateTable(new CreateTableTicket(
            databaseName: branchName, tableName: "shared",
            new ColumnInfo[] { new("id", ColumnType.Id), new("extra", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        string recreatedId = catalogs.GetTableSchema(branchDb, "shared").Id!;

        Assert.AreNotEqual(ancestorId, recreatedId,
            $"Recreated branch table id '{recreatedId}' must not equal ancestor id '{ancestorId}'");

        // The recreated id must encode a strictly higher counter (never reused).
        bool recreatedHigher = recreatedId.Length > ancestorId.Length ||
                               (recreatedId.Length == ancestorId.Length &&
                                string.CompareOrdinal(recreatedId, ancestorId) > 0);
        Assert.IsTrue(recreatedHigher,
            $"Recreated id '{recreatedId}' must encode a higher counter than ancestor id '{ancestorId}'");
    }

    // -----------------------------------------------------------------------
    // Coexistence: a 24-hex ObjectId table id and a short base-62 table id
    // in the same database do not interfere with each other.
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task LegacyObjectIdTable_CoexistsWithShortIdTable_RowsAreIsolated()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        CatalogsManager catalogs = new(logger);

        // Inject a "legacy" table by calling CatalogsManager.CreateTable directly with a
        // 24-hex ObjectId — this is the format used before the short-id scheme was introduced.
        string legacyId = "6849f3a1c2e7d50b4f8a91d3"; // valid 24-hex ObjectId
        KvTransaction tx0 = await db.Transactions.BeginAsync();
        await catalogs.CreateTable(db,
            new CreateTableTicket(
                databaseName: dbname, tableName: "legacy",
                new ColumnInfo[] { new("id", ColumnType.Id), new("val", ColumnType.String) },
                constraints: new ConstraintInfo[]
                {
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
                },
                ifNotExists: false),
            tx0, legacyId);
        await db.Transactions.CommitAsync(tx0);

        // Create a second table the normal way — it gets a short base-62 id.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "modern",
            new ColumnInfo[] { new("id", ColumnType.Id), new("val", ColumnType.String) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        // Verify the two tables have distinct ids and the legacy table kept its 24-hex id.
        string legacySchemaId = catalogs.GetTableSchema(db, "legacy").Id!;
        string modernSchemaId  = catalogs.GetTableSchema(db, "modern").Id!;

        Assert.AreEqual(legacyId, legacySchemaId,
            "Legacy table must retain its injected 24-hex ObjectId");
        Assert.AreNotEqual(legacyId, modernSchemaId,
            "Short-id table must have a different id than the legacy table");
        Assert.IsTrue(IsBase62(modernSchemaId) && modernSchemaId.Length < 24,
            $"Modern table id '{modernSchemaId}' must be a short base-62 id");

        // Insert a row into each table and verify row isolation: each table sees exactly its own row.
        KvTransaction tx1 = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx1, dbname, "legacy",
            new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "val", new(ColumnType.String, "legacy-row") } } }));
        await executor.Insert(new InsertTicket(tx1, dbname, "modern",
            new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "val", new(ColumnType.String, "modern-row") } } }));
        await db.Transactions.CommitAsync(tx1);

        KvTransaction tx2 = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> legacyCursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx2, dbname, "SELECT val FROM legacy", null));
        List<QueryResultRow> legacyRows = await legacyCursor.ToListAsync();

        (_, IAsyncEnumerable<QueryResultRow> modernCursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx2, dbname, "SELECT val FROM modern", null));
        List<QueryResultRow> modernRows = await modernCursor.ToListAsync();
        await db.Transactions.CommitAsync(tx2);

        Assert.AreEqual(1, legacyRows.Count, "Legacy table must contain exactly its own row");
        Assert.AreEqual("legacy-row", legacyRows[0].Row["val"].StrValue);

        Assert.AreEqual(1, modernRows.Count, "Modern table must contain exactly its own row");
        Assert.AreEqual("modern-row", modernRows[0].Row["val"].StrValue);
    }
}
