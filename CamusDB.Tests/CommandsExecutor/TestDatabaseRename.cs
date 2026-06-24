
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for RENAME DATABASE (DB7).
/// Rename is a registry-only operation: the id-based directory, Kahuna keys,
/// and table ids are untouched; only the registry entry's Name field changes.
/// </summary>
internal sealed class TestDatabaseRename : BaseTest
{
    // -----------------------------------------------------------------------
    // Happy-path: rename succeeds, old name is gone, new name returns original data
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rename_OldNameThrows_NewNameOpens()
    {
        (string oldName, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        string newName = "db_" + Guid.NewGuid().ToString("n");

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        // Old name must now throw DatabaseDoesntExist.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(oldName));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        // New name must open the same descriptor (same live node, same id).
        // Name on the cached descriptor may still show the old display name — that is intentional:
        // evicting the descriptor to refresh Name would orphan the running Kahuna node.
        DatabaseDescriptor reopened = await executor.OpenDatabase(newName);
        Assert.AreSame(descriptor, reopened,
            "Open(newName) must return the cached descriptor, not a reloaded one — " +
            "eviction would orphan the live Kahuna node and open a second one on the same storage");
        Assert.AreEqual(descriptor.Id, reopened.Id,
            "Id must be unchanged after rename");
    }

    [Test]
    public async Task Rename_IdUnchanged()
    {
        (string oldName, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        string id = descriptor.Id;
        string newName = "db_" + Guid.NewGuid().ToString("n");

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        Assert.AreEqual(id, descriptor.Id, "Id must be unchanged after rename");
    }

    /// <summary>
    /// Creates a table and inserts a row before rename; verifies the schema (table) is still
    /// accessible through the new name after rename (the table must be found, meaning schema
    /// meta keys were not disturbed).
    /// </summary>
    [Test]
    public async Task Rename_SchemaIntactAfterRename()
    {
        (string oldName, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        string newName = "db_" + Guid.NewGuid().ToString("n");

        await executor.CreateTable(new CreateTableTicket(
            databaseName: oldName,
            tableName: "items",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("val", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        string rowId = ObjectIdGenerator.Generate().ToString();
        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: oldName,
            tableName: "items",
            values: new() { new() { { "id", new(ColumnType.Id, rowId) }, { "val", new(ColumnType.String, "hello") } } }
        ));
        await descriptor.Transactions.CommitAsync(tx);

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        // Table must still be accessible via the new name — schema keys are id-based, not name-based.
        DatabaseDescriptor reopened = await executor.OpenDatabase(newName);
        Assert.IsTrue(reopened.Schema.Tables.ContainsKey("items"),
            "The 'items' table must be present in the schema after rename");

        // Insert via new name to confirm the table is writable.
        KvTransaction tx2 = await reopened.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx2,
            databaseName: newName,
            tableName: "items",
            values: new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "val", new(ColumnType.String, "world") } } }
        ));
        await reopened.Transactions.CommitAsync(tx2);
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rename_UnknownOldName_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = "ghost_" + Guid.NewGuid().ToString("n");
        string newName = "db_" + Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.RenameDatabase(new RenameDatabaseTicket(ghost, newName)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    [Test]
    public async Task Rename_ToExistingName_ThrowsDatabaseAlreadyExists()
    {
        (string nameA, _, CommandExecutor executor) = await CreateDatabase();
        string nameB = "db_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));
        TrackDatabase(nameB, executor);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.RenameDatabase(new RenameDatabaseTicket(nameA, nameB)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);
    }

    [Test]
    public async Task Rename_ToReservedName_Throws()
    {
        (string oldName, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.RenameDatabase(new RenameDatabaseTicket(oldName, "_system")));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseNameReserved, ex!.Code);
    }

    // -----------------------------------------------------------------------
    // SQL surface
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rename_ViaSql_OldNameThrows_NewNameOpens()
    {
        string oldName = "db_" + Guid.NewGuid().ToString("n");
        string newName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(newName, executor);

        DatabaseDescriptor descriptor = await executor.CreateDatabase(new CreateDatabaseTicket(oldName, ifNotExists: false));

        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(tx, oldName, $"RENAME DATABASE {oldName} TO {newName}", null));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(oldName));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        DatabaseDescriptor reopened = await executor.OpenDatabase(newName);
        Assert.AreEqual(descriptor.Id, reopened.Id);
    }

    [Test]
    public async Task Rename_ViaSql_ToReservedName_Throws()
    {
        string oldName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(oldName, executor);

        DatabaseDescriptor descriptor = await executor.CreateDatabase(new CreateDatabaseTicket(oldName, ifNotExists: false));

        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(tx, oldName, $"RENAME DATABASE {oldName} TO information_schema", null)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseNameReserved, ex!.Code);
    }
}
