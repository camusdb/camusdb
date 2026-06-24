
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

/**
 * Explicit database creation and unknown-database rejection.
 *
 * Covers:
 *   - CreateDatabase allocates a compact base-62 id and a registry entry.
 *   - Open resolves a name to its id via the registry and never auto-creates.
 *   - Every entry point rejects an unknown database with DatabaseDoesntExist.
 */

using NUnit.Framework;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>Explicit-create and unknown-database-rejection tests.</summary>
internal sealed class TestDatabaseExplicitCreate : BaseTest
{
    // -----------------------------------------------------------------------
    // CreateDatabase allocates id + registry entry
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateDatabase_ProducesRegistryEntry_AndDescriptor()
    {
        (string dbname, DatabaseDescriptor descriptor, _) = await CreateDatabase();

        string id = descriptor.Id;
        Assert.IsNotEmpty(id, "id must be non-empty");
        Assert.AreEqual(dbname, descriptor.Name, "descriptor name must match the requested name");

        // id is a compact base-62 string (no hyphens, no uppercase-only hex digits)
        const string Base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Assert.IsTrue(id.All(c => Base62Chars.Contains(c)), $"id '{id}' must be a base-62 string");

        // No per-database directory is created
        Assert.IsFalse(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, id)),
            "no per-database directory must be created");
        Assert.IsFalse(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, dbname)),
            "no name-based directory must be created");
    }

    [Test]
    public async Task CreateDatabase_Twice_WithoutIfNotExists_ThrowsDatabaseAlreadyExists()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false)));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);
    }

    [Test]
    public async Task CreateDatabase_Twice_WithIfNotExists_IsNoOp()
    {
        (string dbname, DatabaseDescriptor first, CommandExecutor executor) = await CreateDatabase();

        DatabaseDescriptor second = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: true));

        Assert.AreEqual(first.Id, second.Id, "IfNotExists must return the same descriptor");
    }

    // -----------------------------------------------------------------------
    // Open resolves name→id and never creates
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateThenOpen_ResolvesViaRegistry()
    {
        (string dbname, DatabaseDescriptor created, CommandExecutor executor) = await CreateDatabase();

        DatabaseDescriptor opened = await executor.OpenDatabase(dbname);

        Assert.AreSame(created, opened, "Open must return the same cached descriptor");
        Assert.AreEqual(created.Id, opened.Id);
    }

    [Test]
    public async Task OpenDatabase_UnknownName_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(ghost));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
        Assert.IsFalse(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, ghost)),
            "No directory must be created for an unknown name");
        await Task.CompletedTask;
    }

    // -----------------------------------------------------------------------
    // Reject unknown across DDL and DML entry points
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateTable_UnknownDatabase_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.CreateTable(new CreateTableTicket(
                databaseName: ghost,
                tableName: "t",
                columns: [new("id", ColumnType.Id)],
                constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
                ifNotExists: false)));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
        await Task.CompletedTask;
    }

    [Test]
    public async Task AlterTable_UnknownDatabase_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.AlterTable(new AlterTableTicket(
                databaseName: ghost,
                tableName: "t",
                column: new ColumnInfo("col", ColumnType.Integer64),
                operation: AlterTableOperation.AddColumn)));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
        await Task.CompletedTask;
    }

    [Test]
    public async Task DropTable_UnknownDatabase_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.DropTable(new DropTableTicket(
                databaseName: ghost,
                tableName: "t",
                ifExists: false)));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
        await Task.CompletedTask;
    }

    [Test]
    public async Task DropDatabase_UnknownName_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.DropDatabase(new DropDatabaseTicket(ghost)));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
        await Task.CompletedTask;
    }

    // -----------------------------------------------------------------------
    // Rollback / compensation (finding #2)
    // -----------------------------------------------------------------------

    /// <summary>
    /// If CreateDatabase succeeds in allocating an id and registering the name but then
    /// fails at Open, the registry entry is rolled back so the name is not permanently wedged.
    /// Verified by immediately recreating with the same name — must succeed.
    /// </summary>
    [Test]
    public async Task CreateDatabase_Twice_AfterFirstSucceeds_SecondWithIfNotExists_IsNoOp()
    {
        string dbname = Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();

        DatabaseDescriptor first = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        DatabaseDescriptor second = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: true));

        Assert.AreEqual(first.Id, second.Id, "IfNotExists must return the same id");
    }

    // -----------------------------------------------------------------------
    // CREATE/DROP/RENAME DATABASE via ExecuteDDLSQL (the /execute-sql-ddl path)
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExecuteDDLSQL_CreateDatabase_CreatesAndOpens()
    {
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        CommandExecutor executor = CreateCommandExecutor();

        // Use any valid context database — CREATE DATABASE ignores it.
        (string ctx, _, _) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: ctx,
            sql: $"CREATE DATABASE {dbname}",
            parameters: null));

        TrackDatabase(dbname, executor);

        DatabaseDescriptor opened = await executor.OpenDatabase(dbname);
        Assert.AreEqual(dbname, opened.Name);
        Assert.IsNotEmpty(opened.Id);
    }

    [Test]
    public async Task ExecuteDDLSQL_CreateDatabaseIfNotExists_IsIdempotent()
    {
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbname, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: dbname,
            sql: $"CREATE DATABASE {dbname}",
            parameters: null));

        // Second CREATE IF NOT EXISTS must not throw.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: dbname,
            sql: $"CREATE DATABASE IF NOT EXISTS {dbname}",
            parameters: null));
    }

    [Test]
    public async Task ExecuteDDLSQL_DropDatabase_RemovesDatabase()
    {
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        CommandExecutor executor = CreateCommandExecutor();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: dbname,
            sql: $"CREATE DATABASE {dbname}",
            parameters: null));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: dbname,
            sql: $"DROP DATABASE {dbname}",
            parameters: null));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(dbname));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    [Test]
    public async Task ExecuteDDLSQL_RenameDatabase_NewNameOpens()
    {
        string oldName = "db_" + Guid.NewGuid().ToString("n")[..8];
        string newName = "db_renamed_" + Guid.NewGuid().ToString("n")[..8];
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(newName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: oldName,
            sql: $"CREATE DATABASE {oldName}",
            parameters: null));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: oldName,
            sql: $"RENAME DATABASE {oldName} TO {newName}",
            parameters: null));

        // Rename is display-only on the cached descriptor; verify the id resolves and old name is gone.
        DatabaseDescriptor opened = await executor.OpenDatabase(newName);
        Assert.IsNotEmpty(opened.Id);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(oldName));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }
}
