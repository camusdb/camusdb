
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
 *   - CreateDatabase allocates an id, a registry entry, and an id-based directory.
 *   - Open resolves a name to its id via the registry and never auto-creates.
 *   - Every entry point rejects an unknown database with DatabaseDoesntExist.
 */

using NUnit.Framework;
using System;
using System.IO;
using System.Collections.Generic;
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
    // CreateDatabase allocates id + registry entry + directory
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateDatabase_ProducesRegistryEntry_IdDirectory_Descriptor()
    {
        (string dbname, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();

        string id = descriptor.Id;
        Assert.IsNotEmpty(id);
        Assert.AreEqual(dbname, descriptor.Name);

        // id-based directories exist
        Assert.IsTrue(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, id, "kv")));
        Assert.IsTrue(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, id, "wal")));

        // name-based directory does NOT exist
        Assert.IsFalse(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, dbname)));
    }

    [Test]
    public async Task CreateDatabase_WritesNameManifest()
    {
        (string dbname, DatabaseDescriptor descriptor, CommandExecutor _) = await CreateDatabase();

        string manifestPath = Path.Combine(CamusConfig.DataDirectory, descriptor.Id, "name.txt");
        Assert.IsTrue(File.Exists(manifestPath), "name.txt must be written inside DataDirectory/{id}");
        Assert.AreEqual(dbname, File.ReadAllText(manifestPath),
            "name.txt must contain the user-facing database name");
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
    /// If the directory-creation step fails (Create throws before Open is even reached),
    /// the registry entry must be rolled back so the name is not permanently wedged.
    /// Trigger: place a regular FILE at DataDirectory so Directory.CreateDirectory on any
    /// sub-path (DataDirectory/{id}/kv) throws DirectoryNotFoundException.  After the
    /// failed attempt the name must not appear registered and a retry must succeed.
    /// </summary>
    [Test]
    public async Task CreateDatabase_CreateFails_RegistryRolledBackAndNameCanBeRetried()
    {
        string dbname = Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        string validDir = CamusConfig.DataDirectory;

        // Create a regular FILE at the DataDirectory path so that
        // Directory.CreateDirectory(DataDirectory/{id}/kv) throws IOException
        // ("not a directory" / "cannot create directory, file exists").
        string fileAsDir = Path.Combine(validDir, "not-a-dir");
        await File.WriteAllTextAsync(fileAsDir, "I am a file, not a directory");
        CamusConfig.DataDirectory = fileAsDir;

        Exception? thrown = null;
        try
        {
            await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        }
        catch (Exception ex)
        {
            thrown = ex;
        }
        finally
        {
            // Restore before any assertions so TearDown succeeds.
            CamusConfig.DataDirectory = validDir;
            // Remove the sentinel file so it doesn't accumulate between runs.
            if (File.Exists(fileAsDir))
                File.Delete(fileAsDir);
        }

        Assert.IsNotNull(thrown, "CreateDatabase should have thrown with an invalid DataDirectory");

        // No orphan directory under the valid DataDirectory (Create never ran there).
        Assert.IsFalse(Directory.Exists(Path.Combine(validDir, dbname)),
            "No orphan name-based directory must exist");

        // Name must NOT be wedged — a retry must succeed.
        DatabaseDescriptor retry = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        Assert.AreEqual(dbname, retry.Name);
        Assert.IsNotEmpty(retry.Id);
        Assert.IsTrue(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, retry.Id, "kv")),
            "Retry must produce the id-based kv directory");
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
