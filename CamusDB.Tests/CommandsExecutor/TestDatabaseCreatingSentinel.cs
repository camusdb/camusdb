
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.IO;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DB6.0 — creating.lock sentinel durability tests.
///
/// CreateDatabase (standalone) writes a creating.lock sentinel inside DataDirectory/{id}/
/// BEFORE committing the registry entry.  On the next Open, DatabaseOpener checks for the
/// sentinel and distinguishes three states:
///
///   1. No sentinel, kv/ present   → normal open (sentinel was cleaned up on success)
///   2. Sentinel + kv/ absent      → DatabaseCreationIncomplete (crash between RegisterAsync
///                                   and Create/Open completing)
///   3. Sentinel + kv/ present     → auto-heal (crash after Open returned but before
///                                   File.Delete(sentinel)); sentinel removed, open proceeds
///
/// These tests are standalone-mode only (BaseTest — no cluster node).
/// </summary>
[TestFixture]
internal sealed class TestDatabaseCreatingSentinel : BaseTest
{
    /// <summary>
    /// Positive case: after a fully successful CreateDatabase the sentinel must be gone.
    /// </summary>
    [Test]
    public async Task CreateDatabase_SentinelRemovedAfterSuccess()
    {
        (string name, DatabaseDescriptor descriptor, _) = await CreateDatabase();

        string sentinelPath = Path.Combine(CamusConfig.DataDirectory, descriptor.Id, "creating.lock");
        Assert.IsFalse(File.Exists(sentinelPath),
            "creating.lock must not exist after a successful CreateDatabase");
    }

    /// <summary>
    /// Crash simulation — sentinel present, kv/ absent:
    /// Open must throw DatabaseCreationIncomplete, not SystemSpaceCorrupt.
    /// </summary>
    [Test]
    public async Task Open_SentinelWithoutKv_ThrowsDatabaseCreationIncomplete()
    {
        // 1. Create a real database so the name is registered.
        (string name, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        string id = descriptor.Id;
        string dbPath = Path.Combine(CamusConfig.DataDirectory, id);

        // 2. Close the database so Kahuna releases its SQLite file handles.
        await executor.CloseDatabase(new CloseDatabaseTicket(name));

        // 3. Simulate the crash state: delete kv/ (leaving the id directory), write sentinel.
        string kvPath = Path.Combine(dbPath, "kv");
        if (Directory.Exists(kvPath))
            Directory.Delete(kvPath, recursive: true);

        await File.WriteAllTextAsync(Path.Combine(dbPath, "creating.lock"), name);

        // 4. Open with a fresh executor (empty descriptor cache).
        CommandExecutor freshExecutor = CreateCommandExecutor();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await freshExecutor.OpenDatabase(name));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseCreationIncomplete, ex!.Code,
            "crash-incomplete state must surface DatabaseCreationIncomplete, not SystemSpaceCorrupt");
        Assert.That(ex.Message, Does.Contain("Drop the database and recreate it"),
            "message must give actionable recovery guidance");
    }

    /// <summary>
    /// Recovery path — DatabaseCreationIncomplete → DROP DATABASE → CREATE DATABASE:
    /// DROP must delete the orphaned id-directory (no cached descriptor, Drop was skipping
    /// cleanup via early-return), and the subsequent CREATE must succeed with a fresh id
    /// and an empty, fully-functional database.
    /// </summary>
    [Test]
    public async Task CreationIncomplete_DropThenRecreate_OldDirGone_NewDatabaseEmpty()
    {
        // 1. Create a real database so the name is registered.
        (string name, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        string oldId = descriptor.Id;
        string oldDbPath = Path.Combine(CamusConfig.DataDirectory, oldId);

        // 2. Close so Kahuna releases file handles.
        await executor.CloseDatabase(new CloseDatabaseTicket(name));

        // 3. Simulate crash: delete kv/, write sentinel.
        string kvPath = Path.Combine(oldDbPath, "kv");
        if (Directory.Exists(kvPath))
            Directory.Delete(kvPath, recursive: true);
        await File.WriteAllTextAsync(Path.Combine(oldDbPath, "creating.lock"), name);

        // 4. A fresh process would have no cached descriptor.  Confirm the incomplete state.
        CommandExecutor freshExecutor = CreateCommandExecutor();
        CamusDBException? incomplete = Assert.ThrowsAsync<CamusDBException>(
            async () => await freshExecutor.OpenDatabase(name));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseCreationIncomplete, incomplete!.Code);

        // 5. DROP DATABASE removes the registry entry AND the orphaned directory
        //    (no descriptor is cached — the fix ensures Drop still deletes the directory).
        await freshExecutor.DropDatabase(new DropDatabaseTicket(name, ifExists: false));
        Assert.IsFalse(Directory.Exists(oldDbPath),
            $"Drop must delete the orphaned id-directory {oldDbPath}");

        // 6. CREATE DATABASE with the same name must succeed, yielding a NEW id.
        CommandExecutor recreateExecutor = CreateCommandExecutor();
        TrackDatabase(name, recreateExecutor);
        DatabaseDescriptor newDescriptor = await recreateExecutor.CreateDatabase(
            new CreateDatabaseTicket(name, ifNotExists: false));

        Assert.AreNotEqual(oldId, newDescriptor.Id,
            "recreated database must have a new id — the old id was associated with incomplete creation");
        Assert.AreEqual(0, newDescriptor.Schema.Tables.Count,
            "recreated database must be empty (no tables carried over from the incomplete creation)");

        string newDbPath = Path.Combine(CamusConfig.DataDirectory, newDescriptor.Id);
        Assert.IsTrue(Directory.Exists(Path.Combine(newDbPath, "kv")),
            "new database kv/ directory must exist");
        Assert.IsFalse(File.Exists(Path.Combine(newDbPath, "creating.lock")),
            "new database must not have a stale sentinel after successful creation");
    }

    /// <summary>
    /// Auto-heal — sentinel present, kv/ also present:
    /// Open must remove the stale sentinel and succeed.
    /// </summary>
    [Test]
    public async Task Open_SentinelWithKvPresent_AutoHealsAndSucceeds()
    {
        // 1. Create a fully-initialised database.
        (string name, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        string id = descriptor.Id;

        // 2. Write a stale sentinel into a fully-created directory (simulates File.Delete failure).
        string sentinelPath = Path.Combine(CamusConfig.DataDirectory, id, "creating.lock");
        await File.WriteAllTextAsync(sentinelPath, name);

        // 3. Close so Kahuna releases its file handles.
        await executor.CloseDatabase(new CloseDatabaseTicket(name));

        // 4. Open with a fresh executor — should auto-heal and succeed.
        CommandExecutor freshExecutor = CreateCommandExecutor();
        TrackDatabase(name, freshExecutor);

        DatabaseDescriptor reopened = await freshExecutor.OpenDatabase(name);

        Assert.AreEqual(id, reopened.Id, "reopened descriptor must have the same id");
        Assert.IsFalse(File.Exists(sentinelPath),
            "auto-heal must remove the stale creating.lock");
    }
}
