
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="DatabaseRegistry"/>.
/// </summary>
internal sealed class TestDatabaseRegistry
{
    // Each test gets a unique temp directory so SQLite files never collide.
    private string? tempDir;

    [SetUp]
    public void Setup()
    {
        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-registry-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;
    }

    [TearDown]
    public void Teardown()
    {
        if (tempDir is not null && Directory.Exists(tempDir))
            Directory.Delete(tempDir, recursive: true);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static string NewId() => ObjectIdGenerator.Generate().ToString();
    private static string NewName() => Guid.NewGuid().ToString("n");

    // -----------------------------------------------------------------------
    // Register → resolve by name and by id
    // -----------------------------------------------------------------------

    [Test]
    public async Task Register_ThenResolveByNameAndById()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string name = NewName();
        string id = NewId();

        DatabaseRegistryEntry entry = await registry.RegisterAsync(name, id);

        Assert.AreEqual(name, entry.Name);
        Assert.AreEqual(id, entry.Id);

        // Resolve by name
        Assert.IsTrue(registry.TryResolveId(name, out string resolvedId));
        Assert.AreEqual(id, resolvedId);

        // Get by name
        DatabaseRegistryEntry? fromName = registry.Get(name);
        Assert.IsNotNull(fromName);
        Assert.AreEqual(id, fromName!.Id);

        // Get by id
        DatabaseRegistryEntry? fromId = registry.GetById(id);
        Assert.IsNotNull(fromId);
        Assert.AreEqual(name, fromId!.Name);
    }

    [Test]
    public async Task TryResolveId_UnknownName_ReturnsFalse()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        bool found = registry.TryResolveId("nonexistent-db", out string id);

        Assert.IsFalse(found);
        Assert.AreEqual("", id);
    }

    // -----------------------------------------------------------------------
    // Duplicate-name registration throws
    // -----------------------------------------------------------------------

    [Test]
    public async Task Register_DuplicateName_ThrowsDatabaseAlreadyExists()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string name = NewName();
        await registry.RegisterAsync(name, NewId());

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RegisterAsync(name, NewId()));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);
    }

    // -----------------------------------------------------------------------
    // Reserved names are rejected
    // -----------------------------------------------------------------------

    [Test]
    [TestCase("_system")]
    [TestCase("information_schema")]
    [TestCase("INFORMATION_SCHEMA")]
    public async Task Register_ReservedName_ThrowsDatabaseNameReserved(string reservedName)
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RegisterAsync(reservedName, NewId()));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseNameReserved, ex!.Code);
    }

    // -----------------------------------------------------------------------
    // Entries survive a simulated reopen of the system store
    // -----------------------------------------------------------------------

    [Test]
    public async Task Register_ThenReopen_EntryStillVisible()
    {
        string name = NewName();
        string id = NewId();

        // Open, register, then dispose (closes SQLite).
        DatabaseRegistry first = await DatabaseRegistry.OpenAsync();
        await first.RegisterAsync(name, id);
        await first.DisposeAsync();

        // Reopen from the same DataDirectory — must load persisted entries.
        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync();

        Assert.IsTrue(second.TryResolveId(name, out string resolvedId));
        Assert.AreEqual(id, resolvedId);

        DatabaseRegistryEntry? entry = second.GetById(id);
        Assert.IsNotNull(entry);
        Assert.AreEqual(name, entry!.Name);
    }

    [Test]
    public async Task RegisterMultiple_ThenReopen_AllEntriesVisible()
    {
        const int count = 5;
        List<(string name, string id)> registered = [];

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync();
        for (int i = 0; i < count; i++)
        {
            string name = NewName();
            string id = NewId();
            await first.RegisterAsync(name, id);
            registered.Add((name, id));
        }
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync();
        foreach ((string name, string id) in registered)
        {
            Assert.IsTrue(second.TryResolveId(name, out string resolvedId), $"Name {name} not found");
            Assert.AreEqual(id, resolvedId, $"Id mismatch for {name}");
        }

        Assert.AreEqual(count, second.List().Count);
    }

    // -----------------------------------------------------------------------
    // Unregister
    // -----------------------------------------------------------------------

    [Test]
    public async Task Unregister_RemovesEntry()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string name = NewName();
        string id = NewId();
        await registry.RegisterAsync(name, id);

        await registry.UnregisterAsync(name);

        Assert.IsFalse(registry.TryResolveId(name, out _));
        Assert.IsNull(registry.Get(name));
        Assert.IsNull(registry.GetById(id));
    }

    [Test]
    public async Task Unregister_UnknownName_IsNoOp()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        // Must not throw.
        Assert.DoesNotThrowAsync(async () => await registry.UnregisterAsync("never-registered"));
    }

    [Test]
    public async Task Unregister_ThenReopen_EntryGone()
    {
        string name = NewName();
        string id = NewId();

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync();
        await first.RegisterAsync(name, id);
        await first.UnregisterAsync(name);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync();
        Assert.IsFalse(second.TryResolveId(name, out _));
    }

    // -----------------------------------------------------------------------
    // Rename
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rename_OldNameGone_NewNameResolves_IdUnchanged()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string oldName = NewName();
        string newName = NewName();
        string id = NewId();

        await registry.RegisterAsync(oldName, id);
        await registry.RenameAsync(oldName, newName);

        Assert.IsFalse(registry.TryResolveId(oldName, out _), "old name should be gone");
        Assert.IsTrue(registry.TryResolveId(newName, out string resolvedId));
        Assert.AreEqual(id, resolvedId, "id must not change");

        DatabaseRegistryEntry? fromId = registry.GetById(id);
        Assert.IsNotNull(fromId);
        Assert.AreEqual(newName, fromId!.Name);
    }

    [Test]
    public async Task Rename_UnknownSource_ThrowsDatabaseDoesntExist()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RenameAsync("ghost", NewName()));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    [Test]
    public async Task Rename_TargetAlreadyExists_ThrowsDatabaseAlreadyExists()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string a = NewName();
        string b = NewName();
        await registry.RegisterAsync(a, NewId());
        await registry.RegisterAsync(b, NewId());

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RenameAsync(a, b));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);
    }

    [Test]
    public async Task Rename_ThenReopen_GetByIdReturnsNewName()
    {
        // Regression guard: the old code wrote a dbid:{id} key that was never updated
        // on rename, so after reopen GetById would return the stale old name.
        string oldName = NewName();
        string newName = NewName();
        string id = NewId();

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync();
        await first.RegisterAsync(oldName, id);
        await first.RenameAsync(oldName, newName);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync();

        DatabaseRegistryEntry? entry = second.GetById(id);
        Assert.IsNotNull(entry, "entry must survive reopen");
        Assert.AreEqual(newName, entry!.Name, "GetById must return the post-rename name");
        Assert.IsFalse(second.TryResolveId(oldName, out _), "old name must be gone");
    }

    [Test]
    public async Task Rename_ToReservedName_ThrowsDatabaseNameReserved()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string name = NewName();
        await registry.RegisterAsync(name, NewId());

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RenameAsync(name, "_system"));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseNameReserved, ex!.Code);
    }

    // -----------------------------------------------------------------------
    // Case-insensitive normalization
    // -----------------------------------------------------------------------

    [Test]
    public async Task Register_MixedCase_NormalizedToLowercase()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string id = NewId();
        DatabaseRegistryEntry entry = await registry.RegisterAsync("MyDatabase", id);

        Assert.AreEqual("mydatabase", entry.Name, "stored name must be lowercase");
        Assert.IsTrue(registry.TryResolveId("MyDatabase", out string resolved));
        Assert.AreEqual(id, resolved);
        Assert.IsTrue(registry.TryResolveId("MYDATABASE", out _), "upper-case lookup must hit");
        Assert.IsTrue(registry.TryResolveId("mydatabase", out _), "lower-case lookup must hit");
    }

    [Test]
    public async Task Register_SameNameDifferentCase_ThrowsDuplicate()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        await registry.RegisterAsync("mydb", NewId());

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RegisterAsync("MYDB", NewId()));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);
    }

    // -----------------------------------------------------------------------
    // List
    // -----------------------------------------------------------------------

    [Test]
    public async Task List_ReturnsAllRegistered()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync();

        string a = NewName();
        string b = NewName();
        string c = NewName();

        await registry.RegisterAsync(a, NewId());
        await registry.RegisterAsync(b, NewId());
        await registry.RegisterAsync(c, NewId());

        IReadOnlyList<DatabaseRegistryEntry> list = registry.List();
        Assert.AreEqual(3, list.Count);

        HashSet<string> names = [.. list.Select(e => e.Name)];
        Assert.IsTrue(names.Contains(a));
        Assert.IsTrue(names.Contains(b));
        Assert.IsTrue(names.Contains(c));
    }

    // -----------------------------------------------------------------------
    // Cluster mode: shared node + _system/ prefix
    // -----------------------------------------------------------------------

    [Test]
    public async Task ClusterMode_RegisterAndResolve()
    {
        await using EmbeddedKahuna clusterNode = new(new EmbeddedKahunaOptions
        {
            NodeName = "registry-cluster-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
        });

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);

        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(clusterNode);

        string name = NewName();
        string id = NewId();

        DatabaseRegistryEntry entry = await registry.RegisterAsync(name, id);
        Assert.AreEqual(id, entry.Id);
        Assert.IsTrue(registry.TryResolveId(name, out string resolved));
        Assert.AreEqual(id, resolved);
    }

    /// <summary>
    /// Verifies the cross-node read-your-writes fix: a second registry instance that
    /// missed the RegisterAsync (stale cache) can still resolve the name via the async
    /// live-KV fallback in TryResolveIdAsync.
    /// </summary>
    [Test]
    public async Task ClusterMode_TwoRegistries_SameSharedNode_LiveKvFallbackWorks()
    {
        await using EmbeddedKahuna clusterNode = new(new EmbeddedKahunaOptions
        {
            NodeName = "registry-fallback-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
        });

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);

        // Open two registry instances on the same node (simulates two nodes in a cluster
        // that opened their registries at different times).
        await using DatabaseRegistry r1 = await DatabaseRegistry.OpenAsync(clusterNode);
        await using DatabaseRegistry r2 = await DatabaseRegistry.OpenAsync(clusterNode);

        string name = NewName();
        string id = NewId();

        await r1.RegisterAsync(name, id);

        // Fast path: in-memory cache — r2 loaded before r1's write, so it's stale.
        Assert.IsFalse(r2.TryResolveId(name, out _),
            "r2 in-memory cache should not reflect r1's write (stale snapshot)");

        // Slow path: TryResolveIdAsync falls back to Kahuna and backfills the local cache.
        string? resolved = await r2.TryResolveIdAsync(name);
        Assert.AreEqual(id, resolved,
            "TryResolveIdAsync must find the entry via live KV read");

        // After the live read, the cache is backfilled — subsequent sync reads are fast.
        Assert.IsTrue(r2.TryResolveId(name, out string cachedId),
            "after live KV backfill, in-memory cache must be warm");
        Assert.AreEqual(id, cachedId);
    }
}
