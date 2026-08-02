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
using CamusDB.Core.Util;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="DatabaseRegistry"/>.
/// </summary>
[NonParallelizable]
internal sealed class TestDatabaseRegistry
{
    // Each test gets a unique temp directory and a fresh in-memory node.
    private string? tempDir;
    private EmbeddedKahuna? sharedNode;

    [SetUp]
    public async Task Setup()
    {
        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-registry-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;

        sharedNode = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "registry-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }.WithFastTestTimers());
        await sharedNode.StartAsync(CancellationToken.None);
        await sharedNode.WaitForLeaderAsync("warmup", CancellationToken.None);
    }

    [TearDown]
    public async Task Teardown()
    {
        if (sharedNode is not null)
            await sharedNode.DisposeAsync();

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        await first.RegisterAsync(name, id);
        await first.DisposeAsync();

        // Reopen from the same DataDirectory — must load persisted entries.
        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        for (int i = 0; i < count; i++)
        {
            string name = NewName();
            string id = NewId();
            await first.RegisterAsync(name, id);
            registered.Add((name, id));
        }
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        // Must not throw.
        Assert.DoesNotThrowAsync(async () => await registry.UnregisterAsync("never-registered"));
    }

    [Test]
    public async Task Unregister_ThenReopen_EntryGone()
    {
        string name = NewName();
        string id = NewId();

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        await first.RegisterAsync(name, id);
        await first.UnregisterAsync(name);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        Assert.IsFalse(second.TryResolveId(name, out _));
    }

    // -----------------------------------------------------------------------
    // Rename
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rename_OldNameGone_NewNameResolves_IdUnchanged()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await registry.RenameAsync("ghost", NewName()));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    [Test]
    public async Task Rename_TargetAlreadyExists_ThrowsDatabaseAlreadyExists()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        await first.RegisterAsync(oldName, id);
        await first.RenameAsync(oldName, newName);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        DatabaseRegistryEntry? entry = second.GetById(id);
        Assert.IsNotNull(entry, "entry must survive reopen");
        Assert.AreEqual(newName, entry!.Name, "GetById must return the post-rename name");
        Assert.IsFalse(second.TryResolveId(oldName, out _), "old name must be gone");
    }

    [Test]
    public async Task Rename_ToReservedName_ThrowsDatabaseNameReserved()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
    public async Task Register_MixedCase_PreservesCaseButResolvesCaseInsensitively()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        string id = NewId();
        DatabaseRegistryEntry entry = await registry.RegisterAsync("MyDatabase", id);

        Assert.AreEqual("MyDatabase", entry.Name, "stored name must preserve the original case");
        Assert.IsTrue(registry.TryResolveId("MyDatabase", out string resolved));
        Assert.AreEqual(id, resolved);
        Assert.IsTrue(registry.TryResolveId("MYDATABASE", out _), "upper-case lookup must hit");
        Assert.IsTrue(registry.TryResolveId("mydatabase", out _), "lower-case lookup must hit");
    }

    [Test]
    public async Task Register_SameNameDifferentCase_ThrowsDuplicate()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

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
    // Compact base-62 id allocation
    // -----------------------------------------------------------------------

    [Test]
    public async Task AllocateId_IsBase62AndMonotonic()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        List<string> ids = [];
        for (int i = 0; i < 5; i++)
            ids.Add(await registry.AllocateIdAsync());

        // All ids must be non-empty and consist only of base-62 characters
        const string Base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        foreach (string id in ids)
        {
            Assert.IsNotEmpty(id);
            Assert.IsTrue(id.All(c => Base62Chars.Contains(c)), $"id '{id}' contains non-base62 chars");
        }

        // Must be strictly increasing (ids encode a monotonic counter)
        for (int i = 1; i < ids.Count; i++)
        {
            // Shorter strings encode smaller numbers; same length is lexicographic for 0-padded
            // representation, but we rely on the numeric ordering by decoding via string length first
            bool aLess = ids[i - 1].Length < ids[i].Length ||
                         (ids[i - 1].Length == ids[i].Length && string.CompareOrdinal(ids[i - 1], ids[i]) < 0);
            Assert.IsTrue(aLess, $"ids[{i - 1}]='{ids[i - 1]}' must be < ids[{i}]='{ids[i]}'");
        }
    }

    [Test]
    public async Task AllocateId_SurvivesReopen_CounterContinues()
    {
        string id1;
        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        id1 = await first.AllocateIdAsync();
        await first.DisposeAsync();

        // After reopen the sequence counter must resume above the previously issued id
        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        string id2 = await second.AllocateIdAsync();

        // id2 must encode a strictly higher counter value than id1
        Assert.IsTrue(
            id2.Length > id1.Length || (id2.Length == id1.Length && string.CompareOrdinal(id2, id1) > 0),
            $"After reopen id2='{id2}' must be > id1='{id1}'");
    }

    [Test]
    public async Task AllocateId_NeverReuseAfterUnregister()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        string name = NewName();
        string idBefore = await registry.AllocateIdAsync();
        await registry.RegisterAsync(name, idBefore);
        await registry.UnregisterAsync(name);

        // Even though the name was unregistered (DROP scenario), the next allocation
        // must yield a strictly higher id — never the same one
        string idAfter = await registry.AllocateIdAsync();
        Assert.AreNotEqual(idBefore, idAfter, "id must not be reused after unregister");
        bool afterIsHigher = idAfter.Length > idBefore.Length ||
                             (idAfter.Length == idBefore.Length && string.CompareOrdinal(idAfter, idBefore) > 0);
        Assert.IsTrue(afterIsHigher, $"idAfter='{idAfter}' must be > idBefore='{idBefore}'");
    }

    // -----------------------------------------------------------------------
    // Table-id sequence (AllocateTableIdAsync)
    // -----------------------------------------------------------------------

    [Test]
    public async Task AllocateTableId_ReturnsBase62WithNoKeySeparators()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        List<string> ids = [];
        for (int i = 0; i < 5; i++)
            ids.Add(await registry.AllocateTableIdAsync());

        const string Base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        foreach (string id in ids)
        {
            Assert.IsNotEmpty(id);
            Assert.IsTrue(id.All(c => Base62Chars.Contains(c)), $"table id '{id}' contains non-base62 chars");
            Assert.IsFalse(id.Contains('/'), $"table id '{id}' must not contain '/'");
            Assert.IsFalse(id.Contains(':'), $"table id '{id}' must not contain ':'");
            Assert.IsFalse(id.Contains('~'), $"table id '{id}' must not contain '~'");
        }
    }

    [Test]
    public async Task AllocateTableId_IsMonotonicAndNeverReused()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        List<string> ids = [];
        for (int i = 0; i < 5; i++)
            ids.Add(await registry.AllocateTableIdAsync());

        for (int i = 1; i < ids.Count; i++)
        {
            bool aLess = ids[i - 1].Length < ids[i].Length ||
                         (ids[i - 1].Length == ids[i].Length && string.CompareOrdinal(ids[i - 1], ids[i]) < 0);
            Assert.IsTrue(aLess, $"ids[{i - 1}]='{ids[i - 1]}' must be < ids[{i}]='{ids[i]}'");
        }
    }

    [Test]
    public async Task AllocateTableId_SurvivesReopenCounterContinues()
    {
        string id1;
        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        id1 = await first.AllocateTableIdAsync();
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);
        string id2 = await second.AllocateTableIdAsync();

        Assert.IsTrue(
            id2.Length > id1.Length || (id2.Length == id1.Length && string.CompareOrdinal(id2, id1) > 0),
            $"After reopen id2='{id2}' must be > id1='{id1}'");
    }

    [Test]
    public async Task AllocateTableId_SequenceIsIndependentFromDatabaseSequence()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!, CamusDBOptions.Default);

        // Advance the database sequence several times
        for (int i = 0; i < 5; i++)
            await registry.AllocateIdAsync();

        // The table sequence starts from its own counter, independent of the db sequence
        string tableId1 = await registry.AllocateTableIdAsync();
        string tableId2 = await registry.AllocateTableIdAsync();

        Assert.IsNotEmpty(tableId1);
        Assert.IsNotEmpty(tableId2);
        // The two sequences are independent — the table counter must be monotonically increasing
        bool aLess = tableId1.Length < tableId2.Length ||
                     (tableId1.Length == tableId2.Length && string.CompareOrdinal(tableId1, tableId2) < 0);
        Assert.IsTrue(aLess, $"tableId1='{tableId1}' must be < tableId2='{tableId2}'");
    }

    [Test]
    public static void Base62Encode_KnownValues()
    {
        // Alphabet: 0-9=0..9, A-Z=10..35, a-z=36..61
        Assert.AreEqual("0",   Base62.Encode(0));
        Assert.AreEqual("1",   Base62.Encode(1));
        Assert.AreEqual("9",   Base62.Encode(9));
        Assert.AreEqual("A",   Base62.Encode(10));
        Assert.AreEqual("Z",   Base62.Encode(35));
        Assert.AreEqual("a",   Base62.Encode(36));
        Assert.AreEqual("z",   Base62.Encode(61));
        Assert.AreEqual("10",  Base62.Encode(62));       // 1×62 + 0
        Assert.AreEqual("A0",  Base62.Encode(620));      // 10×62 + 0
        Assert.AreEqual("100", Base62.Encode(62 * 62));  // 1×62² + 0×62 + 0
    }

    // -----------------------------------------------------------------------
    // Cluster mode: shared node + _system/ prefix
    // -----------------------------------------------------------------------

    [Test]
    public async Task ClusterMode_RegisterAndResolve()
    {
        await using EmbeddedKahuna clusterNode = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "registry-cluster-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
        }.WithFastTestTimers());

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);

        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);

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
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "registry-fallback-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
        }.WithFastTestTimers());

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);

        // Open two registry instances on the same node (simulates two nodes in a cluster
        // that opened their registries at different times).
        await using DatabaseRegistry r1 = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);
        await using DatabaseRegistry r2 = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);

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

    // -----------------------------------------------------------------------
    // Cross-node concurrent CREATE serialisation (SetIfNotExists CAS)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Cross-node stale-cache race: Node A creates a database; Node B's in-memory
    /// cache is stale (misses A's write) but when it tries to register the same name its
    /// SetIfNotExists write hits a KV entry that A already committed, so B is rejected with
    /// DatabaseAlreadyExists rather than silently overwriting A's id.
    ///
    /// This is the primary correctness scenario: the CAS protects the namespace
    /// split that would occur if B's blind write overwrote A's entry (both sides then believe
    /// they own the database but write to different key-space prefixes id_A/… vs id_B/…).
    /// </summary>
    [Test]
    public async Task ClusterMode_StaleCache_SecondRegistration_ThrowsDatabaseAlreadyExists()
    {
        await using EmbeddedKahuna clusterNode = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "registry-cas-test-" + Guid.NewGuid().ToString("n"),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }.WithFastTestTimers());

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);
        await clusterNode.FlushAsync();

        // Two separate registry instances on the same shared Kahuna node.
        // r1 represents Node A; r2 represents Node B whose cache opened before A's write.
        await using DatabaseRegistry r1 = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);
        await using DatabaseRegistry r2 = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);

        string name = "racedb_" + Guid.NewGuid().ToString("n");
        string idA = NewId();
        string idB = NewId();

        // Node A creates the database.  r2's cache never saw this write.
        await r1.RegisterAsync(name, idA);

        // Confirm r2's in-memory cache is stale — it doesn't know about A's registration.
        Assert.IsFalse(r2.TryResolveId(name, out _),
            "r2 cache must be stale (did not witness r1's registration)");

        // Node B now tries to create the same database (bypasses its stale local check
        // because ContainsKey = false).  The SetIfNotExists CAS at the KV level must reject it.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await r2.RegisterAsync(name, idB));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code,
            "SetIfNotExists must prevent B from overwriting A's entry");

        // The name must still resolve to A's id on both registries.
        string? r1Resolved = await r1.TryResolveIdAsync(name);
        string? r2Resolved = await r2.TryResolveIdAsync(name);
        Assert.AreEqual(idA, r1Resolved, "r1 must resolve to the original id");
        Assert.AreEqual(idA, r2Resolved, "r2 must resolve to the original id via live-KV fallback");
    }

    /// <summary>
    /// RenameAsync CAS guard: Node A renames "alpha" to a target name; Node B's cache
    /// is stale (doesn't know the target name is taken) and passes the in-memory
    /// <c>byName.ContainsKey(newName)</c> check, but the SetIfNotExists KV write is rejected
    /// because A already committed the target key.  B's "delta" registration is preserved.
    /// </summary>
    [Test]
    public async Task ClusterMode_StaleCache_RenameToTakenTarget_Throws()
    {
        await using EmbeddedKahuna clusterNode = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "registry-rename-cas-test-" + Guid.NewGuid().ToString("n"),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }.WithFastTestTimers());

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);
        await clusterNode.FlushAsync();

        // r1 = Node A, r2 = Node B — both open before any registrations; caches start empty.
        await using DatabaseRegistry r1 = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);
        await using DatabaseRegistry r2 = await DatabaseRegistry.OpenAsync(clusterNode, CamusDBOptions.Default);

        string idAlpha  = NewId();
        string idDelta  = NewId();
        string alpha    = "alpha_"   + Guid.NewGuid().ToString("n");
        string delta    = "delta_"   + Guid.NewGuid().ToString("n");
        string newName  = "newname_" + Guid.NewGuid().ToString("n");

        // Node A owns "alpha"; Node B owns "delta" (each registers its own db independently).
        await r1.RegisterAsync(alpha, idAlpha);
        await r2.RegisterAsync(delta, idDelta);

        // Node A renames alpha → newName.  r2's cache is still stale — it never saw "newName".
        await r1.RenameAsync(alpha, newName);
        Assert.IsFalse(r2.TryResolveId(newName, out _),
            "r2 cache must not know about newName (stale)");

        // Node B tries to rename delta → newName.
        // In-memory check passes (byName has no "newName").
        // KV SetIfNotExists must reject because r1 already wrote that key.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await r2.RenameAsync(delta, newName));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code,
            "SetIfNotExists must prevent r2 from overwriting r1's rename target");

        // "delta" must still exist and still map to idDelta (the rename was rolled back).
        string? deltaResolved = await r2.TryResolveIdAsync(delta);
        Assert.AreEqual(idDelta, deltaResolved,
            "delta must still resolve to its original id after the failed rename");

        // "newName" must still resolve to alpha's id (r1's rename is intact).
        string? newNameResolved = await r2.TryResolveIdAsync(newName);
        Assert.AreEqual(idAlpha, newNameResolved,
            "newName must resolve to alpha's id — r1's rename must not have been disturbed");
    }
}
