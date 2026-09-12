/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cross-node auth-catalog cache coherence: a user or grant created, dropped, or changed through ONE
/// node's <see cref="AuthCatalog"/> must become visible (or stop being visible) through ANOTHER node
/// whose in-memory cache was warmed with the old state. Two <see cref="AuthCatalog"/> instances over
/// the same shared node stand in for two cluster nodes — each keeps its own cache but shares the
/// Raft-replicated persistent store, which is exactly the surface the generation stamp closes. Mirrors
/// <see cref="TestRegistryCoherence"/> for the database registry.
/// </summary>
internal sealed class TestAuthCatalogCoherence : BaseTest
{
    // Cluster mode: two catalogs stand in for two cluster nodes, so each must run the cross-node
    // generation revalidation on a read — the path under test. In standalone mode a read is trusted
    // without revalidation, which is not what these tests exercise.
    private async Task<(AuthCatalog a, AuthCatalog b)> TwoNodesAsync()
        => (await AuthCatalog.OpenAsync(TestNode!, Options, isClusterMode: true),
            await AuthCatalog.OpenAsync(TestNode!, Options, isClusterMode: true));

    private static GrantScope DbScope(string databaseId) =>
        new() { Kind = GrantScopeKind.Database, DatabaseId = databaseId, DatabaseName = databaseId };

    /// <summary>A user created on node A must become visible on node B after B's stale generation revalidates.</summary>
    [Test]
    [NonParallelizable]
    public async Task CreateUser_OnOneNode_VisibleOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");

        // Warm B with a miss (loads B's cache at the current generation).
        Assert.IsNull(await b.TryGetUserAsync(user));

        await a.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);

        UserRecord? seen = await b.TryGetUserAsync(user);
        Assert.IsNotNull(seen, "a user created on another node must become visible after revalidation");
        Assert.AreEqual(user, seen!.Name);
    }

    /// <summary>A grant added on node A must be visible on node B.</summary>
    [Test]
    [NonParallelizable]
    public async Task Grant_OnOneNode_VisibleOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);

        // Warm B (sees the user, no grants yet).
        Assert.IsEmpty(await b.ListGrantsAsync(user));

        await a.GrantAsync(user, DbScope("db1"), Privilege.Select | Privilege.Insert, revoke: false);

        // A cold catalog must load the grant from KV — proves it was durably persisted, not just held
        // in a cache. (This is the check that first caught the grant key landing in the wrong routing
        // bucket because its leaf contained a '/'.)
        AuthCatalog c = await AuthCatalog.OpenAsync(TestNode!, Options, isClusterMode: true);
        Assert.AreEqual(1, (await c.ListGrantsAsync(user)).Count, "cold node must load the persisted grant from KV");

        IReadOnlyList<GrantRecord> grants = await b.ListGrantsAsync(user);
        Assert.AreEqual(1, grants.Count, "a grant added on another node must be visible after revalidation");
        Assert.AreEqual(Privilege.Select | Privilege.Insert, grants[0].Privileges);
    }

    /// <summary>A user dropped on node A must stop resolving through node B's stale cache hit.</summary>
    [Test]
    [NonParallelizable]
    public async Task DropUser_OnOneNode_RevalidatesGoneOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);

        // Warm B with a real hit.
        Assert.IsNotNull(await b.TryGetUserAsync(user));

        await a.DropUserAsync(user, ifExists: false);

        Assert.IsNull(await b.TryGetUserAsync(user),
            "a user dropped on another node must no longer resolve from a stale cache hit");
    }

    /// <summary>A revoke on node A must shrink the privilege mask node B sees.</summary>
    [Test]
    [NonParallelizable]
    public async Task Revoke_OnOneNode_UpdatesMaskOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);
        await a.GrantAsync(user, DbScope("db1"), Privilege.Select | Privilege.Insert, revoke: false);

        // Warm B with the full mask.
        Assert.AreEqual(Privilege.Select | Privilege.Insert, (await b.ListGrantsAsync(user)).Single().Privileges);

        await a.GrantAsync(user, DbScope("db1"), Privilege.Insert, revoke: true);

        Assert.AreEqual(Privilege.Select, (await b.ListGrantsAsync(user)).Single().Privileges,
            "a revoke on another node must be reflected after revalidation");
    }

    /// <summary>H4 — a user dropped on node A must not be resurrected by an ALTER on node B whose cache
    /// still shows the user. The locked authoritative read must see the user gone.</summary>
    [Test]
    [NonParallelizable]
    public async Task DropThenAlterOnStaleNode_DoesNotResurrectUser()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);
        Assert.IsNotNull(await b.TryGetUserAsync(user)); // warm B's cache with the user

        await a.DropUserAsync(user, ifExists: false);

        // B's cache still shows the user, but the locked read must see it dropped.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await b.SetPasswordAsync(user, PasswordHasher.Hash("new", Options.PasswordHashIterations)))!;
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, ex.Code);

        AuthCatalog c = await AuthCatalog.OpenAsync(TestNode!, Options, isClusterMode: true);
        Assert.IsNull(await c.TryGetUserAsync(user), "the dropped user must stay gone");
    }

    /// <summary>H4 — a GRANT on a stale node must not resurrect a dropped user.</summary>
    [Test]
    [NonParallelizable]
    public async Task DropThenGrantOnStaleNode_DoesNotResurrectUser()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);
        Assert.IsNotNull(await b.TryGetUserAsync(user));

        await a.DropUserAsync(user, ifExists: false);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await b.GrantAsync(user, DbScope("db1"), Privilege.Select, revoke: false))!;
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, ex.Code);
    }

    /// <summary>H11 — DROP USER must remove that user's sessions, not just the user and grant keys.</summary>
    [Test]
    [NonParallelizable]
    public async Task DropUser_RemovesSessions()
    {
        (AuthCatalog a, _) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);

        SessionRecord session = new()
        {
            TokenId = "tok" + Guid.NewGuid().ToString("n"),
            User = user,
            SecretMac = [1, 2, 3],
            IssuedAt = DateTime.UtcNow,
            ExpiresAt = DateTime.UtcNow.AddMinutes(15),
        };
        await a.CreateSessionAsync(session);
        Assert.IsNotNull(await a.TryGetSessionAsync(session.TokenId));

        await a.DropUserAsync(user, ifExists: false);

        Assert.IsNull(await a.TryGetSessionAsync(session.TokenId),
            "DROP USER must delete the user's sessions");
    }

    /// <summary>
    /// A flush on node A must reach node B, and the bound must be a property of the design rather than
    /// a hope. It is: the generation is a durable, replicated key, and a cluster-mode read consults it,
    /// so B observes the move on its very next read — no waiting, and no node-to-node call.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task FlushOnOneNodeIsObservedOnAnotherOnItsNextRead()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);

        // Warm B so it stands at a known generation.
        Assert.IsNotNull(await b.TryGetUserAsync(user));
        long before = b.LocalGeneration;

        await a.FlushPrivilegesAsync();

        Assert.Greater(a.LocalGeneration, before, "the flushing node advances the generation");

        // One ordinary read is all it takes; nothing sleeps and nothing polls.
        await b.TryGetUserAsync(user);

        Assert.AreEqual(a.LocalGeneration, b.LocalGeneration,
            "the other node picks the flush up on its next read");
    }

    /// <summary>
    /// A flush reloads the caches from storage rather than trusting them, which is the whole reason the
    /// statement exists. Proven with a grant written straight to the keyspace, so no cache anywhere was
    /// told about it: only a node that genuinely re-reads storage can see it.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task FlushRereadsTheCatalogFromStorage()
    {
        (AuthCatalog a, _) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);
        Assert.IsEmpty(await a.ListGrantsAsync(user));

        await a.WriteRawGrantForTestingAsync(new GrantRecord
        {
            User = user.ToLowerInvariant(),
            Scope = DbScope("db_flush"),
            Privileges = Privilege.Select,
        });

        await a.FlushPrivilegesAsync();

        Assert.AreEqual(1, (await a.ListGrantsAsync(user)).Count,
            "the flush must re-read the keyspace, not revalidate against what it already believed");
    }

    /// <summary>
    /// The snapshot the listing statements read is taken from storage too, and it carries every account
    /// with its grants in one consistent picture.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SnapshotReadsEveryAccountFromStorage()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string first = "coh_a" + Guid.NewGuid().ToString("n");
        string second = "coh_b" + Guid.NewGuid().ToString("n");

        await a.CreateUserAsync(first, null, ifNotExists: false);
        await a.GrantAsync(first, DbScope("db1"), Privilege.Select, revoke: false);
        await a.CreateUserAsync(second, null, ifNotExists: false);

        // Taken through the OTHER catalog, which never saw either write, so the picture can only have
        // come from storage.
        AuthCatalogSnapshot snapshot = await b.SnapshotAsync();

        AuthCatalogEntry firstEntry = snapshot.Entries.Single(e => e.User.Name == first);
        AuthCatalogEntry secondEntry = snapshot.Entries.Single(e => e.User.Name == second);

        Assert.AreEqual(1, firstEntry.Grants.Count);
        Assert.AreEqual(Privilege.Select, firstEntry.Grants[0].Privileges);
        Assert.IsEmpty(secondEntry.Grants);

        List<string> names = snapshot.Entries.Select(e => e.User.Name.ToLowerInvariant()).ToList();
        Assert.AreEqual(names.OrderBy(n => n, StringComparer.Ordinal).ToList(), names,
            "the snapshot is name-ordered, which is what pins the listing statements' row order");
    }

    /// <summary>H3 — two nodes granting different privileges to the same (user, scope) must not lose an
    /// update: the second grant reads the first's committed mask under lock and unions onto it.</summary>
    [Test]
    [NonParallelizable]
    public async Task ConcurrentGrantsFromTwoNodes_DoNotLoseUpdate()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);
        Assert.IsEmpty(await b.ListGrantsAsync(user)); // warm B (user present, no grants)

        await a.GrantAsync(user, DbScope("db1"), Privilege.Select, revoke: false);

        // B's cache still shows no grant, but its locked read must see A's committed Select and union.
        await b.GrantAsync(user, DbScope("db1"), Privilege.Insert, revoke: false);

        AuthCatalog c = await AuthCatalog.OpenAsync(TestNode!, Options, isClusterMode: true);
        Assert.AreEqual(Privilege.Select | Privilege.Insert, (await c.ListGrantsAsync(user)).Single().Privileges,
            "both nodes' grants must survive — no lost update");
    }
}
