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
