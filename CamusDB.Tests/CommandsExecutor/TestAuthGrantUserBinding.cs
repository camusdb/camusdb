/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A grant belongs to the account it was written for, not to the name that account happened to hold.
///
/// <para>Grant keys are keyed by user name, so a grant that outlives its own <c>DROP USER</c> sits in
/// the keyspace under a name anyone can take again. Without a binding to the user record itself, the
/// next account created under that name would load those grants as its own — privileges nobody
/// granted it. <c>GrantRecord.UserId</c> is that binding, and these tests are what say so.</para>
/// </summary>
internal sealed class TestAuthGrantUserBinding : BaseTest
{
    private static GrantScope DbScope(string databaseId) =>
        new() { Kind = GrantScopeKind.Database, DatabaseId = databaseId, DatabaseName = databaseId };

    private Task<AuthCatalog> OpenAsync() => AuthCatalog.OpenAsync(TestNode!, Options, isClusterMode: true);

    [Test]
    [NonParallelizable]
    public async Task GrantRecordsTheIdOfTheUserItWasWrittenFor()
    {
        AuthCatalog auth = await OpenAsync();
        string user = "gb_" + Guid.NewGuid().ToString("n");

        await auth.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);
        UserRecord? created = await auth.TryGetUserAsync(user);
        Assert.IsNotNull(created);

        await auth.GrantAsync(user, DbScope("db_one"), Privilege.Select, revoke: false);

        IReadOnlyList<GrantRecord> grants = await auth.ListGrantsAsync(user);
        Assert.AreEqual(1, grants.Count);
        Assert.AreEqual(created!.Id, grants[0].UserId, "a grant must name the user record it was written for");
    }

    [Test]
    [NonParallelizable]
    public async Task ARecreatedUserDoesNotInheritAGrantLeftBehindByTheDroppedOne()
    {
        AuthCatalog auth = await OpenAsync();
        string user = "gb_" + Guid.NewGuid().ToString("n");

        await auth.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);
        UserRecord? first = await auth.TryGetUserAsync(user);
        await auth.GrantAsync(user, DbScope("db_one"), Privilege.Select, revoke: false);

        // Stand in for the hazard: DROP USER scans for the account's grant keys and deletes what the
        // scan returned, so a scan that comes back short leaves a grant behind under a freed name.
        // Writing the orphan directly reproduces that end state without depending on a scan failing.
        await auth.DropUserAsync(user, ifExists: false);
        await WriteOrphanGrantAsync(auth, user, first!.Id!, DbScope("db_one"), Privilege.Select);

        await auth.CreateUserAsync(user, PasswordHasher.Hash("y", Options.PasswordHashIterations), ifNotExists: false);
        UserRecord? second = await auth.TryGetUserAsync(user);
        Assert.IsNotNull(second);
        Assert.AreNotEqual(first.Id, second!.Id, "a recreated user must be a new record, or this test proves nothing");

        // Read through a second catalog so the grants are loaded from the keyspace rather than served
        // from the cache of the instance that performed the writes.
        AuthCatalog reader = await OpenAsync();
        IReadOnlyList<GrantRecord> grants = await reader.ListGrantsAsync(user);

        Assert.IsEmpty(grants, "the recreated account must not inherit the dropped account's privileges");
    }

    [Test]
    [NonParallelizable]
    public async Task AGrantWrittenBeforeTheBindingExistedIsStillHonored()
    {
        AuthCatalog auth = await OpenAsync();
        string user = "gb_" + Guid.NewGuid().ToString("n");

        await auth.CreateUserAsync(user, PasswordHasher.Hash("x", Options.PasswordHashIterations), ifNotExists: false);

        // No UserId: the shape every grant on disk has before this field was added. Refusing these
        // would revoke every existing grant on upgrade.
        await WriteOrphanGrantAsync(auth, user, userId: null, DbScope("db_one"), Privilege.Select);

        AuthCatalog reader = await OpenAsync();
        IReadOnlyList<GrantRecord> grants = await reader.ListGrantsAsync(user);

        Assert.AreEqual(1, grants.Count, "a grant predating the user-id binding must keep working");
        Assert.AreEqual(Privilege.Select, grants[0].Privileges);
    }

    /// <summary>
    /// Writes a grant record straight to the auth keyspace, bypassing <c>GrantAsync</c> so the test
    /// controls the recorded user id — including leaving it unset, which is the pre-upgrade shape.
    /// </summary>
    private static async Task WriteOrphanGrantAsync(
        AuthCatalog auth, string user, string? userId, GrantScope scope, Privilege privileges)
    {
        await auth.WriteRawGrantForTestingAsync(new GrantRecord
        {
            User = user.ToLowerInvariant(),
            UserId = userId,
            Scope = scope,
            Privileges = privileges
        });
    }
}
