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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The least-privilege matrix for views and materialized views. Each privilege — on the view, on its
/// source relation, on a refresh's target, at database scope for creation, and for each DDL operation —
/// is granted on its own, and the fixture asserts what it reaches.
///
/// <para><b>The assertion that makes the matrix worth having:</b> every denial must leave no catalog
/// mutation (the schema version does not move), no staging mutation (no staging relation and no refresh
/// record appears) and no row mutation. A denial that rolls back is correct; one that leaves a side
/// effect is a finding.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestViewSecurityMatrix : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string u, string p)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(u, p)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task Ddl(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
            await d.Transactions.CommitAsync(tx);
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task NonQuery(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            await d.Transactions.CommitAsync(tx);
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await d.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Everything a denial must leave untouched.</summary>
    private sealed record Snapshot(long SchemaVersion, int Relations, int Views, int StagingRelations, int OrderRows, int TotalsRows, bool RefreshRecord);

    private async Task<Snapshot> Observe(CommandExecutor ex, string db, Principal root)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        string totalsId = d.Schema.Tables["totals"].Id!;

        return new Snapshot(
            d.Schema.SchemaVersion,
            d.Schema.Tables.Count,
            d.Schema.Views.Count,
            d.Schema.Tables.Keys.Count(MaterializedViewNaming.IsStagingRelation),
            (await Query(ex, db, "SELECT id FROM orders", root)).Count,
            (await Query(ex, db, "SELECT id FROM totals", root)).Count,
            await ex.GetCatalogsManagerForTesting().TryGetRefreshJobAsync(d, totalsId) is not null);
    }

    /// <summary>
    /// <c>orders</c>, <c>secrets</c>; <c>owner1</c> reads <c>orders</c> and owns the view <c>open_v</c>,
    /// the view <c>outer_v</c> over it, and the materialized view <c>totals</c>; <c>nobody</c> holds no
    /// grant at all.
    /// </summary>
    private async Task<(string db, CommandExecutor ex, Principal root, Principal owner1, Principal nobody)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "viewsecurity" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Ddl(ex, db, "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(32) NULL, total int64 NULL)", root);
        await Ddl(ex, db, "CREATE TABLE secrets (id int64 PRIMARY KEY, code string(32) NULL)", root);
        await NonQuery(ex, db, "INSERT INTO orders (id, customer, total) VALUES (1, 'acme', 10), (2, 'globex', 20)", root);
        await NonQuery(ex, db, "INSERT INTO secrets (id, code) VALUES (1, 'x')", root);

        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO owner1", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO owner1", root);
        Principal owner1 = await Login(ex, "owner1", "pw");

        await Ddl(ex, db, "CREATE VIEW open_v AS SELECT id, total FROM orders WHERE customer = 'acme'", owner1);

        // An owner holds no implicit grant on what it owns; reading its own view in another body needs one.
        await ServerDdl(ex, $"GRANT SELECT ON {db}.open_v TO owner1", root);
        owner1 = await Login(ex, "owner1", "pw");
        await Ddl(ex, db, "CREATE VIEW outer_v AS SELECT id FROM open_v", owner1);
        await Ddl(ex, db, "CREATE MATERIALIZED VIEW totals AS SELECT customer, SUM(total) AS spent FROM orders GROUP BY customer", owner1);

        // One unrelated grant, so the database is visible to it at all: a user with no grant in a
        // database is told the database does not exist, which would hide every object-level check.
        await ServerDdl(ex, "CREATE USER nobody IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.secrets TO nobody", root);
        Principal nobody = await Login(ex, "nobody", "pw");

        return (db, ex, root, owner1, nobody);
    }

    private static async Task Denied(Func<Task> act, string what)
    {
        CamusDBException? error = null;
        try
        {
            await act();
        }
        catch (CamusDBException e)
        {
            error = e;
        }

        Assert.IsNotNull(error, $"{what}: expected a denial");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, error!.Code, $"{what}: {error.Message}");
    }

    /// <summary>
    /// Every view and materialized-view statement, run by a user with no grant: each is refused, and
    /// none of them leaves anything behind.
    /// </summary>
    [Test]
    public async Task EveryDenialLeavesNoCatalogStagingOrRowMutation()
    {
        (string db, CommandExecutor ex, Principal root, Principal _, Principal nobody) = await Setup();

        string[] statements =
        [
            "CREATE VIEW mine AS SELECT id FROM orders",
            "CREATE OR REPLACE VIEW open_v AS SELECT id, total FROM orders",
            "DROP VIEW open_v",
            "DROP VIEW outer_v CASCADE",
            "ALTER VIEW open_v RENAME TO renamed_v",
            "ALTER VIEW open_v OWNER TO nobody",
            "COMMENT ON VIEW open_v IS 'x'",
            "CREATE MATERIALIZED VIEW mine_m AS SELECT id FROM orders",
            "REFRESH MATERIALIZED VIEW totals",
            "REFRESH MATERIALIZED VIEW totals WITH NO DATA",
            "DROP MATERIALIZED VIEW totals",
            "ALTER MATERIALIZED VIEW totals RENAME TO renamed_m",
            "COMMENT ON MATERIALIZED VIEW totals IS 'x'",
            "CREATE INDEX totals_spent ON totals (spent)",
            "INSERT INTO open_v (id, total) VALUES (9, 9)",
            "UPDATE open_v SET total = 0 WHERE TRUE",
            "DELETE FROM open_v WHERE TRUE",
        ];

        Snapshot before = await Observe(ex, db, root);

        foreach (string sql in statements)
        {
            bool ddl = sql.StartsWith("CREATE") || sql.StartsWith("DROP") || sql.StartsWith("ALTER") || sql.StartsWith("COMMENT");
            await Denied(() => ddl ? Ddl(ex, db, sql, nobody) : NonQuery(ex, db, sql, nobody), sql);

            Assert.AreEqual(before, await Observe(ex, db, root), $"{sql} was denied but left a side effect");
        }
    }

    /// <summary>
    /// Each grant reaches exactly what it names. SELECT on the view reads it and nothing else; SELECT on
    /// the source reads the source and not the view; a refresh needs INSERT on the materialized view and
    /// SELECT on its source, and grants no read of the view; the
    /// database-scope CREATE privilege creates, but only over sources the creator can read.
    /// </summary>
    [Test]
    public async Task EachGrantReachesExactlyWhatItNames()
    {
        (string db, CommandExecutor ex, Principal root, Principal _, Principal _) = await Setup();

        await ServerDdl(ex, "CREATE USER viewer IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.open_v TO viewer", root);
        Principal viewer = await Login(ex, "viewer", "pw");

        Assert.AreEqual(1, (await Query(ex, db, "SELECT id FROM open_v", viewer)).Count);
        await Denied(() => Query(ex, db, "SELECT id FROM orders", viewer), "source through a view grant");
        await Denied(() => Query(ex, db, "SELECT id FROM outer_v", viewer), "a view over the granted view");

        await ServerDdl(ex, "CREATE USER sourcer IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO sourcer", root);
        Principal sourcer = await Login(ex, "sourcer", "pw");

        Assert.AreEqual(2, (await Query(ex, db, "SELECT id FROM orders", sourcer)).Count);
        await Denied(() => Query(ex, db, "SELECT id FROM open_v", sourcer), "view through a source grant");
        await Denied(() => Query(ex, db, "SELECT customer FROM totals", sourcer), "materialized view through a source grant");

        await ServerDdl(ex, "CREATE USER refresher IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT INSERT ON {db}.totals TO refresher", root);
        Principal refresher = await Login(ex, "refresher", "pw");
        await Denied(() => NonQuery(ex, db, "REFRESH MATERIALIZED VIEW totals", refresher), "refresh without the source");

        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO refresher", root);
        refresher = await Login(ex, "refresher", "pw");
        await NonQuery(ex, db, "REFRESH MATERIALIZED VIEW totals", refresher);
        await Denied(() => Query(ex, db, "SELECT customer FROM totals", refresher), "refresh grants no read");

        await ServerDdl(ex, "CREATE USER creator IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO creator", root);
        Principal creator = await Login(ex, "creator", "pw");
        await Denied(() => Ddl(ex, db, "CREATE VIEW c_v AS SELECT id FROM secrets", creator), "create over an unreadable source");

        await ServerDdl(ex, $"GRANT SELECT ON {db}.secrets TO creator", root);
        creator = await Login(ex, "creator", "pw");
        await Ddl(ex, db, "CREATE VIEW c_v AS SELECT id FROM secrets", creator);
    }

    /// <summary>
    /// Listings omit what the caller cannot reach, and describing a view needs a grant on it. A denial
    /// discloses nothing of what the view reads: not its source, not its columns.
    /// </summary>
    [Test]
    public async Task ListingsOmitAndDescribingNeedsAGrantAndDenialsDiscloseNoShape()
    {
        (string db, CommandExecutor ex, Principal root, Principal _, Principal nobody) = await Setup();

        await ServerDdl(ex, "CREATE USER viewer IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.open_v TO viewer", root);
        Principal viewer = await Login(ex, "viewer", "pw");

        CollectionAssert.AreEquivalent(new[] { "open_v" },
            (await Query(ex, db, "SHOW VIEWS", viewer)).Select(r => r.Row["views"].StrValue).ToArray());
        Assert.IsEmpty(await Query(ex, db, "SHOW VIEWS", nobody));
        Assert.IsEmpty(await Query(ex, db, "SHOW MATERIALIZED VIEWS", nobody));

        Assert.AreEqual(1, (await Query(ex, db, "SHOW CREATE VIEW open_v", viewer)).Count);

        foreach (string sql in new[] { "SHOW CREATE VIEW open_v", "SHOW COLUMNS FROM open_v", "SHOW CREATE MATERIALIZED VIEW totals", "SELECT * FROM open_v" })
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await Query(ex, db, sql, nobody));
            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, error!.Code, sql);

            // What the bodies read: their source table, its columns, their literals. The views' own names are not shape.
            foreach (string shape in new[] { "orders", "customer", "acme", "SUM" })
                StringAssert.DoesNotContain(shape, error.Message, $"{sql} disclosed '{shape}'");
        }
    }

    /// <summary>A view whose owner was dropped fails closed — reads and writes alike — and recovers when transferred.</summary>
    [Test]
    public async Task ADroppedOwnerFailsClosed()
    {
        (string db, CommandExecutor ex, Principal root, Principal _, Principal _) = await Setup();

        await ServerDdl(ex, "CREATE USER viewer IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.open_v TO viewer", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.outer_v TO viewer", root);
        Principal viewer = await Login(ex, "viewer", "pw");

        await ServerDdl(ex, "DROP USER owner1", root);

        await Denied(() => Query(ex, db, "SELECT id FROM open_v", viewer), "view of a dropped owner");
        await Denied(() => Query(ex, db, "SELECT id FROM outer_v", viewer), "nested view of a dropped owner");

        // Re-creating the name does not hand the old owner's views to the new account.
        await ServerDdl(ex, "CREATE USER owner1 IDENTIFIED BY 'pw2'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO owner1", root);
        await Denied(() => Query(ex, db, "SELECT id FROM open_v", viewer), "a re-created owner name");

        await Ddl(ex, db, "ALTER VIEW open_v OWNER TO root", root);
        Assert.AreEqual(1, (await Query(ex, db, "SELECT id FROM open_v", viewer)).Count);
    }

    /// <summary>
    /// A replace keeps the owner and needs ALTER on the view; it is not a way to seize a view. A replace
    /// by its owner who has lost read access to the source is refused.
    /// </summary>
    [Test]
    public async Task ReplacementNeitherSeizesNorWidensAView()
    {
        (string db, CommandExecutor ex, Principal root, Principal owner1, Principal _) = await Setup();

        await ServerDdl(ex, "CREATE USER other IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE TABLE ON {db}.* TO other", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.orders TO other", root);
        Principal other = await Login(ex, "other", "pw");

        await Denied(() => Ddl(ex, db, "CREATE OR REPLACE VIEW open_v AS SELECT id, total FROM orders", other), "replace without ALTER");

        await ServerDdl(ex, $"GRANT ALTER ON {db}.open_v TO other", root);
        other = await Login(ex, "other", "pw");
        await Ddl(ex, db, "CREATE OR REPLACE VIEW open_v AS SELECT id, total FROM orders", other);

        DatabaseDescriptor d = await ex.OpenDatabase(db);
        Assert.AreEqual("owner1", d.Schema.Views["open_v"].Definition!.Owner, "a replace does not re-own the view");

        await Denied(() => Ddl(ex, db, "CREATE OR REPLACE VIEW open_v AS SELECT id, 0 AS total FROM secrets", owner1),
            "replace over a source the owner cannot read");
    }

    /// <summary>
    /// Two sessions with different principals, running concurrently, never see each other's authorization
    /// scope: the owner swap of one session's view does not leak into the other's statement.
    /// </summary>
    [Test]
    public async Task ConcurrentSessionsKeepTheirOwnAuthorizationScope()
    {
        (string db, CommandExecutor ex, Principal root, Principal _, Principal nobody) = await Setup();

        await ServerDdl(ex, "CREATE USER viewer IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.open_v TO viewer", root);
        Principal viewer = await Login(ex, "viewer", "pw");

        List<Task> sessions = [];
        int viewerReads = 0, deniedReads = 0, leaks = 0;

        for (int i = 0; i < 20; i++)
        {
            sessions.Add(Task.Run(async () =>
            {
                if ((await Query(ex, db, "SELECT id FROM open_v", viewer)).Count == 1)
                    System.Threading.Interlocked.Increment(ref viewerReads);
            }));

            sessions.Add(Task.Run(async () =>
            {
                try
                {
                    await Query(ex, db, "SELECT id FROM orders", nobody);
                    System.Threading.Interlocked.Increment(ref leaks);
                }
                catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.InsufficientPrivilege)
                {
                    System.Threading.Interlocked.Increment(ref deniedReads);
                }
            }));
        }

        await Task.WhenAll(sessions);

        Assert.AreEqual(20, viewerReads);
        Assert.AreEqual(20, deniedReads);
        Assert.AreEqual(0, leaks, "no session may borrow another's view-owner scope");
    }
}
