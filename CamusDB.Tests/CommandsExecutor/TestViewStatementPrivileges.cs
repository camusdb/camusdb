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

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Auth;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Every view and materialized-view statement <c>docs/views.md</c> advertises parses, maps to the
/// privilege it needs, and is enforced. A statement documented but absent — or present but mapped to
/// nothing — is what this fixture exists to catch.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestViewStatementPrivileges : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    /// <summary>The advertised statements, and the privilege each one needs.</summary>
    private static IEnumerable<TestCaseData> Advertised()
    {
        yield return Case("CREATE VIEW v AS SELECT id FROM t", NodeType.CreateView, Privilege.CreateTable);
        yield return Case("CREATE OR REPLACE VIEW v AS SELECT id FROM t", NodeType.CreateOrReplaceView, Privilege.CreateTable);
        yield return Case("DROP VIEW IF EXISTS v CASCADE", NodeType.DropViewIfExists, Privilege.Drop);
        yield return Case("ALTER VIEW v RENAME TO w", NodeType.AlterViewRenameTo, Privilege.Alter);
        yield return Case("ALTER VIEW v OWNER TO u", NodeType.AlterViewOwnerTo, Privilege.Alter);
        yield return Case("COMMENT ON VIEW v IS 'x'", NodeType.CommentOnView, Privilege.Alter);
        yield return Case("SHOW VIEWS", NodeType.ShowViews, Privilege.Select);
        yield return Case("SHOW CREATE VIEW v", NodeType.ShowCreateView, Privilege.Select);
        yield return Case("CREATE MATERIALIZED VIEW m AS SELECT id FROM t WITH NO DATA", NodeType.CreateMaterializedView, Privilege.CreateTable);
        yield return Case("DROP MATERIALIZED VIEW IF EXISTS m CASCADE", NodeType.DropMaterializedViewIfExists, Privilege.Drop);
        yield return Case("ALTER MATERIALIZED VIEW m RENAME TO n", NodeType.AlterMaterializedViewRenameTo, Privilege.Alter);
        yield return Case("REFRESH MATERIALIZED VIEW m", NodeType.RefreshMaterializedView, Privilege.Insert);
        yield return Case("COMMENT ON MATERIALIZED VIEW m IS NULL", NodeType.CommentOnMaterializedView, Privilege.Alter);
        yield return Case("SHOW MATERIALIZED VIEWS", NodeType.ShowMaterializedViews, Privilege.Select);
        yield return Case("SHOW CREATE MATERIALIZED VIEW m", NodeType.ShowCreateMaterializedView, Privilege.Select);

        static TestCaseData Case(string sql, NodeType node, Privilege privilege)
            => new TestCaseData(sql, node, privilege).SetName("{m}(" + sql + ")");
    }

    [TestCaseSource(nameof(Advertised))]
    public void EveryAdvertisedStatementParsesAndMapsToItsPrivilege(string sql, NodeType node, Privilege privilege)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(node, ast.nodeType, sql);
        Assert.AreEqual(privilege, StatementAuthorizer.MapRequiredPrivilege(ast.nodeType), sql);
    }

    private static async Task<Principal> Login(CommandExecutor ex, string u, string p)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(u, p)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task TxnDdl(CommandExecutor ex, string db, string sql, Principal? p)
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

    /// <summary>
    /// The comment statements are enforced on the object they name, and a refusal changes nothing.
    /// A view is checked explicitly, because setting its comment opens no relation the per-table check
    /// could see; a materialized view is checked by that per-table check.
    /// </summary>
    [Test]
    public async Task CommentingAViewOrMaterializedViewNeedsAlterOnIt()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "viewprivs" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE t (id int64 PRIMARY KEY)", root);
        await TxnDdl(ex, db, "CREATE VIEW v AS SELECT id FROM t", root);
        await TxnDdl(ex, db, "CREATE MATERIALIZED VIEW m AS SELECT id FROM t", root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.v TO u", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.m TO u", root);
        Principal u = await Login(ex, "u", "pw");

        DatabaseDescriptor database = await ex.OpenDatabase(db);
        long viewGeneration = database.Schema.Views["v"].Generation;

        CamusDBException? view = Assert.ThrowsAsync<CamusDBException>(async () =>
            await TxnDdl(ex, db, "COMMENT ON VIEW v IS 'x'", u));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, view!.Code);

        CamusDBException? materialized = Assert.ThrowsAsync<CamusDBException>(async () =>
            await TxnDdl(ex, db, "COMMENT ON MATERIALIZED VIEW m IS 'x'", u));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, materialized!.Code);

        Assert.IsNull(database.Schema.Views["v"].Comment, "a refused comment changes nothing");
        Assert.AreEqual(viewGeneration, database.Schema.Views["v"].Generation, "not even the generation");
        Assert.IsNull(database.Schema.Tables["m"].Comment);

        await ServerDdl(ex, $"GRANT ALTER ON {db}.v TO u", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.m TO u", root);
        u = await Login(ex, "u", "pw");

        await TxnDdl(ex, db, "COMMENT ON VIEW v IS 'x'", u);
        await TxnDdl(ex, db, "COMMENT ON MATERIALIZED VIEW m IS 'y'", u);

        Assert.AreEqual("x", database.Schema.Views["v"].Comment);
        Assert.AreEqual("y", database.Schema.Tables["m"].Comment);
    }
}
