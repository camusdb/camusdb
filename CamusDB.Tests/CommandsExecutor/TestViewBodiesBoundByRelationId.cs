/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A stored view body names the relations it reads by their immutable ids rather than by name, so a
/// rename is metadata-only and a name is presentation.
///
/// <para>These tests drive the real SQL entry points end to end: <c>CREATE VIEW</c> stores the
/// id-bound form, and reading, refreshing and <c>SHOW CREATE</c> resolve it. The one place a stored
/// body is edited by hand is where a test needs a definition the current writer cannot produce — a
/// pre-id body, or a reference to a relation that does not exist — and each says so.</para>
/// </summary>
[NonParallelizable]
public sealed class TestViewBodiesBoundByRelationId : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task SeedOrders(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(64), total int64, status string(16))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES " +
            "(1, 'acme', 10, 'open'), (2, 'acme', 20, 'open'), (3, 'globex', 30, 'open'), " +
            "(4, 'globex', 40, 'closed'), (5, 'initech', 50, 'closed')");
    }

    /// <summary>The stored body of a view or a materialized view, which live in different maps.</summary>
    private static ViewDefinition StoredDefinition(DatabaseDescriptor database, string viewName) =>
        database.Schema.Views.TryGetValue(viewName, out ViewSchema? view)
            ? view.Definition!
            : database.Schema.Tables[viewName].ViewDefinition!;

    /// <summary>
    /// Stores <paramref name="body"/> as the view's definition, bypassing the builder so a test can
    /// put a pre-id definition on disk — the one thing the current writer cannot produce.
    /// </summary>
    private static async Task PersistLegacyBody(
        DatabaseDescriptor database, CommandExecutor executor, string viewName, string body)
    {
        ViewSchema view = database.Schema.Views[viewName];
        view.Definition!.Sql = body;

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.GetCatalogsManagerForTesting().PersistSchemaViewAsync(database, view, tx);
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>Closes and reopens the database so its schema is read back from KV.</summary>
    private static async Task<DatabaseDescriptor> Reopen(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }

    private static IEnumerable<long> Ids(IEnumerable<QueryResultRow> rows) =>
        rows.Select(r => r.Row["id"].LongValue).OrderBy(id => id);

    private static async Task<CamusDBException?> CaptureError(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (CamusDBException error)
        {
            return error;
        }
    }

    [Test]
    public async Task AnIdBoundBodyReadsExactlyWhatTheInlinedQueryReads()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Assert.IsTrue(StoredDefinition(database, "open_orders").Sql.Contains(StoredRelationRef.Prefix),
            "the body is not id-bound, so the rest of this test would prove nothing");

        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname,
            "SELECT id, total FROM open_orders");

        List<QueryResultRow> inlined = await ExecQuery(database, executor, dbname,
            "SELECT id, total FROM orders WHERE status = 'open'");

        CollectionAssert.AreEqual(Ids(inlined).ToList(), Ids(viaView).ToList(),
            "resolving a relation reference must produce the query the name did");
        Assert.AreEqual(3, viaView.Count);
    }

    [Test]
    public async Task ARenameOfTheReadRelationLeavesTheStoredBodyUntouchedAndTheViewWorking()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        string storedBeforeRename = StoredDefinition(database, "open_orders").Sql;

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        // This is the payoff of binding by id: the rename is metadata-only. A name-bound body would
        // have had to be rewritten here, and the assertion below is what proves it was not.
        Assert.AreEqual(storedBeforeRename, database.Schema.Views["open_orders"].Definition!.Sql,
            "a rename must not touch a body that names the relation by id");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, total FROM open_orders");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Ids(rows).ToList(),
            "the view must follow the relation through its rename");
    }

    [Test]
    public async Task ShowCreateViewPrintsTheCurrentNameRatherThanTheStoredReference()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");

        string rendered = shown.Single().Row["create view"].StrValue!;

        Assert.IsFalse(rendered.Contains(StoredRelationRef.Prefix),
            $"a user must never be shown an internal relation reference: {rendered}");
        Assert.IsTrue(rendered.Contains("sales"),
            $"SHOW CREATE VIEW must render the name the relation answers to now: {rendered}");
    }

    [Test]
    public async Task AnAliasedReferenceKeepsItsAliasSoQualifiedColumnsStillResolve()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        // The body qualifies its columns, so the alias is load-bearing: resolving the reference has
        // to leave it alone or every one of these references stops resolving.
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT orders.id AS id, orders.total AS total FROM orders WHERE orders.status = 'open'");

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id, total FROM open_orders");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Ids(rows).ToList());
    }

    /// <summary>
    /// A view reading another view binds it the same way, so renaming the inner one is transparent.
    /// This is the case the rewrite path got wrong — it consulted only table dependencies, so a
    /// renamed *view* reached none of its readers — and binding by id removes the possibility rather
    /// than fixing the omission.
    /// </summary>
    [Test]
    public async Task RenamingAViewAnotherViewReadsIsTransparent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_open_orders AS SELECT id, total FROM open_orders WHERE total > 10");

        string storedBeforeRename = StoredDefinition(database, "big_open_orders").Sql;

        await ExecDdl(database, executor, dbname, "ALTER VIEW open_orders RENAME TO live_orders");

        Assert.AreEqual(storedBeforeRename, StoredDefinition(database, "big_open_orders").Sql,
            "renaming a view must not touch the definition of a view that reads it");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, total FROM big_open_orders");

        CollectionAssert.AreEqual(new long[] { 2, 3 }, Ids(rows).ToList());

        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname,
            "SHOW CREATE VIEW big_open_orders");

        StringAssert.Contains("live_orders", shown.Single().Row["create view"].StrValue!);
    }

    [Test]
    public async Task AMaterializedViewRebuildsThroughItsStoredReference()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (6, 'acme', 60, 'open')");

        int refreshed = await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        Assert.AreEqual(4, refreshed, "the rebuild must read through the stored reference");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id, total FROM open_orders");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 6 }, Ids(rows).ToList());
    }

    [Test]
    public async Task AReferenceToNoLiveRelationFailsClosed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        // An id nothing answers to. Guessing a name here would be guessing about access, because
        // grants are keyed by these same ids.
        ViewDefinition definition = database.Schema.Views["open_orders"].Definition!;
        definition.Sql = definition.Sql.Replace(
            StoredRelationRef.Format(database.Schema.Tables["orders"].Id!),
            StoredRelationRef.Format("nosuchrelation"));

        CamusDBException? error = await CaptureError(() =>
            ExecQuery(database, executor, dbname, "SELECT id FROM open_orders"));

        Assert.IsNotNull(error, "an unresolvable reference must not fall back to a name lookup");
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, error!.Code);
    }

    /// <summary>
    /// A definition stored before relation ids were used names its relation directly, and must keep
    /// working and rendering exactly as it did — the id-bound form is additive, not a cutover.
    /// </summary>
    [Test]
    public async Task ABodyThatNamesItsRelationDirectlyIsStillReadAndRenderedUnchanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        // Downgraded to the pre-id form, which is what a definition written by an older version
        // looks like on disk.
        ViewDefinition definition = database.Schema.Views["open_orders"].Definition!;
        definition.Sql = "SELECT id, total FROM orders WHERE status = 'open'";

        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");

        Assert.IsTrue(shown.Single().Row["create view"].StrValue!.Contains(definition.Sql),
            "a name-bound body must render character for character as it always did");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders");
        Assert.AreEqual(3, rows.Count);
    }

    /// <summary>
    /// Creation itself must store the id-bound form — the phase before this one could only resolve
    /// what a test planted.
    /// </summary>
    [Test]
    public async Task CreatingAViewStoresItsRelationsByIdWithTheOriginalNameAsAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW recent_orders AS SELECT id, total FROM orders WHERE status = 'closed'");

        string ordersId = database.Schema.Tables["orders"].Id!;
        string expected = "FROM " + StoredRelationRef.Format(ordersId) + " AS orders";

        StringAssert.Contains(expected, database.Schema.Views["open_orders"].Definition!.Sql);

        // Materialized views go through the same builder, and a body that is only bound on one of
        // the two paths would leave the other rewriting definitions on every rename.
        StringAssert.Contains(expected, database.Schema.Tables["recent_orders"].ViewDefinition!.Sql);
    }

    /// <summary>
    /// What a user is shown must not change because of how the body is stored. An alias that merely
    /// repeats the relation's current name carries nothing and is not printed.
    /// </summary>
    [Test]
    public async Task RenderingOmitsAnAliasThatOnlyRepeatsTheRelationName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");
        string rendered = shown.Single().Row["create view"].StrValue!;

        StringAssert.Contains("FROM orders WHERE", rendered);
        StringAssert.DoesNotContain("orders AS orders", rendered);

        // And it is still something the server accepts back, which is the whole point of SHOW CREATE.
        await ExecDdl(database, executor, dbname, "DROP VIEW open_orders");
        Assert.DoesNotThrowAsync(async () => await ExecDdl(database, executor, dbname, rendered));
    }

    /// <summary>
    /// A definition written before relation ids were stored is rebound to ids by the rename that
    /// would otherwise have stranded it, in the same replicated change — and the conversion is
    /// durable, which is the whole point: an in-memory-only conversion would be undone by the next
    /// restart and the body would then name a relation that no longer exists.
    /// </summary>
    [Test]
    public async Task ARenameConvertsANameBoundBodyAndTheConversionSurvivesAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        // Downgraded to the pre-id form and written to KV, so the database genuinely holds what an
        // older version left there. Editing only the in-memory copy would prove nothing: the stored
        // body would still be the id-bound one creation wrote, and every assertion below would pass
        // whether or not the conversion was ever checkpointed.
        await PersistLegacyBody(database, executor, "open_orders",
            "SELECT id, total FROM orders WHERE status = 'open'");

        database = await Reopen(executor, dbname);

        Assert.IsFalse(StoredDefinition(database, "open_orders").Sql.Contains(StoredRelationRef.Prefix),
            "the legacy body was not actually stored, so this test would not exercise a conversion");

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        string converted = StoredDefinition(database, "open_orders").Sql;
        StringAssert.Contains(StoredRelationRef.Prefix, converted,
            "the rename must rebind a name-bound body rather than leaving it naming a relation that is gone");

        DatabaseDescriptor reopened = await Reopen(executor, dbname);

        Assert.AreEqual(converted, StoredDefinition(reopened, "open_orders").Sql,
            "the conversion must have been checkpointed, not only applied in memory");

        List<QueryResultRow> rows = await ExecQuery(reopened, executor, dbname,
            "SELECT id, total FROM open_orders");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Ids(rows).ToList(),
            "a converted view must read the relation through its new name after a restart");
    }

    /// <summary>
    /// The same conversion has to reach a body that reads a renamed <em>view</em>. Consulting only
    /// table dependencies missed exactly this case once before.
    /// </summary>
    [Test]
    public async Task RenamingAViewConvertsANameBoundBodyThatReadsIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_open_orders AS SELECT id, total FROM open_orders WHERE total > 10");

        StoredDefinition(database, "big_open_orders").Sql =
            "SELECT id, total FROM open_orders WHERE total > 10";

        await ExecDdl(database, executor, dbname, "ALTER VIEW open_orders RENAME TO live_orders");

        StringAssert.Contains(StoredRelationRef.Prefix, StoredDefinition(database, "big_open_orders").Sql,
            "renaming a view must convert a name-bound body that reads it");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, total FROM big_open_orders");

        CollectionAssert.AreEqual(new long[] { 2, 3 }, Ids(rows).ToList());
    }

    /// <summary>
    /// With every dependent already bound by id, a rename carries no definitions at all — that is
    /// what "metadata-only" means, and it is the state this leaves a database in.
    /// </summary>
    [Test]
    public async Task ARenameCarriesNoDefinitionsOnceDependentsAreBoundById()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Dictionary<string, ViewDefinition>? carried = ViewDependencyMaintainer.BuildRenameConversions(
            database.Schema, database.Schema.Tables["orders"].Id!, SQLParserProcessor.Parse);

        Assert.IsNull(carried, "an id-bound dependent has nothing to convert, so a rename must carry nothing");
    }

    [Test]
    public async Task TheReservedPrefixIsRefusedForEveryKindOfRelation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        // Every path that can put a name into the relation namespace. Any one of them left open lets
        // a user create a relation that shadows what a stored body refers to.
        //
        // A distinct name per path, and failures accumulated rather than asserted in the loop: with
        // one shared name, the first path to wrongly succeed makes every later one fail with
        // "already exists", which reads as a refusal and hides the hole.
        List<string> accepted = [];

        foreach (string sql in new[]
                 {
                     $"CREATE TABLE {StoredRelationRef.Prefix}shadowa (id int64 PRIMARY KEY)",
                     $"CREATE VIEW {StoredRelationRef.Prefix}shadowb AS SELECT id FROM orders",
                     $"CREATE MATERIALIZED VIEW {StoredRelationRef.Prefix}shadowc AS SELECT id FROM orders",
                     $"ALTER TABLE orders RENAME TO {StoredRelationRef.Prefix}shadowd",
                 })
        {
            CamusDBException? error = await CaptureError(() => ExecDdl(database, executor, dbname, sql));

            if (error is null)
                accepted.Add(sql);
            else
                Assert.AreEqual(CamusDBErrorCodes.InvalidInput, error.Code, $"wrong error for: {sql}");
        }

        CollectionAssert.IsEmpty(accepted,
            "a relation created under the reserved prefix shadows what a stored body refers to");
    }
}
