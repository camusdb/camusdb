/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Compound view DDL validated against one schema snapshot and proposed against another. Each
/// statement used to compute its shape check, its cycle check and its list of dependents before it
/// took the schema lock, so a statement committed in between could leave a stored definition broken or
/// a view reading something gone.
///
/// <para>The competing statement is committed through <see cref="ViewCreator.BeforeProposeForTesting"/>,
/// which runs after the outer statement's checks and before its proposal — exactly the window a real
/// race occupies — with no threads and no sleeps. The required outcome is that the outer statement is
/// refused, retryably, and the competing one stands whole.</para>
/// </summary>
[NonParallelizable]
public sealed class TestViewDdlRaces : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
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

    private static async Task Seed(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE t (id int64 PRIMARY KEY, a int64, b string(16))");
        await ExecDdl(database, executor, dbname, "CREATE VIEW base AS SELECT id, a FROM t");
    }

    /// <summary>Runs <paramref name="competing"/> once, inside the outer statement's window.</summary>
    private static void InWindow(System.Func<Task> competing)
    {
        ViewCreator.BeforeProposeForTesting = async _ =>
        {
            ViewCreator.BeforeProposeForTesting = null;
            await competing();
        };
    }

    [TearDown]
    public void ClearSeam() => ViewCreator.BeforeProposeForTesting = null;

    /// <summary>
    /// Two replaces that are each compatible with the old shape but not with each other. Both used to
    /// land, and the second silently renamed the column the first had appended.
    /// </summary>
    [Test]
    public async Task OfTwoConcurrentReplacesOnlyTheFirstLands()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM t");

        InWindow(() => ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id, a FROM t"));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id, b FROM t"));
        Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, refused!.Code);

        CollectionAssert.AreEqual(
            new[] { "id", "a" },
            database.Schema.Views["v"].Definition!.Columns!.Select(c => c.Name).ToArray(),
            "the replace that landed first keeps its shape");

        // Retried, the refused replace now meets the shape it has to be compatible with.
        CamusDBException? retried = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id, b FROM t"));
        Assert.AreEqual(CamusDBErrorCodes.CannotChangeViewShape, retried!.Code);
    }

    /// <summary>A view created over a view that a drop removed in between must not be left reading nothing.</summary>
    [Test]
    public async Task ADependentCreatedAgainstADropIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        InWindow(() => ExecDdl(database, executor, dbname, "DROP VIEW base"));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW dep AS SELECT id FROM base"));
        Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, refused!.Code);

        Assert.IsFalse(database.Schema.Views.ContainsKey("base"));
        Assert.IsFalse(database.Schema.Views.ContainsKey("dep"), "no view may be left reading a dropped one");
    }

    /// <summary>A drop whose dependents were listed before a new dependent appeared must not orphan it.</summary>
    [Test]
    public async Task ADropAgainstANewDependentIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        InWindow(() => ExecDdl(database, executor, dbname, "CREATE VIEW dep AS SELECT id FROM base"));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW base"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, refused!.Code);
        StringAssert.Contains("dep", refused.Message);

        Assert.IsTrue(database.Schema.Views.ContainsKey("base"));
        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SELECT id FROM dep"));
    }

    /// <summary>
    /// A cascade lists the dependents it drops before it drops the view itself. One created after that
    /// list was taken used to survive the cascade with nothing to read; now the final drop is refused
    /// and the view stays for it.
    /// </summary>
    [Test]
    public async Task ACascadeAgainstANewDependentLeavesNoDanglingEdge()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await ExecDdl(database, executor, dbname, "CREATE VIEW listed AS SELECT id FROM base");

        InWindow(() => ExecDdl(database, executor, dbname, "CREATE VIEW late AS SELECT a FROM base"));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW base CASCADE"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, refused!.Code);

        Assert.IsTrue(database.Schema.Views.ContainsKey("base"), "the view the late dependent reads stays");
        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SELECT a FROM late"));

        // Every remaining dependency edge points at something that exists.
        foreach (ViewSchema view in database.Schema.Views.Values)
            foreach (string id in view.Definition!.DependsOnViewIds ?? [])
                Assert.IsTrue(database.Schema.Views.Values.Any(other => other.Id == id), $"{view.Name} reads a view that is gone");
    }

    /// <summary>
    /// <c>ALTER VIEW … OWNER TO</c> used to edit the live definition in place, before its change was
    /// proposed. It now changes a copy, checked against the generation it was taken from.
    /// </summary>
    [Test]
    public async Task EveryViewChangeAdvancesTheGeneration()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM t");

        long created = database.Schema.Views["v"].Generation;

        await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id, a FROM t");
        Assert.AreEqual(created + 1, database.Schema.Views["v"].Generation);

        await ExecDdl(database, executor, dbname, "ALTER VIEW v RENAME TO w");
        Assert.AreEqual(created + 2, database.Schema.Views["w"].Generation);
    }

    /// <summary>
    /// The apply-time half. A committed entry whose precondition no longer holds is refused alike on
    /// every node: it changes nothing, it still consumes its version so the log keeps moving, and the
    /// refusal is recorded so the proposer reports it. Letting it throw out of a live apply instead
    /// would stop that node's schema pipeline at this entry.
    /// </summary>
    [Test]
    public async Task ARefusedEntryIsANoOpThatConsumesItsVersion()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM t");
        await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id, a FROM t");

        Schema schema = Core.Catalogs.SchemaReplicator.CloneSchema(database.Schema);
        ViewSchema live = schema.Views["v"];
        long version = schema.SchemaVersion;

        // Computed against the generation before the replace that already landed.
        SchemaChangeLogEntry stale = new()
        {
            Ts = new HLCTimestamp(1, 10, 2),
            Database = database.Id,
            FromVersion = version,
            ToVersion = version + 1,
            Op = SchemaOp.SetViewDefinition,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaSetViewDefinitionPayload
            {
                ViewName = "v",
                Definition = new ViewDefinition { Sql = "SELECT 1 AS broken", Columns = [] },
                ExpectedGeneration = live.Generation - 1,
            }),
        };

        Assert.Throws<SchemaDeltaRefusedException>(() => SchemaDeltaApplier.ApplySchemaDelta(schema, database, stale),
            "the dry-run a proposer runs refuses it outright");

        Assert.DoesNotThrow(() => SchemaDeltaApplier.ApplyOrRefuse(schema, database, stale));

        Assert.AreEqual(version + 1, schema.SchemaVersion, "a refused entry still consumes its version");
        Assert.AreSame(live.Definition, schema.Views["v"].Definition, "and changes nothing");
        Assert.IsNotNull(schema.FindRefusal(stale));
        Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, schema.FindRefusal(stale)!.Code);
        Assert.IsTrue(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, stale), "a refusal is how the entry landed");

        // Another entry claiming the same version is not mistaken for the refused one.
        SchemaChangeLogEntry other = new()
        {
            Database = database.Id, FromVersion = version, ToVersion = version + 1, Op = SchemaOp.DropView,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaDropViewPayload { ViewName = "v" }),
        };
        Assert.IsNull(schema.FindRefusal(other));
    }
}
