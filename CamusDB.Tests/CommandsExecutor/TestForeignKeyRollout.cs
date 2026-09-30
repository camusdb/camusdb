/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The cluster rollout of a foreign key through the schema-change coordinator: a WriteOnly constraint is
/// enforced but not trusted, the validation pass publishes it, a failed validation removes it and
/// releases its index, and a resumed job finishes the rollout after a leader change.
///
/// <para>Each scenario creates the child with its constraint already WriteOnly, through the catalog, to
/// stand where a CREATE TABLE is between its delta and its validation. DML does not check constraints
/// yet, so an orphan row can be written directly.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyRollout : SharedNodeBaseTest
{
    private const string ChildSql = "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))";

    private const string ConstraintName = "weather_city_fkey";

    [Test]
    public async Task WriteOnlyConstraintIsEnforcedButNotYetPublic()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string tableId = await CreateWithWriteOnlyConstraint(executor, database, dbname);

        ForeignKeySchema constraint = database.Schema.Tables["weather"].ForeignKeys!.Single();
        Assert.AreEqual(SchemaElementState.WriteOnly, constraint.State);
        Assert.IsTrue(database.Schema.ForeignKeys.ChildPlansOf(tableId).Single().IsEnforced,
            "DML must check a WriteOnly constraint on both sides");
    }

    [Test]
    public async Task ValidationPublishesTheConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string tableId = await CreateWithWriteOnlyConstraint(executor, database, dbname);
        await Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, NULL)");

        await Coordinator(executor).RunJobAsync(database, Job(dbname, tableId));

        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>
    /// An orphan written before the ack fails the validation. The constraint must go, its owned index
    /// must stay as an ordinary index, and no job may be left for a resume to retry.
    /// </summary>
    [Test]
    public async Task FailedValidationRemovesTheConstraintAndReleasesItsIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string tableId = await CreateWithWriteOnlyConstraint(executor, database, dbname);
        string ownedIndexId = database.Schema.Tables["weather"].Indexes!.Single(i => i.Name == "~fk_" + ConstraintName).KvId;
        await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (1, 'atlantis')");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Coordinator(executor).RunJobAsync(database, Job(dbname, tableId)))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("atlantis"));

        TableSchema weather = database.Schema.Tables["weather"];
        Assert.IsNull(weather.ForeignKeys);
        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);

        TableIndexSchema released = weather.Indexes!.Single(i => i.KvId == ownedIndexId);
        Assert.IsNull(released.OwnerConstraintId, "The index must no longer name a constraint that is gone");
        Assert.AreEqual(SchemaElementState.Public, released.State);

        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>Without the validation pass wired in, a constraint must never be published.</summary>
    [Test]
    public async Task ConstraintIsNotPublishedWithoutAValidationPass()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string tableId = await CreateWithWriteOnlyConstraint(executor, database, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await new SchemaChangeCoordinator(executor.Catalogs).RunJobAsync(database, Job(dbname, tableId)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, exception.Code);
        Assert.AreEqual(SchemaElementState.WriteOnly, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
    }

    /// <summary>A leader that took over a recorded job validates and publishes the constraint.</summary>
    [Test]
    public async Task ResumedJobPublishesTheConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string tableId = await CreateWithWriteOnlyConstraint(executor, database, dbname);

        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "weather",
            TableId = tableId,
            ElementName = ConstraintName,
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.ForeignKey,
        });

        await Coordinator(executor).ResumeJobsAsync(database);

        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>A job recorded for a table that was never created is removed, not retried.</summary>
    [Test]
    public async Task ResumedJobOfATableThatWasNeverCreatedIsRemoved()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "never_created",
            TableId = ObjectIdGenerator.Generate().ToString(),
            ElementName = ConstraintName,
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.ForeignKey,
        });

        await Coordinator(executor).ResumeJobsAsync(database);

        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>A foreign key never goes back from Public to WriteOnly.</summary>
    [Test]
    public async Task PublicConstraintCannotBeDemoted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string tableId = await CreateWithWriteOnlyConstraint(executor, database, dbname);
        await Coordinator(executor).RunJobAsync(database, Job(dbname, tableId));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Catalogs.ReplicateElementStateAsync(database, "weather", ConstraintName, SchemaElementState.WriteOnly, SchemaElementKind.ForeignKey))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
    }

    private static SchemaChangeCoordinator Coordinator(CommandExecutor executor) => new(executor.Catalogs)
    {
        ForeignKeyValidationAsync = (db, table, constraint) => executor.ValidateForeignKeyRowsAsync(db, table, constraint)
    };

    private static SchemaChangeJob Job(string dbname, string tableId) =>
        new(dbname, "weather", tableId, ConstraintName, SchemaElementState.Public, SchemaElementKind.ForeignKey);

    /// <summary>Creates cities, then weather with its constraint born WriteOnly. Returns the weather id.</summary>
    private static async Task<string> CreateWithWriteOnlyConstraint(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname,
            sql: "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))", parameters: null));

        CreateTableTicket ticket = new SQLExecutorCreateTableCreator().CreateCreateTableTicket(
            new ExecuteSQLTicket(txnState: null!, database: dbname, sql: ChildSql, parameters: null), SQLParserProcessor.Parse(ChildSql));

        string tableId = ObjectIdGenerator.Generate().ToString();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Catalogs.CreateTable(database, ticket, tx, tableId, SchemaElementState.WriteOnly);
        await database.Transactions.CommitAsync(tx);

        return tableId;
    }

    private static async Task Dml(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }
}
