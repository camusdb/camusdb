/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the standalone <c>DROP INDEX</c> statement: <c>DROP INDEX i ON t</c>,
/// <c>DROP INDEX t.i</c> and <c>IF EXISTS</c> on those and on <c>ALTER TABLE t DROP INDEX</c>. Every
/// form builds the ALTER TABLE ticket, so these tests drive the real SQL entry point in both the
/// standalone and the cluster DDL paths.
/// </summary>
internal abstract class DropIndexScenarios : SharedNodeBaseTest
{
    protected abstract bool ClusterMode { get; }

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    private static async Task<ExecuteDDLSQLResult> Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task NonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task SetupRobots(CommandExecutor executor, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE robots (id int64 PRIMARY KEY NOT NULL, name string, year int64)");
        await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        await NonQuery(executor, dbname, "INSERT INTO robots (id, name, year) VALUES (1, 'a', 2000), (2, 'b', 2001), (3, 'c', 2000)");
    }

    private static bool HasIndex(DatabaseDescriptor database, string table, string index) =>
        database.Schema.Tables[table].Indexes!.Any(ix => string.Equals(ix.Name, index, System.StringComparison.OrdinalIgnoreCase));

    [TestCase("DROP INDEX year_idx ON robots")]
    [TestCase("DROP INDEX robots.year_idx")]
    [TestCase("DROP INDEX IF EXISTS year_idx ON robots")]
    [TestCase("DROP INDEX IF EXISTS robots.year_idx")]
    [TestCase("ALTER TABLE robots DROP INDEX IF EXISTS year_idx")]
    [TestCase("DROP INDEX YEAR_IDX ON ROBOTS")]
    public async Task EachForm_DropsTheIndex_AndTheNameCanBeUsedAgain(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, sql);

        Assert.IsTrue(result.Success, "an index that exists is dropped and reported as applied");
        Assert.IsFalse(HasIndex(database, "robots", "year_idx"));

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000 ORDER BY id");
        Assert.AreEqual(2, rows.Count, "rows stay readable after the drop");

        await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");
        Assert.IsTrue(HasIndex(database, "robots", "year_idx"));

        rows = await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000 ORDER BY id");
        Assert.AreEqual(2, rows.Count, "the index built again under the old name is complete");
    }

    [TestCase("DROP INDEX IF EXISTS no_such_idx ON robots")]
    [TestCase("DROP INDEX IF EXISTS robots.no_such_idx")]
    [TestCase("ALTER TABLE robots DROP INDEX IF EXISTS no_such_idx")]
    [TestCase("DROP INDEX IF EXISTS year_idx ON no_such_table")]
    [TestCase("DROP INDEX IF EXISTS no_such_table.year_idx")]
    public async Task IfExists_OnAMissingIndexOrTable_IsANoOp(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        long versionBefore = database.Schema.SchemaVersion;

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, sql);

        Assert.IsFalse(result.Success, "a no-op reports not applied");
        Assert.AreEqual(versionBefore, database.Schema.SchemaVersion, "a no-op replicates no schema change");
        Assert.IsTrue(HasIndex(database, "robots", "year_idx"), "an index with another name is untouched");
    }

    [TestCase("DROP INDEX no_such_idx ON robots")]
    [TestCase("DROP INDEX robots.no_such_idx")]
    [TestCase("ALTER TABLE robots DROP INDEX no_such_idx")]
    public async Task WithoutIfExists_AMissingIndex_StillFails(string sql)
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("does not exist", exception.Message);
    }

    [TestCase("DROP INDEX year_idx ON no_such_table")]
    [TestCase("DROP INDEX no_such_table.year_idx")]
    public async Task WithoutIfExists_AMissingTable_StillFails(string sql)
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, exception.Code);
    }

    [Test]
    public async Task ABareIndexName_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX year_idx"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        Assert.IsTrue(HasIndex(database, "robots", "year_idx"));
    }

    /// <summary>
    /// The primary key is not a droppable index: its SQL alias names nothing in the index list, so
    /// <c>DROP INDEX</c> cannot reach it, and <c>IF EXISTS</c> turns that into a no-op, not a drop.
    /// </summary>
    [Test]
    public async Task ThePrimaryKey_IsNotDroppedByDropIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        int indexesBefore = database.Schema.Tables["robots"].Indexes!.Count;

        Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, "DROP INDEX robots_pkey ON robots"));
        ExecuteDDLSQLResult result = await Ddl(executor, dbname, "DROP INDEX IF EXISTS robots_pkey ON robots");

        Assert.IsFalse(result.Success);
        Assert.AreEqual(indexesBefore, database.Schema.Tables["robots"].Indexes!.Count);

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT name FROM robots WHERE id = 2");
        Assert.AreEqual(1, rows.Count);
    }

    /// <summary>
    /// <c>IF EXISTS</c> hides only a missing index. An index that exists but that a foreign key needs
    /// is still refused, because the guard runs under the DDL gate before the drop.
    /// </summary>
    [TestCase("DROP INDEX cities_name ON cities")]
    [TestCase("DROP INDEX IF EXISTS cities.cities_name")]
    public async Task AnIndexAForeignKeyNeeds_IsStillRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code);
        Assert.IsTrue(HasIndex(database, "cities", "cities_name"));
    }

    /// <summary>
    /// The IF EXISTS check runs inside the drop, under the DDL gate, not before it. Two concurrent drops
    /// of one index must give exactly one drop and one no-op; a check made outside the gate would let
    /// both pass it and the second fail on an index the first already removed.
    /// </summary>
    [Test]
    public async Task TwoConcurrentDropsIfExists_GiveOneDropAndOneNoOp()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);

        for (int round = 0; round < 5; round++)
        {
            if (round > 0)
                await Ddl(executor, dbname, "CREATE INDEX year_idx ON robots (year)");

            ExecuteDDLSQLResult[] results = await Task.WhenAll(
                Task.Run(() => Ddl(executor, dbname, "DROP INDEX IF EXISTS year_idx ON robots")),
                Task.Run(() => Ddl(executor, dbname, "DROP INDEX IF EXISTS robots.year_idx")));

            Assert.AreEqual(1, results.Count(r => r.Success), $"round {round}: exactly one statement drops the index");
            Assert.IsFalse(HasIndex(database, "robots", "year_idx"));
        }
    }

    [Test]
    public async Task TheDrop_SurvivesCloseAndReopen()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();
        await SetupRobots(executor, dbname);
        await Ddl(executor, dbname, "CREATE INDEX name_idx ON robots (name)");

        await Ddl(executor, dbname, "DROP INDEX year_idx ON robots");
        await Ddl(executor, dbname, "DROP INDEX robots.name_idx");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsFalse(HasIndex(reopened, "robots", "year_idx"));
        Assert.IsFalse(HasIndex(reopened, "robots", "name_idx"));

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT id FROM robots WHERE year = 2000 AND name = 'c'");
        Assert.AreEqual(1, rows.Count);
    }
}

[TestFixture]
[NonParallelizable]
internal sealed class TestDropIndexStandalone : DropIndexScenarios
{
    protected override bool ClusterMode => false;
}

[TestFixture]
[NonParallelizable]
internal sealed class TestDropIndexCluster : DropIndexScenarios
{
    protected override bool ClusterMode => true;
}
