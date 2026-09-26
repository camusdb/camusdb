/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Foreign-key DDL through the real SQL entry point, while nothing can store a constraint yet. The
/// statement must parse, reach the validator and be refused, and it must leave nothing behind: no
/// table, and no constraint on an existing table.
/// </summary>
internal static class ForeignKeyDdlRefusalScenarios
{
    public static async Task CreateTableWithAReferenceIsRefusedAndCreatesNothing(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.IsFalse(database.Schema.Tables.ContainsKey("weather"), "A refused CREATE TABLE must not create the table");
    }

    public static async Task UnsupportedClauseIsNamedBeforeTheInterimRefusal(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, FOREIGN KEY (city) REFERENCES cities (name) ON DELETE CASCADE)"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.That(exception.Message, Does.Contain("ON DELETE CASCADE"));
    }

    public static async Task AlterTableAddForeignKeyIsRefusedAndChangesNothing(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await Ddl(executor, dbname, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)");
        long version = database.Schema.SchemaVersion;

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname,
            "ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name)"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.IsNull(database.Schema.Tables["weather"].ForeignKeys);
        Assert.AreEqual(version, database.Schema.SchemaVersion, "A refused ALTER must not advance the schema");
    }

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql) =>
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: sql, parameters: null));
}

/// <summary>Foreign-key DDL refusal on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyDdlRefusal : BaseTest
{
    [Test]
    public async Task CreateTableWithAReferenceIsRefusedAndCreatesNothing() => await Run(ForeignKeyDdlRefusalScenarios.CreateTableWithAReferenceIsRefusedAndCreatesNothing);

    [Test]
    public async Task UnsupportedClauseIsNamedBeforeTheInterimRefusal() => await Run(ForeignKeyDdlRefusalScenarios.UnsupportedClauseIsNamedBeforeTheInterimRefusal);

    [Test]
    public async Task AlterTableAddForeignKeyIsRefusedAndChangesNothing() => await Run(ForeignKeyDdlRefusalScenarios.AlterTableAddForeignKeyIsRefusedAndChangesNothing);

    private async Task Run(System.Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>Foreign-key DDL refusal on a cluster-mode engine, where DDL can be forwarded to the leader.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyDdlRefusalCluster : SharedNodeBaseTest
{
    [Test]
    public async Task CreateTableWithAReferenceIsRefusedAndCreatesNothing() => await Run(ForeignKeyDdlRefusalScenarios.CreateTableWithAReferenceIsRefusedAndCreatesNothing);

    [Test]
    public async Task UnsupportedClauseIsNamedBeforeTheInterimRefusal() => await Run(ForeignKeyDdlRefusalScenarios.UnsupportedClauseIsNamedBeforeTheInterimRefusal);

    [Test]
    public async Task AlterTableAddForeignKeyIsRefusedAndChangesNothing() => await Run(ForeignKeyDdlRefusalScenarios.AlterTableAddForeignKeyIsRefusedAndChangesNothing);

    private async Task Run(System.Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
