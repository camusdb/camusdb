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
/// <c>CREATE TABLE … AS SELECT</c> and <c>INSERT … SELECT</c> with statement parameters in the select
/// list: a parameter has the type of its value, so the created column has the type of the rows the
/// statement stores. This fixture runs in cluster mode on a shared node;
/// <see cref="TestCreateTableAsSelectParametersStandalone"/> runs the same tests in standalone mode.
/// </summary>
[NonParallelizable]
internal class TestCreateTableAsSelectParameters : SharedNodeBaseTest
{
    private static async Task Ddl(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The declared type of each column of <paramref name="table"/>, by name, as <c>SHOW COLUMNS</c> spells it.</summary>
    private static async Task<Dictionary<string, string>> ColumnTypes(
        CommandExecutor executor, DatabaseDescriptor database, string db, string table) =>
        (await Query(executor, database, db, $"SHOW COLUMNS FROM {table}"))
            .ToDictionary(c => c.Row["Field"].StrValue!, c => c.Row["Type"].StrValue!);

    /// <summary>Three vertices, with ids 1 to 3.</summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE vertex (id INT64 PRIMARY KEY, x FLOAT64)");
        await Dml(executor, database, db, "INSERT INTO vertex (id, x) VALUES (1, 1.5), (2, 2.5), (3, 3.5)");
        return (db, database, executor);
    }

    [Test]
    public async Task AnExpressionOverAFloatParameterCreatesAFloatColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db, "CREATE TABLE ctas_probe AS SELECT @px * 1.0 AS c FROM vertex WHERE id < 3",
            new() { ["@px"] = new ColumnValue(ColumnType.Float64, -416.0) });

        Assert.That((await ColumnTypes(executor, database, db, "ctas_probe"))["c"], Is.EqualTo("FLOAT64"));

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT c FROM ctas_probe");
        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows.All(r => r.Row["c"].Type == ColumnType.Float64 && r.Row["c"].FloatValue == -416.0), Is.True);
    }

    [Test]
    public async Task EachParameterTypeGivesItsColumnType()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE TABLE typed AS SELECT @n + id AS a, @s || 'x' AS b, @f AS f, @px AS d, x * @n AS e FROM vertex",
            new()
            {
                ["@n"] = new ColumnValue(ColumnType.Integer64, 10),
                ["@s"] = new ColumnValue(ColumnType.String, "v"),
                ["@f"] = new ColumnValue(ColumnType.Bool, true),
                ["@px"] = new ColumnValue(ColumnType.Float64, 0.5),
            });

        Dictionary<string, string> types = (await ColumnTypes(executor, database, db, "typed"));
        Assert.That(types["a"], Is.EqualTo("INT64"));
        Assert.That(types["b"], Is.EqualTo("STRING"));
        Assert.That(types["f"], Is.EqualTo("BOOL"));
        Assert.That(types["d"], Is.EqualTo("FLOAT64"));
        Assert.That(types["e"], Is.EqualTo("FLOAT64"));

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT a, b, f, d, e FROM typed ORDER BY a");
        CollectionAssert.AreEqual(new long[] { 11, 12, 13 }, rows.Select(r => r.Row["a"].LongValue).ToArray());
        Assert.That(rows.All(r => r.Row["b"].StrValue == "vx" && r.Row["f"].BoolValue && r.Row["d"].FloatValue == 0.5), Is.True);
        CollectionAssert.AreEqual(new[] { 15.0, 25.0, 35.0 }, rows.Select(r => r.Row["e"].FloatValue).ToArray());
    }

    [Test]
    public async Task ANullParameterKeepsTheStringFallback()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db, "CREATE TABLE nulls AS SELECT id AS k, @z AS e FROM vertex",
            new() { ["@z"] = ColumnValue.Null });

        Assert.That((await ColumnTypes(executor, database, db, "nulls"))["e"], Is.EqualTo("STRING"));

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT e FROM nulls");
        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows.All(r => r.Row["e"].Type == ColumnType.Null), Is.True);
    }

    [Test]
    public async Task AFromlessSelectOfAParameterCreatesAFloatColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db, "CREATE TABLE one AS SELECT @px * 1.0 AS c",
            new() { ["@px"] = new ColumnValue(ColumnType.Float64, -416.0) });

        Assert.That((await ColumnTypes(executor, database, db, "one"))["c"], Is.EqualTo("FLOAT64"));
        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT c FROM one");
        Assert.That(rows.Single().Row["c"].FloatValue, Is.EqualTo(-416.0));
    }

    [Test]
    public async Task AnInsertSelectOfAParameterExpressionStoresItsValues()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE TABLE target (id INT64 PRIMARY KEY, c FLOAT64)");

        await Dml(executor, database, db, "INSERT INTO target (id, c) SELECT id, @px * x FROM vertex",
            new() { ["@px"] = new ColumnValue(ColumnType.Float64, 2.0) });

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT c FROM target ORDER BY id");
        CollectionAssert.AreEqual(new[] { 3.0, 5.0, 7.0 }, rows.Select(r => r.Row["c"].FloatValue).ToArray());
    }
}

/// <summary>The tests of <see cref="TestCreateTableAsSelectParameters"/> on an engine in standalone mode.</summary>
[NonParallelizable]
internal sealed class TestCreateTableAsSelectParametersStandalone : TestCreateTableAsSelectParameters
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
