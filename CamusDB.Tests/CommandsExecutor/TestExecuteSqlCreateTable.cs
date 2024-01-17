
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.Time;
using CamusDB.Core;

namespace CamusDB.Tests.CommandsExecutor;

public class TestExecuteSqlCreateTable : BaseTest
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("factory");
    }

    private async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor)> SetupDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        DatabaseDescriptor descriptor = await executor.CreateDatabase(databaseTicket);

        return (dbname, executor, catalogsManager, descriptor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTable()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        ExecuteSQLTicket createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL)",
            parameters: null
        );

        Assert.IsTrue(await executor.ExecuteDDLSQL(createTableTicket));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.True(tableSchema.Columns![1].NotNull);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);
        Assert.True(tableSchema.Columns![2].NotNull);

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTable2()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        ExecuteSQLTicket createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING DEFAULT (\"hello\"))",
            parameters: null
        );

        Assert.IsTrue(await executor.ExecuteDDLSQL(createTableTicket));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(2, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.AreEqual(0, (new ColumnValue(ColumnType.String, "hello")).CompareTo(tableSchema.Columns![1].DefaultValue));
        Assert.False(tableSchema.Columns![1].NotNull);

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableIfNotExists()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        ExecuteSQLTicket createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE IF NOT EXISTS robots (id OID PRIMARY KEY NOT NULL, name STRING DEFAULT (\"hello\"))",
            parameters: null
        );

        Assert.IsTrue(await executor.ExecuteDDLSQL(createTableTicket));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(2, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.AreEqual(0, (new ColumnValue(ColumnType.String, "hello")).CompareTo(tableSchema.Columns![1].DefaultValue));
        Assert.False(tableSchema.Columns![1].NotNull);

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);

        createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE IF NOT EXISTS robots (id OID PRIMARY KEY NOT NULL, name STRING DEFAULT (\"hello\"))",
            parameters: null
        );

        Assert.IsFalse(await executor.ExecuteDDLSQL(createTableTicket));
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableConstraints()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        ExecuteSQLTicket createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE robots (id OID NOT NULL, name STRING NOT NULL, year INT64 NOT NULL) PRIMARY KEY (id)",
            parameters: null
        );

        Assert.IsTrue(await executor.ExecuteDDLSQL(createTableTicket));

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);
        Assert.True(tableSchema.Columns![0].NotNull);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);
        Assert.True(tableSchema.Columns![1].NotNull);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);
        Assert.True(tableSchema.Columns![2].NotNull);

        ExecuteSQLTicket queryTicket = new(
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );

        List<QueryResultRow> result = await (await executor.ExecuteSQLQuery(queryTicket)).ToListAsync();
        Assert.IsEmpty(result);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExecuteCreateTableDoublePk()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        ExecuteSQLTicket createTableTicket = new(
            database: dbname,
            sql: "CREATE TABLE robots (id OID NOT NULL PRIMARY KEY, name STRING NOT NULL, year INT64 NOT NULL) PRIMARY KEY (id)",
            parameters: null
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteDDLSQL(createTableTicket));
        Assert.AreEqual("Primary key already exists on table 'robots'", exception!.Message);
    }
}