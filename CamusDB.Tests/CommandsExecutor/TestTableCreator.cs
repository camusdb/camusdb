
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.Time;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableCreator : BaseTest
{    
    private async Task<(string, DatabaseDescriptor, CommandExecutor, TransactionsManager, CatalogsManager)> SetupDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        TransactionsManager transactions = new(hlc);
        CommandExecutor executor = new(hlc, validator, catalogs, logger);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        DatabaseDescriptor database = await executor.CreateDatabase(databaseTicket);

        return (dbname, database, executor, transactions, catalogs);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_table",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("age", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CreateTableResult result = await executor.CreateTable(ticket);
        Assert.True(result.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "my_table");

        Assert.AreEqual("my_table", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("age", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_table",
            columns: new ColumnInfo[] { },
            constraints: Array.Empty<ConstraintInfo>(),
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table requires at least one column", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: "",
            tableName: "my_table",
            columns: new ColumnInfo[] {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Database name is required", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableNoTableName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "",
            columns: new ColumnInfo[] {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table name is required", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableDuplicateColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_table",
            columns: new ColumnInfo[] {
                new("id", ColumnType.Id),
                new("id", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Duplicate column name: id", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableDuplicatePrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_table",
            columns: new ColumnInfo[] {
                new("id", ColumnType.Id),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("name", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Primary key already exists on table 'my_table'", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableInvalidTableName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: new('a', 300),
            columns: new ColumnInfo[] {
                new("id", ColumnType.Id),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table name is too long", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableInvalidTableNameCharacters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_täble",
            columns: new ColumnInfo[] {
                new("id", ColumnType.Id),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table name has invalid characters", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableTwice()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_table",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("age", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CreateTableResult result = await executor.CreateTable(ticket);
        Assert.True(result.Success);

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.CreateTable(ticket));
        Assert.AreEqual("Table 'my_table' already exists", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableIfNotExists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "my_table",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("age", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: true
        );

        CreateTableResult result = await executor.CreateTable(ticket);
        Assert.True(result.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "my_table");

        Assert.AreEqual("my_table", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("age", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);

        result = await executor.CreateTable(ticket);
        Assert.False(result.Success);
    }
}
