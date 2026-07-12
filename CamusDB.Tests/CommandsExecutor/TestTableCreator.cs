
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Linq;
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

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
internal sealed class TestTableCreator : SharedNodeBaseTest
{    
    private async Task<(string, DatabaseDescriptor, CommandExecutor, CatalogsManager)> SetupDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return (dbname, database, executor, new CatalogsManager(logger));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        Assert.That(e!.Message, Does.StartWith("Table name '") & Does.Contain("is too long"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableInvalidTableNameCharacters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();
        
        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket ticket = new(
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

    // -----------------------------------------------------------------------
    // Short base-62 table id allocation
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task CreateTable_AssignsShortBase62TableId()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        CreateTableTicket ticket = new(
            databaseName: dbname,
            tableName: "short_id_table",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("val", ColumnType.String)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        CreateTableResult result = await executor.CreateTable(ticket);
        Assert.True(result.Success);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "short_id_table");
        string tableId = tableSchema.Id!;

        // Must be non-empty and contain only base-62 characters
        Assert.IsNotEmpty(tableId);
        const string Base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Assert.IsTrue(tableId.All(c => Base62Chars.Contains(c)),
            $"Table id '{tableId}' contains non-base62 characters");

        // Must not contain KV key separators
        Assert.IsFalse(tableId.Contains('/'), $"Table id '{tableId}' must not contain '/'");
        Assert.IsFalse(tableId.Contains(':'), $"Table id '{tableId}' must not contain ':'");
        Assert.IsFalse(tableId.Contains('~'), $"Table id '{tableId}' must not contain '~'");

        // Must be shorter than a 24-hex ObjectId (the old scheme was always exactly 24 chars)
        Assert.Less(tableId.Length, 24,
            $"Table id '{tableId}' should be shorter than the 24-char ObjectId it replaces");
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_TableIdsAreMonotonicAcrossCreates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs) = await SetupDatabase();

        async Task<string> CreateAndGetId(string name)
        {
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: name,
                new ColumnInfo[] { new("id", ColumnType.Id) },
                constraints: new ConstraintInfo[]
                {
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
                },
                ifNotExists: false
            ));
            return catalogs.GetTableSchema(database, name).Id!;
        }

        string id1 = await CreateAndGetId("t1");
        string id2 = await CreateAndGetId("t2");
        string id3 = await CreateAndGetId("t3");

        // Each successive table gets a strictly larger encoded counter value
        static bool IsLess(string a, string b) =>
            a.Length < b.Length || (a.Length == b.Length && string.CompareOrdinal(a, b) < 0);

        Assert.IsTrue(IsLess(id1, id2), $"id1='{id1}' must be < id2='{id2}'");
        Assert.IsTrue(IsLess(id2, id3), $"id2='{id2}' must be < id3='{id3}'");
    }
}
