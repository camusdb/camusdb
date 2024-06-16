
using NUnit.Framework;

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableAlterer : BaseTest
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

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs)> SetupEmptyTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();

        TransactionState txnState = await transactions.Start();

        CreateTableTicket createTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);
        
        await transactions.Commit(database, txnState);

        return (dbname, database, executor, transactions, catalogs);
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor, TransactionsManager, CatalogsManager, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name " + i) },
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }
        
        await transactions.Commit(database, txnState);

        return (dbname, database, executor, transactions, catalogs, objectsId);
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor, TransactionsManager, CatalogsManager, List<string> objectsId)> SetupTableRepeatedData()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name") },
                        { "year", new(ColumnType.Integer64, 2000) },
                        { "enabled", new(ColumnType.Bool, false) },
                    }
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }
        
        await transactions.Commit(database, txnState);

        return (dbname, database, executor, transactions, catalogs, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndDropColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(ticket);
        
        await transactions.Commit(database, txnState);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
        
        txnState = await transactions.Start();

        AlterTableTicket alterTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.DropColumn,
            new("name", ColumnType.Null)
        );

        await executor.AlterTable(alterTableTicket);
        
        await transactions.Commit(database, txnState);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);        

        Assert.AreEqual("year", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![1].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![2].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndDropColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupBasicTable();

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
        
        TransactionState txnState = await transactions.Start();

        AlterTableTicket alterTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.DropColumn,
            new("name", ColumnType.Null)
        );

        await executor.AlterTable(alterTableTicket);
        
        await transactions.Commit(database, txnState);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);
        
        txnState = await transactions.Start();

        QueryTicket queryTicket = new(
           txnState: txnState,
           txnType: TransactionType.ReadOnly,
           databaseName: dbname,
           tableName: "robots",
           index: null,
           projection: null,
           where: null,
           filters: null,
           orderBy: null,
           limit: null,
           offset: null,
           parameters: null
        );

        //List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        //Assert.IsNotEmpty(result);
        
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);
        
        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
            Assert.AreEqual(3, resultRow.Row.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddExistingColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(ticket);
        
        await transactions.Commit(database, txnState);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
        
        txnState = await transactions.Start();

        AlterTableTicket alterTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            new("name", ColumnType.Integer64)
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterTable(alterTableTicket));
        Assert.AreEqual("Duplicate column 'name'", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddNewColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupDatabase();
        
        TransactionState txnState = await transactions.Start();

        CreateTableTicket ticket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(ticket);
        
        await transactions.Commit(database, txnState);

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
        
        txnState = await transactions.Start();

        AlterTableTicket alterTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            new("type", ColumnType.Integer64)
        );

        await executor.AlterTable(alterTableTicket);
        
        await transactions.Commit(database, txnState);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("type", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndAddColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupBasicTable();

        TableSchema tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(0, tableSchema.Version);

        Assert.AreEqual(4, tableSchema.Columns!.Count);

        Assert.AreEqual("id", tableSchema.Columns![0].Name);
        Assert.AreEqual(ColumnType.Id, tableSchema.Columns![0].Type);

        Assert.AreEqual("name", tableSchema.Columns![1].Name);
        Assert.AreEqual(ColumnType.String, tableSchema.Columns![1].Type);

        Assert.AreEqual("year", tableSchema.Columns![2].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![2].Type);

        Assert.AreEqual("enabled", tableSchema.Columns![3].Name);
        Assert.AreEqual(ColumnType.Bool, tableSchema.Columns![3].Type);
        
        TransactionState txnState = await transactions.Start();

        AlterTableTicket alterTableTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            new("type", ColumnType.Integer64)
        );

        await executor.AlterTable(alterTableTicket);
        
        await transactions.Commit(database, txnState);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("type", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);
        
        txnState = await transactions.Start();

        QueryTicket queryTicket = new(
            txnState: txnState,
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);
        
        List<QueryResultRow> result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
        {
            Assert.AreEqual(5, resultRow.Row.Count);
            Assert.AreEqual(ColumnType.Null, resultRow.Row["type"].Type);
        }

        UpdateTicket updateTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new()
            {
                { "type", new(ColumnType.Integer64, 100) }
            },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null
        );

        UpdateResult execResult = await executor.Update(updateTicket);
        Assert.AreEqual(25, execResult.UpdatedRows);

        queryTicket = new(
            txnState: txnState,
            txnType: TransactionType.ReadOnly,
            databaseName: dbname,
            tableName: "robots",
            index: null,
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (DatabaseDescriptor _, cursor) = await executor.Query(queryTicket);
        
        result = await cursor.ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
        {
            Assert.AreEqual(5, resultRow.Row.Count);
            Assert.AreEqual(ColumnType.Integer64, resultRow.Row["type"].Type);
            Assert.AreEqual(100, resultRow.Row["type"].LongValue);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.TryGetValue("name_idx", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Multi, index!.Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddDuplicatedIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterIndex(alterIndexTicket));
        Assert.AreEqual("Index 'name_idx' already exists on table 'robots'", exception!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddTwoIndexes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));
        
        await transactions.Commit(database, txnState);
        
        txnState = await transactions.Start();

        alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);

        Assert.True(table.Indexes.TryGetValue("name_idx", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Multi, index!.Type);

        Assert.True(table.Indexes.TryGetValue("year_idx", out index));
        Assert.AreEqual(IndexType.Multi, index!.Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillAndAddIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupBasicTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.TryGetValue("name_idx", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Multi, index!.Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillRepeatedAndAddIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupTableRepeatedData();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.TryGetValue("name_idx", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Multi, index!.Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddUniqueIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddUniqueIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.TryGetValue("name_idx", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Unique, index!.Type);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableFillRepeatedAndAddUniqueIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs, _) = await SetupTableRepeatedData();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddUniqueIndex
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterIndex(alterIndexTicket));
        Assert.True(exception!.Message.StartsWith("Duplicate entry for key 'name_idx'"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddDropIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));
        
        txnState = await transactions.Start();

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.TryGetValue("name_idx", out TableIndexSchema? index));
        Assert.AreEqual(IndexType.Multi, index!.Type);

        alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.DropIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        table = await executor.OpenTable(openTableTicket);
        Assert.False(table.Indexes.ContainsKey("name_idx"));        
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddDropPrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "~pk",
            columns: Array.Empty<ColumnIndexInfo>(),
            operation: AlterIndexOperation.DropPrimaryKey
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));
        
        await transactions.Commit(database, txnState);

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.False(table.Indexes.ContainsKey("~pk"));        
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddDropPrimaryKeyAndAddItAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions, CatalogsManager catalogs) = await SetupEmptyTable();
        
        TransactionState txnState = await transactions.Start();

        AlterIndexTicket alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "~pk",
            columns: Array.Empty<ColumnIndexInfo>(),
            operation: AlterIndexOperation.DropPrimaryKey
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));
        
        await transactions.Commit(database, txnState);

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.False(table.Indexes.ContainsKey("~pk"));
        
        txnState = await transactions.Start();

        alterIndexTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            indexName: "~pk",
            columns: new ColumnIndexInfo[] { new("id", OrderType.Ascending) },
            operation: AlterIndexOperation.AddPrimaryKey
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.ContainsKey("~pk"));
    }
}
