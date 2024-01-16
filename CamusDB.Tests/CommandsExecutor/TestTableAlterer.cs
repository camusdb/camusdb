
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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestTableAlterer
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("test");
    }

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor)> SetupDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogs = new();

        CommandExecutor executor = new(hlc, validator, catalogs);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        DatabaseDescriptor descriptor = await executor.CreateDatabase(databaseTicket);

        return (dbname, executor, catalogs, descriptor);
    }

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor)> SetupEmptyTable()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket createTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);

        return (dbname, executor, catalogs, database);
    }

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor, List<string> objectsId)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupEmptyTable();

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "name", new ColumnValue(ColumnType.String, "some name " + i) },
                    { "year", new ColumnValue(ColumnType.Integer64, 2000 + i) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, catalogs, database, objectsId);
    }

    private static async Task<(string, CommandExecutor, CatalogsManager, DatabaseDescriptor, List<string> objectsId)> SetupTableRepeatedData()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupEmptyTable();

        List<string> objectsId = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: dbname,
                tableName: "robots",
                values: new Dictionary<string, ColumnValue>()
                {
                    { "id", new ColumnValue(ColumnType.Id, objectId) },
                    { "name", new ColumnValue(ColumnType.String, "some name") },
                    { "year", new ColumnValue(ColumnType.Integer64, 2000) },
                    { "enabled", new ColumnValue(ColumnType.Bool, false) },
                }
            );

            await executor.Insert(ticket);

            objectsId.Add(objectId);
        }

        return (dbname, executor, catalogs, database, objectsId);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndDropColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(ticket);

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

        AlterTableTicket alterTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.DropColumn,
            new ColumnInfo("name", ColumnType.Null)
        );

        await executor.AlterTable(alterTableTicket);

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

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

        AlterTableTicket alterTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.DropColumn,
            new ColumnInfo("name", ColumnType.Null)
        );

        await executor.AlterTable(alterTableTicket);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(3, tableSchema.Columns!.Count);

        QueryTicket queryTicket = new(
           txnId: await executor.NextTxnId(),
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

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
            Assert.AreEqual(3, resultRow.Row.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddExistingColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(ticket);

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

        AlterTableTicket alterTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            new ColumnInfo("name", ColumnType.Integer64)
        );

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterTable(alterTableTicket));
        Assert.AreEqual("Duplicate column 'name'", e!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddNewColumn()
    {
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupDatabase();

        CreateTableTicket ticket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(ticket);

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

        AlterTableTicket alterTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            new ColumnInfo("type", ColumnType.Integer64)
        );

        await executor.AlterTable(alterTableTicket);

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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

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

        AlterTableTicket alterTableTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            new ColumnInfo("type", ColumnType.Integer64)
        );

        await executor.AlterTable(alterTableTicket);

        tableSchema = catalogs.GetTableSchema(database, "robots");

        Assert.AreEqual("robots", tableSchema.Name);
        Assert.AreEqual(1, tableSchema.Version);

        Assert.AreEqual(5, tableSchema.Columns!.Count);

        Assert.AreEqual("type", tableSchema.Columns![4].Name);
        Assert.AreEqual(ColumnType.Integer64, tableSchema.Columns![4].Type);

        QueryTicket queryTicket = new(
           txnId: await executor.NextTxnId(),
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

        List<QueryResultRow> result = await (await executor.Query(queryTicket)).ToListAsync();
        Assert.IsNotEmpty(result);

        foreach (QueryResultRow resultRow in result)
        {
            Assert.AreEqual(5, resultRow.Row.Count);
            Assert.AreEqual(ColumnType.Null, resultRow.Row["type"].Type);
        }

        UpdateTicket updateTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            plainValues: new Dictionary<string, ColumnValue>()
            {
                { "type", new ColumnValue(ColumnType.Integer64, 100) }
            },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null
        );

        Assert.AreEqual(25, await executor.Update(updateTicket));

        queryTicket = new(
           txnId: await executor.NextTxnId(),
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

        result = await (await executor.Query(queryTicket)).ToListAsync();
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
            operation: AlterIndexOperation.AddIndex
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columnName: "year",
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupBasicTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupTableRepeatedData();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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
        (string dbname, CommandExecutor executor, CatalogsManager catalogs, DatabaseDescriptor database, _) = await SetupTableRepeatedData();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
            operation: AlterIndexOperation.AddUniqueIndex
        );

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterIndex(alterIndexTicket));
        Assert.True(exception!.Message.StartsWith("Duplicate entry for key \"name_idx\""));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateTableAndAddDropIndex()
    {
        (string dbname, CommandExecutor executor, CatalogsManager _, DatabaseDescriptor _) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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

        alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columnName: "name",
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
        (string dbname, CommandExecutor executor, CatalogsManager _, DatabaseDescriptor _) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "~pk",
            columnName: "",
            operation: AlterIndexOperation.DropPrimaryKey
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

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
        (string dbname, CommandExecutor executor, CatalogsManager _, DatabaseDescriptor _) = await SetupEmptyTable();

        AlterIndexTicket alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "~pk",
            columnName: "",
            operation: AlterIndexOperation.DropPrimaryKey
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        OpenTableTicket openTableTicket = new(
            databaseName: dbname,
            tableName: "robots"
        );

        TableDescriptor table = await executor.OpenTable(openTableTicket);
        Assert.False(table.Indexes.ContainsKey("~pk"));

        alterIndexTicket = new(
            txnId: await executor.NextTxnId(),
            databaseName: dbname,
            tableName: "robots",
            indexName: "~pk",
            columnName: "id",
            operation: AlterIndexOperation.AddPrimaryKey
        );

        Assert.True(await executor.AlterIndex(alterIndexTicket));

        table = await executor.OpenTable(openTableTicket);
        Assert.True(table.Indexes.ContainsKey("~pk"));
    }
}
