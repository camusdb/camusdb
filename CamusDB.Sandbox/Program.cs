// See https://aka.ms/new-console-template for more information

/*using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using Microsoft.Extensions.Logging;

Console.WriteLine("Hello, World!");

int i;
(string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupMultiIndexTable();

string[] userIds = new string[5];
for (i = 0; i < 5; i++)
    userIds[i] = ObjectIdGenerator.Generate().ToString();

List<string> objectIds = new(50);

for (i = 0; i < 50; i++)
{
    TransactionState txnState = await transactions.Start();
            
    string objectId = ObjectIdGenerator.Generate().ToString();
    objectIds.Add(objectId);

    InsertTicket insertTicket = new(
        txnState: txnState,
        databaseName: dbname,
        tableName: "user_robots",
        values: new()
        {
            new()
            {
                { "id", new(ColumnType.Id, objectId) },
                { "usersId", new(ColumnType.Id, userIds[i % 5]) },
                { "amount", new(ColumnType.Integer64, 50) },
            }
        }
    );

    await executor.Insert(insertTicket);

    await transactions.Commit(database, txnState);

    if ((i + 1) % 5 == 0)
    {
        CloseDatabaseTicket closeTicket = new(dbname);
        await executor.CloseDatabase(closeTicket);
    }
}

TransactionState txnState2 = await transactions.Start();

i = 0;

foreach (string objectId in objectIds)
{
    QueryByIdTicket queryTicket = new(
        txnState: txnState2,
        databaseName: dbname,
        tableName: "user_robots",
        id: objectId
    );

    List<Dictionary<string, ColumnValue>> result = await (await executor.QueryById(queryTicket)).ToListAsync();

    Dictionary<string, ColumnValue> row = result[0];

    //Assert.AreEqual(ColumnType.Id, row["id"].Type);
    //Assert.AreEqual(24, row["id"].StrValue!.Length);            

    i++;
}

//Assert.AreEqual(50, i);

async Task<(string, DatabaseDescriptor, CommandExecutor, TransactionsManager)> SetupDatabase()
{
    ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
    {
        builder.AddFilter("Camus", LogLevel.Debug).AddConsole();
    });

    ILogger<ICamusDB> logger = loggerFactory.CreateLogger<ICamusDB>();
    
    string dbname = Guid.NewGuid().ToString("n");

    HybridLogicalClock hlc = new();
    CommandValidator validator = new();
    CatalogsManager catalogsManager = new(logger);
    TransactionsManager transactions = new(hlc);
    CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

    CreateDatabaseTicket databaseTicket = new(
        name: dbname,
        ifNotExists: false
    );

    DatabaseDescriptor database = await executor.CreateDatabase(databaseTicket);

    return (dbname, database, executor, transactions);
}

async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions)> SetupMultiIndexTable()
{
    (string dbname, DatabaseDescriptor database, CommandExecutor executor, TransactionsManager transactions) = await SetupDatabase();

    TransactionState txnState = await transactions.Start();

    CreateTableTicket tableTicket = new(
        txnState: txnState,
        databaseName: dbname,
        tableName: "user_robots",
        columns: new ColumnInfo[]
        {
            new("id", ColumnType.Id),
            new("usersId", ColumnType.Id, notNull: true),
            new("amount", ColumnType.Integer64)
        },            
        constraints: new ConstraintInfo[]
        {
            new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            new(ConstraintType.IndexMulti, "usersId", new ColumnIndexInfo[] { new("usersId", OrderType.Ascending) })
        },
        ifNotExists: false
    );

    await executor.CreateTable(tableTicket);

    await transactions.Commit(database, txnState);

    return (dbname, database, executor, transactions);
}*/

using System.Diagnostics;
using FASTER.core;

using var settings = new FasterKVSettings<string, long>("/tmp/xdb") { TryRecoverLatest = true };
using var store = new FasterKV<string, long>(settings);

Console.WriteLine($"Recovered store to version {store.RecoveredVersion}");

var x = new SimpleFunctions<string, long>((a, b) => a + b);

using var session = store.NewSession(x);

/*long key = 1, value = 1, input = 10, output = 0;

session.Upsert(ref key, ref value);

session.Read(ref key, ref output);
Debug.Assert(output == value);

session.RMW(ref key, ref input);
session.RMW(ref key, ref input, ref output);

Debug.Assert(output == value + 20);*/

long value = System.Random.Shared.NextInt64();

string key = "1000";
//long output = 0;

//session.Upsert(ref key, ref value);

//(Status status, long output) status = (await session.ReadAsync(ref key, ref output)).Complete();
//Console.WriteLine("{0} {1}", status.output, output);

var status = (await session.ReadAsync(key)).Complete();

//if (status.Status.NotFound)

Console.WriteLine("{0} {1} {2}", status.status, status.output, value);

//Debug.Assert(status.output == value);

/*if (status == )
{
    Console.WriteLine("(0) Success! {0}", output);
}*/

//await session.WaitForCommitAsync();

store.Log.FlushAndEvict(true);

/*session.Read(ref key, ref output);

status = session.Read(ref key, ref output);
if (status.Found)
{
    Console.WriteLine("(1) Success! {0}", output);
}
else
{
    if (status.IsPending)
    {
        session.CompletePendingWithOutputs(out var iter, true);
        while (iter.Next())
        {
            if (iter.Current.Status.Found && iter.Current.Output == value)
            {
                Console.WriteLine("(2) Success! {0} {1}", value, iter.Current.Output);
                break;
            }
            else
                Console.WriteLine("(2) Error!");
        }
        iter.Dispose();
    }
    else
        Console.WriteLine("(2) Error!");
}
*/