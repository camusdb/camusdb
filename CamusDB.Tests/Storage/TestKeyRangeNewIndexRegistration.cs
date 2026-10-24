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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Storage;

/// <summary>
/// With key-range routing on, an index added to an open table on a standalone node must have its key
/// space registered before the index becomes writable. Kahuna requires registration before the first
/// write to a space. If the registration waited for the next table open, it would change the routing
/// of a space that already holds entries and write intents placed by hash routing.
/// </summary>
[TestFixture]
// Serial: boots per-test embedded nodes with two partitions, and sets a static test hook.
[NonParallelizable]
public sealed class TestKeyRangeNewIndexRegistration : BaseTest
{
    protected override int NodeInitialPartitions => 2;

    private static async Task ExecuteDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction ddl = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(ddl);
        }
    }

    private bool IsKeyRangeRouted(string keySpace) =>
        TestNode!.Kahuna.GetRangeMap()
            .KeySpaces
            .Any(s => s.RoutingMode == "KeyRange" && s.KeySpace == keySpace);

    [Test]
    public async Task CreateIndexOnAnOpenTable_RegistersTheIndexSpace_BeforeTheIndexIsWritable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = true });

        await ExecuteDdl(database, executor, dbname, "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)");

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, dbname,
            "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)", null));
        await database.Transactions.CommitAsync(insert);

        // The table is open, so the index is added to the live descriptor and no reopen registers it.
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "accounts"));

        string? writeOnlySpace = null;
        bool registeredWhileWriteOnly = false;

        // The hook fires while the index is WriteOnly: concurrent statements already write its entries.
        TableIndexAdder.TestHookBeforePublish = () =>
        {
            TableIndexSchema index = table.Indexes["balance_idx"];
            Assert.That(index.State, Is.EqualTo(SchemaElementState.WriteOnly));

            writeOnlySpace = table.Store.IndexKeySpace(index.KvId);
            registeredWhileWriteOnly = IsKeyRangeRouted(writeOnlySpace);
            return Task.CompletedTask;
        };

        try
        {
            await ExecuteDdl(database, executor, dbname, "CREATE INDEX balance_idx ON accounts (balance)");
        }
        finally
        {
            TableIndexAdder.TestHookBeforePublish = null;
        }

        Assert.That(writeOnlySpace, Is.Not.Null, "the hook must fire during CREATE INDEX");
        Assert.That(registeredWhileWriteOnly, Is.True,
            "the index key space must be key-range routed before the index is writable");

        // The published index answers from the registered space and holds every row.
        KvTransaction read = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(read, dbname,
            "SELECT id FROM accounts@{FORCE_INDEX=balance_idx} WHERE balance >= 100", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(read);

        Assert.That(rows, Has.Count.EqualTo(2));
    }
}
