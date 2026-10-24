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

using Kahuna.Shared.Communication.Rest;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Key-range routing applies to table data only. With the flag on, the only key spaces in the range
/// map are a table's row, large-value and index spaces. The catalog family <c>{dbId}/meta</c>, the
/// system keys and the statistics keys stay hash-routed, so a <c>{dbId}/meta</c> scan still reaches
/// one bucket and the schema log stays on the one partition that hashing <c>{dbId}/meta</c> names.
/// </summary>
[TestFixture]
// Serial: boots per-test embedded nodes with two partitions for key-range routing.
[NonParallelizable]
public sealed class TestKeyRangeLeavesMetaHashRouted : BaseTest
{
    protected override int NodeInitialPartitions => 2;

    [Test]
    public async Task WithKeyRangeOn_OnlyTableSpacesAreInTheRangeMap()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = true });

        // Each statement writes a different meta or system key family: the table and its column
        // history, the index, the view, and the collected statistics.
        foreach (string sql in new[]
        {
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)",
            "CREATE INDEX balance_idx ON accounts (balance)",
            "CREATE VIEW rich AS SELECT id FROM accounts WHERE balance > 100",
        })
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

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, dbname,
            "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)", null));
        await database.Transactions.CommitAsync(insert);

        KvTransaction analyze = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(analyze, dbname, "ANALYZE accounts", null));
        await foreach (QueryResultRow _ in cursor) { }
        await database.Transactions.CommitAsync(analyze);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "accounts"));
        string group = table.Store.PlacementGroup;

        List<string> rangeSpaces = TestNode!.Kahuna.GetRangeMap()
            .KeySpaces
            .Where(s => s.RoutingMode == "KeyRange")
            .Select(s => s.KeySpace)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(rangeSpaces, Does.Contain(table.Store.RowKeySpace), "the row space is key-range routed");
            Assert.That(rangeSpaces, Does.Contain(table.Store.IndexKeySpace(table.Indexes["balance_idx"].KvId)),
                "the index space is key-range routed");

            Assert.That(rangeSpaces.Where(s => s.StartsWith(database.Id)),
                Is.All.StartsWith(group + "|"), "every key-range space of this database belongs to the table");
            Assert.That(rangeSpaces, Has.None.Contains("/meta"), "no meta key space is key-range routed");
            Assert.That(rangeSpaces, Has.None.StartsWith("_system"), "no system key space is key-range routed");
            Assert.That(rangeSpaces, Has.None.Contains(":stats:"), "no statistics key space is key-range routed");

            Assert.That(TestNode.ReadPlacementUncached(database.Id + "/meta", out _).IsKeyRange, Is.False,
                "the meta bucket is hash-routed");
        });
    }
}
