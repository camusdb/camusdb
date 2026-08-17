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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The distributed-execution flag must be inert on a standalone node: no Gather appears in any
/// plan (a standalone node has exactly one span by construction) and queries behave exactly as
/// with the flag off. This is the byte-identical-fallback guarantee's cheap standalone half;
/// the cluster half (fragmentation actually engaging after a split) lives in the cluster test
/// project.
/// </summary>
[NonParallelizable]
public sealed class TestDistributedQueryGating : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DistributedQueryExecutionEnabled = true, KeyRangeShardingEnabled = true };

    [Test]
    public async Task StandaloneWithFlagOn_NoGatherInPlan_AndQueriesWork()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("num", ColumnType.Integer64, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 20; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn, databaseName: dbname, tableName: "items",
                values: new() { new() {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "num", new(ColumnType.Integer64, (long)i) },
                }}));
        }
        await database.Transactions.CommitAsync(txn);

        KvTransaction queryTx = await database.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: queryTx, database: dbname, sql: "SELECT id, num FROM items", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(20, rows.Count);

        (_, IAsyncEnumerable<QueryResultRow> explainCursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: queryTx, database: dbname, sql: "EXPLAIN SELECT id, num FROM items", parameters: null));
        List<QueryResultRow> explain = await explainCursor.ToListAsync();

        Assert.IsFalse(
            explain.Any(r => r.Row.Values.Any(v => v.StrValue?.Contains("gather") == true)),
            "A standalone node must never plan a Gather, even with the distribution flag on");

        // Eligibility must be observable: with the flag on, EXPLAIN says why the plan stayed
        // local instead of leaving the reader to guess.
        Assert.IsTrue(
            explain.Any(r => r.Row.Values.Any(v => v.StrValue == "distributed=no (standalone node)")),
            "EXPLAIN must carry the distribution row with the standalone skip reason. Got: "
            + string.Join(" | ", explain.Select(r => string.Join(",", r.Row.Values.Select(v => v.StrValue)))));

        await database.Transactions.CommitAsync(queryTx);
    }
}
