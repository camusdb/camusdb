/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The standalone arm of <see cref="TestUpdateFromDeleteUsing"/>, which runs in cluster mode: here the
/// engine persists schema and writes in the single-node path, so the join of the read phase, the
/// primary-key lookup and the writes of the write phase run without the replicated schema log.
/// </summary>
[NonParallelizable]
internal sealed class TestUpdateFromDeleteUsingStandalone : BaseTest
{
    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<ExecuteNonSQLResult> Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result;
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

    private static async Task Seed(CommandExecutor executor, DatabaseDescriptor database, string db)
    {
        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64)");
        await Ddl(executor, database, db, "CREATE TABLE changes (cid INT64 PRIMARY KEY, item_id INT64, v INT64)");
        await Dml(executor, database, db, "INSERT INTO items (id, v) SELECT g, 0 FROM generate_series(1, 40) g");
        await Dml(executor, database, db, "INSERT INTO changes (cid, item_id, v) SELECT g, g, g * 10 FROM generate_series(1, 30) g");
    }

    [Test]
    public async Task UpdateFrom_WritesEveryMatchedRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(executor, database, db);

        ExecuteNonSQLResult result = await Dml(executor, database, db,
            "UPDATE items i SET v = i.v + c.v FROM changes c WHERE c.item_id = i.id");

        Assert.AreEqual(30, result.ModifiedRows);
        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, v FROM items ORDER BY id");
        long[] expected = Enumerable.Range(1, 40).Select(i => i <= 30 ? (long)i * 10 : 0L).ToArray();
        CollectionAssert.AreEqual(expected, rows.Select(r => r.Row["v"].LongValue).ToArray());
    }

    [Test]
    public async Task UpdateFrom_ATargetRowThatMatchesTwiceFails()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(executor, database, db);
        await Dml(executor, database, db, "INSERT INTO changes (cid, item_id, v) VALUES (100, 7, 1)");

        CamusDBException error = Assert.ThrowsAsync<CamusDBException>(() => Dml(executor, database, db,
            "UPDATE items i SET v = c.v FROM changes c WHERE c.item_id = i.id"))!;

        StringAssert.Contains("(id)=(7)", error.Message);
        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id FROM items WHERE v <> 0");
        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    public async Task DeleteUsing_DeletesEveryMatchedRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(executor, database, db);

        ExecuteNonSQLResult result = await Dml(executor, database, db,
            "DELETE FROM items i USING changes c WHERE c.item_id = i.id AND c.v > 150");

        Assert.AreEqual(15, result.ModifiedRows);
        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id FROM items ORDER BY id");
        long[] expected = Enumerable.Range(1, 40).Where(i => i <= 15 || i > 30).Select(i => (long)i).ToArray();
        CollectionAssert.AreEqual(expected, rows.Select(r => r.Row["id"].LongValue).ToArray());
    }
}
