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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage of the rule that SQL UPDATE and DELETE need a WHERE clause: the statement is
/// refused with an error that names the rule, nothing changes, and <c>WHERE TRUE</c> is the way to
/// touch every row on purpose.
/// </summary>
[NonParallelizable]
public sealed class TestUpdateDeleteRequireWhere : SharedNodeBaseTest
{
    private static async Task Exec(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
            else
                _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<long>> Values(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, "SELECT v FROM items ORDER BY id", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows.Select(r => r.Row["v"].LongValue).ToList();
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Exec(database, executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64)", ddl: true);
        await Exec(database, executor, dbname, "INSERT INTO items (id, v) VALUES (1, 10), (2, 20), (3, 30)");
        return (dbname, database, executor);
    }

    [Test]
    public async Task UpdateWithoutWhere_IsRefused_AndNoRowChanges()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(database, executor, dbname, "UPDATE items SET v = 0"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.StartsWith("UPDATE requires a WHERE clause", ex.Message);
        CollectionAssert.AreEqual(new[] { 10L, 20L, 30L }, await Values(database, executor, dbname));
    }

    [Test]
    public async Task DeleteWithoutWhere_IsRefused_AndNoRowIsDeleted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(database, executor, dbname, "DELETE FROM items"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.StartsWith("DELETE requires a WHERE clause", ex.Message);
        CollectionAssert.AreEqual(new[] { 10L, 20L, 30L }, await Values(database, executor, dbname));
    }

    [Test]
    public async Task WhereTrue_UpdatesAndDeletesEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        await Exec(database, executor, dbname, "UPDATE items SET v = 0 WHERE TRUE");
        CollectionAssert.AreEqual(new[] { 0L, 0L, 0L }, await Values(database, executor, dbname));

        await Exec(database, executor, dbname, "DELETE FROM items WHERE TRUE");
        CollectionAssert.IsEmpty(await Values(database, executor, dbname));
    }
}
