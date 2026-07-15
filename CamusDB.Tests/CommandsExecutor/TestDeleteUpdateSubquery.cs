
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DELETE / UPDATE predicates (and UPDATE SET values) may contain uncorrelated scalar / IN /
/// NOT IN subqueries. These used to throw "&lt;kind&gt; subquery must be resolved before expression
/// evaluation" because only SELECT ran the subquery rewrite; these tests pin the anti-join DELETE
/// bug plus the semi-join, scalar-SET, and no-op (plain WHERE) paths.
/// </summary>
[NonParallelizable]
public sealed class TestDeleteUpdateSubquery : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // sessions: some referenced by messages, some orphaned.
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE sessions (id int64 primary key, tag string(64) not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE messages (id int64 primary key, sessionid int64 not null)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO sessions (id, tag) VALUES (1, 'keep'), (2, 'keep'), (3, 'orphan'), (4, 'orphan')");
        // messages reference sessions 1 and 2 only.
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO messages (id, sessionid) VALUES (10, 1), (11, 1), (12, 2)");

        return (dbname, database, executor);
    }

    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static List<long> RemainingSessionIds(List<QueryResultRow> rows)
        => rows.Select(r => r.Row["id"].LongValue).OrderBy(x => x).ToList();

    [Test]
    public async Task Delete_NotInSubquery_RemovesAntiJoinRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await ExecNonQuery(database, executor, dbname,
            "DELETE FROM sessions WHERE id NOT IN (SELECT sessionid FROM messages)");

        Assert.AreEqual(2, deleted); // sessions 3 and 4 have no messages
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM sessions");
        CollectionAssert.AreEqual(new List<long> { 1, 2 }, RemainingSessionIds(rows));
    }

    [Test]
    public async Task Delete_InSubquery_RemovesSemiJoinRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await ExecNonQuery(database, executor, dbname,
            "DELETE FROM sessions WHERE id IN (SELECT sessionid FROM messages)");

        Assert.AreEqual(2, deleted); // sessions 1 and 2 are referenced
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM sessions");
        CollectionAssert.AreEqual(new List<long> { 3, 4 }, RemainingSessionIds(rows));
    }

    [Test]
    public async Task Update_WhereNotInSubquery_UpdatesAntiJoinRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int updated = await ExecNonQuery(database, executor, dbname,
            "UPDATE sessions SET tag = 'dead' WHERE id NOT IN (SELECT sessionid FROM messages)");

        Assert.AreEqual(2, updated);
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM sessions WHERE tag = 'dead'");
        CollectionAssert.AreEqual(new List<long> { 3, 4 }, RemainingSessionIds(rows));
    }

    [Test]
    public async Task Update_ScalarSubqueryInSetValue_WritesScalarResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // messages has 3 rows; write that count into every session's tag-as-number via a numeric column.
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE counters (id int64 primary key, n int64)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO counters (id, n) VALUES (1, 0)");

        int updated = await ExecNonQuery(database, executor, dbname,
            "UPDATE counters SET n = (SELECT COUNT(*) FROM messages) WHERE id = 1");

        Assert.AreEqual(1, updated);
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT n FROM counters WHERE id = 1");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(3L, rows[0].Row["n"].LongValue);
    }

    [Test]
    public async Task Delete_PlainWhere_StillWorks()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await ExecNonQuery(database, executor, dbname,
            "DELETE FROM sessions WHERE id = 3");

        Assert.AreEqual(1, deleted);
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM sessions");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 4 }, RemainingSessionIds(rows));
    }
}
