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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Equality on the leading column(s) of a composite index is a prefix seek: the bound pins only
/// some key columns, so stored keys carry the remaining columns' encodings after it. On a unique
/// index the scan's raw end key must still be widened with the high sentinel, or the range
/// [enc(prefix), enc(prefix)] matches no key of the form enc(prefix)+enc(rest) and the query
/// silently returns zero rows. That only bites when the leading column has no computable successor
/// value (String / Id), because otherwise the planner caps the scan with an exclusive [v, next(v))
/// bound instead — which is why the Int64 case here passed while the String case did not.
/// </summary>
public sealed class TestCompositePrefixIndexSeek : SharedNodeBaseTest
{
    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
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

    [Test]
    public async Task PrefixEqualityOnCompositeUniquePk_StringLeading()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE vals (owner string(64), tag string(64), n int64, PRIMARY KEY (owner, tag))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO vals (owner, tag, n) VALUES ('a', 't1', 1), ('a', 't2', 2), ('b', 't1', 3)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT owner, tag FROM vals WHERE owner = 'a'");

        Assert.AreEqual(2, rows.Count, "prefix equality on composite unique PK (String leading) must return both rows");
    }

    [Test]
    public async Task PrefixEqualityOnCompositeUniquePk_IdLeading()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // The EAV shape: PRIMARY KEY (owner_id, tag) with an Id-typed leading column.
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE item_values (owner_id oid, tag string(64), n int64, PRIMARY KEY (owner_id, tag))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO item_values (owner_id, tag, n) VALUES "
            + "('65a1f3a1c2e7d50b4f8a91d3', 't1', 1), "
            + "('65a1f3a1c2e7d50b4f8a91d3', 't2', 2), "
            + "('65a1f3a1c2e7d50b4f8a91d4', 't1', 3)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT owner_id, tag FROM item_values WHERE owner_id = '65a1f3a1c2e7d50b4f8a91d3'");

        Assert.AreEqual(2, rows.Count, "prefix equality on composite unique PK (Id leading) must return both rows");
    }

    [Test]
    public async Task PrefixEqualityOnCompositeUniquePk_Int64Leading()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE vals2 (owner int64, tag string(64), n int64, PRIMARY KEY (owner, tag))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO vals2 (owner, tag, n) VALUES (1, 't1', 1), (1, 't2', 2), (2, 't1', 3)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT owner, tag FROM vals2 WHERE owner = 1");

        Assert.AreEqual(2, rows.Count, "prefix equality on composite unique PK (Int64 leading) must return both rows");
    }

    [Test]
    public async Task PrefixEqualityOnCompositeMultiIndex_StringLeading()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE vals3 (id int64 primary key, owner string(64), tag string(64))");
        await ExecDdl(database, executor, dbname,
            "CREATE INDEX vals3_owner_tag ON vals3 (owner, tag)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO vals3 (id, owner, tag) VALUES (1, 'a', 't1'), (2, 'a', 't2'), (3, 'b', 't1')");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT owner, tag FROM vals3 WHERE owner = 'a'");

        Assert.AreEqual(2, rows.Count, "prefix equality on composite multi index (String leading) must return both rows");
    }
}
