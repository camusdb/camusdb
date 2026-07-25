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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>AS OF SYSTEM TIME</c> time-travel reads: a write after the chosen
/// snapshot must be invisible to the historical read but visible to a plain read, and the invalid
/// shapes (future/malformed value, use inside a read-write transaction) must be rejected.
/// </summary>
[NonParallelizable]
public sealed class TestAsOfSystemTime : SharedNodeBaseTest
{
    private async Task<(string db, DatabaseDescriptor descriptor, CommandExecutor executor)> SetupLeaderboard()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE leaderboard (id OBJECT_ID PRIMARY KEY, name STRING, score INT64)",
            parameters: null));

        return (dbName, db, executor);
    }

    private static async Task RunNonQuery(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs a SELECT under a fresh lock-free read-only snapshot — the shape autocommit uses.</summary>
    private static async Task<List<QueryResultRow>> RunSelect(string dbName, CommandExecutor executor, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        return await cursor.ToListAsync();
    }

    private long NowMillis() =>
        SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;

    [Test]
    public async Task AbsoluteSnapshot_SeesPreUpdateValue()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupLeaderboard();

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO leaderboard (id, name, score) VALUES (gen_id(), \"player1\", 10)");

        await Task.Delay(60);
        long snapshotMs = NowMillis();
        await Task.Delay(60);

        await RunNonQuery(dbName, db, executor, "UPDATE leaderboard SET score = 20 WHERE name = \"player1\"");

        // Historical read at the captured instant still sees the pre-update score.
        List<QueryResultRow> historical = await RunSelect(dbName, executor,
            $"SELECT score FROM leaderboard AS OF SYSTEM TIME {snapshotMs}");
        Assert.AreEqual(1, historical.Count);
        Assert.AreEqual(10L, historical[0].Row["score"].LongValue);

        // Plain read sees the latest committed value.
        List<QueryResultRow> latest = await RunSelect(dbName, executor, "SELECT score FROM leaderboard");
        Assert.AreEqual(1, latest.Count);
        Assert.AreEqual(20L, latest[0].Row["score"].LongValue);
    }

    [Test]
    public async Task RelativeOffset_SeesPreUpdateValue()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupLeaderboard();

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO leaderboard (id, name, score) VALUES (gen_id(), \"player1\", 10)");

        // Insert at t0; update ~1.5s later. A '-1s' snapshot lands between the two, so it sees score 10.
        await Task.Delay(1500);
        await RunNonQuery(dbName, db, executor, "UPDATE leaderboard SET score = 20 WHERE name = \"player1\"");

        List<QueryResultRow> historical = await RunSelect(dbName, executor,
            "SELECT score FROM leaderboard AS OF SYSTEM TIME '-1s'");
        Assert.AreEqual(1, historical.Count);
        Assert.AreEqual(10L, historical[0].Row["score"].LongValue);
    }

    [Test]
    public async Task PlaceholderSnapshot_SeesPreUpdateValue()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupLeaderboard();

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO leaderboard (id, name, score) VALUES (gen_id(), \"player1\", 10)");

        await Task.Delay(60);
        long snapshotMs = NowMillis();
        await Task.Delay(60);

        await RunNonQuery(dbName, db, executor, "UPDATE leaderboard SET score = 20 WHERE name = \"player1\"");

        KvTransaction tx = KvTransaction.CreateReadOnly();
        Dictionary<string, ColumnValue> parameters = new() { ["@ts"] = new ColumnValue(ColumnType.Integer64, snapshotMs) };
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbName, "SELECT score FROM leaderboard AS OF SYSTEM TIME @ts", parameters));
        List<QueryResultRow> historical = await cursor.ToListAsync();

        Assert.AreEqual(1, historical.Count);
        Assert.AreEqual(10L, historical[0].Row["score"].LongValue);
    }

    [Test]
    public async Task FutureSnapshot_Rejected()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupLeaderboard();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunSelect(dbName, executor, "SELECT score FROM leaderboard AS OF SYSTEM TIME '2099-01-01T00:00:00Z'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
    }

    [Test]
    public async Task MalformedValue_Rejected()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupLeaderboard();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunSelect(dbName, executor, "SELECT score FROM leaderboard AS OF SYSTEM TIME '-10x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
    }

    [Test]
    public async Task AsOf_InsideReadWriteTransaction_Rejected()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupLeaderboard();

        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbName, "SELECT score FROM leaderboard AS OF SYSTEM TIME '-10s'", null));
            await cursor.ToListAsync();
        })!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
        await db.Transactions.RollbackAsync(tx);
    }
}
