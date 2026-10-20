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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// What a concurrent writer of the source table does to an <c>INSERT … SELECT</c> at Read Committed,
/// per locking mode.
///
/// <para>Under optimistic locking every key a read touches is a commit dependency, at every isolation
/// level: the scan registers each key it visits with the coordinator, and the commit is refused when
/// the committed revision of any of them moved. The dependency is on the row key, so a write to a
/// column the copy never reads counts, and it covers every row the scan passed over, including rows
/// the <c>WHERE</c> clause rejected — the filter runs in the engine after the store returned the page.
/// That is the optimistic contract (<c>OptimisticUpdate_RejectedWhenATableScanReadGoesStale</c> pins
/// it for a plain <c>SELECT</c>); this fixture pins its consequence for a bulk copy and the two ways
/// out of it that keep the statement's semantics: a pessimistic transaction, whose Read Committed scan
/// records nothing, and a time-travel source, which reads a snapshot no later write can disturb.</para>
///
/// <para>The interleavings are sequenced rather than raced: the peer's write always lands after the
/// copy read its source and before the copy commits, so the verdict comes only from commit-time
/// validation and never from write-time lock contention.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInsertSelectOptimisticSourceDependencies : SharedNodeBaseTest
{
    private const string CopyStatement =
        "INSERT INTO workload_holds (id, account_id, amount) " +
        "SELECT id, id, owner FROM workload_accounts WHERE owner < 50";

    /// <summary>
    /// A write to a row the copy does not select, of a column the copy does not read. Under optimistic
    /// locking it still invalidates the copy, because the scan visited the row.
    /// </summary>
    private const string PeerWrite = "UPDATE workload_accounts SET balance = 5 WHERE id = 4";

    /// <summary>
    /// <c>workload_accounts(id, owner, balance)</c> with four rows, two of them with <c>owner &lt; 50</c>,
    /// and an empty <c>workload_holds(id, account_id, amount)</c>. <c>owner</c> is deliberately not
    /// indexed, so the copy's source is a full table scan that visits all four rows.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE workload_accounts (id INT64 NOT NULL PRIMARY KEY, owner INT64 NOT NULL, balance INT64 NOT NULL)");
        await ExecDDL(executor, database, dbname,
            "CREATE TABLE workload_holds (id INT64 NOT NULL PRIMARY KEY, account_id INT64 NOT NULL, amount INT64 NOT NULL)");

        KvTransaction seed = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, seed,
            "INSERT INTO workload_accounts (id, owner, balance) VALUES (1, 10, 1000), (2, 20, 1000), (3, 60, 1000), (4, 70, 1000)");
        await database.Transactions.CommitAsync(seed);

        return (dbname, database, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecIn(CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static Task<KvTransaction> BeginReadCommittedAsync(DatabaseDescriptor database, KeyValueTransactionLocking locking)
        => database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: locking);

    /// <summary>The peer runs in its own default (pessimistic) transaction and commits at once.</summary>
    private static async Task CommitPeerWriteAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction peer = await database.Transactions.BeginAsync();
        Assert.That(await ExecIn(executor, dbname, peer, PeerWrite), Is.EqualTo(1));
        await database.Transactions.CommitAsync(peer);
    }

    private long NowMillis() =>
        SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;

    [Test]
    public async Task OptimisticCopy_IsRefusedAtCommit_WhenAPeerRewritesASourceRowTheCopyDidNotSelect()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction copy = await BeginReadCommittedAsync(database, KeyValueTransactionLocking.Optimistic);
        try
        {
            Assert.That(await ExecIn(executor, dbname, copy, CopyStatement), Is.EqualTo(2),
                "two of the four accounts have owner < 50");

            await CommitPeerWriteAsync(executor, database, dbname);

            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
                async () => await database.Transactions.CommitAsync(copy),
                "the copy's source scan visited row 4, so the peer's rewrite of it must refuse the commit");

            Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionConflict),
                "a stale read dependency is a retryable serialization failure: " + ex.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(copy);
        }

        Assert.That(await Select(executor, database, dbname, "SELECT id FROM workload_holds"), Is.Empty,
            "a refused copy must leave no row behind");
    }

    [Test]
    public async Task PessimisticCopy_CommitsAfterTheSamePeerWrite()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction copy = await BeginReadCommittedAsync(database, KeyValueTransactionLocking.Pessimistic);
        try
        {
            Assert.That(await ExecIn(executor, dbname, copy, CopyStatement), Is.EqualTo(2));

            await CommitPeerWriteAsync(executor, database, dbname);

            // A Read Committed pessimistic scan carries no identity and registers nothing: the copy is
            // a non-repeatable read, which is what this level permits, and the peer's write is irrelevant
            // to its commit.
            await database.Transactions.CommitAsync(copy);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(copy);
        }

        Assert.That(await Select(executor, database, dbname, "SELECT id FROM workload_holds"), Has.Count.EqualTo(2));
    }

    [Test]
    public async Task TimeTravelCopy_UnderOptimisticLocking_CommitsAfterTheSamePeerWrite()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // A snapshot strictly after the seed commit and strictly before the peer's write.
        await Task.Delay(60);
        long snapshotMs = NowMillis();
        await Task.Delay(60);

        KvTransaction copy = await BeginReadCommittedAsync(database, KeyValueTransactionLocking.Optimistic);
        try
        {
            // The historical source reads through its own snapshot and folds nothing; only the writes
            // belong to the optimistic transaction.
            Assert.That(await ExecIn(executor, dbname, copy,
                "INSERT INTO workload_holds (id, account_id, amount) " +
                $"SELECT id, id, owner FROM workload_accounts AS OF SYSTEM TIME {snapshotMs} WHERE owner < 50"), Is.EqualTo(2));

            await CommitPeerWriteAsync(executor, database, dbname);

            await database.Transactions.CommitAsync(copy);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(copy);
        }

        List<QueryResultRow> holds = await Select(executor, database, dbname, "SELECT id, amount FROM workload_holds");
        Assert.That(holds, Has.Count.EqualTo(2));
        Assert.That(holds.Select(r => r.Row["amount"].LongValue).OrderBy(v => v), Is.EqualTo(new long[] { 10, 20 }),
            "the copy carries the owners as they were at the snapshot");
    }
}
