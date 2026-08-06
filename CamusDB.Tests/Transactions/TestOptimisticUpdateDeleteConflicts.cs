/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Conflict resolution and index maintenance for optimistic UPDATE and DELETE.
///
/// <para>An optimistic transaction takes no explicit exclusive lock on any key it writes — not the
/// row key, and not the secondary index entries the statement rewrites. Existing coverage proves
/// the resulting guarantees for INSERT; UPDATE and DELETE are the harder case, because one statement
/// rewrites several keys — the row plus every index entry that moves — so a loser that half-applied
/// would leave an index entry pointing at a value no row holds, or a row no index can reach.</para>
///
/// <para><b>Two distinct rejection points, and both are exercised here.</b> Skipping the client-side
/// acquire does not make an optimistic write invisible to its peers: a confirmed write folds an
/// implicit point lock into the coordinator working set, so a second writer of the same key is
/// refused at <em>write</em> time with <see cref="CamusDBErrorCodes.TransactionMustRetry"/> — the
/// conflict never reaches commit. What is genuinely deferred to commit is read-set validation: a
/// transaction whose folded read was overwritten by a committed peer is rejected only when it tries
/// to finalize, and its staged writes — including index moves — must leave nothing behind.</para>
///
/// <para>The interleavings are sequenced rather than raced, so the winner is deterministic and the
/// assertions can name it. All transactions are Read Committed so the optimistic path runs unaided:
/// under Serializable the shared predicate locks fence these conflicts earlier, which is the hybrid
/// behaviour the anomaly suite covers.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestOptimisticUpdateDeleteConflicts : SharedNodeBaseTest
{
    // accounts(id String PK, tier String [secondary index], balance Integer64), seeded with two rows.
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAccountsAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE accounts (id STRING NOT NULL PRIMARY KEY, tier STRING NOT NULL, balance INT64 NOT NULL)");
        await ExecDDL(executor, database, dbname, "CREATE INDEX accounts_tier ON accounts (tier)");

        KvTransaction seed = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, seed, "INSERT INTO accounts (id, tier, balance) VALUES (\"a\", \"gold\", 100)");
        await ExecIn(executor, dbname, seed, "INSERT INTO accounts (id, tier, balance) VALUES (\"b\", \"gold\", 200)");
        await database.Transactions.CommitAsync(seed);

        return (dbname, database, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static Task<KvTransaction> BeginOptimisticAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: KeyValueTransactionLocking.Optimistic);

    private static Task ExecIn(CommandExecutor executor, string dbname, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

    private static async Task<List<QueryResultRow>> SelectIn(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        return await cursor.ToListAsync();
    }

    private static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        List<QueryResultRow> rows = await SelectIn(executor, dbname, tx, sql);
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// Runs a statement that must be rejected as a transient serialization conflict, and returns the
    /// error. Both retryable codes are accepted: the batched write path reports
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> once its bounded lock-wait is exhausted,
    /// the single-key path <see cref="CamusDBErrorCodes.TransactionConflict"/>. Which one a statement
    /// takes is a batching detail; that it is retryable is the guarantee.
    /// </summary>
    private static async Task AssertRejectedAsRetryableConflict(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql, string because)
    {
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecIn(executor, dbname, tx, sql), because);

        Assert.That(ex?.Code,
            Is.AnyOf(CamusDBErrorCodes.TransactionMustRetry, CamusDBErrorCodes.TransactionConflict),
            "a write-write conflict must be reported as a retryable serialization failure");
    }

    /// <summary>Commits and reports whether the commit was accepted, rolling back a rejected one.</summary>
    private static async Task<bool> TryCommit(DatabaseDescriptor database, KvTransaction tx)
    {
        try
        {
            await database.Transactions.CommitAsync(tx);
            return true;
        }
        catch (CamusDBException)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            return false;
        }
    }

    // -----------------------------------------------------------------------
    // Write-time rejection: an in-flight optimistic write is not invisible to
    // its peers. The second writer of the same row is refused, and only the
    // first transaction's value can persist.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ConcurrentOptimisticUpdates_OfTheSameRow_RejectTheSecondWriter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction first  = await BeginOptimisticAsync(database);
        KvTransaction second = await BeginOptimisticAsync(database);

        await ExecIn(executor, dbname, first, "UPDATE accounts SET balance = 111 WHERE id = \"a\"");

        await AssertRejectedAsRetryableConflict(executor, dbname, second,
            "UPDATE accounts SET balance = 222 WHERE id = \"a\"",
            "a second optimistic writer of the same row must be refused: the first write folded an implicit point lock");

        await database.Transactions.RollbackAsync(second);
        Assert.That(await TryCommit(database, first), Is.True, "the surviving writer must commit");

        List<QueryResultRow> rows = await Select(executor, database, dbname,
            "SELECT balance FROM accounts WHERE id = \"a\"");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(111L, rows[0].Row["balance"].LongValue,
            "the surviving writer's value must persist and the refused one's must not");
    }

    [Test]
    public async Task OptimisticDelete_OfARowAnotherTransactionIsUpdating_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction updater = await BeginOptimisticAsync(database);
        KvTransaction deleter = await BeginOptimisticAsync(database);

        await ExecIn(executor, dbname, updater, "UPDATE accounts SET balance = 500 WHERE id = \"a\"");

        await AssertRejectedAsRetryableConflict(executor, dbname, deleter,
            "DELETE FROM accounts WHERE id = \"a\"",
            "a delete of a row another optimistic transaction is writing must be refused");

        await database.Transactions.RollbackAsync(deleter);
        Assert.That(await TryCommit(database, updater), Is.True);

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT id, balance FROM accounts WHERE id = \"a\"");
        Assert.AreEqual(1, rows.Count, "the refused delete must not have removed the row");
        Assert.AreEqual(500L, rows[0].Row["balance"].LongValue, "the committed update must be intact");
    }

    // -----------------------------------------------------------------------
    // Index maintenance on the committed path: an optimistic UPDATE that moves a
    // row between index entries must apply both halves of the move, with no
    // exclusive lock held on either entry.
    // -----------------------------------------------------------------------

    [Test]
    public async Task CommittedOptimisticUpdate_MovesTheRowBetweenIndexEntries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction tx = await BeginOptimisticAsync(database);
        await ExecIn(executor, dbname, tx, "UPDATE accounts SET tier = \"silver\" WHERE id = \"a\"");
        await database.Transactions.CommitAsync(tx);

        List<QueryResultRow> gold   = await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"gold\"");
        List<QueryResultRow> silver = await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"silver\"");

        Assert.AreEqual(1, gold.Count, "only the untouched row may remain under the old index value");
        Assert.AreEqual("b", gold[0].Row["id"].StrValue);
        Assert.AreEqual(1, silver.Count, "the new index entry must be written");
        Assert.AreEqual("a", silver[0].Row["id"].StrValue);
    }

    [Test]
    public async Task CommittedOptimisticDelete_RemovesTheRowAndItsIndexEntry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction tx = await BeginOptimisticAsync(database);
        await ExecIn(executor, dbname, tx, "DELETE FROM accounts WHERE id = \"a\"");
        await database.Transactions.CommitAsync(tx);

        List<QueryResultRow> byIndex = await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"gold\"");
        Assert.AreEqual(1, byIndex.Count, "the deleted row's index entry must not outlive it");
        Assert.AreEqual("b", byIndex[0].Row["id"].StrValue);
        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE id = \"a\"")).Count,
            "the deleted row must be gone from the primary key too");
    }

    // -----------------------------------------------------------------------
    // Commit-time rejection: a transaction whose folded read was overwritten by a
    // committed peer is rejected at finalize. Its staged index move must leave no
    // trace — this is the failure the absent exclusive lock could otherwise cause.
    // -----------------------------------------------------------------------

    [Test]
    public async Task OptimisticUpdate_RejectedOnAStaleReadSet_LeavesNoIndexEntryBehind()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        // The doomed transaction folds a read observation on row "a" at its current revision.
        KvTransaction doomed = await BeginOptimisticAsync(database);
        Assert.AreEqual(100L,
            (await SelectIn(executor, dbname, doomed, "SELECT balance FROM accounts WHERE id = \"a\"")).Single().Row["balance"].LongValue);

        // A peer overwrites the row "doomed" read, and commits — invalidating that observation.
        KvTransaction peer = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, peer, "UPDATE accounts SET balance = 900 WHERE id = \"a\"");
        await database.Transactions.CommitAsync(peer);

        // "doomed" now moves a DIFFERENT row between index entries. The write itself is unopposed —
        // the rejection can only come from read-set validation at commit.
        await ExecIn(executor, dbname, doomed, "UPDATE accounts SET tier = \"bronze\" WHERE id = \"b\"");
        Assert.That(await TryCommit(database, doomed), Is.False,
            "commit must be rejected: the transaction's folded read was overwritten by a committed peer");

        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"bronze\"")).Count,
            "the rejected transaction must not leave an index entry for a value no row holds");
        Assert.AreEqual(2, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"gold\"")).Count,
            "both rows must remain reachable under their original index value");

        List<QueryResultRow> b = await Select(executor, database, dbname, "SELECT tier FROM accounts WHERE id = \"b\"");
        Assert.AreEqual("gold", b.Single().Row["tier"].StrValue, "the staged row write must have been discarded too");
    }

    [Test]
    public async Task OptimisticDelete_RejectedOnAStaleReadSet_LeavesTheRowReachableThroughItsIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction doomed = await BeginOptimisticAsync(database);
        Assert.AreEqual(100L,
            (await SelectIn(executor, dbname, doomed, "SELECT balance FROM accounts WHERE id = \"a\"")).Single().Row["balance"].LongValue);

        KvTransaction peer = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, peer, "UPDATE accounts SET balance = 900 WHERE id = \"a\"");
        await database.Transactions.CommitAsync(peer);

        await ExecIn(executor, dbname, doomed, "DELETE FROM accounts WHERE id = \"b\"");
        Assert.That(await TryCommit(database, doomed), Is.False,
            "commit must be rejected: the transaction's folded read was overwritten by a committed peer");

        Assert.AreEqual(2, (await Select(executor, database, dbname, "SELECT id FROM accounts")).Count,
            "a rejected delete must not remove the row");
        Assert.AreEqual(2, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"gold\"")).Count,
            "a rejected delete must not remove the index entries it staged");
        Assert.AreEqual(1, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE id = \"b\"")).Count,
            "the row must remain reachable by primary key");
    }

    // -----------------------------------------------------------------------
    // Read-set validation must not depend on the access path. A predicate on an
    // unindexed column forces a full table scan; every row that scan observes is
    // still a folded read, so a peer overwriting one of them invalidates the
    // transaction exactly as a stale point read would. (Scans historically
    // registered no read observations at all, so whichever plan the planner
    // chose silently decided whether the optimistic contract was enforced.)
    // -----------------------------------------------------------------------

    [Test]
    public async Task OptimisticUpdate_RejectedWhenATableScanReadGoesStale()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        // "balance" is unindexed, so this read is served by a full table scan.
        KvTransaction doomed = await BeginOptimisticAsync(database);
        Assert.AreEqual(1,
            (await SelectIn(executor, dbname, doomed, "SELECT id FROM accounts WHERE balance = 100")).Count);

        // A peer overwrites a row the scan observed, and commits.
        KvTransaction peer = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, peer, "UPDATE accounts SET balance = 900 WHERE id = \"a\"");
        await database.Transactions.CommitAsync(peer);

        // The doomed transaction writes a different row; the rejection can only come from
        // commit-time validation of the scanned read observation on row "a".
        await ExecIn(executor, dbname, doomed, "UPDATE accounts SET tier = \"bronze\" WHERE id = \"b\"");
        Assert.That(await TryCommit(database, doomed), Is.False,
            "commit must be rejected: a row observed by the transaction's table scan was overwritten by a committed peer");

        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"bronze\"")).Count,
            "the rejected transaction must not leave an index entry behind");
        Assert.AreEqual(2, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE tier = \"gold\"")).Count,
            "both rows must remain reachable under their original index value");
    }

    // -----------------------------------------------------------------------
    // The rejection an optimistic conflict produces must be in the retryable class
    // the HTTP/gRPC autocommit wrapper replays from BEGIN — otherwise a conflict a
    // retry would resolve surfaces to the client as a hard error.
    // -----------------------------------------------------------------------

    [Test]
    public async Task OptimisticConflict_IsRetryable_AndTheAutocommitWrapperResolvesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        int attempts = 0;

        await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
        {
            int attempt = ++attempts;

            KvTransaction tx = await BeginOptimisticAsync(database);
            try
            {
                // A primary-key lookup folds a read observation at the row's current revision.
                long balance = (await SelectIn(executor, dbname, tx, "SELECT balance FROM accounts WHERE id = \"a\""))
                    .Single().Row["balance"].LongValue;

                // On the first attempt only, a peer commits over the row this attempt just read,
                // invalidating its read set. The replay then runs unopposed.
                if (attempt == 1)
                {
                    KvTransaction peer = await database.Transactions.BeginAsync(cancellationToken: ct);
                    await ExecIn(executor, dbname, peer, "UPDATE accounts SET balance = 900 WHERE id = \"a\"");
                    await database.Transactions.CommitAsync(peer, ct);
                }

                await ExecIn(executor, dbname, tx,
                    $"INSERT INTO accounts (id, tier, balance) VALUES (\"c\", \"gold\", {balance + 1})");
                await database.Transactions.CommitAsync(tx, ct);
            }
            catch
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
                throw;
            }
        }, cancellationToken: CancellationToken.None);

        Assert.AreEqual(2, attempts,
            "the first attempt must fail on a stale read set and the wrapper must replay it from BEGIN");

        // The replay read the peer's committed value, so the derived row proves the retry
        // recomputed rather than re-emitting the first attempt's stale result.
        List<QueryResultRow> rows = await Select(executor, database, dbname,
            "SELECT balance FROM accounts WHERE id = \"c\"");
        Assert.AreEqual(1, rows.Count, "the retried statement must have committed exactly one row");
        Assert.AreEqual(901L, rows[0].Row["balance"].LongValue,
            "the retry must recompute from the value the peer committed");
    }
}
