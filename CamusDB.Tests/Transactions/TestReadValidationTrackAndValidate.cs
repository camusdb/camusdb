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
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// <see cref="ReadValidation.TrackAndValidate"/> on a pessimistic transaction.
///
/// <para>Read-set validation is normally a property of optimistic locking, which folds and validates
/// its reads by definition. This option decouples the two: a pessimistic transaction keeps its
/// blocking write locks and additionally registers each read with the coordinator, so a peer that
/// commits over something it observed rejects it at commit. Under Read Committed — where reads take
/// no locks at all — that is the only thing standing between a transaction and a decision made on a
/// value that has since changed.</para>
///
/// <para>The pairing matters because the two knobs are independent inputs to
/// <see cref="KvTransaction.FoldReads"/>, and the shipped default is
/// <see cref="ReadValidation.None"/>: without a test that turns it on, nothing proves the non-default
/// arm does anything. Each behavioural case here is asserted against its own control — the same
/// interleaving with validation off — so the assertion is that validation changed the outcome, not
/// merely that the outcome was the expected one.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestReadValidationTrackAndValidate : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAccountsAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE accounts (id STRING NOT NULL PRIMARY KEY, balance INT64 NOT NULL)", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction seed = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, seed, "INSERT INTO accounts (id, balance) VALUES (\"a\", 100)");
        await database.Transactions.CommitAsync(seed);

        return (dbname, database, executor);
    }

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
    /// Read Committed + Pessimistic, with read validation as given. Read Committed on purpose: it
    /// takes no read locks, so nothing but validation can detect that an observed row changed, and
    /// the peer below is never blocked.
    /// </summary>
    private static Task<KvTransaction> BeginAsync(DatabaseDescriptor database, ReadValidation readValidation)
        => database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: KeyValueTransactionLocking.Pessimistic,
            readValidation: readValidation);

    /// <summary>
    /// Runs the read-then-peer-write-then-write-elsewhere interleaving under the given read-validation
    /// policy and reports whether the transaction was allowed to commit. The transaction reads row
    /// "a" by primary key, a peer overwrites "a" and commits, and the transaction then writes a
    /// disjoint key — so no write-write conflict exists and the read set is the only thing that can
    /// reject it.
    /// </summary>
    private static async Task<bool> ReadThenPeerWriteThenCommitAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname,
        ReadValidation readValidation, string newRowId)
    {
        KvTransaction tx = await BeginAsync(database, readValidation);

        List<QueryResultRow> read = await SelectIn(executor, dbname, tx, "SELECT balance FROM accounts WHERE id = \"a\"");
        Assert.AreEqual(1, read.Count, "the observed row must exist before the peer overwrites it");

        KvTransaction peer = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, peer, "UPDATE accounts SET balance = 900 WHERE id = \"a\"");
        await database.Transactions.CommitAsync(peer);

        await ExecIn(executor, dbname, tx, $"INSERT INTO accounts (id, balance) VALUES (\"{newRowId}\", 1)");

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
    // Whether the read is folded at all — the mechanism the behaviour rests on.
    // -----------------------------------------------------------------------

    [Test]
    public async Task PessimisticTransaction_FoldsItsReads_OnlyWhenValidationIsRequested()
    {
        (_, DatabaseDescriptor database, _) = await SetupAccountsAsync();

        KvTransaction validating = await BeginAsync(database, ReadValidation.TrackAndValidate);
        KvTransaction plain      = await BeginAsync(database, ReadValidation.None);
        try
        {
            Assert.AreEqual(ReadValidation.TrackAndValidate, validating.ReadValidation);
            Assert.IsTrue(validating.FoldReads,
                "a pessimistic transaction that asked for validation must register its reads with the coordinator");

            Assert.AreEqual(ReadValidation.None, plain.ReadValidation);
            Assert.IsFalse(plain.FoldReads,
                "the default pessimistic transaction relies on its locks alone and must not fold reads");
        }
        finally
        {
            await database.Transactions.RollbackAsync(validating);
            await database.Transactions.RollbackAsync(plain);
        }
    }

    // -----------------------------------------------------------------------
    // The behaviour itself, each arm against its control.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ValidatingTransaction_IsRejected_WhenAPeerOverwritesWhatItRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        bool committed = await ReadThenPeerWriteThenCommitAsync(
            executor, database, dbname, ReadValidation.TrackAndValidate, "v");

        Assert.IsFalse(committed,
            "commit must be rejected: the transaction asked for its read set to be validated and a peer invalidated it");

        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE id = \"v\"")).Count,
            "the rejected transaction's write must not persist");
        Assert.AreEqual(900L,
            (await Select(executor, database, dbname, "SELECT balance FROM accounts WHERE id = \"a\"")).Single().Row["balance"].LongValue,
            "the peer's committed write must be the surviving value");
    }

    [Test]
    public async Task DefaultPessimisticTransaction_CommitsThroughTheSameInterleaving()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        bool committed = await ReadThenPeerWriteThenCommitAsync(
            executor, database, dbname, ReadValidation.None, "n");

        Assert.IsTrue(committed,
            "the control arm must commit — without validation a Read Committed read carries no commit dependency, " +
            "which is what makes the rejection above attributable to the option and not to the interleaving");

        Assert.AreEqual(1, (await Select(executor, database, dbname, "SELECT id FROM accounts WHERE id = \"n\"")).Count,
            "the control transaction's write must persist");
    }

    [Test]
    public async Task ValidatingTransaction_CommitsWhenNothingItReadWasTouched()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction tx = await BeginAsync(database, ReadValidation.TrackAndValidate);

        Assert.AreEqual(100L,
            (await SelectIn(executor, dbname, tx, "SELECT balance FROM accounts WHERE id = \"a\"")).Single().Row["balance"].LongValue);

        // A peer writes a row this transaction never observed — not a dependency, not a conflict.
        KvTransaction peer = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, peer, "INSERT INTO accounts (id, balance) VALUES (\"z\", 5)");
        await database.Transactions.CommitAsync(peer);

        await ExecIn(executor, dbname, tx, "INSERT INTO accounts (id, balance) VALUES (\"y\", 7)");

        Assert.DoesNotThrowAsync(async () => await database.Transactions.CommitAsync(tx),
            "read-set validation must not reject a transaction whose observations are all still current");

        Assert.AreEqual(3, (await Select(executor, database, dbname, "SELECT id FROM accounts")).Count,
            "both the peer's row and the validating transaction's row must persist");
    }

    // -----------------------------------------------------------------------
    // The option is also reachable as a server-wide default, so an engine
    // configured with it hands out validating transactions without every caller
    // having to ask.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ConfiguredDefault_MakesUnqualifiedTransactionsValidateTheirReads()
    {
        (_, DatabaseDescriptor database, _) = await CreateDatabase(
            Options with { DefaultReadValidation = ReadValidation.TrackAndValidate });

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            Assert.AreEqual(ReadValidation.TrackAndValidate, tx.ReadValidation,
                "a transaction begun with no arguments must inherit the configured read-validation policy");
            Assert.IsTrue(tx.FoldReads);
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }
}
