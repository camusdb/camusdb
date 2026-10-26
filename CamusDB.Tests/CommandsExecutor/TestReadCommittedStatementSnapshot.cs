/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A SELECT in a Read Committed transaction reads at one statement timestamp, in Kahuna's statement
/// read mode: it sees one consistent view across every page and partition it reads, it sees the
/// transaction's own writes, and a later statement sees the commits that came between. Write checks and
/// the reads of a mutation stay on the latest committed state.
///
/// <para>Runs with hash routing, and with key-range routing over a row space split across two
/// partitions, so a scan's pages come from more than one partition. The key-range run is the one that
/// shows the torn scan without a statement timestamp: one partition serves the later pages of a scan at
/// the snapshot its first page took, but the next partition reads its own latest state. The point-read
/// and write-check tests fail in both runs without it, on the abort of the read pin an earlier read
/// left.</para>
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[NonParallelizable]
internal sealed class TestReadCommittedStatementSnapshot : BaseTest
{
    /// <summary>More than five scan pages (<c>KvStoreConstants.DefaultPageSize</c> is 512), so the last
    /// rows of a full scan are read long after its first page.</summary>
    private const int Accounts = 3000;

    private const long StartBalance = 100;

    private readonly bool keyRange;

    public TestReadCommittedStatementSnapshot(bool keyRange) => this.keyRange = keyRange;

    protected override int NodeInitialPartitions => keyRange ? 2 : 1;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = keyRange };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    /// <summary>Runs <paramref name="sql"/> in its own committed Read Committed transaction.</summary>
    private static async Task CommitAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            await ExecAsync(executor, db, tx, sql);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private Task<KvTransaction> BeginReadCommittedAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

    /// <summary>
    /// Creates <c>accounts</c> with <see cref="Accounts"/> rows (ids 0..N-1 in insertion order, so a full
    /// scan reads them in id order) and, under key-range routing, splits its row space in the middle so
    /// the scan crosses a partition boundary.
    /// </summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, email STRING UNIQUE, balance INT64)", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE INDEX balance_idx ON accounts (balance)", null));

        const int batch = 250;
        for (int from = 0; from < Accounts; from += batch)
        {
            string values = string.Join(", ", Enumerable.Range(from, Math.Min(batch, Accounts - from))
                .Select(i => $"({i}, 'user{i}@x', {StartBalance})"));
            await CommitAsync(executor, database, db, "INSERT INTO accounts (id, email, balance) VALUES " + values);
        }

        if (keyRange)
            await SplitRowSpaceInTheMiddleAsync(executor, db);

        return (db, database, executor);
    }

    private async Task SplitRowSpaceInTheMiddleAsync(CommandExecutor executor, string db)
    {
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "accounts"));

        List<ObjectIdValue> rowIds = [];
        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(KvTransaction.CreateReadOnly()))
            rowIds.Add(rowId);

        rowIds.Sort((a, b) => string.CompareOrdinal(a.ToString(), b.ToString()));
        string keySpace = table.Store.RowKeySpace;
        string splitKey = table.Store.RowPointKey(rowIds[rowIds.Count / 2]);

        KahunaSplitRangeResponse response = new();
        for (int attempt = 0; attempt < 20 && !response.Success; attempt++)
        {
            response = await TestNode!.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);
            if (!response.Success)
                await Task.Delay(100);
        }

        Assert.That(response.Success, Is.True, $"the split of the accounts row space failed: {response.Status} ({response.Reason})");
    }

    [Test]
    public async Task Scan_ThatStraddlesATransfer_ReadsOneTimestamp()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction reader = await BeginReadCommittedAsync(database);
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(reader, db, "SELECT id, balance FROM accounts", null));

            Dictionary<long, long> balances = [];
            await using (IAsyncEnumerator<QueryResultRow> rows = cursor.GetAsyncEnumerator())
            {
                // The first page is read. Now a transfer commits between the first row and the last row.
                Assert.That(await rows.MoveNextAsync(), Is.True);
                balances[rows.Current.Row["id"].LongValue] = rows.Current.Row["balance"].LongValue;
                Assert.That(reader.QueryReadView.Mode, Is.EqualTo(KeyValueReadMode.StatementSnapshot),
                    "the SELECT reads in the statement mode while its cursor runs");

                KvTransaction transfer = await BeginReadCommittedAsync(database);
                await ExecAsync(executor, db, transfer, "UPDATE accounts SET balance = balance - 10 WHERE id = 0");
                await ExecAsync(executor, db, transfer, $"UPDATE accounts SET balance = balance + 10 WHERE id = {Accounts - 1}");
                await database.Transactions.CommitAsync(transfer);

                while (await rows.MoveNextAsync())
                    balances[rows.Current.Row["id"].LongValue] = rows.Current.Row["balance"].LongValue;
            }

            Assert.That(balances, Has.Count.EqualTo(Accounts));
            Assert.That(balances[Accounts - 1], Is.EqualTo(StartBalance),
                "the last row is read at the statement timestamp, before the transfer");
            Assert.That(balances.Values.Sum(), Is.EqualTo(Accounts * StartBalance),
                "the scan never sees half of the transfer");
            Assert.That(reader.QueryReadView.Mode, Is.EqualTo(KeyValueReadMode.Default),
                "the statement snapshot closes when the cursor ends");

            // The next statement of the same transaction reads at a new timestamp and sees the transfer.
            List<QueryResultRow> after = await QueryAsync(executor, db, reader, $"SELECT balance FROM accounts WHERE id = {Accounts - 1}");
            Assert.That(after.Single().Row["balance"].LongValue, Is.EqualTo(StartBalance + 10));

            await database.Transactions.CommitAsync(reader);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    [Test]
    public async Task LaterStatement_SeesACommitThatCameBetween_WithNoAbort()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction reader = await BeginReadCommittedAsync(database);
        try
        {
            // A point read, an index read and a scan, each of which leaves no read pin.
            Assert.That((await QueryAsync(executor, db, reader, "SELECT balance FROM accounts WHERE id = 5")).Single().Row["balance"].LongValue,
                Is.EqualTo(StartBalance));
            Assert.That(await QueryAsync(executor, db, reader, "SELECT id FROM accounts WHERE email = 'user5@x'"), Has.Count.EqualTo(1));
            Assert.That(await QueryAsync(executor, db, reader, "SELECT id FROM accounts WHERE balance = 200"), Is.Empty);

            await CommitAsync(executor, database, db, "UPDATE accounts SET balance = 200 WHERE id = 5");

            Assert.That((await QueryAsync(executor, db, reader, "SELECT balance FROM accounts WHERE id = 5")).Single().Row["balance"].LongValue,
                Is.EqualTo(200));
            Assert.That((await QueryAsync(executor, db, reader, "SELECT id FROM accounts@{FORCE_INDEX=balance_idx} WHERE balance = 200"))
                .Select(r => r.Row["id"].LongValue), Is.EqualTo(new long[] { 5 }));
            Assert.That((await QueryAsync(executor, db, reader, "SELECT balance FROM accounts WHERE email = 'user5@x'")).Single().Row["balance"].LongValue,
                Is.EqualTo(200));

            await database.Transactions.CommitAsync(reader);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    [Test]
    public async Task OwnWrites_AreVisible_ToEveryReadPath()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE refs (id INT64 PRIMARY KEY, account_id INT64)", null));
        await CommitAsync(executor, database, db, "INSERT INTO refs (id, account_id) VALUES (1, 1), (2, 2), (3, 9000)");

        KvTransaction tx = await BeginReadCommittedAsync(database);
        try
        {
            await ExecAsync(executor, db, tx, "INSERT INTO accounts (id, email, balance) VALUES (9000, 'new@x', 7)");
            await ExecAsync(executor, db, tx, "UPDATE accounts SET balance = 55 WHERE id = 1");
            await ExecAsync(executor, db, tx, "DELETE FROM accounts WHERE id = 2");

            // Point reads.
            Assert.That((await QueryAsync(executor, db, tx, "SELECT balance FROM accounts WHERE id = 9000")).Single().Row["balance"].LongValue, Is.EqualTo(7));
            Assert.That((await QueryAsync(executor, db, tx, "SELECT balance FROM accounts WHERE id = 1")).Single().Row["balance"].LongValue, Is.EqualTo(55));
            Assert.That(await QueryAsync(executor, db, tx, "SELECT balance FROM accounts WHERE id = 2"), Is.Empty);

            // A unique index read and a secondary index scan.
            Assert.That((await QueryAsync(executor, db, tx, "SELECT id FROM accounts WHERE email = 'new@x'")).Single().Row["id"].LongValue, Is.EqualTo(9000));
            Assert.That((await QueryAsync(executor, db, tx, "SELECT id FROM accounts@{FORCE_INDEX=balance_idx} WHERE balance = 55"))
                .Select(r => r.Row["id"].LongValue), Is.EqualTo(new long[] { 1 }));

            // A full scan.
            List<long> ids = (await QueryAsync(executor, db, tx, "SELECT id FROM accounts")).Select(r => r.Row["id"].LongValue).ToList();
            Assert.That(ids, Has.Count.EqualTo(Accounts));
            Assert.That(ids, Does.Contain(9000L));
            Assert.That(ids, Does.Not.Contain(2L));

            // A join, whose probe side reads the account rows in batches.
            Dictionary<long, long> joined = (await QueryAsync(executor, db, tx,
                    "SELECT r.id AS ref_id, a.balance AS balance FROM refs r JOIN accounts a ON a.id = r.account_id"))
                .ToDictionary(r => r.Row["ref_id"].LongValue, r => r.Row["balance"].LongValue);
            Assert.That(joined, Is.EqualTo(new Dictionary<long, long> { [1] = 55, [3] = 7 }));

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task OwnLargeValueWrite_IsVisible_ToALaterSelect()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db, "CREATE TABLE docs (id INT64 PRIMARY KEY, body STRING)", null));

        string first = Incompressible(6000, seed: 1);
        string second = Incompressible(6000, seed: 2);
        await CommitAsync(executor, database, db, $"INSERT INTO docs (id, body) VALUES (1, '{first}')");

        KvTransaction tx = await BeginReadCommittedAsync(database);
        try
        {
            Assert.That((await QueryAsync(executor, db, tx, "SELECT body FROM docs WHERE id = 1")).Single().Row["body"].StrValue, Is.EqualTo(first));

            await ExecAsync(executor, db, tx, $"UPDATE docs SET body = '{second}' WHERE id = 1");

            Assert.That((await QueryAsync(executor, db, tx, "SELECT body FROM docs WHERE id = 1")).Single().Row["body"].StrValue, Is.EqualTo(second));
            Assert.That((await QueryAsync(executor, db, tx, "SELECT body FROM docs")).Single().Row["body"].StrValue, Is.EqualTo(second));

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task WriteChecks_AfterASelect_ReadTheLatestCommit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // A uniqueness check sees a key that another transaction committed after this transaction's
        // last SELECT.
        KvTransaction inserter = await BeginReadCommittedAsync(database);
        try
        {
            Assert.That(await QueryAsync(executor, db, inserter, "SELECT id FROM accounts WHERE email = 'dup@x'"), Is.Empty);

            await CommitAsync(executor, database, db, "INSERT INTO accounts (id, email, balance) VALUES (9000, 'dup@x', 999)");

            CamusDBException duplicate = Assert.ThrowsAsync<CamusDBException>(() =>
                ExecAsync(executor, db, inserter, "INSERT INTO accounts (id, email, balance) VALUES (9001, 'dup@x', 1)"))!;
            Assert.That(duplicate.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(inserter);
        }

        // An UPDATE locates and changes a row that another transaction committed after this
        // transaction's last SELECT.
        KvTransaction updater = await BeginReadCommittedAsync(database);
        try
        {
            Assert.That(await QueryAsync(executor, db, updater, "SELECT id FROM accounts WHERE balance = 888"), Is.Empty);

            await CommitAsync(executor, database, db, "INSERT INTO accounts (id, email, balance) VALUES (9002, 'late@x', 888)");

            ExecuteNonSQLResult updated = await ExecAsync(executor, db, updater, "UPDATE accounts SET balance = 1 WHERE balance = 888");
            Assert.That(updated.ModifiedRows, Is.EqualTo(1));

            await database.Transactions.CommitAsync(updater);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(updater);
        }

        KvTransaction check = await BeginReadCommittedAsync(database);
        Assert.That((await QueryAsync(executor, db, check, "SELECT balance FROM accounts WHERE id = 9002")).Single().Row["balance"].LongValue, Is.EqualTo(1));
        await database.Transactions.CommitAsync(check);
    }

    [Test]
    public async Task OptimisticLoser_OfARowItSelected_NeverCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction loser = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Optimistic);
        try
        {
            Assert.That((await QueryAsync(executor, db, loser, "SELECT balance FROM accounts WHERE id = 7")).Single().Row["balance"].LongValue,
                Is.EqualTo(StartBalance));

            KvTransaction winner = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Optimistic);
            await ExecAsync(executor, db, winner, "UPDATE accounts SET balance = 500 WHERE id = 7");
            await database.Transactions.CommitAsync(winner);

            CamusDBException? refused = null;
            try
            {
                await ExecAsync(executor, db, loser, "UPDATE accounts SET balance = balance + 1 WHERE id = 7");
                await database.Transactions.CommitAsync(loser);
            }
            catch (CamusDBException ex)
            {
                refused = ex;
            }

            Assert.That(refused, Is.Not.Null, "the loser read a value another transaction then changed; it must not commit");
            TestContext.Out.WriteLine($"loser refused with {refused!.Code}: {refused.Message}");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(loser);
        }

        KvTransaction check = await BeginReadCommittedAsync(database);
        Assert.That((await QueryAsync(executor, db, check, "SELECT balance FROM accounts WHERE id = 7")).Single().Row["balance"].LongValue, Is.EqualTo(500));
        await database.Transactions.CommitAsync(check);
    }

    [Test]
    public async Task StatementTimestamp_IsAtLeastTheCausalToken_AndOnlyPessimisticReadCommittedTakesOne()
    {
        (string _, DatabaseDescriptor database, CommandExecutor _) = await CreateDatabase();

        HLCTimestamp token = TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(TestNode.Raft.GetLocalNodeId());

        KvTransaction readCommitted = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, causalToken: token);
        try
        {
            Assert.That(readCommitted.CausalFloor, Is.EqualTo(token));
            Assert.That(database.Transactions.TryMintStatementTimestamp(readCommitted, out HLCTimestamp statementTimestamp), Is.True);
            Assert.That(statementTimestamp.CompareTo(token), Is.GreaterThan(0));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(readCommitted);
        }

        KvTransaction serializable = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(database.Transactions.TryMintStatementTimestamp(serializable, out _), Is.False);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(serializable);
        }

        KvTransaction optimistic = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Optimistic);
        try
        {
            Assert.That(database.Transactions.TryMintStatementTimestamp(optimistic, out _), Is.False,
                "a transaction that validates its reads at commit keeps its read pins");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(optimistic);
        }

        Assert.That(database.Transactions.TryMintStatementTimestamp(KvTransaction.CreateReadOnly(), out _), Is.False,
            "the autocommit read keeps reading the latest committed value");
    }

    private static string Incompressible(int length, int seed)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new(seed);
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = alphabet[random.Next(alphabet.Length)];
        return new string(chars);
    }
}
