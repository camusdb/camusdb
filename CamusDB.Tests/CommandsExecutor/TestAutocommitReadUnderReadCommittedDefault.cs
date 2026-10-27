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
/// Under a Read Committed default, an autocommit SELECT reads one snapshot, pinned at a local
/// timestamp when the statement begins, as under the Serializable default. A scan whose pages come
/// from two partitions never sees half of a transfer that commits while the scan runs, and the
/// snapshot is at least the causal token the client passes.
///
/// <para>Runs with hash routing, and with key-range routing over a row space split across two
/// partitions. The key-range run is the one that shows a torn scan without the pin: one partition
/// serves the later pages of a scan at the snapshot its first page took, but the next partition reads
/// its own latest state.</para>
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[NonParallelizable]
internal sealed class TestAutocommitReadUnderReadCommittedDefault : BaseTest
{
    /// <summary>More than five scan pages (<c>KvStoreConstants.DefaultPageSize</c> is 512), so the last
    /// rows of a full scan are read long after its first page.</summary>
    private const int Accounts = 3000;

    private const long StartBalance = 100;

    private readonly bool keyRange;

    public TestAutocommitReadUnderReadCommittedDefault(bool keyRange) => this.keyRange = keyRange;

    protected override int NodeInitialPartitions => keyRange ? 2 : 1;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = keyRange, DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    /// <summary>Runs <paramref name="sql"/> in its own committed transaction.</summary>
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

    /// <summary>
    /// Creates <c>accounts</c> with <see cref="Accounts"/> rows (ids 0..N-1 in insertion order, so a full
    /// scan reads them in id order) and, under key-range routing, splits its row space in the middle so
    /// the scan crosses a partition boundary.
    /// </summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)", null));

        const int batch = 250;
        for (int from = 0; from < Accounts; from += batch)
        {
            string values = string.Join(", ", Enumerable.Range(from, Math.Min(batch, Accounts - from))
                .Select(i => $"({i}, {StartBalance})"));
            await CommitAsync(executor, database, db, "INSERT INTO accounts (id, balance) VALUES " + values);
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
    public async Task AutocommitScan_ThatStraddlesATransfer_ReadsOneSnapshot()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The transaction every autocommit read path begins (HttpTransactionCoordinator.BeginReadOnlyAsync).
        KvTransaction reader = database.Transactions.BeginReadOnly();
        Assert.That(reader.ReadTimestamp.IsNull(), Is.False, "the autocommit read is pinned");
        Assert.That(reader.IsolationLevel, Is.EqualTo(CamusIsolationLevel.ReadCommitted));

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(reader, db, "SELECT id, balance FROM accounts", null));

        Dictionary<long, long> balances = [];
        await using (IAsyncEnumerator<QueryResultRow> rows = cursor.GetAsyncEnumerator())
        {
            // The first page is read. Now a transfer commits between the first row and the last row.
            Assert.That(await rows.MoveNextAsync(), Is.True);
            balances[rows.Current.Row["id"].LongValue] = rows.Current.Row["balance"].LongValue;

            KvTransaction transfer = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
            try
            {
                await ExecAsync(executor, db, transfer, "UPDATE accounts SET balance = balance - 10 WHERE id = 0");
                await ExecAsync(executor, db, transfer, $"UPDATE accounts SET balance = balance + 10 WHERE id = {Accounts - 1}");
                await database.Transactions.CommitAsync(transfer);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(transfer);
            }

            while (await rows.MoveNextAsync())
                balances[rows.Current.Row["id"].LongValue] = rows.Current.Row["balance"].LongValue;
        }

        Assert.That(balances, Has.Count.EqualTo(Accounts));
        Assert.That(balances[Accounts - 1], Is.EqualTo(StartBalance),
            "the last row is read at the snapshot, before the transfer");
        Assert.That(balances.Values.Sum(), Is.EqualTo(Accounts * StartBalance),
            "the scan never sees half of the transfer");

        // The next autocommit read takes a new snapshot and sees the transfer.
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> after) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(database.Transactions.BeginReadOnly(), db, $"SELECT balance FROM accounts WHERE id = {Accounts - 1}", null));
        Assert.That((await after.ToListAsync()).Single().Row["balance"].LongValue, Is.EqualTo(StartBalance + 10));
    }

    [Test]
    public async Task AutocommitRead_IsAtLeastTheCausalToken()
    {
        (string _, DatabaseDescriptor database, CommandExecutor _) = await CreateDatabase();

        // A token ahead of the local clock, as from a node whose clock leads.
        HLCTimestamp now = database.Transactions.BeginReadOnly().ReadTimestamp;
        HLCTimestamp token = new(now.N, now.L + 1_000, 0);

        KvTransaction reader = database.Transactions.BeginReadOnly(token);

        Assert.That(reader.ReadTimestamp.CompareTo(token), Is.GreaterThanOrEqualTo(0),
            "the snapshot of an autocommit read is at least the causal token");
    }
}
