/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A scan whose rows decide what a statement writes (the source of <c>INSERT … SELECT</c>, the locate
/// scan of <c>UPDATE</c> and <c>DELETE</c>) takes an exclusive lock over the range it reads. Kahuna
/// places a write intent of the locking transaction on every resident key of that range, and every
/// page of the scan after the first reads at the snapshot the first page took. A scan that carried no
/// transaction identity met those intents as another transaction's, waited for them to settle, and
/// failed after the page retry budget, because their owner was the transaction waiting for the scan.
/// Each test reads more than one 512-row page of freshly written rows, the shape that failed.
/// </summary>
[NonParallelizable]
internal sealed class TestWriteDrivingScanOwnLocks : SharedNodeBaseTest
{
    private const int Rows = 10_000;

    /// <summary>
    /// A row of <c>src</c> is three keys (the row, the primary key, the index on <c>k</c>), so deleting
    /// every row in one transaction is over the default mutation ceiling.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { MaxMutationsPerTransaction = 100_000 };

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<long> CountRows(CommandExecutor executor, string dbname, string table)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT COUNT(*) AS c FROM {table}", null));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            return rows[0].Row["c"].LongValue;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Creates <c>src</c> with <see cref="Rows"/> rows, written just before the test so they are resident,
    /// an index on <c>k</c>, and an empty <c>dst</c>.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = options is null
            ? await CreateDatabase()
            : await CreateDatabase(options);

        await ExecuteDdl(executor, dbname, "CREATE TABLE src (id INT64 PRIMARY KEY, k INT64, x INT64)");
        await ExecuteDdl(executor, dbname, "CREATE INDEX src_k ON src (k)");
        await ExecuteDdl(executor, dbname, "CREATE TABLE dst (id INT64 PRIMARY KEY, x INT64)");

        for (int first = 1; first <= Rows; first += 2_000)
        {
            StringBuilder sql = new("INSERT INTO src (id, k, x) VALUES ");

            for (int id = first; id < first + 2_000; id++)
            {
                if (id > first)
                    sql.Append(", ");

                sql.Append('(').Append(id).Append(", ").Append(id).Append(", ").Append(id).Append(')');
            }

            KvTransaction tx = await database.Transactions.BeginAsync();
            try
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql.ToString(), null));
                await database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }

        return (dbname, database, executor);
    }

    private static async Task<int> RunInTransaction(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, CamusIsolationLevel isolation, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(isolation);
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task InsertSelect_OverManyPages_CopiesEveryRow(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int inserted = await RunInTransaction(executor, database, dbname, isolation, "INSERT INTO dst (id, x) SELECT id, x FROM src");

        Assert.That(inserted, Is.EqualTo(Rows));
        Assert.That(await CountRows(executor, dbname, "dst"), Is.EqualTo(Rows));
    }

    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task FullScanUpdate_OverManyPages_UpdatesEveryRow(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // x is not indexed, so the rows are located by a full scan of the table.
        int updated = await RunInTransaction(executor, database, dbname, isolation, "UPDATE src SET x = 0 WHERE x > 0");

        Assert.That(updated, Is.EqualTo(Rows));
        Assert.That(await RunInTransaction(executor, database, dbname, isolation, "UPDATE src SET x = 1 WHERE x = 0"), Is.EqualTo(Rows));
    }

    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task FullScanDelete_OverManyPages_DeletesEveryRow(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await RunInTransaction(executor, database, dbname, isolation, "DELETE FROM src WHERE x > 0");

        Assert.That(deleted, Is.EqualTo(Rows));
        Assert.That(await CountRows(executor, dbname, "src"), Is.EqualTo(0));
    }

    [Test]
    public async Task IndexRangeUpdate_OverManyPages_UpdatesEveryRowInTheRange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // k is indexed: the rows are located by a range scan of the index, under an exclusive lock over
        // the index range. 2,000 index entries are more than three pages.
        int updated = await RunInTransaction(executor, database, dbname, CamusIsolationLevel.Serializable,
            "UPDATE src SET x = 0 WHERE k >= 1001 AND k <= 3000");

        Assert.That(updated, Is.EqualTo(2_000));
    }

    [Test]
    public async Task InsertSelect_UnderKeyRangeSharding_ReadCommitted_CopiesEveryRow()
    {
        // With key-range sharding a write-driving scan takes its exclusive range lock at every isolation
        // level, not only under Serializable.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupAsync(Options with { KeyRangeShardingEnabled = true });

        int inserted = await RunInTransaction(executor, database, dbname, CamusIsolationLevel.ReadCommitted,
            "INSERT INTO dst (id, x) SELECT id, x FROM src");

        Assert.That(inserted, Is.EqualTo(Rows));
    }

    [Test]
    public async Task ScanOfAnotherTable_AfterAnExclusiveLock_StaysUntracked()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable);
        try
        {
            // Matches no row, so the transaction holds the exclusive lock over src but has written nothing.
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "UPDATE src SET x = 0 WHERE x < 0", null));

            TableDescriptor src = await database.TableDescriptors["src"];
            TableDescriptor dst = await database.TableDescriptors["dst"];

            Assert.That(tx.HasPendingWrites, Is.False);
            Assert.That(tx.RangeScanIdentityFor(src.Store.RowKeySpace), Is.EqualTo(tx.TransactionId),
                "a scan of the range the transaction holds exclusively must carry its identity");
            Assert.That(tx.RangeScanIdentityFor(dst.Store.RowKeySpace), Is.EqualTo(Kommander.Time.HLCTimestamp.Zero),
                "a scan of a range the transaction does not hold exclusively stays untracked");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
