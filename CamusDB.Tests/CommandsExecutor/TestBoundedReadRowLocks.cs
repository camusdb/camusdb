/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Kahuna.Shared.KeyValue;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A Serializable read-write read that names a bounded set of rows through an index locks the rows
/// it read and no other row, at the row count of an ordinary page read. The fixture runs with the
/// default configuration on purpose: the defect it guards against was a default. With an escalation
/// threshold below a page read, the row locks of one owner's rows became a shared lock on the whole
/// row range, and every write to the table by another transaction failed until the reader ended.
///
/// <para>Each test checks both sides of the contract: a write to a row the reader did not read
/// commits, and a write to a row the reader read is refused.</para>
/// </summary>
[TestFixture(KeyValueTransactionLocking.Pessimistic)]
[TestFixture(KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
internal sealed class TestBoundedReadRowLocks : BaseTest
{
    private const int Owners = 4;

    private const int RowsPerOwner = 120;

    private readonly KeyValueTransactionLocking locking;

    public TestBoundedReadRowLocks(KeyValueTransactionLocking locking) => this.locking = locking;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        // This fixture describes the locking form of Serializable reads: a shared lock on each key read,
        // upgraded at the write. It states the setting so that it holds whatever the default is; the form
        // that proves single-key reads at commit is covered by TestSerializableOptimisticPointReads.
        => defaults with { DefaultTransactionLocking = locking, SerializableOptimisticPointLocks = true };

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql, bool ddl = false)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
            else
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql, KvTransaction tx)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
        return await cursor.ToListAsync();
    }

    /// <summary>True when the statement commits; false when a lock of another transaction refuses it.</summary>
    private static async Task<bool> TryWrite(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction writer = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, db.Name, sql, null));
            await db.Transactions.CommitAsync(writer);
            return true;
        }
        catch (CamusDBException)
        {
            return false;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(writer);
        }
    }

    private static bool HoldsWholeBucketLock(KvTransaction tx)
        => tx.GetAcquiredRangeLocks().Any(b => b.StartKey is null && b.EndKey is null);

    /// <summary>
    /// <c>items</c>: <see cref="Owners"/> owners with <see cref="RowsPerOwner"/> rows each. The
    /// <c>payload</c> column is in no index, so a read must fetch the row and lock it.
    /// </summary>
    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> CreateItems()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db,
            "CREATE TABLE items (owner INT64 NOT NULL, id INT64 NOT NULL, payload STRING, PRIMARY KEY (owner, id))", ddl: true);

        for (int owner = 1; owner <= Owners; owner++)
        {
            List<string> tuples = new(RowsPerOwner);
            for (int id = 1; id <= RowsPerOwner; id++)
                tuples.Add($"({owner}, {id}, 'p{owner}-{id}')");
            await Exec(executor, db, "INSERT INTO items (owner, id, payload) VALUES " + string.Join(", ", tuples));
        }

        // ANALYZE returns a row, so it goes through the query entry point.
        KvTransaction analyze = await db.Transactions.BeginAsync();
        try
        {
            await Select(executor, db, "ANALYZE TABLE items", analyze);
            await db.Transactions.CommitAsync(analyze);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(analyze);
        }
        return (db, executor);
    }

    [Test]
    public async Task ReadOfOneOwnerDoesNotBlockAWriteToAnotherOwner()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateItems();

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            List<QueryResultRow> rows = await Select(executor, db, "SELECT owner, id, payload FROM items WHERE owner = 2 ORDER BY id", reader);
            Assert.That(rows, Has.Count.EqualTo(RowsPerOwner));

            Assert.That(HoldsWholeBucketLock(reader), Is.False,
                "a read of one owner's rows must not take a lock on a whole table or index");

            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 3 AND id = 7"), Is.True,
                "a write to a row of another owner must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 2 AND id = 7"), Is.False,
                "a write to a row the reader read must be refused");
            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 2 AND id = 120"), Is.False,
                "the last row of the read is locked as the first is");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    [Test]
    public async Task InListReadDoesNotBlockAWriteToAnotherOwner()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateItems();

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            List<QueryResultRow> rows = await Select(executor, db, "SELECT owner, id, payload FROM items WHERE owner IN (1, 3)", reader);
            Assert.That(rows, Has.Count.EqualTo(2 * RowsPerOwner));

            Assert.That(HoldsWholeBucketLock(reader), Is.False,
                "a read of two owners' rows must not take a lock on a whole table or index");

            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 2 AND id = 50"), Is.True,
                "a write to a row of an owner between the list values must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 3 AND id = 50"), Is.False,
                "a write to a row the reader read must be refused");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    /// <summary>
    /// The row locks are counted over the whole transaction, so several statements of one
    /// transaction that each read one page must not add up to a table lock.
    /// </summary>
    [Test]
    public async Task SeveralReadsOfOneTransactionDoNotAddUpToATableLock()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateItems();

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            for (int owner = 1; owner <= 3; owner++)
            {
                List<QueryResultRow> rows = await Select(executor, db, $"SELECT owner, id, payload FROM items WHERE owner = {owner}", reader);
                Assert.That(rows, Has.Count.EqualTo(RowsPerOwner));
            }

            Assert.That(HoldsWholeBucketLock(reader), Is.False);

            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 4 AND id = 1"), Is.True,
                "a write to a row of the owner the transaction did not read must not conflict");
            Assert.That(await TryWrite(executor, db, "UPDATE items SET payload = 'changed' WHERE owner = 1 AND id = 1"), Is.False,
                "a row of the first statement stays locked");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    /// <summary>
    /// The reader can write a row it read: the batch acquire tracks each point lock, so the write
    /// promotes that lock and commits.
    /// </summary>
    [Test]
    public async Task ReaderWritesARowItReadAndCommits()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateItems();

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            await Select(executor, db, "SELECT owner, id, payload FROM items WHERE owner = 2", reader);
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(reader, db.Name, "UPDATE items SET payload = 'mine' WHERE owner = 2 AND id = 60", null));
            await db.Transactions.CommitAsync(reader);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }

        KvTransaction check = await db.Transactions.BeginAsync();
        try
        {
            List<QueryResultRow> rows = await Select(executor, db, "SELECT payload FROM items WHERE owner = 2 AND id = 60", check);
            Assert.That(rows.Single().Row["payload"].StrValue, Is.EqualTo("mine"));
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(check);
        }
    }
}
