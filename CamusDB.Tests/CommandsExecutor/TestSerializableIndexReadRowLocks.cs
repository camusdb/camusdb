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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A Serializable read-write read that finds its rows through a secondary index (a GIN index over a
/// jsonb column, or an ordinary index) must hold a shared lock on every row it fetched, as a read by
/// primary key does. The index range lock alone does not protect the read: an UPDATE that leaves the
/// indexed value alone writes no index entry, so it never meets that range lock.
///
/// <para>The shape is the classic write skew. Two doctors are on call. Each transaction reads one
/// doctor through the index and then takes the other doctor off call by primary key. Each write
/// depends on a row the other transaction changes, so no serial order allows both commits. One
/// transaction must wait or be refused, and at least one doctor must stay on call.</para>
/// </summary>
[TestFixture(KeyValueTransactionLocking.Pessimistic)]
[TestFixture(KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
internal sealed class TestSerializableIndexReadRowLocks : BaseTest
{
    private readonly KeyValueTransactionLocking locking;

    public TestSerializableIndexReadRowLocks(KeyValueTransactionLocking locking) => this.locking = locking;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultTransactionLocking = locking };

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

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql, KvTransaction? tx = null)
    {
        KvTransaction own = tx ?? await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(own, db.Name, sql, null));
            return await cursor.ToListAsync();
        }
        finally
        {
            if (tx is null)
                await db.Transactions.RollbackIfNotCompletedAsync(own);
        }
    }

    private static string PlanOf(List<QueryResultRow> explain) =>
        string.Join(" | ", explain.Select(r => r.Row["node"].StrValue));

    /// <summary>Runs the write skew over the two reads and asserts that the outcome is serializable.</summary>
    private static async Task AssertNoWriteSkew(CommandExecutor executor, DatabaseDescriptor db, string readDoctor1, string readDoctor2, string expectedNode)
    {
        KvTransaction probe = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        string plan = PlanOf(await Select(executor, db, "EXPLAIN " + readDoctor1, probe));
        await db.Transactions.RollbackAsync(probe);
        Assert.That(plan, Does.Contain(expectedNode), "the read must use the index under test");

        KvTransaction a = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction b = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        int committed = 0;
        try
        {
            Assert.That((await Select(executor, db, readDoctor1, a)).Single().Row["oncall"].LongValue, Is.EqualTo(1));
            Assert.That((await Select(executor, db, readDoctor2, b)).Single().Row["oncall"].LongValue, Is.EqualTo(1));

            try
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(b, db.Name, "UPDATE doctors SET oncall = 0 WHERE id = 1", null));
                await db.Transactions.CommitAsync(b);
                committed++;
            }
            catch (CamusDBException)
            {
                await db.Transactions.RollbackIfNotCompletedAsync(b);
            }

            try
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(a, db.Name, "UPDATE doctors SET oncall = 0 WHERE id = 2", null));
                await db.Transactions.CommitAsync(a);
                committed++;
            }
            catch (CamusDBException)
            {
                await db.Transactions.RollbackIfNotCompletedAsync(a);
            }
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(a);
            await db.Transactions.RollbackIfNotCompletedAsync(b);
        }

        long total = (await Select(executor, db, "SELECT oncall FROM doctors")).Sum(r => r.Row["oncall"].LongValue);
        Assert.That(committed, Is.LessThan(2), "both Serializable transactions committed a write skew");
        Assert.That(total, Is.GreaterThanOrEqualTo(1), "write skew: both doctors are off call");
    }

    [Test]
    public async Task GinRead_WriteSkewIsRefused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE doctors (id int64 PRIMARY KEY, oncall int64, doc jsonb, INVERTED INDEX gin_doc (doc))", ddl: true);
        await Exec(executor, db, "INSERT INTO doctors (id, oncall, doc) VALUES (1, 1, '{\"k\":1}'), (2, 1, '{\"k\":2}')");

        await AssertNoWriteSkew(executor, db,
            "SELECT oncall FROM doctors WHERE doc @> '{\"k\":1}'",
            "SELECT oncall FROM doctors WHERE doc @> '{\"k\":2}'",
            "gin");
    }

    /// <summary>
    /// The GIN predicate recheck reads rows that the filter then drops. Those rows were read, so they
    /// are locked too: a write that would make a dropped row match must not commit while the reader
    /// lives. Here <c>doc ? 'k'</c> gives both doctors as candidates, and <c>oncall = 1</c> drops
    /// doctor 2.
    /// </summary>
    [Test]
    public async Task GinRead_RowsRejectedByTheRecheckAreLocked()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE doctors (id int64 PRIMARY KEY, oncall int64, doc jsonb, INVERTED INDEX gin_doc (doc))", ddl: true);
        await Exec(executor, db, "INSERT INTO doctors (id, oncall, doc) VALUES (1, 1, '{\"k\":1}'), (2, 0, '{\"k\":2}')");

        const string read = "SELECT id FROM doctors WHERE doc ? 'k' AND oncall = 1";

        KvTransaction a = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(PlanOf(await Select(executor, db, "EXPLAIN " + read, a)), Does.Contain("gin"));

            List<QueryResultRow> rows = await Select(executor, db, read, a);
            Assert.That(rows.Select(r => r.Row["id"].LongValue), Is.EquivalentTo(new long[] { 1 }));

            KvTransaction writer = await db.Transactions.BeginAsync();
            bool refused = false;
            try
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, db.Name, "UPDATE doctors SET oncall = 1 WHERE id = 2", null));
                await db.Transactions.CommitAsync(writer);
            }
            catch (CamusDBException)
            {
                refused = true;
            }
            finally
            {
                await db.Transactions.RollbackIfNotCompletedAsync(writer);
            }

            Assert.That(refused, Is.True, "a row the GIN read examined and dropped is locked against a concurrent commit");

            rows = await Select(executor, db, read, a);
            Assert.That(rows.Select(r => r.Row["id"].LongValue), Is.EquivalentTo(new long[] { 1 }), "the read repeats");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(a);
        }
    }

    [Test]
    public async Task SecondaryIndexEqualityRead_WriteSkewIsRefused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE doctors (id int64 PRIMARY KEY, oncall int64, k int64)", ddl: true);
        await Exec(executor, db, "CREATE INDEX doctors_k ON doctors (k)", ddl: true);
        await Exec(executor, db, "INSERT INTO doctors (id, oncall, k) VALUES (1, 1, 1), (2, 1, 2)");

        await AssertNoWriteSkew(executor, db,
            "SELECT oncall FROM doctors@{FORCE_INDEX=doctors_k} WHERE k = 1",
            "SELECT oncall FROM doctors@{FORCE_INDEX=doctors_k} WHERE k = 2",
            "index-");
    }

    [Test]
    public async Task SecondaryIndexRangeRead_WriteSkewIsRefused()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE doctors (id int64 PRIMARY KEY, oncall int64, k int64)", ddl: true);
        await Exec(executor, db, "CREATE INDEX doctors_k ON doctors (k)", ddl: true);
        await Exec(executor, db, "INSERT INTO doctors (id, oncall, k) VALUES (1, 1, 10), (2, 1, 20)");

        await AssertNoWriteSkew(executor, db,
            "SELECT oncall FROM doctors@{FORCE_INDEX=doctors_k} WHERE k >= 5 AND k <= 15",
            "SELECT oncall FROM doctors@{FORCE_INDEX=doctors_k} WHERE k >= 15 AND k <= 25",
            "index-");
    }
}
