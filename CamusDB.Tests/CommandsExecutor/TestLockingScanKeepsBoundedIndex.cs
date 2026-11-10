/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// A statement whose scan holds predicate locks never gives up a predicate-bounded index scan for
/// a table scan. The cost model prices reads only, and on a table of one or two rows it finds the
/// table scan as cheap as the index. In a Serializable read-write transaction that table scan
/// share-locks the whole row range, so one player's read conflicts with every write to the table.
///
/// <para>The plan cache made the small-table choice last: the first execution of a statement shape
/// on a table with one or two rows stored a table-scan decision, and every later execution
/// replayed it while the table grew. The cache tests state the two guards: a locking statement and
/// a lock-free statement of one shape do not share a decision, and a decision is planned again
/// when the row count of the table moved far from the one it was made for.</para>
/// </summary>
[TestFixture(KeyValueTransactionLocking.Pessimistic)]
[TestFixture(KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
internal sealed class TestLockingScanKeepsBoundedIndex : BaseTest
{
    private const string ItemRead =
        "SELECT u.id, u.stackKey FROM items AS u WHERE u.usersId = 'p1' AND u.itemId = 'sword' ORDER BY u.id";

    private const string WalletRead = "SELECT u.currencyId, u.amount FROM wallet AS u WHERE u.usersId = 'p1'";

    private readonly KeyValueTransactionLocking locking;

    public TestLockingScanKeepsBoundedIndex(KeyValueTransactionLocking locking) => this.locking = locking;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultTransactionLocking = locking, PlanCacheEnabled = true };

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

    private static async Task<string> PlanOf(CommandExecutor executor, DatabaseDescriptor db, string sql, KvTransaction? tx = null) =>
        string.Join(" | ", (await Select(executor, db, "EXPLAIN " + sql, tx)).Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));

    /// <summary>The plan of <paramref name="sql"/> in a new Serializable read-write transaction.</summary>
    private static async Task<string> SerializablePlanOf(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            return await PlanOf(executor, db, sql, tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Runs <paramref name="sql"/> in a Read Committed transaction, whose scans hold no predicate
    /// locks: the planner keeps its cost choice for it, table scan included.
    /// </summary>
    private static async Task<List<QueryResultRow>> LockFreeSelect(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            return await Select(executor, db, sql, tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<string> LockFreePlanOf(CommandExecutor executor, DatabaseDescriptor db, string sql) =>
        string.Join(" | ", (await LockFreeSelect(executor, db, "EXPLAIN " + sql)).Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));

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

    /// <summary>The two tables, with <paramref name="players"/> players of one item and one currency each.</summary>
    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> CreateTables(int players)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await Exec(executor, db,
            "CREATE TABLE items (id INT64 PRIMARY KEY, usersId STRING(36) NOT NULL, itemId STRING(36) NOT NULL, stackKey STRING)", ddl: true);
        await Exec(executor, db, "CREATE INDEX items_user_item ON items (usersId, itemId, id)", ddl: true);
        await Exec(executor, db,
            "CREATE TABLE wallet (usersId STRING(36) NOT NULL, currencyId STRING(36) NOT NULL, amount INT64 NOT NULL, PRIMARY KEY (usersId, currencyId))", ddl: true);

        await AddPlayers(executor, db, first: 1, count: players);
        return (db, executor);
    }

    private static async Task AddPlayers(CommandExecutor executor, DatabaseDescriptor db, int first, int count)
    {
        if (count == 0)
            return;

        IEnumerable<int> ids = Enumerable.Range(first, count);
        await Exec(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES "
            + string.Join(", ", ids.Select(i => $"({i}, 'p{i}', 'sword', 's{i}')")));
        await Exec(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES "
            + string.Join(", ", ids.Select(i => $"('p{i}', 'gold', {i})")));
    }

    // ── Plans ────────────────────────────────────────────────────────────────

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(40)]
    public async Task SerializableReadUsesTheIndexAtEveryTableSize(int players)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players);

        Assert.That(await SerializablePlanOf(executor, db, ItemRead), Does.Contain("items_user_item").And.Not.Contain("table-scan"));
        Assert.That(await SerializablePlanOf(executor, db, WalletRead), Does.Contain(CamusDBConstants.PrimaryKeyInternalName).And.Not.Contain("table-scan"));

        // Analyzed statistics select the cost-based path, which prices a table scan of a small
        // table below an index scan plus its row fetches.
        await Select(executor, db, "ANALYZE TABLE items");
        await Select(executor, db, "ANALYZE TABLE wallet");

        Assert.That(await SerializablePlanOf(executor, db, ItemRead), Does.Contain("items_user_item").And.Not.Contain("table-scan"), "after ANALYZE");
        Assert.That(await SerializablePlanOf(executor, db, WalletRead), Does.Contain(CamusDBConstants.PrimaryKeyInternalName).And.Not.Contain("table-scan"), "after ANALYZE");
        Assert.That(await SerializablePlanOf(executor, db, ItemRead + " LIMIT 1"), Does.Contain("items_user_item").And.Not.Contain("table-scan"), "with a LIMIT");
    }

    /// <summary>
    /// One player owns every row, as in a database that one test uses. After ANALYZE the equality
    /// selects the whole table, so by read cost the table scan is cheaper than the index at any
    /// size. A lock-free read takes that table scan; a locking read keeps the index.
    /// </summary>
    [TestCase(4)]
    [TestCase(60)]
    public async Task SerializableReadUsesTheIndexWhenTheFilterSelectsTheWholeTable(int rows)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players: 0);
        await Exec(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES "
            + string.Join(", ", Enumerable.Range(1, rows).Select(i => $"({i}, 'p1', 'sword', 's{i}')")));
        await Exec(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES "
            + string.Join(", ", Enumerable.Range(1, rows).Select(i => $"('p1', 'c{i}', {i})")));
        await Select(executor, db, "ANALYZE TABLE items");
        await Select(executor, db, "ANALYZE TABLE wallet");

        Assert.That(await LockFreePlanOf(executor, db, ItemRead), Does.Contain("table-scan"), "the cost choice of a lock-free read");
        Assert.That(await LockFreePlanOf(executor, db, WalletRead), Does.Contain("table-scan"), "the cost choice of a lock-free read");

        Assert.That(await SerializablePlanOf(executor, db, ItemRead), Does.Contain("items_user_item").And.Not.Contain("table-scan"));
        Assert.That(await SerializablePlanOf(executor, db, WalletRead), Does.Contain(CamusDBConstants.PrimaryKeyInternalName).And.Not.Contain("table-scan"));
    }

    /// <summary>
    /// The list gate prices each value at a tenth of a table with no column statistics, so a list
    /// of five or more values reads the whole table. A locking statement has no such gate.
    /// </summary>
    [Test]
    public async Task SerializableListReadSeeksWithoutStatistics()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players: 40);

        const string read = "SELECT u.currencyId FROM wallet AS u WHERE u.usersId IN ('p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8')";

        Assert.That(await SerializablePlanOf(executor, db, read), Does.Contain("index-in-list").And.Not.Contain("table-scan"));
    }

    /// <summary>The same rule for the locate scan of a write, whose lock is exclusive.</summary>
    [Test]
    public async Task UpdateOnATinyTableLocksOneKeyRange()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players: 2);

        KvTransaction writer = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, db.Name, "UPDATE wallet SET amount = 100 WHERE usersId = 'p1'", null));

            Assert.That(await TryWrite(executor, db, "UPDATE wallet SET amount = 200 WHERE usersId = 'p2' AND currencyId = 'gold'"), Is.True,
                "a write to another player's row must not conflict with the update");

            await db.Transactions.CommitAsync(writer);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(writer);
        }

        Assert.That((await Select(executor, db, "SELECT amount FROM wallet WHERE usersId = 'p2'")).Single().Row["amount"].LongValue, Is.EqualTo(200));
    }

    // ── Locks ────────────────────────────────────────────────────────────────

    [TestCase(1)]
    [TestCase(2)]
    public async Task SerializableReadOnATinyTableDoesNotLockOtherPlayers(int players)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players);

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(await Select(executor, db, ItemRead, reader), Has.Count.EqualTo(1));
            Assert.That(await Select(executor, db, WalletRead, reader), Has.Count.EqualTo(1));

            Assert.That(await TryWrite(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES (900, 'p9', 'sword', 'new')"), Is.True,
                "an insert for another player must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES ('p9', 'gold', 9)"), Is.True,
                "an insert for another player must not conflict with the read");

            Assert.That(await TryWrite(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES (901, 'p1', 'sword', 'phantom')"), Is.False,
                "a new row of the player the read named is a phantom");
            Assert.That(await TryWrite(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES ('p1', 'gems', 1)"), Is.False,
                "a new row of the player the read named is a phantom");

            Assert.That(await Select(executor, db, ItemRead, reader), Has.Count.EqualTo(1), "the read repeats");
            Assert.That(await Select(executor, db, WalletRead, reader), Has.Count.EqualTo(1), "the read repeats");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    /// <summary>
    /// The leaves of a join follow the same rule: each leaf whose filter bounds an index reads
    /// through it, so the join locks the named player in both tables and no other row.
    /// </summary>
    [TestCase(1)]
    [TestCase(2)]
    public async Task SerializableJoinOnTinyTablesDoesNotLockOtherPlayers(int players)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players);

        const string join =
            "SELECT i.id AS id, w.amount AS amount FROM items i JOIN wallet w ON w.usersId = i.usersId "
            + "WHERE i.usersId = 'p1' AND i.itemId = 'sword' AND w.usersId = 'p1'";

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            string plan = await PlanOf(executor, db, join, reader);
            Assert.That(plan, Does.Not.Contain("table-scan"), plan);

            Assert.That(await Select(executor, db, join, reader), Has.Count.EqualTo(1));

            Assert.That(await TryWrite(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES (900, 'p9', 'sword', 'new')"), Is.True,
                "an insert for another player must not conflict with the join");
            Assert.That(await TryWrite(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES ('p9', 'gold', 9)"), Is.True,
                "an insert for another player must not conflict with the join");

            Assert.That(await TryWrite(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES (901, 'p1', 'sword', 'phantom')"), Is.False,
                "a new row of the player the join named is a phantom");
            Assert.That(await TryWrite(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES ('p1', 'gems', 1)"), Is.False,
                "a new row of the player the join named is a phantom");

            Assert.That(await Select(executor, db, join, reader), Has.Count.EqualTo(1), "the join repeats");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    // ── Plan cache ───────────────────────────────────────────────────────────

    /// <summary>
    /// A lock-free execution on a table of two rows stores its decision first. The locking
    /// execution of the same shape must not replay it.
    /// </summary>
    [Test]
    public async Task LockingStatementDoesNotReplayALockFreeDecision()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players: 2);

        // The lock-free statement plans a table scan on two rows and stores that decision.
        Assert.That(await LockFreePlanOf(executor, db, ItemRead), Does.Contain("table-scan"));
        Assert.That(await LockFreePlanOf(executor, db, WalletRead), Does.Contain("table-scan"));
        Assert.That(await LockFreeSelect(executor, db, ItemRead), Has.Count.EqualTo(1));
        Assert.That(await LockFreeSelect(executor, db, WalletRead), Has.Count.EqualTo(1));

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(await Select(executor, db, ItemRead, reader), Has.Count.EqualTo(1));
            Assert.That(await Select(executor, db, WalletRead, reader), Has.Count.EqualTo(1));

            Assert.That(await TryWrite(executor, db, "INSERT INTO items (id, usersId, itemId, stackKey) VALUES (900, 'p9', 'sword', 'new')"), Is.True,
                "the locking read must use the index, not the cached decision of the lock-free read");
            Assert.That(await TryWrite(executor, db, "INSERT INTO wallet (usersId, currencyId, amount) VALUES ('p9', 'gold', 9)"), Is.True,
                "the locking read must use the index, not the cached decision of the lock-free read");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    /// <summary>
    /// A decision made for a table of two rows is planned again after the table grew: the row
    /// count is part of what the decision was made for.
    /// </summary>
    [Test]
    public async Task DecisionMadeOnATinyTableIsPlannedAgainAfterTheTableGrows()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateTables(players: 2);

        Assert.That(await LockFreePlanOf(executor, db, ItemRead), Does.Contain("table-scan"));
        Assert.That(await LockFreePlanOf(executor, db, WalletRead), Does.Contain("table-scan"));
        Assert.That(await LockFreeSelect(executor, db, ItemRead), Has.Count.EqualTo(1));
        Assert.That(await LockFreeSelect(executor, db, WalletRead), Has.Count.EqualTo(1));

        await AddPlayers(executor, db, first: 3, count: 400);

        Assert.That(await LockFreePlanOf(executor, db, ItemRead), Does.Contain("items_user_item").And.Not.Contain("table-scan"));
        Assert.That(await LockFreePlanOf(executor, db, WalletRead), Does.Contain(CamusDBConstants.PrimaryKeyInternalName).And.Not.Contain("table-scan"));
    }
}
