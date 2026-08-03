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
/// Single-statement DML correctness must not depend on which isolation level or concurrency
/// strategy a transaction runs under. The engine ships with Serializable + Pessimistic, and almost
/// the whole suite therefore exercises only that combination — every one of the other three cells
/// takes a materially different code path through the store:
///
/// <list type="bullet">
///   <item>Read Committed skips the shared range/point predicate locks that Serializable acquires on
///   every read and scan, so reads, and the read-then-write upgrade, run through different branches.</item>
///   <item>Optimistic skips the explicit exclusive write lock entirely — index and row writes reach
///   Kahuna with no prior acquire, and conflicts surface at commit instead of at write time.</item>
/// </list>
///
/// This fixture runs the same uncontended CRUD, index-maintenance, and rollback assertions in all
/// four cells so a divergence between them fails loudly. Concurrency-specific behaviour is not tested
/// here — see the anomaly and optimistic-conflict fixtures for that. The cell is applied through
/// <see cref="CamusDBOptions"/> and picked up by the parameterless <c>BeginAsync()</c> every test
/// uses, so nothing below states its isolation or locking mode explicitly.
/// </summary>
[TestFixture(CamusIsolationLevel.Serializable,  KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable,  KeyValueTransactionLocking.Optimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestTransactionModeMatrix : SharedNodeBaseTest
{
    private readonly CamusIsolationLevel isolationLevel;
    private readonly KeyValueTransactionLocking locking;

    public TestTransactionModeMatrix(CamusIsolationLevel isolationLevel, KeyValueTransactionLocking locking)
    {
        this.isolationLevel = isolationLevel;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultIsolationLevel = isolationLevel, DefaultTransactionLocking = locking };

    // items(id String PK, name String, tier String [secondary index], code String [unique index])
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupItemsAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE items (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL, tier STRING NOT NULL, code STRING NOT NULL)");
        await ExecDDL(executor, database, dbname, "CREATE INDEX items_tier ON items (tier)");
        await ExecDDL(executor, database, dbname, "CREATE UNIQUE INDEX items_code_uq ON items (code)");

        return (dbname, database, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs one DML statement in its own transaction and commits it.</summary>
    private static async Task ExecDML(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>Reads inside an already-open transaction, leaving it open for the caller.</summary>
    private static async Task<List<QueryResultRow>> SelectIn(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        return await cursor.ToListAsync();
    }

    /// <summary>Reads in a fresh committed transaction — what a later observer of the database sees.</summary>
    private static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        List<QueryResultRow> rows = await SelectIn(executor, dbname, tx, sql);
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task SeedGoldItemAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
        => await ExecDML(executor, database, dbname,
            "INSERT INTO items (id, name, tier, code) VALUES (\"i1\", \"widget\", \"gold\", \"C1\")");

    // -----------------------------------------------------------------------
    // Guard for the fixture itself: every test below relies on the parameterless
    // BeginAsync() inheriting the cell from the options. If that wiring broke, all
    // four cells would quietly run as the shipped default and this fixture would
    // report four times the coverage it actually has.
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheTransactionsThisFixtureBeginsCarryItsCell()
    {
        (_, DatabaseDescriptor database, _) = await SetupItemsAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            Assert.AreEqual(isolationLevel, tx.IsolationLevel,
                "a transaction begun with no arguments must inherit the fixture's isolation level");
            Assert.AreEqual(locking, tx.Locking,
                "a transaction begun with no arguments must inherit the fixture's locking mode");
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    // -----------------------------------------------------------------------
    // A committed write is durable and visible to a later transaction.
    // -----------------------------------------------------------------------

    [Test]
    public async Task CommittedInsert_IsVisibleToALaterTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        await SeedGoldItemAsync(executor, database, dbname);

        List<QueryResultRow> rows = await Select(executor, database, dbname,
            "SELECT id, name FROM items WHERE id = \"i1\"");

        Assert.AreEqual(1, rows.Count, "the committed row must be visible to a later transaction");
        Assert.AreEqual("widget", rows[0].Row["name"].StrValue);
    }

    // -----------------------------------------------------------------------
    // A transaction sees its own uncommitted writes, and nobody else ever does
    // once it rolls back.
    // -----------------------------------------------------------------------

    [Test]
    public async Task UncommittedInsert_IsReadableByItsOwnTransaction_AndVanishesOnRollback()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "INSERT INTO items (id, name, tier, code) VALUES (\"i1\", \"widget\", \"gold\", \"C1\")", null));

        List<QueryResultRow> own = await SelectIn(executor, dbname, tx, "SELECT id FROM items WHERE id = \"i1\"");
        Assert.AreEqual(1, own.Count, "a transaction must read its own uncommitted insert");

        await database.Transactions.RollbackAsync(tx);

        List<QueryResultRow> after = await Select(executor, database, dbname, "SELECT id FROM items");
        Assert.AreEqual(0, after.Count, "a rolled-back insert must leave no row behind");
    }

    // -----------------------------------------------------------------------
    // Index maintenance: an UPDATE that changes an indexed column must remove the
    // old index entry and write the new one, in the same transaction as the row.
    // -----------------------------------------------------------------------

    [Test]
    public async Task Update_MovesTheRowBetweenSecondaryIndexEntries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        await SeedGoldItemAsync(executor, database, dbname);
        await ExecDML(executor, database, dbname, "UPDATE items SET tier = \"silver\" WHERE id = \"i1\"");

        List<QueryResultRow> gold   = await Select(executor, database, dbname, "SELECT id FROM items WHERE tier = \"gold\"");
        List<QueryResultRow> silver = await Select(executor, database, dbname, "SELECT id FROM items WHERE tier = \"silver\"");

        Assert.AreEqual(0, gold.Count,   "the stale secondary index entry must be removed by the update");
        Assert.AreEqual(1, silver.Count, "the new secondary index entry must be written by the update");
    }

    // -----------------------------------------------------------------------
    // A DELETE must take the row and every index entry that pointed at it.
    // -----------------------------------------------------------------------

    [Test]
    public async Task Delete_RemovesTheRowAndItsIndexEntries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        await SeedGoldItemAsync(executor, database, dbname);
        await ExecDML(executor, database, dbname, "DELETE FROM items WHERE id = \"i1\"");

        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM items")).Count,
            "the deleted row must be gone from a full scan");
        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM items WHERE tier = \"gold\"")).Count,
            "the secondary index entry must not outlive the row it pointed at");
        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM items WHERE id = \"i1\"")).Count,
            "the primary key entry must not outlive the row");

        // The unique index entry must be released too, or this insert would be rejected as a duplicate.
        Assert.DoesNotThrowAsync(async () => await ExecDML(executor, database, dbname,
            "INSERT INTO items (id, name, tier, code) VALUES (\"i2\", \"other\", \"gold\", \"C1\")"),
            "deleting a row must free its unique index entry for reuse");
    }

    // -----------------------------------------------------------------------
    // A rolled-back UPDATE must leave both the row and its index entries exactly
    // as they were — a half-applied index is the failure this guards.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RolledBackUpdate_LeavesTheRowAndIndexEntriesUntouched()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        await SeedGoldItemAsync(executor, database, dbname);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            "UPDATE items SET tier = \"silver\" WHERE id = \"i1\"", null));
        await database.Transactions.RollbackAsync(tx);

        List<QueryResultRow> gold   = await Select(executor, database, dbname, "SELECT id FROM items WHERE tier = \"gold\"");
        List<QueryResultRow> silver = await Select(executor, database, dbname, "SELECT id FROM items WHERE tier = \"silver\"");

        Assert.AreEqual(1, gold.Count,   "the original index entry must survive a rolled-back update");
        Assert.AreEqual(0, silver.Count, "the rolled-back update must leave no index entry behind");
    }

    // -----------------------------------------------------------------------
    // Duplicate rejection. Where the failure surfaces differs by cell — pessimistic
    // detects it at write time, optimistic can defer it to commit — so the assertion
    // is on the outcome both must produce: the statement fails and one row exists.
    // -----------------------------------------------------------------------

    [Test]
    public async Task DuplicatePrimaryKey_IsRejected_AndOnlyTheFirstRowPersists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        await SeedGoldItemAsync(executor, database, dbname);

        Assert.That(await TryExecDml(executor, database, dbname,
            "INSERT INTO items (id, name, tier, code) VALUES (\"i1\", \"clone\", \"gold\", \"C2\")"),
            Is.False, "re-inserting an existing primary key must fail");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT id, name FROM items");
        Assert.AreEqual(1, rows.Count, "the rejected duplicate must not have persisted");
        Assert.AreEqual("widget", rows[0].Row["name"].StrValue, "the original row must be untouched");
    }

    [Test]
    public async Task DuplicateUniqueIndexValue_IsRejected_AndOnlyTheFirstRowPersists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupItemsAsync();

        await SeedGoldItemAsync(executor, database, dbname);

        Assert.That(await TryExecDml(executor, database, dbname,
            "INSERT INTO items (id, name, tier, code) VALUES (\"i2\", \"clone\", \"gold\", \"C1\")"),
            Is.False, "re-using an existing unique index value must fail");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT id FROM items");
        Assert.AreEqual(1, rows.Count, "the rejected duplicate must not have persisted");
    }

    /// <summary>
    /// Runs one DML statement in its own transaction and reports whether it committed. Both the
    /// statement and the commit are treated as the same outcome on purpose: a pessimistic
    /// transaction rejects a constraint violation at write time, while an optimistic one may only
    /// discover it at commit, and this fixture asserts the result rather than the timing.
    /// </summary>
    private static async Task<bool> TryExecDml(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
            return true;
        }
        catch (CamusDBException)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            return false;
        }
    }
}
