
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies explicit multi-statement Serializable read-only transactions (Task 13).
///
/// Before Task 13, Serializable+ReadOnly transactions carried TransactionId = Zero (because the
/// Kahuna minting transaction was immediately rolled back), making them unregisterable and
/// unresumable by the HTTP layer. Task 13 keeps the minting transaction alive, giving the snapshot
/// a real, unique TransactionId that can be tracked and resumed across requests.
///
/// These tests confirm:
///   - Begin returns a real, non-Zero TransactionId and a non-Zero ReadTimestamp (= T).
///   - Multiple reads within the same tx all observe the snapshot pinned at T.
///   - Rows committed before T are visible; rows committed after T are invisible.
///   - Phantom inserts after T are invisible on a re-scan.
///   - Concurrent writers are not blocked (no locks held by the RO snapshot).
///   - Commit and rollback both finalize cleanly.
/// </summary>
[TestFixture]
public sealed class TestSerializableReadOnlyExplicit : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupDbAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("value", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        return (dbname, db, executor);
    }

    private static async Task<List<QueryResultRow>> ScanAsync(
        string dbname, CommandExecutor executor, KvTransaction tx)
    {
        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "items",
            index: null, projection: null, where: null,
            filters: null, orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(q);
        return await cursor.ToListAsync();
    }

    private static async Task<List<QueryResultRow>> ReadByIdAsync(
        string dbname, CommandExecutor executor, KvTransaction tx, string id)
    {
        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "items",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(q);
        return await cursor.ToListAsync();
    }

    private static async Task InsertAsync(
        string dbname, CommandExecutor executor, KvTransaction tx, string id, long value)
    {
        await executor.Insert(new InsertTicket(
            txnState: tx, databaseName: dbname, tableName: "items",
            values: new() { new() {
                { "id",    new(ColumnType.Id,        id)    },
                { "value", new(ColumnType.Integer64, value) },
            }}));
    }

    // -----------------------------------------------------------------------
    // 1. Begin returns a real (non-Zero) TransactionId and ReadTimestamp
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_ExplicitBegin_HasRealTransactionId()
    {
        (string _, DatabaseDescriptor db, CommandExecutor _) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.AreNotEqual(HLCTimestamp.Zero, tx.TransactionId,
            "Serializable+RO transaction must have a real, non-Zero TransactionId for HTTP resumability");
        Assert.AreNotEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "Serializable+RO transaction must carry a non-Zero snapshot ReadTimestamp");
        Assert.AreEqual(tx.TransactionId, tx.ReadTimestamp,
            "TransactionId (= handle) and ReadTimestamp (= snapshot T) must be the same minted HLC timestamp");
        Assert.IsTrue(tx.IsReadOnly, "Transaction must be flagged read-only");

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 2. Multiple reads observe the same snapshot — non-repeatable read eliminated
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_MultiStatement_ReadsAreAtSnapshotT()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id = ObjectIdGenerator.Generate().ToString();

        // Seed row before snapshot.
        KvTransaction setup = await db.Transactions.BeginAsync();
        await InsertAsync(dbname, executor, setup, id, 42L);
        await db.Transactions.CommitAsync(setup);

        // Open the RO snapshot — T is captured here.
        KvTransaction ro = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // First read: sees the seeded row.
        List<QueryResultRow> first = await ReadByIdAsync(dbname, executor, ro, id);
        Assert.AreEqual(1, first.Count, "Row seeded before T must be visible");
        Assert.AreEqual(42L, first[0].Row["value"].LongValue);

        // A concurrent writer updates the row after T.
        KvTransaction writer = await db.Transactions.BeginAsync();
        await executor.Update(new UpdateTicket(
            txnState: writer, databaseName: dbname, tableName: "items",
            plainValues: new() { { "value", new(ColumnType.Integer64, 99L) } },
            exprValues: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            parameters: null));
        await db.Transactions.CommitAsync(writer);

        // Second read within the RO snapshot: must still see 42, not 99.
        List<QueryResultRow> second = await ReadByIdAsync(dbname, executor, ro, id);
        Assert.AreEqual(1, second.Count, "Row must still be visible on second read");
        Assert.AreEqual(42L, second[0].Row["value"].LongValue,
            "Snapshot must be stable — concurrent update after T must not bleed through");

        await db.Transactions.RollbackAsync(ro);
    }

    // -----------------------------------------------------------------------
    // 3. Phantom eliminated — rows inserted after T are invisible on re-scan
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_Phantom_Eliminated_MultiStatement()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string idA = ObjectIdGenerator.Generate().ToString();

        KvTransaction setup = await db.Transactions.BeginAsync();
        await InsertAsync(dbname, executor, setup, idA, 1L);
        await db.Transactions.CommitAsync(setup);

        KvTransaction ro = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        List<QueryResultRow> scan1 = await ScanAsync(dbname, executor, ro);
        Assert.AreEqual(1, scan1.Count, "Initial scan must return only the one seeded row");

        // Phantom insert after T.
        string idB = ObjectIdGenerator.Generate().ToString();
        KvTransaction phantom = await db.Transactions.BeginAsync();
        await InsertAsync(dbname, executor, phantom, idB, 2L);
        await db.Transactions.CommitAsync(phantom);

        // Re-scan within the same RO tx: phantom row must be invisible.
        List<QueryResultRow> scan2 = await ScanAsync(dbname, executor, ro);
        Assert.AreEqual(1, scan2.Count,
            "Phantom row committed after T must not appear in a re-scan within the same snapshot");

        await db.Transactions.CommitAsync(ro);
    }

    // -----------------------------------------------------------------------
    // 4. Commit finalizes the empty transaction cleanly
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_CommitIsClean()
    {
        (string _, DatabaseDescriptor db, CommandExecutor _) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.DoesNotThrowAsync(() => db.Transactions.CommitAsync(tx),
            "CommitAsync on an empty Serializable+RO transaction must not throw");

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status,
            "Status must be Committed after CommitAsync");
    }

    // -----------------------------------------------------------------------
    // 5. Rollback finalizes the empty transaction cleanly
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_RollbackIsClean()
    {
        (string _, DatabaseDescriptor db, CommandExecutor _) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.DoesNotThrowAsync(() => db.Transactions.RollbackAsync(tx),
            "RollbackAsync on an empty Serializable+RO transaction must not throw");

        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status,
            "Status must be RolledBack after RollbackAsync");
    }

    // -----------------------------------------------------------------------
    // 6. RO snapshot holds no locks — concurrent writer succeeds immediately
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_LockFree_ConcurrentWriterSucceeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id = ObjectIdGenerator.Generate().ToString();

        KvTransaction setup = await db.Transactions.BeginAsync();
        await InsertAsync(dbname, executor, setup, id, 10L);
        await db.Transactions.CommitAsync(setup);

        // Open the RO snapshot.
        KvTransaction ro = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Read the row (no lock should be acquired by an RO snapshot tx).
        List<QueryResultRow> before = await ReadByIdAsync(dbname, executor, ro, id);
        Assert.AreEqual(10L, before[0].Row["value"].LongValue);

        // A concurrent writer should not be blocked (no lock contention from the RO snapshot).
        KvTransaction writer = await db.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(async () =>
        {
            await executor.Update(new UpdateTicket(
                txnState: writer, databaseName: dbname, tableName: "items",
                plainValues: new() { { "value", new(ColumnType.Integer64, 20L) } },
                exprValues: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, id)) },
                parameters: null));
            await db.Transactions.CommitAsync(writer);
        }, "Concurrent writer must not be blocked by the Serializable+RO snapshot (no locks held)");

        // RO snapshot still sees the original value.
        List<QueryResultRow> after = await ReadByIdAsync(dbname, executor, ro, id);
        Assert.AreEqual(10L, after[0].Row["value"].LongValue,
            "Snapshot must still return original value even after concurrent write+commit");

        await db.Transactions.RollbackAsync(ro);
    }
}
