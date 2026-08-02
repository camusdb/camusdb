
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

using Kahuna;

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
/// Verifies the Task 2 cheap serializable snapshot for autocommit SELECT.
///
/// Under Serializable default, <see cref="KvTransactionsManager.BeginReadOnlyAsync"/> (non-key-range
/// mode) mints a local HLC timestamp <c>T</c> and returns a zero-identity
/// <see cref="KvTransaction"/> carrying <c>ReadTimestamp = T</c> — no Kahuna
/// StartTransaction/CommitTransaction round-trip, no session, no cleanup.
///
/// Every engine this fixture builds defaults to Serializable; the one case that needs Read Committed
/// builds its own engine rather than changing anything the others can see.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableCheapSnapshot : SharedNodeBaseTest
{
    /// <summary>
    /// The cheap snapshot is a Serializable-only path, so every engine this fixture builds defaults to
    /// Serializable unless a test asks for something else.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultIsolationLevel = CamusIsolationLevel.Serializable };

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupTableAsync(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options ?? Options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("balance", ColumnType.Integer64),
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

    private static async Task InsertRowAsync(
        string dbname, DatabaseDescriptor db, CommandExecutor executor,
        string id, long balance)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        id)      },
                { "balance", new(ColumnType.Integer64, balance) },
            }}));
        await db.Transactions.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 1. Structural: Serializable default → BeginReadOnlyAsync returns a zero-identity
    //    transaction carrying a non-Zero ReadTimestamp (cheap local HLC snapshot).
    // -----------------------------------------------------------------------

    [Test]
    public async Task BeginReadOnly_SerializableDefault_CheapSnapshot_HasNonZeroReadTimestamp()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync();

        KvTransaction tx = await db.Transactions.BeginReadOnlyAsync(promote: false);

        Assert.AreEqual(HLCTimestamp.Zero, tx.TransactionId,
            "Cheap snapshot must be zero-identity — no Kahuna tx object");
        Assert.AreNotEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "Cheap snapshot must carry a non-Zero local HLC timestamp T for AS-OF reads");
        Assert.IsTrue(tx.IsReadOnly);
        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadOnly, tx.TransactionMode);
    }

    // -----------------------------------------------------------------------
    // 2. RC default: BeginReadOnlyAsync still returns Zero ReadTimestamp (unchanged).
    // -----------------------------------------------------------------------

    [Test]
    public async Task BeginReadOnly_ReadCommittedDefault_HasZeroReadTimestamp()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync(
            Options with { DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted });

        KvTransaction tx = await db.Transactions.BeginReadOnlyAsync(promote: false);

        Assert.AreEqual(HLCTimestamp.Zero, tx.TransactionId,
            "RC fast-path must be zero-identity");
        Assert.AreEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "RC path must keep Zero ReadTimestamp — latest-committed-per-key semantics");
        Assert.IsTrue(tx.IsReadOnly);
    }

    // -----------------------------------------------------------------------
    // 3. Snapshot monotonicity: two consecutive cheap snapshots each advance T.
    // -----------------------------------------------------------------------

    [Test]
    public async Task BeginReadOnly_SerializableDefault_ConsecutiveSnapshotsAreMonotone()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync();

        KvTransaction t1 = await db.Transactions.BeginReadOnlyAsync(promote: false);
        KvTransaction t2 = await db.Transactions.BeginReadOnlyAsync(promote: false);

        // The local HLC SendOrLocalEvent guarantees strictly increasing timestamps for events
        // on the same node. So T2 must be strictly greater than T1.
        Assert.IsTrue(
            t2.ReadTimestamp.L > t1.ReadTimestamp.L ||
            (t2.ReadTimestamp.L == t1.ReadTimestamp.L && t2.ReadTimestamp.C > t1.ReadTimestamp.C),
            $"Snapshot T2 ({t2.ReadTimestamp}) must be > T1 ({t1.ReadTimestamp})");
    }

    // -----------------------------------------------------------------------
    // 4. Behavioral: data committed AFTER the snapshot timestamp is not visible.
    //    Proves the AS-OF gate works with the cheap zero-identity snapshot.
    // -----------------------------------------------------------------------

    [Test]
    public async Task CheapSnapshot_SerializableDefault_WritesAfterTAreNotVisible()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        // Mint a cheap snapshot T1 (before the update below).
        KvTransaction snapshot = await db.Transactions.BeginReadOnlyAsync(promote: false);
        Assert.AreNotEqual(HLCTimestamp.Zero, snapshot.ReadTimestamp);

        // Commit an update at T2 > T1.
        KvTransaction rw = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await executor.Update(new UpdateTicket(
            txnState: rw, databaseName: dbname, tableName: "accounts",
            plainValues: new() { { "balance", new(ColumnType.Integer64, 999L) } },
            exprValues: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            parameters: null));
        await db.Transactions.CommitAsync(rw);

        // Read via the snapshot — should still see 100, not 999.
        QueryTicket q = new(
            txnState: snapshot, databaseName: dbname, tableName: "accounts",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        List<QueryResultRow> list = await rows.ToListAsync();

        Assert.AreEqual(1, list.Count);
        Assert.AreEqual(100L, list[0].Row["balance"].LongValue,
            "Snapshot at T1 must not see the update committed after T1");
    }

    // -----------------------------------------------------------------------
    // 5. Behavioral: data committed BEFORE the snapshot IS visible.
    // -----------------------------------------------------------------------

    [Test]
    public async Task CheapSnapshot_SerializableDefault_WritesBeforeTAreVisible()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        // Mint snapshot AFTER the seed insert.
        KvTransaction snapshot = await db.Transactions.BeginReadOnlyAsync(promote: false);

        QueryTicket q = new(
            txnState: snapshot, databaseName: dbname, tableName: "accounts",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        List<QueryResultRow> list = await rows.ToListAsync();

        Assert.AreEqual(1, list.Count, "Row inserted before T must be visible in the snapshot");
        Assert.AreEqual(100L, list[0].Row["balance"].LongValue);
    }

    // -----------------------------------------------------------------------
    // 6. Zero-identity: commit and rollback on a cheap snapshot are no-ops
    //    (no CamusDBException thrown).
    // -----------------------------------------------------------------------

    [Test]
    public async Task CheapSnapshot_SerializableDefault_CommitAndRollbackAreNoOps()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync();

        KvTransaction snapshot = await db.Transactions.BeginReadOnlyAsync(promote: false);

        Assert.DoesNotThrowAsync(async () =>
            await db.Transactions.CommitAsync(snapshot),
            "CommitAsync on a zero-identity cheap snapshot must be a no-op");

        Assert.DoesNotThrowAsync(async () =>
            await db.Transactions.RollbackIfNotCompletedAsync(snapshot),
            "RollbackIfNotCompletedAsync on a zero-identity cheap snapshot must be a no-op");
    }
}
