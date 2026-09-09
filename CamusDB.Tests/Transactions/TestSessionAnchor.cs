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

using Kahuna.Shared.Routing;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// A client-owned (deferred-start) transaction anchors its Kahuna coordinator session to the
/// placement group of the first table it touches, so the session lives on that table's data
/// partition. The anchor is a pure function of the coordinator key — the same hash rule Kahuna's
/// partition router applies to data keys — so the property is asserted here without a
/// multi-partition cluster: for every pool size the anchored key and the table's row key space
/// must fall in the same bucket. Eager transactions and deferred transactions that never touch
/// data keep the bare unique id.
/// </summary>
[TestFixture]
public sealed class TestSessionAnchor : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE accounts (id int64 primary key, balance int64 not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE audits (id int64 primary key, accountid int64 not null)");
        await ExecNonQuery(database, executor, dbname, null,
            "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)");
        await ExecNonQuery(database, executor, dbname, null,
            "INSERT INTO audits (id, accountid) VALUES (10, 1)");

        return (dbname, database, executor);
    }

    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs a mutation on <paramref name="tx"/>, or on a fresh eager transaction when null.</summary>
    private static async Task<int> ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, KvTransaction? tx, string sql)
    {
        bool own = tx is null;
        tx ??= await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        if (own)
            await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        return await cursor.ToListAsync();
    }

    private static async Task<KvTableStore> OpenStoreAsync(CommandExecutor executor, string dbname, string table)
        => (await executor.OpenTable(new OpenTableTicket(dbname, table))).Store;

    /// <summary>
    /// The anchored key and every key space of the table share a bucket for every pool size, which
    /// is what puts the session on the table's partition whatever the cluster's partition count.
    /// </summary>
    private static void AssertSharesBucketWithTable(string coordinatorKey, KvTableStore store)
    {
        for (int poolSize = 1; poolSize <= 16; poolSize++)
        {
            int session = HashPlacement.BucketOfKey(coordinatorKey, poolSize);
            Assert.That(session, Is.EqualTo(HashPlacement.BucketOfKeySpace(store.RowKeySpace, poolSize)),
                $"session bucket must equal the row bucket at pool size {poolSize}");
        }
    }

    [Test]
    public async Task DeferredTransaction_FirstStatement_AnchorsSessionToItsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        KvTableStore accounts = await OpenStoreAsync(executor, dbname, "accounts");

        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        Assert.That(tx.CoordinatorKey, Is.EqualTo(tx.UniqueId), "before the first operation the key is the bare id");

        List<QueryResultRow> rows = await ExecQuery(executor, dbname, tx, "SELECT balance FROM accounts WHERE id = 1");
        Assert.That(rows, Has.Count.EqualTo(1));

        Assert.That(tx.CoordinatorKey, Is.EqualTo(KvKeyBuilder.SessionAnchorKeyOf(accounts.PlacementGroup, tx.UniqueId)));
        Assert.That(tx.CoordinatorKey, Does.StartWith(accounts.PlacementGroup + KvKeyBuilder.SessionSpaceSuffix + "/"));
        Assert.That(tx.Handle.CoordinatorKey, Is.EqualTo(tx.CoordinatorKey), "finalize routes by the anchored key");
        AssertSharesBucketWithTable(tx.CoordinatorKey, accounts);

        await database.Transactions.CommitAsync(tx);
    }

    [Test]
    public async Task DeferredTransaction_LaterStatementsOnOtherTables_DoNotMoveTheAnchor()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        KvTableStore accounts = await OpenStoreAsync(executor, dbname, "accounts");

        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        _ = await ExecQuery(executor, dbname, tx, "SELECT balance FROM accounts WHERE id = 1");
        string anchored = tx.CoordinatorKey;

        Assert.That(await ExecNonQuery(database, executor, dbname, tx, "UPDATE audits SET accountid = 2 WHERE id = 10"), Is.EqualTo(1));
        Assert.That(await ExecNonQuery(database, executor, dbname, tx, "UPDATE accounts SET balance = 150 WHERE id = 1"), Is.EqualTo(1));

        Assert.That(tx.CoordinatorKey, Is.EqualTo(anchored), "a session anchors once and never moves");
        AssertSharesBucketWithTable(tx.CoordinatorKey, accounts);

        await database.Transactions.CommitAsync(tx);

        KvTransaction verify = await database.Transactions.BeginAsync();
        List<QueryResultRow> rows = await ExecQuery(executor, dbname, verify, "SELECT balance FROM accounts WHERE id = 1");
        await database.Transactions.CommitAsync(verify);
        Assert.That(rows[0].Row["balance"].LongValue, Is.EqualTo(150), "the anchored transaction committed its writes");
    }

    [Test]
    public async Task DeferredTransaction_MutationAsFirstStatement_AnchorsAndRollsBack()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        KvTableStore audits = await OpenStoreAsync(executor, dbname, "audits");

        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        Assert.That(await ExecNonQuery(database, executor, dbname, tx, "INSERT INTO audits (id, accountid) VALUES (11, 2)"), Is.EqualTo(1));

        Assert.That(tx.CoordinatorKey, Is.EqualTo(KvKeyBuilder.SessionAnchorKeyOf(audits.PlacementGroup, tx.UniqueId)));
        AssertSharesBucketWithTable(tx.CoordinatorKey, audits);

        await database.Transactions.RollbackAsync(tx);

        KvTransaction verify = await database.Transactions.BeginAsync();
        List<QueryResultRow> rows = await ExecQuery(executor, dbname, verify, "SELECT id FROM audits");
        await database.Transactions.CommitAsync(verify);
        Assert.That(rows, Has.Count.EqualTo(1), "the rolled-back insert left nothing behind");
    }

    [Test]
    public async Task EagerTransaction_KeepsBareUniqueId()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();
        _ = await ExecQuery(executor, dbname, tx, "SELECT balance FROM accounts WHERE id = 1");

        Assert.That(tx.CoordinatorKey, Is.EqualTo(tx.UniqueId));
        Assert.That(tx.CoordinatorKey, Does.Not.Contain(KvKeyBuilder.SessionSpaceSuffix));

        await database.Transactions.CommitAsync(tx);
    }

    [Test]
    public async Task DeferredTransaction_CommittedWithoutOperation_KeepsBareUniqueId()
    {
        (_, DatabaseDescriptor database, _) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        await database.Transactions.CommitAsync(tx);

        Assert.That(tx.CoordinatorKey, Is.EqualTo(tx.UniqueId));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));
    }
}
