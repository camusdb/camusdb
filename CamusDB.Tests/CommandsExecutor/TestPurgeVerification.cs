
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Threading;
using System.Threading.Tasks;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The keyspace purge must return <b>verified</b> completion: it deletes the orphan/recovery record only
/// after every data key is confirmed gone. When a delete fails, the purge reports incomplete and leaves
/// the recovery record intact so a later sweep / startup resume finishes it — never abandoning leaked data.
/// </summary>
[NonParallelizable]
internal sealed class TestPurgeVerification : SharedNodeBaseTest
{
    /// <summary>Wraps a real node but fails every delete of a key under a given prefix, to simulate a partial purge.</summary>
    private sealed class FailDeleteKahuna : DelegatingKahuna
    {
        private readonly string failPrefix;
        public FailDeleteKahuna(IKahuna inner, string failPrefix) : base(inner) => this.failPrefix = failPrefix;

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
            HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.StartsWith(failPrefix, StringComparison.Ordinal))
                throw new InvalidOperationException("injected delete failure");
            return base.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken, coordinatorKey, operationId);
        }
    }

    private async Task<(string dbId, string tableId)> SetupDroppedTableOrphan(int rows)
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbName, executor);
        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

        for (int i = 0; i < rows; i++)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName,
                $"INSERT INTO robots (id, name) VALUES (gen_id(), \"r{i}\")", null));
            await db.Transactions.CommitAsync(tx);
        }

        string tableId = db.Schema.Tables["robots"].Id!;
        await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false)); // deferred → orphan record
        return (db.Id, tableId);
    }

    private async Task<bool> OrphanRecordExists(string dbId, string tableId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await SharedKahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{dbId}/meta/orphan:{tableId}", -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, CancellationToken.None);
        return type == KeyValueResponseType.Get && entry?.Value is not null;
    }

    [Test]
    [NonParallelizable]
    public async Task PurgeTable_DeleteFailure_ReturnsFalse_KeepsOrphanRecord()
    {
        (string dbId, string tableId) = await SetupDroppedTableOrphan(5);
        Assert.IsTrue(await OrphanRecordExists(dbId, tableId), "sanity: orphan record present after deferred drop");

        DatabaseDropper dropper = new(new DatabaseDescriptors(), logger);
        // Fail deletes of the table's row keys → the row bucket never drains → purge is incomplete.
        IKahuna faulty = new FailDeleteKahuna(SharedKahuna, $"{dbId}:{tableId}:r/");

        bool completed = await dropper.PurgeTableKeyspaceAsync(faulty, dbId, tableId);

        Assert.IsFalse(completed, "an unverified/failed delete must report the purge as incomplete");
        Assert.IsTrue(await OrphanRecordExists(dbId, tableId),
            "the recovery record must be preserved on an incomplete purge so a later sweep finishes it");
    }

    [Test]
    [NonParallelizable]
    public async Task PurgeTable_Success_ReturnsTrue_DeletesOrphanRecord()
    {
        (string dbId, string tableId) = await SetupDroppedTableOrphan(5);

        DatabaseDropper dropper = new(new DatabaseDescriptors(), logger);
        bool completed = await dropper.PurgeTableKeyspaceAsync(SharedKahuna, dbId, tableId);

        Assert.IsTrue(completed, "a fully-verified purge must report completion");
        Assert.IsFalse(await OrphanRecordExists(dbId, tableId), "the recovery record is deleted last on a completed purge");
    }
}
