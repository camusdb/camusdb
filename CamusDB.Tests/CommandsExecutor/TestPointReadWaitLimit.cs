/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using static CamusDB.Tests.CommandsExecutor.WriteReturningTestSql;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A point read at a statement timestamp that meets a foreign intent whose commit is in progress waits
/// in Kahuna for about 16.5 s and then answers <c>MustRetry</c>. The statement must then fail with the
/// retryable <see cref="CamusDBErrorCodes.TransactionMustRetry"/>. It must not re-issue the read once per
/// attempt of the retry budget: 32 attempts of one Kahuna wait held a statement for about 9 minutes,
/// and a pessimistic UPDATE blocked by a foreign row lock waited for the holder's session ceiling
/// (330 s) and then wrote over the formerly locked row. The limit is
/// <see cref="KahunaRetryPolicy.TransientRetryWindowMs"/>.
///
/// <para>The blocker is a staged write on the row key that a foreign transaction then claims, with the
/// probe a coordinator sends right before it proposes its commit, and never decides. A read steps over
/// an open writer's intent, but not over a claimed one, so each read of the row waits.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestPointReadWaitLimit : BaseTest
{
    /// <summary>
    /// One Kahuna point-read wait (16.5 s) plus a margin for the setup and the call. A statement that
    /// started a second wait would take at least 33 s.
    /// </summary>
    private static readonly TimeSpan MaxStatementTime = TimeSpan.FromSeconds(28);

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, Dictionary<string, ColumnValue> byId)> SetupBlockedRowAsync()
    {
        CancellationToken ct = CancellationToken.None;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, dbname, "CREATE TABLE robots (id oid NOT NULL DEFAULT(gen_id()), name string, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO robots (name) VALUES ('alpha')");

        string id = (await Select(executor, database, dbname, "SELECT id FROM robots")).Single().Row["id"].StrValue!;

        // The row key of the row, from its physical row id: the primary key index stays free, so the
        // lookup finds the row and the read of the row is the one that waits.
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "robots"));
        KvTransaction lookup = await database.Transactions.BeginAsync();
        ObjectIdValue? rowId = await table.Store.LookupUnique(
            lookup, table.Indexes["~pk"].KvId, new CompositeColumnValue(new ColumnValue(ColumnType.Id, id)));
        await database.Transactions.CommitAsync(lookup);
        Assert.That(rowId, Is.Not.Null);
        string rowKey = table.Store.Keys.BuildRowKey(rowId!.Value);

        HLCTimestamp foreignTx = TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(TestNode.Raft.GetLocalNodeId());
        (KeyValueResponseType staged, _, _) = await TestNode.Kahuna.LocateAndTrySetKeyValue(
            foreignTx, rowKey, Encoding.UTF8.GetBytes("staged"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.That(staged, Is.EqualTo(KeyValueResponseType.Set));

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> claimed =
            await TestNode.Kahuna.LocateAndTryCheckManyWriteIntents(
                foreignTx,
                [new KeyValueConflictProbe(rowKey, KeyValueDurability.Persistent,
                    KeyValueConflictChecks.OwnStagedIntent | KeyValueConflictChecks.CommitFloor)],
                ct);
        Assert.That(claimed[0].type, Is.EqualTo(KeyValueResponseType.DoesNotExist),
            "the staged intent is still held, so the probe claims it");

        return (dbname, database, executor, new Dictionary<string, ColumnValue> { { "@id", new ColumnValue(ColumnType.Id, id) } });
    }

    private static async Task<(CamusDBException? error, TimeSpan elapsed)> RunInPessimisticReadCommittedAsync(
        DatabaseDescriptor database, Func<KvTransaction, Task> statement)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted, locking: KeyValueTransactionLocking.Pessimistic);
        Stopwatch clock = Stopwatch.StartNew();
        try
        {
            await statement(tx);
            return (null, clock.Elapsed);
        }
        catch (CamusDBException e)
        {
            return (e, clock.Elapsed);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task PointSelect_OnARowWithAClaimedForeignIntent_FailsRetryablyAfterOneWait()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, Dictionary<string, ColumnValue> byId) = await SetupBlockedRowAsync();

        (CamusDBException? error, TimeSpan elapsed) = await RunInPessimisticReadCommittedAsync(database, async tx =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, "SELECT name FROM robots WHERE id = @id", byId));
            await cursor.ToListAsync();
        });

        TestContext.Out.WriteLine($"SELECT ended after {elapsed.TotalMilliseconds:F0} ms: {error?.Code}: {error?.Message}");

        Assert.Multiple(() =>
        {
            Assert.That(error, Is.Not.Null, "the read of a row with a claimed foreign intent cannot be served");
            Assert.That(error?.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
            Assert.That(elapsed, Is.LessThan(MaxStatementTime), "the statement started a second Kahuna wait");
        });
    }

    [Test]
    public async Task PessimisticUpdate_OnARowWithAClaimedForeignIntent_FailsRetryablyAfterOneWait()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, Dictionary<string, ColumnValue> byId) = await SetupBlockedRowAsync();

        (CamusDBException? error, TimeSpan elapsed) = await RunInPessimisticReadCommittedAsync(database, async tx =>
        {
            await NonQuery(executor, dbname, tx, "UPDATE robots SET name = 'moved' WHERE id = @id", byId);
            await database.Transactions.CommitAsync(tx);
        });

        TestContext.Out.WriteLine($"UPDATE ended after {elapsed.TotalMilliseconds:F0} ms: {error?.Code}: {error?.Message}");

        Assert.Multiple(() =>
        {
            Assert.That(error, Is.Not.Null, "the UPDATE must not write over a row whose state it cannot read");
            Assert.That(error?.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
            Assert.That(elapsed, Is.LessThan(MaxStatementTime), "the statement started a second Kahuna wait");
        });
    }
}
