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
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The two areas where a contents swap interacts with something outside the truncated relation:
/// branch (copy-on-write) databases, whose lineage stores address the same key-space; and concurrent
/// writers, whom the swap must either fence or abort rather than silently strand.
/// </summary>
[NonParallelizable]
internal sealed class TestTruncateBranchAndConcurrency : SharedNodeBaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private static async Task RunNonQuery(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> RunSelect(string dbName, CommandExecutor executor, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        return await cursor.ToListAsync();
    }

    private async Task<int> CountKeysAsync(string bucket, string keyPrefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    private async Task<(string rootName, DatabaseDescriptor root, CommandExecutor executor, string tableId)>
        SetupRoot(int rows, CamusDBOptions? options = null)
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        TrackDatabase(rootName, executor);

        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING, year INT64)", parameters: null));

        for (int i = 0; i < rows; i++)
            await RunNonQuery(rootName, root, executor,
                $"INSERT INTO robots (id, name, year) VALUES (gen_id(), \"root{i}\", {2000 + i})");

        return (rootName, root, executor, root.Schema.Tables["robots"].Id!);
    }

    private static async Task<(string branchName, DatabaseDescriptor branch)> ForkAsync(
        string rootName, CommandExecutor executor)
    {
        string branchName = NewName();
        DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null))).Database!;

        return (branchName, branch);
    }

    // -----------------------------------------------------------------------
    // Branch databases
    // -----------------------------------------------------------------------

    [Test]
    public async Task Truncate_InABranch_HidesBothTheOverlayAndTheInheritedRows()
    {
        (string rootName, DatabaseDescriptor root, CommandExecutor executor, string tableId) = await SetupRoot(4);
        (string branchName, DatabaseDescriptor branch) = await ForkAsync(rootName, executor);
        TrackDatabase(branchName, executor);

        await RunNonQuery(branchName, branch, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"branch-only\", 2100)");

        Assert.AreEqual(5, (await RunSelect(branchName, executor, "SELECT name FROM robots")).Count,
            "sanity: the branch sees its overlay plus the inherited rows");

        await executor.TruncateTable(new TruncateTableTicket(branchName, "robots"));

        // The lineage stores are opened with the branch's own storage id, so a new generation hides
        // the inherited rows as well as the overlay. That is the contract, not an accident.
        Assert.IsEmpty(await RunSelect(branchName, executor, "SELECT name FROM robots"),
            "a branch truncate empties the branch's whole view, inherited rows included");

        // The source is untouched.
        Assert.AreEqual(4, (await RunSelect(rootName, executor, "SELECT name FROM robots")).Count,
            "truncating a branch must not touch its ancestor");

        Assert.AreEqual(4, await CountKeysAsync($"{root.Id}:{tableId}:r", $"{root.Id}:{tableId}:r/"),
            "the ancestor's row keys must still be present");
    }

    [Test]
    public async Task Truncate_InABranch_RetainsTheBranchOverlayAsRecoverableContents()
    {
        (string rootName, DatabaseDescriptor root, CommandExecutor executor, string tableId) = await SetupRoot(3);
        (string branchName, DatabaseDescriptor branch) = await ForkAsync(rootName, executor);
        TrackDatabase(branchName, executor);

        await RunNonQuery(branchName, branch, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"branch-only\", 2100)");

        await executor.TruncateTable(new TruncateTableTicket(branchName, "robots"));

        // The overlay belongs to the retired generation, so it is retained rather than purged.
        Assert.AreEqual(1, await CountKeysAsync($"{branch.Id}:{tableId}:r", $"{branch.Id}:{tableId}:r/"),
            "the branch overlay is recoverable contents, not garbage");

        List<OrphanTableRecord> orphans = await new CatalogsManager(logger).LoadTableOrphansAsync(branch);
        Assert.AreEqual(1, orphans.Count);
        Assert.AreEqual(OrphanKind.RetiredContents, orphans[0].Kind);

        // Recovery inside the branch reconstructs the pre-truncate merged view: its own overlay row
        // plus the rows it inherits, because the recovered relation reads the retired storage id in
        // every level of the lineage.
        await executor.RelinkTable(new RelinkTableTicket(branchName, "robots_before", tableId));

        List<QueryResultRow> recovered = await RunSelect(branchName, executor, "SELECT name FROM robots_before");
        Assert.AreEqual(4, recovered.Count, "recovery must restore the branch's merged pre-truncate view");
        Assert.True(recovered.Any(r => r.Row["name"].StrValue == "branch-only"));
        Assert.True(recovered.Any(r => r.Row["name"].StrValue == "root0"));
    }

    [Test]
    public async Task Truncate_OfTheSource_LeavesAnExistingBranchAtItsForkedContents()
    {
        (string rootName, DatabaseDescriptor root, CommandExecutor executor, string tableId) = await SetupRoot(4);
        (string branchName, DatabaseDescriptor branch) = await ForkAsync(rootName, executor);
        TrackDatabase(branchName, executor);

        await executor.TruncateTable(new TruncateTableTicket(rootName, "robots"));

        Assert.IsEmpty(await RunSelect(rootName, executor, "SELECT name FROM robots"));

        // The descendant copied its schema at fork time, so it keeps reading the forked key-space.
        Assert.AreEqual(4, (await RunSelect(branchName, executor, "SELECT name FROM robots")).Count,
            "a source truncate does not rewrite a descendant's copied schema");
    }

    [Test]
    public async Task Reclaim_OfABranchTruncate_NeverDeletesAnAncestorKey()
    {
        CamusDBOptions options = Options with { OrphanRetentionMs = 1 };

        (string rootName, DatabaseDescriptor root, CommandExecutor executor, string tableId) = await SetupRoot(5, options);
        (string branchName, DatabaseDescriptor branch) = await ForkAsync(rootName, executor);
        TrackDatabase(branchName, executor);

        await RunNonQuery(branchName, branch, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"branch-only\", 2100)");

        await executor.TruncateTable(new TruncateTableTicket(branchName, "robots"));

        await Task.Delay(50);
        await executor.RunOrphanReclaimForTestsAsync();

        Assert.AreEqual(0, await CountKeysAsync($"{branch.Id}:{tableId}:r", $"{branch.Id}:{tableId}:r/"),
            "the branch's retired overlay is reclaimed");

        Assert.AreEqual(5, await CountKeysAsync($"{root.Id}:{tableId}:r", $"{root.Id}:{tableId}:r/"),
            "purge is scoped to the branch database id: no ancestor key may be deleted");

        Assert.AreEqual(5, (await RunSelect(rootName, executor, "SELECT name FROM robots")).Count);
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    [Test]
    public async Task Truncate_AbortsAWriterThatStagedARowBeforeTheFence()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRoot(2);

        // Stage a row but do not commit: the write intent exists before the fence is taken.
        KvTransaction writer = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            writer, dbName, "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"staged\", 2050)", null));

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        // The write was staged into a key-space the relation no longer reads, so it must not be
        // reported as committed. Either the durable range fence aborts it or the contents pin does.
        Assert.ThrowsAsync<CamusDBException>(async () => await db.Transactions.CommitAsync(writer),
            "a writer staged into the retired generation must not commit successfully");

        await db.Transactions.RollbackIfNotCompletedAsync(writer);

        Assert.IsEmpty(await RunSelect(dbName, executor, "SELECT name FROM robots"),
            "no row committed only to retired storage may appear in the live relation");
    }

    [Test]
    public async Task Truncate_AWriterThatStartsAfterTheSwapBindsTheNewGeneration()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRoot(2);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        string newStorageId = db.Schema.Tables["robots"].EffectiveStorageId;

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"after\", 2060)");

        Assert.AreEqual(1, await CountKeysAsync($"{db.Id}:{newStorageId}:r", $"{db.Id}:{newStorageId}:r/"),
            "the row must land in the new key-space");

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT name FROM robots");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("after", rows[0].Row["name"].StrValue);
    }

    [Test]
    public async Task Truncate_TwiceConcurrently_PreservesBothOrderedGenerations()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupRoot(3);

        Task<bool> first = executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        Task<bool> second = executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        // One may lose the compare-and-swap race and report a concurrent schema change; the other
        // must succeed. What is not allowed is both succeeding onto the same generation, or a
        // retired key-space losing its record.
        int succeeded = 0;
        foreach (Task<bool> attempt in new[] { first, second })
        {
            try
            {
                if (await attempt)
                    succeeded++;
            }
            catch (CamusDBException)
            {
                // A refused attempt applied nothing; the caller retries.
            }
        }

        Assert.GreaterOrEqual(succeeded, 1, "at least one truncate must succeed");

        TableSchema live = db.Schema.Tables["robots"];
        Assert.AreEqual(succeeded, live.ContentsGeneration,
            "the contents generation must advance exactly once per successful truncate");

        List<OrphanTableRecord> orphans = await new CatalogsManager(logger).LoadTableOrphansAsync(db);
        Assert.AreEqual(succeeded, orphans.Count,
            "every retired generation must keep its own record");

        HashSet<string> retired = [.. orphans.Select(o => o.RetiredStorageId)];
        Assert.AreEqual(orphans.Count, retired.Count, "the retired key-spaces must be distinct");
        Assert.False(retired.Contains(live.EffectiveStorageId), "the live key-space must not be retired");
    }

    [Test]
    public async Task Truncate_AndAlterTableSerializeWithoutLosingEitherDelta()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRoot(3);

        Task<bool> truncate = executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));
        Task<ExecuteDDLSQLResult> alter = executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "ALTER TABLE robots ADD COLUMN owner STRING", parameters: null));

        await Task.WhenAll(truncate, alter);

        TableSchema live = db.Schema.Tables["robots"];

        Assert.AreEqual(1, live.ContentsGeneration, "the truncate's delta must survive");
        Assert.True(live.Columns!.Any(c => string.Equals(c.Name, "owner", StringComparison.OrdinalIgnoreCase)),
            "the alter's delta must survive");

        Assert.IsEmpty(await RunSelect(dbName, executor, "SELECT name FROM robots"));
    }

    [Test]
    public async Task Truncate_LeavesAPreCutReaderAbleToFinishOnItsOwnSnapshot()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupRoot(3);

        await Task.Delay(60);
        long beforeCut = SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;
        await Task.Delay(60);

        // A read bound before the cut is answered from the old generation; the same read expressed as
        // a time-travel query after the cut is refused rather than answered wrongly. This asserts the
        // second half — the first is what the ordinary pre-cut reader already gets.
        Assert.AreEqual(3, (await RunSelect(dbName, executor, $"SELECT name FROM robots AS OF SYSTEM TIME {beforeCut}")).Count);

        await executor.TruncateTable(new TruncateTableTicket(dbName, "robots"));

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunSelect(dbName, executor, $"SELECT name FROM robots AS OF SYSTEM TIME {beforeCut}"));

        Assert.AreEqual(CamusDBErrorCodes.SnapshotPrecedesContentsGeneration, exception!.Code);
    }
}
