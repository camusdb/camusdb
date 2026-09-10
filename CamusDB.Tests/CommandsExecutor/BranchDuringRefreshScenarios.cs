/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Scenario bodies shared by the standalone and the cluster-mode fixtures that prove a branch taken
/// while its parent refreshes a materialized view inherits the view's published contents and none of
/// the parent's rebuild. The two fixtures differ only in how the engine is built, so the assertions
/// live here once.
/// </summary>
internal static class BranchDuringRefreshScenarios
{
    internal static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    internal static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    internal static async Task Ddl(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    internal static async Task<List<string>> Keys(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await db.Transactions.CommitAsync(tx);
            return rows.Select(r => r.Row["k"].StrValue!).OrderBy(k => k, StringComparer.Ordinal).ToList();
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// A root with table <c>t</c> holding rows <c>a</c> and <c>b</c>, and materialized view <c>mv</c>
    /// materialized when only <c>a</c> existed — so a refresh visibly changes the view from one row to
    /// two, and "left alone" cannot be mistaken for "rebuilt to the same answer".
    /// </summary>
    internal static async Task<DatabaseDescriptor> SeedRoot(CommandExecutor executor, string rootName)
    {
        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, false));
        await Ddl(executor, root, "CREATE TABLE t (k STRING PRIMARY KEY)");
        await Exec(executor, root, "INSERT INTO t VALUES ('a')");
        await Ddl(executor, root, "CREATE MATERIALIZED VIEW mv AS SELECT k FROM t");
        await Exec(executor, root, "INSERT INTO t VALUES ('b')");
        return root;
    }

    /// <summary>Every meta key of one database, so a test can prove what the fork did and did not copy.</summary>
    internal static async Task<List<string>> MetaKeysOf(IKahuna kahuna, string dbId)
    {
        List<string> keys = [];
        string prefix = dbId + "/meta/";

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, MetaKeys.MetaBucketPrefix(dbId), null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (entry.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal))
                keys.Add(key);
        }

        return keys;
    }

    /// <summary>
    /// Forks the root from inside a running refresh — after the job record and the staging relation
    /// exist, before any row is rebuilt — and returns the branch plus the staging id the run used.
    /// </summary>
    internal static async Task<(DatabaseDescriptor branch, MaterializedViewRefreshJob parentJob)> ForkMidRefresh(
        CommandExecutor executor, DatabaseDescriptor root, string branchName, string refreshSql)
    {
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        string viewTableId = root.Schema.Tables["mv"].Id!;

        DatabaseDescriptor? branch = null;
        MaterializedViewRefreshJob? parentJob = null;

        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;

            parentJob = await catalogs.TryGetRefreshJobAsync(root, viewTableId);
            Assert.IsNotNull(parentJob, "the seam runs with the job record already written");
            Assert.IsTrue(root.Schema.Tables.ContainsKey(parentJob!.StagingName),
                "the seam runs with the staging relation already in the parent's schema");

            branch = await executor.CreateDatabase(new CreateDatabaseTicket(branchName, false, root.Name));
        };

        try
        {
            await Exec(executor, root, refreshSql);
        }
        finally
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
        }

        Assert.IsNotNull(branch, "the branch must have been created inside the refresh window");
        return (branch!, parentJob!);
    }

    /// <summary>
    /// The branch carries nothing of the parent's rebuild: no job record, no staging relation in its
    /// schema, and none of the staging relation's meta keys in its namespace.
    /// </summary>
    internal static async Task AssertNoRefreshWorkInBranch(IKahuna kahuna, DatabaseDescriptor branch, MaterializedViewRefreshJob parentJob)
    {
        CollectionAssert.IsEmpty(await CatalogsManager.ListRefreshJobsAsync(kahuna, branch.Id),
            "a fork must not copy the parent's refresh job record");

        CollectionAssert.IsEmpty(
            branch.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList(),
            "a fork must not copy the parent's staging relation into the branch schema");

        List<string> branchKeys = await MetaKeysOf(kahuna, branch.Id);
        Assert.IsFalse(branchKeys.Contains(MetaKeys.TableKey(branch.Id, parentJob.StagingTableId)),
            "the staging relation's table record must not exist in the branch namespace");
        Assert.IsFalse(branchKeys.Any(k => k.StartsWith(MetaKeys.HistoryKeyPrefix(branch.Id, parentJob.StagingTableId), StringComparison.Ordinal)),
            "the staging relation's history must not exist in the branch namespace");
        Assert.IsFalse(branchKeys.Any(k => k.StartsWith(MetaKeys.RefreshJobKeyPrefix(branch.Id), StringComparison.Ordinal)),
            "no refresh job key of any kind may exist in the branch namespace");
    }

    /// <summary>
    /// The full scenario the defect was reported with: fork inside a refresh, let the parent finish,
    /// write to the branch, run the branch's abandoned-refresh sweep, and prove the branch's view still
    /// holds its fork-time contents — then prove the branch can still refresh on its own.
    /// </summary>
    internal static async Task ForkMidRefresh_BranchKeepsForkTimeContentsAndInheritsNoRebuild(
        CommandExecutor executor, IKahuna kahuna, string rootName, string branchName)
    {
        DatabaseDescriptor root = await SeedRoot(executor, rootName);
        string parentStorageBefore = root.Schema.Tables["mv"].EffectiveStorageId;

        (DatabaseDescriptor branch, MaterializedViewRefreshJob parentJob) =
            await ForkMidRefresh(executor, root, branchName, "REFRESH MATERIALIZED VIEW mv");

        // The parent's refresh completed and published the two-row contents.
        CollectionAssert.AreEqual(new[] { "a", "b" }, await Keys(executor, root, "SELECT k FROM mv"),
            "the parent's refresh must complete normally around the fork");
        Assert.AreEqual(parentJob.StagingTableId, root.Schema.Tables["mv"].EffectiveStorageId,
            "the parent's view must have adopted the staging storage");

        await AssertNoRefreshWorkInBranch(kahuna, branch, parentJob);

        // The branch forked before the swap, so its view still names the pre-refresh storage and reads
        // the one-row contents through ancestry.
        Assert.AreEqual(parentStorageBefore, branch.Schema.Tables["mv"].EffectiveStorageId,
            "the branch's view must name the storage that was published at the fork point");
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "the branch's view must hold its fork-time contents");

        // A branch-only write, then the sweep: nothing to take over, nothing rewritten.
        await Exec(executor, branch, "INSERT INTO t VALUES ('branch_only')");
        Assert.AreEqual(0, await executor.ReclaimAbandonedRefreshesForTesting(branchName),
            "the branch's sweep must find no refresh job to take over");
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "background recovery must not refresh a branch's materialized view on the parent's account");

        // The branch's own refresh is ordinary work and still runs.
        await Exec(executor, branch, "REFRESH MATERIALIZED VIEW mv");
        CollectionAssert.AreEqual(new[] { "a", "b", "branch_only" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "a refresh the branch asks for must rebuild from the branch's own base table");
        CollectionAssert.IsEmpty(await CatalogsManager.ListRefreshJobsAsync(kahuna, branch.Id));

        // And the parent never saw any of it.
        CollectionAssert.AreEqual(new[] { "a", "b" }, await Keys(executor, root, "SELECT k FROM mv"));
    }
}
