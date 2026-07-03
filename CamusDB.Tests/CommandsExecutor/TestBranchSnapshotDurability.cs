
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The core BR7 acceptance: a branch keeps reading its parent as of <c>forkT</c> even after the
/// parent's row is overwritten far more than the node's in-memory revision-retention window. The
/// snapshot-floor hold acquired at fork time keeps the fork-boundary revision reachable; without it
/// that version would be reclaimed and the branch read would miss.
///
/// Runs on a dedicated node with a deliberately tiny <c>RevisionRetention</c> so a handful of updates
/// is enough to push the fork version out of the ordinary retention window — the hold, not retention,
/// is what preserves it.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestBranchSnapshotDurability
{
    private const int RetentionWindow = 2;
    private const int ChurnCount = 25; // well past RetentionWindow

    private string tempDir = "";
    private EmbeddedKahuna node = null!;
    private DatabaseRegistry registry = null!;
    private CommandExecutor executor = null!;
    private ILogger<ICamusDB> logger = null!;

    [SetUp]
    public async Task SetUp()
    {
        logger = LoggerFactory.Create(b => { }).CreateLogger<ICamusDB>();

        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-snaptest-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;

        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = $"snaptest-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            RevisionRetention = RetentionWindow,
            RevisionsToKeepCached = RetentionWindow,
        });
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);
        await node.FlushAsync();

        registry = await DatabaseRegistry.OpenAsync(node);
        executor = new CommandExecutor(new CommandValidator(), new CatalogsManager(logger), logger,
                                       sharedNode: node, registry: registry, isClusterMode: false);
    }

    [TearDown]
    public async Task TearDown()
    {
        await executor.DisposeAsync();
        await registry.DisposeAsync();
        await node.DisposeAsync();
        try { Directory.Delete(tempDir, recursive: true); } catch { /* best effort */ }
    }

    [Test]
    [NonParallelizable]
    public async Task BranchReadsForkValue_AfterParentChurnedPastRetention()
    {
        string root = "r_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor rootDb = await executor.CreateDatabase(new CreateDatabaseTicket(root, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, root,
            "CREATE TABLE t (k STRING PRIMARY KEY, v STRING)", null));

        await ExecOn(root, rootDb, "INSERT INTO t (k, v) VALUES (\"row\", \"v0\")");

        // Fork: the branch pins the parent at this instant (v = "v0").
        string branch = "b_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor branchDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(branch, ifNotExists: false, branchFrom: root));

        // Churn the parent row far past the in-memory retention window.
        for (int i = 1; i <= ChurnCount; i++)
            await ExecOn(root, rootDb, $"UPDATE t SET v = \"v{i}\" WHERE k = \"row\"");

        // The parent now reads its latest value.
        List<QueryResultRow> parentRows = await QueryOn(root, rootDb, "SELECT v FROM t WHERE k = \"row\"");
        Assert.That(parentRows, Has.Count.EqualTo(1));
        Assert.That(parentRows[0].Row["v"].StrValue, Is.EqualTo($"v{ChurnCount}"),
            "sanity: the parent itself sees the churned value");

        // The branch must still see the fork-time value, preserved only by the snapshot-floor hold.
        List<QueryResultRow> branchRows = await QueryOn(branch, branchDb, "SELECT v FROM t WHERE k = \"row\"");
        Assert.That(branchRows, Has.Count.EqualTo(1),
            "branch must still find the row at forkT despite the parent being churned past retention");
        Assert.That(branchRows[0].Row["v"].StrValue, Is.EqualTo("v0"),
            "branch must read the fork-time value; the snapshot-floor hold kept that revision reachable");
    }

    private async Task ExecOn(string db, DatabaseDescriptor descriptor, string sql)
    {
        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        await descriptor.Transactions.CommitAsync(tx);
    }

    private async Task<List<QueryResultRow>> QueryOn(string db, DatabaseDescriptor descriptor, string sql)
    {
        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await descriptor.Transactions.CommitAsync(tx);
        return rows;
    }
}
