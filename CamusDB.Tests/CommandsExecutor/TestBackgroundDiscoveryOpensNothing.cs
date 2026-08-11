/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The periodic background schedulers must scale with the work they find, not with the number of
/// registered databases.
///
/// <para>Row-level TTL discovery, its metadata reaper, and auto-analyze discovery all enumerate
/// authoritative KV metadata rather than this node's open-object list — which is what lets them see a
/// table configured on another node. The trap that behavior invites is opening every registered
/// database in order to look at it: the metadata scan needs only a database id, so opening first
/// costs a full catalog per database, per tick, forever, and turns lazy opening into eager opening
/// with extra steps. These tests pin the discovery paths to opening a database only after its
/// metadata has already shown there is work to do in it.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node and drives timing-sensitive background schedulers.
[NonParallelizable]
public sealed class TestBackgroundDiscoveryOpensNothing : BaseTest
{
    private const string TableName = "robots";

    /// <summary>
    /// Creates an additional database on an existing engine and returns its name. Several databases on
    /// <em>one</em> engine is the shape under test: the assertions are about that engine's open-object
    /// set, which a second engine would not share.
    /// </summary>
    private async Task<string> CreateExtraDatabaseAsync(CommandExecutor executor)
    {
        string dbname = Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);
        return dbname;
    }

    private static async Task CreateRobotsTableAsync(
        CommandExecutor executor, string dbname, Dictionary<string, string>? settings = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        if (settings is not null)
            await executor.AlterTableSettings(new AlterTableSettingsTicket(dbname, TableName, settings));
    }

    private static async Task InsertRobotsAsync(CommandExecutor executor, string dbname, int count)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: TableName,
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(2000 + i)) },
                    }
                }));
        }
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task CloseAsync(CommandExecutor executor, params string[] dbnames)
    {
        foreach (string dbname in dbnames)
            await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
    }

    // ── Row-level TTL ────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// With TTL enabled and no table anywhere configured for it, a sweep must leave every database
    /// closed. Discovery still reads each database's table metadata — that is how it stays
    /// cluster-visible — but finding nothing configured, it has no reason to materialize a descriptor.
    /// </summary>
    [Test]
    public async Task TtlSweepOpensNoDatabaseWhenNoTableHasTtl()
    {
        CamusDBOptions options = Options with { TtlEnabled = true };

        (string first, _, CommandExecutor executor) = await CreateDatabase(options);
        string second = await CreateExtraDatabaseAsync(executor);
        string third = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, first);
        await CreateRobotsTableAsync(executor, second);
        await CreateRobotsTableAsync(executor, third);

        await CloseAsync(executor, first, second, third);
        Assert.AreEqual(0, executor.OpenDatabaseCount, "Precondition: every database is closed before the sweep");

        await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(
            0, executor.OpenDatabaseCount,
            "TTL discovery must not open a database whose metadata shows no TTL-configured table");
    }

    /// <summary>
    /// The counterpart: a database that <em>does</em> have a TTL-configured table is opened, and only
    /// that one. Without this the test above would also pass if discovery had simply stopped working.
    /// </summary>
    [Test]
    public async Task TtlSweepOpensOnlyTheDatabaseThatHasATtlTable()
    {
        CamusDBOptions options = Options with { TtlEnabled = true };

        (string withTtl, _, CommandExecutor executor) = await CreateDatabase(options);
        string withoutTtl = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, withTtl, new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "ttl_expiration_expression", "year" },
        });
        await CreateRobotsTableAsync(executor, withoutTtl);

        await CloseAsync(executor, withTtl, withoutTtl);
        Assert.AreEqual(0, executor.OpenDatabaseCount, "Precondition: every database is closed before the sweep");

        await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(
            1, executor.OpenDatabaseCount,
            "Exactly the database holding the TTL-configured table must be opened");
    }

    // ── Auto-analyze ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A freshness poll must be free of side effects in both directions: it opens no database, and it
    /// leaves no statistics cache entry behind for the tables it merely inspected. The second half is
    /// the subtler one — loading a table's statistics in order to ask whether they are stale allocates
    /// a cache entry per table checked, so the poll's own residue grows without bound across a cluster
    /// of databases that are never actually analyzed.
    /// </summary>
    [Test]
    public async Task AutoAnalyzeSweepOpensNothingAndCachesNothingWhenAllTablesAreFresh()
    {
        CamusDBOptions options = Options with
        {
            AutoAnalyzeEnabled = true,
            AutoAnalyzeFractionStaleRows = 0.0,
            AutoAnalyzeMinStaleRows = 1_000_000, // nothing here can reach the threshold
        };

        (string first, _, CommandExecutor executor) = await CreateDatabase(options);
        string second = await CreateExtraDatabaseAsync(executor);
        string third = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, first);
        await CreateRobotsTableAsync(executor, second);
        await CreateRobotsTableAsync(executor, third);

        await CloseAsync(executor, first, second, third);
        Assert.AreEqual(0, executor.OpenDatabaseCount, "Precondition: every database is closed before the sweep");

        int cachedBefore = executor.Statistics.CachedTableCount;

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();

        Assert.AreEqual(0, analyzed, "No table is stale, so none should be analyzed");
        Assert.AreEqual(
            0, executor.OpenDatabaseCount,
            "Auto-analyze discovery must not open a database whose tables are all fresh");
        Assert.AreEqual(
            cachedBefore, executor.Statistics.CachedTableCount,
            "Probing a table's freshness must not create a statistics cache entry for it");
    }

    // ── Steady state costs nothing ───────────────────────────────────────────────────────────

    /// <summary>
    /// A tick over databases whose schemas have not changed must not re-read their metadata at all.
    /// Discovery keys its memo on the database schema version, which every DDL advances, so an
    /// unchanged version is proof the previous answer still holds — and the alternative is a range
    /// scan per registered database per tick, forever, which is most of what an otherwise idle node
    /// spends its time on once there are many databases.
    /// </summary>
    [Test]
    public async Task RepeatedSweepsDoNotRescanUnchangedMetadata()
    {
        CamusDBOptions options = Options with { TtlEnabled = true };

        (string first, _, CommandExecutor executor) = await CreateDatabase(options);
        string second = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, first, new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "ttl_expiration_expression", "year" },
        });
        await CreateRobotsTableAsync(executor, second);

        // First sweep populates the memo.
        await executor.RunTtlSweepForTestsAsync();
        int scansAfterFirst = executor.MetaDiscoveryScanCount;
        Assert.Greater(scansAfterFirst, 0, "Precondition: the first sweep must actually read metadata");

        await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(
            scansAfterFirst, executor.MetaDiscoveryScanCount,
            "A sweep over unchanged schemas must not rescan any database's metadata bucket");
    }

    /// <summary>
    /// The memo must not outlive its subject: changing a table's settings is what turns TTL on, and it
    /// advances the database schema version precisely so a sweep re-reads. A memo that survived that
    /// change would leave the table permanently unswept — the failure would be silent, and visible
    /// only as rows that never expire.
    /// </summary>
    [Test]
    public async Task EnablingTtlInvalidatesTheDiscoveryMemo()
    {
        CamusDBOptions options = Options with { TtlEnabled = true };

        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);
        await CreateRobotsTableAsync(executor, dbname);

        await executor.RunTtlSweepForTestsAsync();
        await executor.RunTtlSweepForTestsAsync();
        int scansBefore = executor.MetaDiscoveryScanCount;

        // Turn TTL on for the table that the memo currently records as having none.
        await executor.AlterTableSettings(new AlterTableSettingsTicket(
            dbname, TableName, new Dictionary<string, string>(StringComparer.Ordinal)
            {
                { "ttl_expiration_expression", "year" },
            }));

        await executor.RunTtlSweepForTestsAsync();

        Assert.Greater(
            executor.MetaDiscoveryScanCount, scansBefore,
            "Changing table settings must invalidate the memo so the next sweep re-reads the metadata");

        // And the re-read must actually find the newly-configured table.
        await CloseAsync(executor, dbname);
        await executor.RunTtlSweepForTestsAsync();

        Assert.AreEqual(
            1, executor.OpenDatabaseCount,
            "The newly TTL-configured table must be discovered, which means opening its database");
    }

    /// <summary>
    /// The counterpart: a genuinely stale table is still found and analyzed, and only its database is
    /// opened. This is what proves the cache-free probe reads the same state the loading path did —
    /// a probe that always answered "fresh" would satisfy the test above.
    /// </summary>
    [Test]
    public async Task AutoAnalyzeSweepStillFindsAStaleTableAndOpensOnlyItsDatabase()
    {
        CamusDBOptions options = Options with
        {
            AutoAnalyzeEnabled = true,
            AutoAnalyzeFractionStaleRows = 0.0,
            AutoAnalyzeMinStaleRows = 5,
        };

        (string stale, _, CommandExecutor executor) = await CreateDatabase(options);
        string fresh = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, stale);
        await CreateRobotsTableAsync(executor, fresh);

        // Churn well past the staleness floor in one database only.
        await InsertRobotsAsync(executor, stale, 20);

        await CloseAsync(executor, stale, fresh);
        Assert.AreEqual(0, executor.OpenDatabaseCount, "Precondition: every database is closed before the sweep");

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();

        Assert.GreaterOrEqual(analyzed, 1, "The churned table must still be discovered and analyzed");
        Assert.AreEqual(
            1, executor.OpenDatabaseCount,
            "Only the database holding the stale table should have been opened");
    }
}
