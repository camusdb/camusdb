/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The schema load rejects a metadata scan that cannot have reached the end of the bucket.
///
/// <para>The version key is point-read before the scan runs, so it is known to exist; it lives in the
/// same bucket and sorts after every table key. A scan that yields the tables must therefore yield it
/// too, and one that does not has ended early — which at the enumerable's surface is indistinguishable
/// from a bucket that simply holds nothing. That is how a node once loaded a real schema version
/// holding no tables at all, and then re-persisted that emptiness over the shared checkpoint.</para>
///
/// <para>The ordering is the whole basis of the check, so it is pinned here rather than assumed. A
/// rename of the table-key prefix that moved it after the version key would leave the check passing
/// while protecting nothing.</para>
/// </summary>
internal sealed class TestSchemaMetaScanCompleteness : BaseTest
{
    [Test]
    public void EveryTableKeySortsBeforeTheVersionKey()
    {
        const string db = "somedatabaseid";
        string versionKey = MetaKeys.VersionKey(db);

        // Short base-62 ids, a legacy 24-hex id, and the extremes of each character set.
        foreach (string tableId in new[] { "1", "A0", "zz", "0", "ZZZZ", "6849f3a1c2e7d50b4f8a91d3" })
        {
            Assert.That(
                string.CompareOrdinal(MetaKeys.TableKey(db, tableId), versionKey),
                Is.LessThan(0),
                $"table key for id '{tableId}' must sort before the version key, or the completeness check protects nothing");
        }

        Assert.That(
            string.CompareOrdinal(MetaKeys.TableKeyPrefix(db), versionKey),
            Is.LessThan(0),
            "the table key prefix itself must sort before the version key");
    }

    /// <summary>
    /// Views sort after the version key, so the check cannot prove a view scan ran to completion.
    /// Pinned so the limitation stays a known one rather than an assumed guarantee.
    /// </summary>
    [Test]
    public void ViewKeysSortAfterTheVersionKey()
    {
        const string db = "somedatabaseid";

        Assert.That(
            string.CompareOrdinal(MetaKeys.ViewKeyPrefix(db), MetaKeys.VersionKey(db)),
            Is.GreaterThan(0),
            "views sort after the version key; the completeness check is partial for them by construction");
    }

    /// <summary>
    /// The guard must not fire on a healthy load. A database reopened with tables and a view has to
    /// come back with all of them — if the version key were not actually yielded by the bucket scan,
    /// every open would throw instead.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ReopeningADatabaseWithTablesAndViewsLoadsThemAll()
    {
        (string dbname, DatabaseDescriptor database, CamusDB.Core.CommandsExecutor.CommandExecutor executor) =
            await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)");
        await Ddl(database, executor, dbname, "CREATE TABLE machines (id OBJECT_ID PRIMARY KEY, label STRING)");
        await Ddl(database, executor, dbname, "CREATE VIEW live_robots AS SELECT id FROM robots");

        long versionBefore = database.Schema.SchemaVersion;
        Assert.Greater(versionBefore, 0, "sanity: the DDL above must have advanced the schema version");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.AreEqual(versionBefore, reopened.Schema.SchemaVersion);
        Assert.True(reopened.Schema.Tables.ContainsKey("robots"));
        Assert.True(reopened.Schema.Tables.ContainsKey("machines"));
        Assert.True(reopened.Schema.Views.ContainsKey("live_robots"));
    }

    private static async Task Ddl(
        DatabaseDescriptor database, CamusDB.Core.CommandsExecutor.CommandExecutor executor, string dbname, string sql)
    {
        CamusDB.Core.Transactions.KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }
}
