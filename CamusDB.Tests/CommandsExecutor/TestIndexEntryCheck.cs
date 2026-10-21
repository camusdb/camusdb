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
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An index entry whose row no longer holds the entry's key must not surface as a row: every read
/// that fetches a row through an index entry checks the row against the entry and skips a row that
/// disagrees (<c>IndexEntryCheck</c>). The writers are built not to leave such an entry; these tests
/// plant one directly in the store, the way a defect would, and read through every index path.
///
/// <para>Row 7 of <c>items</c> holds <c>val = 7</c> and <c>tag = 't7'</c>. A stale entry says it holds
/// <c>val = 70</c> in the non-unique index and <c>tag = 't70'</c> in the unique one. Reads for the stale
/// values must find nothing, reads for the true values must still find the row, and a read that sees
/// both entries must return the row once.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIndexEntryCheck : SharedNodeBaseTest
{
    private const int RowCount = 20;

    private const long StaleVal = 70;

    private const string StaleTag = "t70";

    private string dbname = "";

    private DatabaseDescriptor database = null!;

    private CommandExecutor executor = null!;

    private ObjectIdValue row7 = default;

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options, sharedNode: SharedNode, registry: sharedRegistry!);
    }

    [SetUp]
    public async Task PlantStaleEntries()
    {
        (dbname, database, executor) = await CreateDatabase();

        await Ddl("CREATE TABLE items (id INT64 PRIMARY KEY, val INT64 NOT NULL, tag STRING NOT NULL)");
        await NonQuery("INSERT INTO items (id, val, tag) VALUES " + string.Join(", ", Enumerable.Range(1, RowCount).Select(i => $"({i}, {i}, 't{i}')")));
        await Ddl("CREATE INDEX val_idx ON items (val)");
        await Ddl("CREATE UNIQUE INDEX tag_uniq ON items (tag)");

        row7 = (await Rows7()).Single();

        TableSchema schema = database.Schema.Tables["items"];
        TableIndexSchema valIdx = schema.Indexes!.First(ix => ix.Name == "val_idx");
        TableIndexSchema tagUniq = schema.Indexes!.First(ix => ix.Name == "tag_uniq");
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "items"));

        // The shape a writer would leave behind: the non-unique key carries the row id as its last
        // component, the unique key does not.
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await table.Store.PutIndexEntry(tx, valIdx.Id!,
                new CompositeColumnValue([new ColumnValue(ColumnType.Integer64, StaleVal), new ColumnValue(ColumnType.Id, row7.ToString())]),
                row7, unique: false, backfillMode: true);
            await table.Store.PutIndexEntry(tx, tagUniq.Id!,
                new CompositeColumnValue([new ColumnValue(ColumnType.String, StaleTag)]),
                row7, unique: true, backfillMode: true);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    // -----------------------------------------------------------------------
    // Non-unique index
    // -----------------------------------------------------------------------

    [Test]
    public async Task EqualityLookup_SkipsTheStaleEntry_AndStillFindsTheTrueOne()
    {
        // A non-unique equality is a lookup or a range scan with equal bounds; both fetch through the entry.
        Assert.That(await PlanOf($"SELECT id FROM items WHERE val = {StaleVal}"), Does.Contain("index-lookup").Or.Contain("index-range-scan"));

        Assert.That(await Ids($"SELECT id FROM items WHERE val = {StaleVal}"), Is.Empty, "a row that no longer holds the value must not be returned for it");
        Assert.That(await Ids("SELECT id FROM items WHERE val = 7"), Is.EqualTo(new long[] { 7 }), "the row's true entry still finds it");

        // A read the index alone answers (COUNT(*), or a projection the entry covers) fetches no row
        // and cannot be checked; it is outside this check and counts the stale entry. Only a row
        // fetched through an entry is verified, which is what keeps a wrong row out of a result.
        Assert.That(await Count($"SELECT COUNT(*) AS n FROM items WHERE val = {StaleVal} AND tag <> ''"), Is.EqualTo(0), "a count that reads the row is exact");
    }

    [Test]
    public async Task RangeScan_ReturnsTheRowOnce_AtItsTrueValue()
    {
        Assert.That(await PlanOf($"SELECT id FROM items WHERE val >= 5 AND val <= {StaleVal + 10}"), Does.Contain("index-range-scan"));

        List<(long id, long val)> rows = await IdVals($"SELECT id, val FROM items WHERE val >= 5 AND val <= {StaleVal + 10} ORDER BY val");
        Assert.That(rows.Count(r => r.id == 7), Is.EqualTo(1), "the row appears once, not once per entry");
        Assert.That(rows.Single(r => r.id == 7).val, Is.EqualTo(7));
        Assert.That(rows.Select(r => r.id), Is.EqualTo(Enumerable.Range(5, RowCount - 4).Select(i => (long)i)));
    }

    [Test]
    public async Task InListSeek_SkipsTheStaleEntry()
    {
        Assert.That(await PlanOf($"SELECT id FROM items WHERE val IN ({StaleVal}, 8)"), Does.Contain("index-in-list"));

        Assert.That(await Ids($"SELECT id FROM items WHERE val IN ({StaleVal}, 8)"), Is.EqualTo(new long[] { 8 }));
    }

    [Test]
    public async Task ForcedFullIndexScan_ReturnsEveryRowOnce()
    {
        List<(long id, long val)> rows = await IdVals("SELECT id, val FROM items@{FORCE_INDEX=val_idx} ORDER BY val");
        Assert.That(rows.Select(r => r.id), Is.EqualTo(Enumerable.Range(1, RowCount).Select(i => (long)i)), "the stale entry adds no row");
        Assert.That(rows.Single(r => r.id == 7).val, Is.EqualTo(7));
    }

    // -----------------------------------------------------------------------
    // Unique index
    // -----------------------------------------------------------------------

    [Test]
    public async Task UniqueLookup_SkipsTheStaleEntry_AndStillFindsTheTrueOne()
    {
        Assert.That(await PlanOf($"SELECT id FROM items WHERE tag = '{StaleTag}'"), Does.Contain("index-lookup"));

        Assert.That(await Ids($"SELECT id FROM items WHERE tag = '{StaleTag}'"), Is.Empty);
        Assert.That(await Ids("SELECT id FROM items WHERE tag = 't7'"), Is.EqualTo(new long[] { 7 }));
    }

    [Test]
    public async Task UniqueInListSeek_SkipsTheStaleEntry()
    {
        Assert.That(await PlanOf($"SELECT id FROM items WHERE tag IN ('{StaleTag}', 't8')"), Does.Contain("index-in-list"));

        Assert.That(await Ids($"SELECT id FROM items WHERE tag IN ('{StaleTag}', 't8')"), Is.EqualTo(new long[] { 8 }));
    }

    /// <summary>
    /// A projection that does not name the key column still decodes it for the check: the stale entry
    /// is skipped even though nothing in the query reads <c>val</c> after the seek.
    /// </summary>
    [Test]
    public async Task KeyColumnOutsideTheProjection_IsStillChecked()
    {
        List<Dictionary<string, ColumnValue>> rows = await Query($"SELECT tag FROM items WHERE val = {StaleVal}");
        Assert.That(rows, Is.Empty);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task Ddl(string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task NonQuery(string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<List<QueryResultRow>> Rows(string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<List<ObjectIdValue>> Rows7()
        => (await Rows("SELECT id FROM items WHERE id = 7")).Select(r => r.RowId).ToList();

    private async Task<List<Dictionary<string, ColumnValue>>> Query(string sql)
        => (await Rows(sql)).Select(r => new Dictionary<string, ColumnValue>(r.Row, StringComparer.OrdinalIgnoreCase)).ToList();

    private async Task<List<long>> Ids(string sql)
        => (await Query(sql)).Select(r => r["id"].LongValue).OrderBy(v => v).ToList();

    private async Task<List<(long id, long val)>> IdVals(string sql)
        => (await Query(sql)).Select(r => (r["id"].LongValue, r["val"].LongValue)).ToList();

    private async Task<long> Count(string sql)
        => (await Query(sql)).Single()["n"].LongValue;

    private async Task<string> PlanOf(string sql)
        => string.Join("\n", (await Query("EXPLAIN " + sql)).Select(r => r.TryGetValue("node", out ColumnValue? node) ? node.StrValue ?? "" : ""));
}
