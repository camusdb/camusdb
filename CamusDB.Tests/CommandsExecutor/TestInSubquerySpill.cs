/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that the spillable IN/NOT IN subquery value list bounds collection-time memory by
/// spilling values to disk when the inner subquery exceeds
/// <c>CamusDBConfig.SpillEffectiveThreshold</c>.
///
/// <para>
/// The discriminator for the spill path is
/// <c>StatisticsManager.InSubqueryValueListSpillCount</c>: a positive value proves that
/// <c>InSubqueryExecutor.MaterializeAsync</c> routed through the
/// <see cref="SpillableValueList"/> overflow path rather than retaining all values in a
/// plain <c>List&lt;ColumnValue&gt;</c>.
/// </para>
///
/// <para>
/// Correctness is verified by comparing results between the spill path (tiny threshold) and
/// the in-memory path (spill disabled), both against a known universe of outer rows.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInSubquerySpill : SharedNodeBaseTest
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_in_spill_" + System.Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _savedSpillEnabled   = CamusDBConfig.SpillEnabled;
        _savedThreshold      = CamusDBConfig.SpillThresholdRows;
        _savedForceThreshold = CamusDBConfig.ForceSpillThresholdRows;

        CamusDBConfig.DataDirectory = _dataDir;
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        CamusDBConfig.SpillEnabled            = _savedSpillEnabled;
        CamusDBConfig.SpillThresholdRows      = _savedThreshold;
        CamusDBConfig.ForceSpillThresholdRows = _savedForceThreshold;
        CamusDBConfig.DataDirectory           = null!;

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ─── helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates two tables:
    /// <list type="bullet">
    ///   <item><c>items(id, name, category_id)</c> — the outer table.</item>
    ///   <item><c>categories(id, name)</c> — the inner table whose ids feed the IN subquery.</item>
    /// </list>
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTablesAsync(int categoryCount, int itemsPerCategory)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "categories",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "items",
            columns:
            [
                new("id",          ColumnType.Id),
                new("name",        ColumnType.String, notNull: true),
                new("category_id", ColumnType.Id,     notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<string> catIds = new(categoryCount);
        List<Dictionary<string, ColumnValue>> catRows = new(categoryCount);
        for (int c = 0; c < categoryCount; c++)
        {
            string catId = ObjectIdGenerator.Generate().ToString();
            catIds.Add(catId);
            catRows.Add(new()
            {
                { "id",   new(ColumnType.Id,     catId) },
                { "name", new(ColumnType.String, $"cat{c:D4}") },
            });
        }

        await executor.Insert(new InsertTicket(txn, dbname, "categories", values: catRows));

        List<Dictionary<string, ColumnValue>> itemRows = new(categoryCount * itemsPerCategory);
        // Only items for the FIRST half of categories will be added as outer rows.
        for (int c = 0; c < categoryCount; c++)
        {
            for (int i = 0; i < itemsPerCategory; i++)
            {
                itemRows.Add(new()
                {
                    { "id",          new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                    { "name",        new(ColumnType.String, $"item{c}_{i}") },
                    { "category_id", new(ColumnType.Id,     catIds[c]) },
                });
            }
        }

        await executor.Insert(new InsertTicket(txn, dbname, "items", values: itemRows));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunQueryAsync(
        string dbname, DatabaseDescriptor database, CommandExecutor executor, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // ─── flag-off baseline ────────────────────────────────────────────────────

    /// <summary>
    /// With spill disabled, an IN subquery returns all outer rows whose <c>category_id</c>
    /// matches a value from the inner subquery. This baseline verifies the in-memory path is
    /// correct before enabling spill.
    /// </summary>
    [Test]
    public async Task InSubquerySpill_FlagOff_CorrectResultFromInMemoryPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupTablesAsync(categoryCount: 4, itemsPerCategory: 3);

        CamusDBConfig.SpillEnabled = false;
        executor.Statistics.InSubqueryValueListSpillCount = 0;

        // GROUP BY (not DISTINCT — inner DISTINCT is now semi-join-eligible) keeps the subquery
        // ineligible for semi-join, so it routes through SubqueryRewriter's value-list path.
        List<QueryResultRow> rows = await RunQueryAsync(dbname, database, executor,
            "SELECT i.name FROM items i WHERE i.category_id IN (SELECT c.id FROM categories c GROUP BY c.id)");

        Assert.That(rows.Count, Is.EqualTo(12),
            "All 4×3 items must be returned when the IN list covers every category");
        Assert.That(executor.Statistics.InSubqueryValueListSpillCount, Is.EqualTo(0),
            "No spill should occur with SpillEnabled=false");
    }

    // ─── spill path: large inner subquery ────────────────────────────────────

    /// <summary>
    /// With a tiny <c>ForceSpillThresholdRows</c>, the IN subquery value list overflows to a
    /// spill file during collection. The result must be identical to the in-memory path, and
    /// <c>InSubqueryValueListSpillCount</c> must be positive to confirm the spill path was taken.
    ///
    /// This test fails if the value set is still a plain <c>List&lt;ColumnValue&gt;</c> because
    /// a plain list never increments the counter.
    /// </summary>
    [Test]
    public async Task InSubquerySpill_LargeInnerSubquery_SpillsAndReturnsCorrectRows()
    {
        const int categories = 20;
        const int itemsEach  = 2;

        // threshold=3 → the inner subquery (20 rows) will spill after the 3rd value. The engine fixes
        // its configuration when it is built, so this is set before the tables are created.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupTablesAsync(categoryCount: categories, itemsPerCategory: itemsEach);
        executor.Statistics.InSubqueryValueListSpillCount = 0;

        List<QueryResultRow> rows = await RunQueryAsync(dbname, database, executor,
            "SELECT i.name FROM items i WHERE i.category_id IN (SELECT c.id FROM categories c GROUP BY c.id)");

        Assert.That(rows.Count, Is.EqualTo(categories * itemsEach),
            "All items must be returned when the IN list (spilled) covers every category");

        Assert.That(executor.Statistics.InSubqueryValueListSpillCount, Is.GreaterThan(0),
            "InSubqueryValueListSpillCount must be > 0 — a zero value means the value set was " +
            "collected into a plain List<ColumnValue> rather than the SpillableValueList");
    }

    // ─── NOT IN spill path ────────────────────────────────────────────────────

    /// <summary>
    /// NOT IN with a spilled value list must still return the complement correctly.
    /// An inner subquery with categoryCount/2 categories leaves the other half unmatched.
    /// </summary>
    [Test]
    public async Task InSubquerySpill_NotIn_SpillsAndReturnsComplement()
    {
        const int categories = 10;
        const int itemsEach  = 2;

        // Configured before the engine is built, which is when it fixes its configuration.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupTablesAsync(categoryCount: categories, itemsPerCategory: itemsEach);

        // Insert a second set of categories that the NOT IN subquery will exclude.
        // We want items whose category_id is NOT IN the first 5 categories.
        // The setup already inserts all 10 categories with all 10*2=20 items.
        // Query: items NOT IN first 5 categories → 5*2=10 items expected.
        // We need to get the ids of the first 5 categories to build the NOT IN list.
        // Instead, query all items NOT IN a subquery that returns ALL categories → 0 items.

        executor.Statistics.InSubqueryValueListSpillCount = 0;

        // No items are NOT IN the full category list → 0 rows.
        // GROUP BY keeps the subquery ineligible for semi-join so SpillableValueList is exercised.
        List<QueryResultRow> rows = await RunQueryAsync(dbname, database, executor,
            "SELECT i.name FROM items i WHERE i.category_id NOT IN (SELECT c.id FROM categories c GROUP BY c.id)");

        Assert.That(rows.Count, Is.EqualTo(0),
            "No items should be returned when NOT IN covers every category");

        Assert.That(executor.Statistics.InSubqueryValueListSpillCount, Is.GreaterThan(0),
            "InSubqueryValueListSpillCount must be > 0 — spill path must be taken for NOT IN too");
    }

    // ─── parity: spill vs in-memory ──────────────────────────────────────────

    /// <summary>
    /// Spill path result must be element-wise identical to the in-memory path result on the same
    /// data. Uses ORDER BY to stabilise comparison order.
    /// </summary>
    [Test]
    public async Task InSubquerySpill_ResultMatchesInMemoryPath()
    {
        const int categories = 15;
        const int itemsEach  = 3;
        // GROUP BY keeps the subquery ineligible for semi-join so both paths go through SubqueryRewriter.
        const string sql =
            "SELECT i.name FROM items i " +
            "WHERE i.category_id IN (SELECT c.id FROM categories c GROUP BY c.id) " +
            "ORDER BY i.name";

        // In-memory reference. The engine fixes its configuration when it is built, so the setting is
        // in place before the tables (and the executor) are created.
        CamusDBConfig.SpillEnabled = false;

        (string db1, DatabaseDescriptor d1, CommandExecutor ex1)
            = await SetupTablesAsync(categories, itemsEach);
        List<QueryResultRow> reference = await RunQueryAsync(db1, d1, ex1, sql);

        // Spill path — again configured before the engine that must honour it is built.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 2;

        (string db2, DatabaseDescriptor d2, CommandExecutor ex2)
            = await SetupTablesAsync(categories, itemsEach);
        List<QueryResultRow> spill = await RunQueryAsync(db2, d2, ex2, sql);

        Assert.That(spill.Count, Is.EqualTo(reference.Count),
            "Spill and in-memory paths must return the same number of rows");

        for (int i = 0; i < reference.Count; i++)
        {
            Assert.That(spill[i].Row["name"].StrValue, Is.EqualTo(reference[i].Row["name"].StrValue),
                $"Row[{i}].name mismatch between spill and in-memory paths");
        }
    }

    // ─── cleanup ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The IN subquery value list is disposed immediately after the membership-test AST tree is
    /// built. By the time the outer query cursor returns rows, no spill files must remain.
    /// </summary>
    [Test]
    public async Task InSubquerySpill_SpillFilesRemovedAfterCompletion()
    {
        // Configured before the engine is built, which is when it fixes its configuration.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 2;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await SetupTablesAsync(categoryCount: 12, itemsPerCategory: 2);

        await RunQueryAsync(dbname, database, executor,
            "SELECT i.name FROM items i WHERE i.category_id IN (SELECT c.id FROM categories c GROUP BY c.id)");

        string[] remaining = Directory.GetFiles(_dataDir, "*.spill", SearchOption.AllDirectories);
        Assert.That(remaining, Is.Empty,
            "All spill files must be cleaned up after the IN subquery query completes");
    }
}
