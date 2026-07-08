
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies covering (index-only) scan execution in QueryScanner.ScanUsingIndex.
/// Tests assert result parity between the covered path and a forced-non-covered
/// baseline, and confirm that zero primary-row reads occur when the index covers
/// every required column.
/// </summary>
[NonParallelizable]
public class TestQueryScannerIndexOnly : BaseTest
{
    // ── Fixture helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Creates a robots table (id, name, year, enabled) with a single-column year_idx.
    /// 100 rows (years 1–100) are inserted so the cost model favours the index for
    /// selective predicates (≤ ~10 rows returned).
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupRobotsWithYearIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String, notNull: true),
                new("year",    ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        ));

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 1; i <= 100; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name",    new(ColumnType.String, "robot" + i) },
                        { "year",    new(ColumnType.Integer64, (long)i) },
                        { "enabled", new(ColumnType.Bool, i % 2 == 0) },
                    }
                }
            ));
        }
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    /// <summary>
    /// Creates a robots table (id, name, year, enabled) with a composite index on
    /// (year, name) — index column order (year first, name second) differs from schema
    /// column order (name is schema column 1, year is schema column 2). Used to verify
    /// that BuildIndexOnlyLayout produces the correct cross-column slot mapping.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupRobotsWithCompositeYearNameIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String, notNull: true),
                new("year",    ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        // Index on (year, name): year is index[0], name is index[1].
        // Schema order: name is column 1, year is column 2 — deliberately reversed vs the index.
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_name_idx",
            columns: new ColumnIndexInfo[]
            {
                new("year", OrderType.Ascending),
                new("name", OrderType.Ascending),
            },
            operation: AlterIndexOperation.AddIndex
        ));

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 1; i <= 100; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name",    new(ColumnType.String, "robot" + i) },
                        { "year",    new(ColumnType.Integer64, (long)i) },
                        { "enabled", new(ColumnType.Bool, i % 2 == 0) },
                    }
                }
            ));
        }
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    /// <summary>
    /// Returns the rows_read value from the first index scan node in EXPLAIN ANALYZE output,
    /// or null if no index-lookup / index-range-scan node appears (which means the cost model
    /// chose a full table scan and the covering path was never invoked).
    /// </summary>
    private static async Task<long?> ExplainAnalyzeScanRowsRead(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname,
            sql: "EXPLAIN (ANALYZE) " + sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        foreach (QueryResultRow r in rows)
        {
            if (r.Row.TryGetValue("node", out ColumnValue? node)
                && node.StrValue is "index-lookup" or "index-range-scan"
                && r.Row.TryGetValue("rows_read", out ColumnValue? rr)
                && rr.Type == ColumnType.Integer64)
                return rr.LongValue;
        }
        return null;
    }

    // ── Parity tests ─────────────────────────────────────────────────────────

    [Test]
    public async Task CoveredScan_ReturnsIdenticalResultsToSelectStar()
    {
        // Covered: SELECT year via year_idx synthesizes values from the index key.
        // Baseline: SELECT * reads the primary row.
        // WHERE year > 98 selects 2 rows out of 100 (~2% selectivity) so the cost model
        // picks the index. rows_read = 0 is asserted to confirm the covering path ran —
        // if the planner falls back to a table scan the assertion catches the regression.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithYearIndex();

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year > 98");
        List<QueryResultRow> covered = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year > 98");

        Assert.AreEqual(baseline.Count, covered.Count,
            "covered and non-covered paths must return the same number of rows");

        HashSet<long> baselineYears = baseline.Select(r => r.Row["year"].LongValue).ToHashSet();
        HashSet<long> coveredYears  = covered.Select(r => r.Row["year"].LongValue).ToHashSet();
        Assert.That(coveredYears, Is.EquivalentTo(baselineYears),
            "covered scan must return identical year values to the full-fetch baseline");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year > 98");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");
    }

    [Test]
    public async Task NonCoveredIndexScan_IdColumnMatchesInsertedValue()
    {
        // The id column is NOT part of year_idx, so SELECT id FROM robots WHERE year > 98
        // cannot be a covering scan — the planner must fall back to a non-covering index
        // scan that fetches each primary row to retrieve the stored id value.
        //
        // The stored id is the user-provided ObjectId (distinct from the internal KV row
        // key), so the non-covering fetch is the only correct path; covering synthesis from
        // the KV row key would return the wrong value.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithYearIndex();

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year > 98");
        List<QueryResultRow> idOnly = await RunSql(executor, database, dbname,
            "SELECT id FROM robots WHERE year > 98");

        Assert.AreEqual(baseline.Count, idOnly.Count,
            "both queries must return the same number of rows");

        HashSet<string?> baselineIds = baseline.Select(r => r.Row["id"].StrValue).ToHashSet();
        HashSet<string?> idOnlyIds   = idOnly.Select(r => r.Row["id"].StrValue).ToHashSet();
        Assert.That(idOnlyIds, Is.EquivalentTo(baselineIds),
            "id values from the index-scan path must equal the user-provided id values in the rows");

        // Non-covering: the planner must read at least one primary row.
        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT id FROM robots WHERE year > 98");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.Greater(rowsRead!.Value, 0L,
            "SELECT id is not covered by year_idx, so primary rows must be fetched (rows_read > 0)");
    }

    [Test]
    public async Task CoveredScan_ExactPredicateMatch_ReturnsSingleRow()
    {
        // SELECT year FROM robots WHERE year = 42 — equality predicate, covered scan.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithYearIndex();

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");

        Assert.AreEqual(1, rows.Count,
            "exactly one row has year = 42");
        Assert.AreEqual(42L, rows[0].Row["year"].LongValue,
            "the returned year value must be 42");
    }

    [Test]
    public async Task CoveredScan_CompositeIndex_SchemaOrderDiffersFromIndexOrder()
    {
        // The composite index is (year, name) — year is index[0], name is index[1].
        // Schema column order is (id, name, year, enabled) — name precedes year in the schema.
        // BuildIndexOnlyLayout must map each schema column to the correct index slot:
        //   name → slot 1 (decodedKey.Values[1])
        //   year → slot 0 (decodedKey.Values[0])
        // This test exercises cross-column slot mapping and the String-type covered column.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithCompositeYearNameIndex();

        // WHERE year = 42 selects 1 row out of 100 (~1% selectivity) — index preferred.
        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year = 42");
        List<QueryResultRow> covered = await RunSql(executor, database, dbname,
            "SELECT name, year FROM robots WHERE year = 42");

        Assert.AreEqual(1, baseline.Count, "exactly one row has year = 42");
        Assert.AreEqual(baseline.Count, covered.Count,
            "covered and baseline must return the same number of rows");

        Assert.AreEqual(baseline[0].Row["name"].StrValue, covered[0].Row["name"].StrValue,
            "String column 'name' must be synthesized correctly from index slot 1");
        Assert.AreEqual(baseline[0].Row["year"].LongValue, covered[0].Row["year"].LongValue,
            "Integer64 column 'year' must be synthesized correctly from index slot 0");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT name, year FROM robots WHERE year = 42");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value,
            "composite covered scan must read zero primary rows");
    }

    [Test]
    public async Task CoveredScan_ZeroRowsRead_InExplainAnalyze()
    {
        // EXPLAIN ANALYZE for a covered query must show rows_read = 0 on the scan node:
        // no primary-row fetch happens in the covering path.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithYearIndex();

        // year = 42 selects 1 row out of 100 — highly selective, so the cost model picks the index.
        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");

        Assert.IsNotNull(rowsRead,
            "EXPLAIN ANALYZE must include a scan node with rows_read for an index scan");
        Assert.AreEqual(0L, rowsRead!.Value,
            "a covered scan must not read any primary rows (rows_read should be 0)");
    }

    [Test]
    public async Task NonCoveredScan_HasPositiveRowsRead_InExplainAnalyze()
    {
        // Sanity check: a non-covered query (SELECT * needs all columns) must still
        // read primary rows — rows_read > 0.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithYearIndex();

        // year = 42 selects 1 row out of 100 — highly selective, so the cost model picks the index.
        // SELECT * means IndexOnly=false, so the covering path is not taken and a primary row IS read.
        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT * FROM robots WHERE year = 42");

        Assert.IsNotNull(rowsRead,
            "EXPLAIN ANALYZE must include a scan node with rows_read for an index scan");
        Assert.Greater(rowsRead!.Value, 0L,
            "a non-covered scan must read primary rows (rows_read > 0)");
    }

    [Test]
    public async Task CoveredScan_EmptyResult_WhenNoRowsMatch()
    {
        // Covered path must correctly return an empty result when no index entries match.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobotsWithYearIndex();

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 9999");

        Assert.IsEmpty(rows, "no rows should be returned for a predicate that matches nothing");
    }
}
