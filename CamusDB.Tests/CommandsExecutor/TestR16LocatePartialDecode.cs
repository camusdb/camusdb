
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// R16 — Minimize Row Decoding in UPDATE/DELETE Locate Phase.
///
/// The locate scan now decodes only the columns referenced by the WHERE clause
/// and any expression-based SET values. Rejected candidates are skipped after
/// the partial decode. The write phase still does a full DecodeWritableAsync per
/// matched row. Tests here verify observable correctness: the right rows are
/// modified and their values are correct after the optimized locate.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestR16LocatePartialDecode : BaseTest
{
    /// <summary>
    /// Wide table with many non-indexed columns; rows split between two regions.
    /// The locate phase will need only the WHERE column ("region") — all other
    /// columns are skipped for candidates that do not match.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids)>
        SetupWideTable(int count = 20)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "sensors",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("region",   ColumnType.String,    notNull: true),
                new("active",   ColumnType.Bool),
                new("score",    ColumnType.Integer64),
                new("reading",  ColumnType.Float64),
                new("tag",      ColumnType.String),
                new("category", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        List<string> ids = new(count);

        for (int i = 0; i < count; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket insert = new(
                txnState: tx,
                databaseName: dbname,
                tableName: "sensors",
                values: new()
                {
                    new()
                    {
                        { "id",       new(ColumnType.Id,        objectId) },
                        { "region",   new(ColumnType.String,     i < 10 ? "north" : "south") },
                        { "active",   new(ColumnType.Bool,       i % 2 == 0) },
                        { "score",    new(ColumnType.Integer64,  (long)i * 10) },
                        { "reading",  new(ColumnType.Float64,    (double)i * 1.5) },
                        { "tag",      new(ColumnType.String,     "tag-" + i) },
                        { "category", new(ColumnType.Integer64,  (long)(i % 3)) },
                    }
                }
            );

            await executor.Insert(insert);
            ids.Add(objectId);
        }

        await database.Transactions.CommitAsync(tx);
        return (dbname, database, executor, ids);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // UPDATE tests
    // ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Plain-value UPDATE with WHERE on one column. Locate phase decodes only
    /// "region". Write phase full-decodes each matched row before applying the change.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UpdatePlainValue_SelectiveWhere_CorrectRowsChanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) =
            await SetupWideTable();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new(tx, dbname, "UPDATE sensors SET active = true WHERE region = 'north'", null));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(10, result.ModifiedRows); // rows 0-9 have region='north'

        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, "SELECT region, active FROM sensors", null));
        int northActive = 0, southActive = 0;
        await foreach (QueryResultRow row in cursor)
        {
            bool active = row.Row["active"].BoolValue;
            string region = row.Row["region"].StrValue!;
            if (region == "north" && active) northActive++;
            if (region == "south" && active) southActive++;
        }
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual(10, northActive);
        Assert.AreEqual(5, southActive); // rows 10,12,14,16,18 were already true
    }

    /// <summary>
    /// Expression-value UPDATE: SET score = score + 100 WHERE region = 'south'.
    /// ComputeForLocate must include "region" (WHERE) and "score" (RHS of the
    /// SET expression). Write phase re-reads each matched row and applies the result.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UpdateExprValue_ReferencedColumnDecoded_CorrectResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) =
            await SetupWideTable();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new(tx, dbname, "UPDATE sensors SET score = score + 100 WHERE region = 'south'", null));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(10, result.ModifiedRows); // rows 10-19 have region='south'

        // Rows 10-19 had score = i*10 (min 100). After update: score = i*10 + 100 (min 200).
        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, "SELECT score FROM sensors WHERE region = 'south'", null));
        int checked_ = 0;
        await foreach (QueryResultRow row in cursor)
        {
            long score = row.Row["score"].LongValue;
            Assert.GreaterOrEqual(score, 200L, "south score should be >= 200 after +100 (was min i=10 → score=100)");
            checked_++;
        }
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual(10, checked_);
    }

    /// <summary>
    /// UPDATE via filter (legacy API): the locate phase decodes only the filtered
    /// column. Correct row is updated; others are unchanged.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UpdateWithFilter_SingleColumn_CorrectRowChanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupWideTable();

        KvTransaction tx = await database.Transactions.BeginAsync();

        UpdateTicket update = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "sensors",
            plainValues: new() { { "tag", new(ColumnType.String, "updated") } },
            exprValues: null,
            where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, ids[3])) },
            parameters: null
        );

        UpdateResult execResult = await executor.Update(update);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, execResult.UpdatedRows);

        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, $"SELECT tag FROM sensors WHERE id = '{ids[3]}'", null));
        string? tag = null;
        await foreach (QueryResultRow row in cursor)
            tag = row.Row["tag"].StrValue;
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual("updated", tag);
    }

    /// <summary>
    /// UPDATE with no WHERE at all: ComputeForLocate returns an empty set, so
    /// ScanRequiredColumns = {} (decode zero columns, just the row id). All rows
    /// must still be updated correctly via the write phase's full decode.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UpdateNoWhere_EmptyLocateColumns_AllRowsUpdated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) =
            await SetupWideTable(count: 10);

        // SQL UPDATE requires WHERE; use the ticket API for a no-WHERE full-table update.
        KvTransaction tx = await database.Transactions.BeginAsync();
        UpdateTicket update = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "sensors",
            plainValues: new() { { "score", new(ColumnType.Integer64, 999L) } },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null
        );
        UpdateResult result = await executor.Update(update);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(10, result.UpdatedRows);

        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, "SELECT score FROM sensors", null));
        int wrongCount = 0;
        await foreach (QueryResultRow row in cursor)
            if (row.Row["score"].LongValue != 999L) wrongCount++;
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual(0, wrongCount);
    }

    // ────────────────────────────────────────────────────────────────────────────
    // DELETE tests
    // ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// DELETE with a WHERE clause: locate phase decodes only "region" to find
    /// candidates; write phase full-decodes for index maintenance.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DeleteWithWhere_SelectiveColumn_CorrectRowsRemoved()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) =
            await SetupWideTable(count: 20);

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new(tx, dbname, "DELETE FROM sensors WHERE region = 'north'", null));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(10, result.ModifiedRows);

        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, "SELECT region FROM sensors", null));
        int remaining = 0;
        await foreach (QueryResultRow row in cursor)
        {
            Assert.AreEqual("south", row.Row["region"].StrValue);
            remaining++;
        }
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual(10, remaining);
    }

    /// <summary>
    /// DELETE via filter (legacy API path). Locate phase decodes only the "id"
    /// filter column; one row is removed, the rest untouched.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DeleteWithFilter_MatchesExactRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupWideTable();

        KvTransaction tx = await database.Transactions.BeginAsync();

        DeleteTicket delete = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "sensors",
            where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, ids[7])) }
        );

        DeleteResult execResult = await executor.Delete(delete);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, execResult.DeletedRows);

        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, $"SELECT id FROM sensors WHERE id = '{ids[7]}'", null));
        int found = 0;
        await foreach (QueryResultRow _ in cursor)
            found++;
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual(0, found);
    }

    /// <summary>
    /// UPDATE then DELETE on the same table: verifies the partial-decode path
    /// doesn't corrupt state across multiple operations in the same database.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UpdateThenDelete_BothUsePartialLocate_NoCorruption()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) =
            await SetupWideTable(count: 10);

        KvTransaction tx1 = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(
            new(tx1, dbname, "UPDATE sensors SET score = score + 1 WHERE region = 'north'", null));
        await database.Transactions.CommitAsync(tx1);

        KvTransaction tx2 = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult delResult = await executor.ExecuteNonSQLQuery(
            new(tx2, dbname, "DELETE FROM sensors WHERE region = 'south'", null));
        await database.Transactions.CommitAsync(tx2);

        Assert.AreEqual(0, delResult.ModifiedRows); // no south in 10-row table (0-9 = north)

        KvTransaction txRead = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new(txRead, dbname, "SELECT score FROM sensors WHERE region = 'north'", null));
        int updated = 0;
        await foreach (QueryResultRow row in cursor)
        {
            long score = row.Row["score"].LongValue;
            Assert.GreaterOrEqual(score, 1L, "north score should be >= 1 after +1");
            updated++;
        }
        await database.Transactions.CommitAsync(txRead);

        Assert.AreEqual(10, updated);
    }
}
