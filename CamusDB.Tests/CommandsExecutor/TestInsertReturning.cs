/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for <c>INSERT … RETURNING</c> through the engine's two SQL entry points. Every
/// test drives real SQL through <see cref="CommandExecutor"/>, and the values a test checks against
/// are read back from storage with a separate SELECT, so a RETURNING value that differs from the
/// stored value fails here.
/// </summary>
[NonParallelizable]
public sealed class TestInsertReturning : SharedNodeBaseTest
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
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

    private static Task<ExecuteNonSQLResult> NonQuery(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql,
        Dictionary<string, ColumnValue>? parameters = null, bool discardReturningRows = false)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            tx, dbname, sql, parameters, discardReturningRows: discardReturningRows));

    /// <summary>Runs one autocommit no-rows statement and commits it.</summary>
    private static async Task<ExecuteNonSQLResult> NonQueryCommitted(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null, bool discardReturningRows = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await NonQuery(executor, dbname, tx, sql, parameters, discardReturningRows);
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<(IReadOnlyList<DerivedColumnSchema> Schema, List<QueryResultRow> Rows)> Query(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        QuerySchemaHolder schema = new();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, sql, null), schemaOut: schema);

        List<QueryResultRow> rows = await cursor.ToListAsync();
        return (schema.Schema, rows);
    }

    private static async Task<List<QueryResultRow>> QueryCommitted(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, List<QueryResultRow> rows) = await Query(executor, dbname, tx, sql);
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The cell of <paramref name="column"/> in <paramref name="row"/>, by the column's row key.</summary>
    private static ColumnValue Cell(QueryResultRow row, DerivedColumnSchema column) => row.Row[column.RowKey];

    private static ColumnValue Cell(ExecuteNonSQLResult result, int row, int column)
        => Cell(result.ReturningRows![row], result.ReturningColumns![column]);

    private static string[] ColumnNames(IReadOnlyList<DerivedColumnSchema> columns) => columns.Select(c => c.Name).ToArray();

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname,
            "CREATE TABLE robots (id oid NOT NULL DEFAULT(gen_id()), name string, year int64 DEFAULT(1999), note string, PRIMARY KEY (id))");
        return (dbname, database, executor);
    }

    // -----------------------------------------------------------------------
    // VALUES
    // -----------------------------------------------------------------------

    [Test]
    public async Task ValuesReturnsListedColumnsInInsertOrder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name, year) VALUES ('alpha', 2001), ('beta', 2002), ('gamma', 2003) RETURNING name, year");

        Assert.AreEqual(3, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "name", "year" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(ColumnType.String, result.ReturningColumns![0].Type);
        Assert.AreEqual(ColumnType.Integer64, result.ReturningColumns![1].Type);
        Assert.AreEqual(3, result.ReturningRows!.Count);

        string[] names = { "alpha", "beta", "gamma" };
        for (int i = 0; i < 3; i++)
        {
            Assert.AreEqual(names[i], Cell(result, i, 0).StrValue);
            Assert.AreEqual(2001 + i, Cell(result, i, 1).LongValue);
        }
    }

    [Test]
    public async Task WithoutReturningTheResultCarriesNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name) VALUES ('alpha')");

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
        Assert.IsNull(result.ReturningRows);
    }

    /// <summary>
    /// <c>*</c> covers every column in schema order, including the ones the statement did not name:
    /// a function default, a constant default, and a column with no value at all, which is NULL.
    /// The generated id must be the id that was stored.
    /// </summary>
    [Test]
    public async Task StarReturnsEveryColumnWithStoredDefaults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name) VALUES ('alpha') RETURNING *");

        CollectionAssert.AreEqual(new[] { "id", "name", "year", "note" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(1, result.ReturningRows!.Count);

        Assert.AreEqual(ColumnType.Id, Cell(result, 0, 0).Type);
        Assert.AreEqual("alpha", Cell(result, 0, 1).StrValue);
        Assert.AreEqual(1999, Cell(result, 0, 2).LongValue);
        Assert.AreEqual(ColumnType.Null, Cell(result, 0, 3).Type);

        List<QueryResultRow> stored = await QueryCommitted(executor, database, dbname, "SELECT id FROM robots WHERE name = 'alpha'");
        Assert.AreEqual(1, stored.Count);
        Assert.AreEqual(stored[0].Row["id"].StrValue, Cell(result, 0, 0).StrValue);
    }

    [Test]
    public async Task ExpressionsAliasesAndPlaceholdersAreEvaluatedPerRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        Dictionary<string, ColumnValue> parameters = new() { ["@shift"] = new ColumnValue(ColumnType.Integer64, 100) };

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name, year) VALUES ('alpha', 10), ('beta', 20) " +
            "RETURNING year * 2 AS doubled, year + @shift AS shifted, NAME",
            parameters);

        CollectionAssert.AreEqual(new[] { "doubled", "shifted", "NAME" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(20, Cell(result, 0, 0).LongValue);
        Assert.AreEqual(110, Cell(result, 0, 1).LongValue);
        Assert.AreEqual("alpha", Cell(result, 0, 2).StrValue);
        Assert.AreEqual(40, Cell(result, 1, 0).LongValue);
        Assert.AreEqual(120, Cell(result, 1, 1).LongValue);
        Assert.AreEqual("beta", Cell(result, 1, 2).StrValue);
    }

    [Test]
    public async Task QualifiedColumnNamesResolveAgainstTheTarget()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name) VALUES ('alpha') RETURNING robots.name");

        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual("alpha", Cell(result, 0, 0).StrValue);
    }

    /// <summary>
    /// A sequence default and an identity column are drawn by the insert itself. RETURNING must report
    /// the drawn values — one distinct value per row — and they must be the values stored.
    /// </summary>
    [Test]
    public async Task SequenceAndIdentityDefaultsReturnTheDrawnValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE SEQUENCE invoice_no");
        await Ddl(executor, dbname,
            "CREATE TABLE invoices (id oid PRIMARY KEY, no int64 DEFAULT(nextval('invoice_no')), " +
            "line int64 GENERATED BY DEFAULT AS IDENTITY, label string)");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO invoices (id, label) VALUES (gen_id(), 'a'), (gen_id(), 'b'), (gen_id(), 'c') RETURNING label, no, line");

        Assert.AreEqual(3, result.ReturningRows!.Count);

        long[] numbers = result.ReturningRows.Select(r => r.Row[result.ReturningColumns![1].RowKey].LongValue).ToArray();
        long[] lines = result.ReturningRows.Select(r => r.Row[result.ReturningColumns![2].RowKey].LongValue).ToArray();
        Assert.AreEqual(3, numbers.Distinct().Count());
        Assert.AreEqual(3, lines.Distinct().Count());

        foreach (QueryResultRow row in result.ReturningRows)
        {
            string label = row.Row[result.ReturningColumns![0].RowKey].StrValue!;
            List<QueryResultRow> stored = await QueryCommitted(executor, database, dbname,
                $"SELECT no, line FROM invoices WHERE label = '{label}'");

            Assert.AreEqual(stored[0].Row["no"].LongValue, row.Row[result.ReturningColumns![1].RowKey].LongValue);
            Assert.AreEqual(stored[0].Row["line"].LongValue, row.Row[result.ReturningColumns![2].RowKey].LongValue);
        }
    }

    [Test]
    public async Task CoercedValuesReturnTheStoredType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE prices (id oid NOT NULL DEFAULT(gen_id()), amount float64, PRIMARY KEY (id))");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO prices (amount) VALUES (10) RETURNING amount");

        Assert.AreEqual(ColumnType.Float64, result.ReturningColumns![0].Type);
        Assert.AreEqual(ColumnType.Float64, Cell(result, 0, 0).Type);
        Assert.AreEqual(10.0, Cell(result, 0, 0).FloatValue);
    }

    /// <summary>
    /// A value large enough to be stored out of line is returned whole, not as the pointer the row
    /// keeps for it.
    /// </summary>
    [Test]
    public async Task LargeValuesAreReturnedWhole()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        string large = new('x', 200_000);
        Dictionary<string, ColumnValue> parameters = new() { ["@note"] = new ColumnValue(ColumnType.String, large) };

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name, note) VALUES ('big', @note) RETURNING note", parameters);

        Assert.AreEqual(large, Cell(result, 0, 0).StrValue);

        List<QueryResultRow> stored = await QueryCommitted(executor, database, dbname, "SELECT note FROM robots WHERE name = 'big'");
        Assert.AreEqual(large, stored[0].Row["note"].StrValue);
    }

    // -----------------------------------------------------------------------
    // INSERT … SELECT
    // -----------------------------------------------------------------------

    [Test]
    public async Task InsertSelectReturnsTheCopiedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await Ddl(executor, dbname, "CREATE TABLE archive (id oid NOT NULL DEFAULT(gen_id()), name string, year int64, PRIMARY KEY (id))");

        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name, year) VALUES ('alpha', 2001), ('beta', 2002), ('gamma', 2003)");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO archive (name, year) SELECT name, year FROM robots WHERE year >= 2002 RETURNING name, year");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEquivalent(
            new[] { "beta", "gamma" },
            result.ReturningRows!.Select(r => r.Row[result.ReturningColumns![0].RowKey].StrValue));
    }

    /// <summary>
    /// A source that yields nothing still answers with the full schema and an empty row list, so a
    /// client can tell "returned no rows" from "returns no rows".
    /// </summary>
    [Test]
    public async Task InsertSelectOfNoRowsReturnsTheSchemaAndNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await Ddl(executor, dbname, "CREATE TABLE archive (id oid NOT NULL DEFAULT(gen_id()), name string, year int64, PRIMARY KEY (id))");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO archive (name, year) SELECT name, year FROM robots WHERE year > 3000 RETURNING id, name");

        Assert.AreEqual(0, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "id", "name" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(ColumnType.Id, result.ReturningColumns![0].Type);
        Assert.IsNotNull(result.ReturningRows);
        Assert.AreEqual(0, result.ReturningRows!.Count);
    }

    // -----------------------------------------------------------------------
    // Refusals
    // -----------------------------------------------------------------------

    [TestCase("INSERT INTO robots (name) VALUES ('alpha') RETURNING missing", CamusDBErrorCodes.UnknownColumn)]
    [TestCase("INSERT INTO robots (name) VALUES ('alpha') RETURNING COUNT(*)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("INSERT INTO robots (name) VALUES ('alpha') RETURNING year + COUNT(*)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("INSERT INTO robots (name) VALUES ('alpha') RETURNING (SELECT COUNT(*) FROM robots)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("INSERT INTO robots (name) VALUES ('alpha') RETURNING nextval('robot_no')", CamusDBErrorCodes.SequenceCallNotAllowedHere)]
    [TestCase("INSERT INTO robots (name) SELECT name FROM robots RETURNING missing", CamusDBErrorCodes.UnknownColumn)]
    public async Task RefusedListsFailBeforeAnyRowIsWritten(string sql, string expectedCode)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await Ddl(executor, dbname, "CREATE SEQUENCE robot_no");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(
            async () => await NonQueryCommitted(executor, database, dbname, sql));
        Assert.AreEqual(expectedCode, error!.Code, error.Message);

        // The same refusal with the count-only flag: the list is still checked.
        CamusDBException? discarded = Assert.ThrowsAsync<CamusDBException>(
            async () => await NonQueryCommitted(executor, database, dbname, sql, discardReturningRows: true));
        Assert.AreEqual(expectedCode, discarded!.Code, discarded.Message);

        Assert.AreEqual(0, (await QueryCommitted(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    [Test]
    public async Task AUniqueViolationReturnsNothingAndStoresNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE codes (id oid NOT NULL DEFAULT(gen_id()), code string, PRIMARY KEY (id))");
        await Ddl(executor, dbname, "CREATE UNIQUE INDEX ux_code ON codes (code)");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO codes (code) VALUES ('a'), ('b'), ('a') RETURNING code"));
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, error!.Code);

        Assert.AreEqual(0, (await QueryCommitted(executor, database, dbname, "SELECT id FROM codes")).Count);
    }

    // -----------------------------------------------------------------------
    // The row-returning entry point
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheQueryEntryPointReturnsTheSameRowsAndSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        KvTransaction tx = await database.Transactions.BeginAsync();
        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) = await Query(executor, dbname, tx,
            "INSERT INTO robots (name, year) VALUES ('alpha', 2001), ('beta', 2002) RETURNING name, year + 1 AS next");
        await database.Transactions.CommitAsync(tx);

        CollectionAssert.AreEqual(new[] { "name", "next" }, ColumnNames(schema));
        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual("alpha", Cell(rows[0], schema[0]).StrValue);
        Assert.AreEqual(2002, Cell(rows[0], schema[1]).LongValue);
        Assert.AreEqual("beta", Cell(rows[1], schema[0]).StrValue);
        Assert.AreEqual(2003, Cell(rows[1], schema[1]).LongValue);

        Assert.AreEqual(2, (await QueryCommitted(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    [Test]
    public async Task TheQueryEntryPointRefusesTheCountOnlyFlag()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        foreach (string sql in new[] { "SELECT name FROM robots", "INSERT INTO robots (name) VALUES ('alpha') RETURNING name" })
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            try
            {
                CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(tx, dbname, sql, null, discardReturningRows: true)));
                Assert.AreEqual(CamusDBErrorCodes.InvalidInput, error!.Code, sql);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }

        Assert.AreEqual(0, (await QueryCommitted(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    // -----------------------------------------------------------------------
    // Count only
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheCountOnlyFlagInsertsAndReturnsTheCountOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name) VALUES ('alpha'), ('beta') RETURNING *", discardReturningRows: true);

        Assert.AreEqual(2, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
        Assert.IsNull(result.ReturningRows);
        Assert.AreEqual(2, (await QueryCommitted(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    [Test]
    public async Task TheCountOnlyFlagHasNoEffectWithoutReturning()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name) VALUES ('alpha')", discardReturningRows: true);

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
    }

    // -----------------------------------------------------------------------
    // Transactions and concurrency
    // -----------------------------------------------------------------------

    /// <summary>
    /// Inside an explicit transaction the rows come back before the commit. A rollback then removes
    /// them like any other write of the transaction.
    /// </summary>
    [Test]
    public async Task ARollbackRemovesTheReturnedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await NonQuery(executor, dbname, tx,
            "INSERT INTO robots (name) VALUES ('alpha'), ('beta') RETURNING name");
        Assert.AreEqual(2, result.ReturningRows!.Count);

        (_, List<QueryResultRow> visibleInside) = await Query(executor, dbname, tx, "SELECT id FROM robots");
        Assert.AreEqual(2, visibleInside.Count);

        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(0, (await QueryCommitted(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    /// <summary>
    /// Concurrent inserts into one table each see only their own rows, and a sequence default never
    /// hands the same value to two of them.
    /// </summary>
    [Test]
    public async Task ConcurrentInsertsEachReturnTheirOwnRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE tickets (id oid PRIMARY KEY, no serial, owner string)");

        const int writers = 4;
        const int rowsEach = 25;

        async Task<ExecuteNonSQLResult> Writer(int w)
        {
            string values = string.Join(", ", Enumerable.Range(0, rowsEach).Select(_ => $"(gen_id(), 'w{w}')"));
            return await NonQueryCommitted(executor, database, dbname,
                $"INSERT INTO tickets (id, owner) VALUES {values} RETURNING owner, no");
        }

        ExecuteNonSQLResult[] results = await Task.WhenAll(Enumerable.Range(0, writers).Select(Writer));

        List<long> allNumbers = new();
        for (int w = 0; w < writers; w++)
        {
            ExecuteNonSQLResult result = results[w];
            Assert.AreEqual(rowsEach, result.ReturningRows!.Count);

            foreach (QueryResultRow row in result.ReturningRows)
            {
                Assert.AreEqual($"w{w}", row.Row[result.ReturningColumns![0].RowKey].StrValue);
                allNumbers.Add(row.Row[result.ReturningColumns![1].RowKey].LongValue);
            }
        }

        Assert.AreEqual(writers * rowsEach, allNumbers.Distinct().Count());
        Assert.AreEqual(writers * rowsEach, (await QueryCommitted(executor, database, dbname, "SELECT id FROM tickets")).Count);
    }
}
