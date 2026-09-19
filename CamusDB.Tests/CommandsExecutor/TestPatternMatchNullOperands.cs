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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A NULL subject or pattern makes <c>LIKE</c>, <c>ILIKE</c> and the four regex operators UNKNOWN,
/// the same as a comparison. Before, the evaluator type-checked for two strings first and raised
/// "No matching signature for operator LIKE", so any pattern predicate over a nullable column failed
/// as soon as the scan reached a NULL row. Every query runs through the SQL entry point.
/// </summary>
public sealed class TestPatternMatchNullOperands : SharedNodeBaseTest
{
    private static async Task Exec(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        if (ddl)
            await executor.ExecuteDDLSQL(ticket);
        else
            await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<List<long>> Ids(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null) =>
        (await Query(database, executor, dbname, sql, parameters)).Select(r => r.Row["id"].LongValue).OrderBy(x => x).ToList();

    /// <summary>names: 1 'apple', 2 NULL, 3 'Avocado', 4 'banana', 5 NULL.</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SetupAsync(bool indexed)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE fruits (id int64 primary key, name string(32))", ddl: true);
        if (indexed)
            await Exec(database, executor, dbname, "CREATE INDEX fruits_name_idx ON fruits (name)", ddl: true);

        await Exec(database, executor, dbname,
            "INSERT INTO fruits (id, name) VALUES (1, 'apple'), (2, NULL), (3, 'Avocado'), (4, 'banana'), (5, NULL)",
            ddl: false);

        return (dbname, database, executor);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task EveryOperator_OverNullableColumn_DropsNullRows(bool indexed)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(indexed);

        CollectionAssert.AreEqual(new[] { 1L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name LIKE 'a%'"));
        CollectionAssert.AreEqual(new[] { 1L, 3L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name ILIKE 'a%'"));
        CollectionAssert.AreEqual(new[] { 1L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name ~ '^a'"));
        CollectionAssert.AreEqual(new[] { 1L, 3L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name ~* '^a'"));

        // The negated regex forms are UNKNOWN over NULL too, not TRUE: rows 2 and 5 must not appear.
        CollectionAssert.AreEqual(new[] { 3L, 4L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name !~ '^a'"));
        CollectionAssert.AreEqual(new[] { 4L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name !~* '^a'"));
    }

    [Test]
    public async Task NotOverLike_DropsNullRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(indexed: false);

        CollectionAssert.AreEqual(new[] { 3L, 4L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE NOT (name LIKE 'a%')"));
        CollectionAssert.AreEqual(new[] { 4L }, await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE NOT (name ILIKE 'a%')"));
    }

    [Test]
    public async Task Projection_ReturnsNullForNullSubject()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(indexed: false);

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT id, name LIKE 'a%' AS m FROM fruits ORDER BY id");

        Assert.AreEqual(5, rows.Count);
        Assert.AreEqual(ColumnType.Bool, rows[0].Row["m"].Type);
        Assert.IsTrue(rows[0].Row["m"].BoolValue);
        Assert.AreEqual(ColumnType.Null, rows[1].Row["m"].Type);
        Assert.IsFalse(rows[2].Row["m"].BoolValue);
        Assert.AreEqual(ColumnType.Null, rows[4].Row["m"].Type);
    }

    [Test]
    public async Task NullPattern_IsUnknown()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(indexed: false);

        CollectionAssert.IsEmpty(await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name LIKE NULL"));
        CollectionAssert.IsEmpty(await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE NOT (name LIKE NULL)"));
        CollectionAssert.IsEmpty(await Ids(database, executor, dbname, "SELECT id FROM fruits WHERE name ~ NULL"));

        CollectionAssert.IsEmpty(await Ids(database, executor, dbname,
            "SELECT id FROM fruits WHERE name LIKE @p",
            new Dictionary<string, ColumnValue> { { "@p", ColumnValue.Null } }));
    }

    [Test]
    public async Task AndWithUnknownLeftOperand_StillEvaluatesLikeWithoutError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE t (id int64 primary key, s string(16), n int64)", ddl: true);
        await Exec(database, executor, dbname, "INSERT INTO t (id, s, n) VALUES (1, 'abc', 1), (2, NULL, NULL), (3, 'xyz', 3)", ddl: false);

        // Row 2: n > 0 is UNKNOWN, not FALSE, so AND evaluates the LIKE over the NULL s.
        CollectionAssert.AreEqual(new[] { 1L }, await Ids(database, executor, dbname, "SELECT id FROM t WHERE n > 0 AND s LIKE 'a%'"));
    }

    [Test]
    public async Task NonStringOperand_IsStillATypeError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(indexed: false);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(database, executor, dbname, "SELECT id FROM fruits WHERE id LIKE 'a%'"))!;

        StringAssert.StartsWith("No matching signature for operator LIKE", ex.Message);
    }

    [Test]
    public async Task CheckConstraintWithLike_AcceptsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE codes (id int64 primary key, code string(16))", ddl: true);
        await Exec(database, executor, dbname, "ALTER TABLE codes ADD CONSTRAINT chk_code CHECK (code LIKE 'C-%')", ddl: true);

        await Exec(database, executor, dbname, "INSERT INTO codes (id, code) VALUES (1, 'C-1'), (2, NULL)", ddl: false);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(database, executor, dbname, "INSERT INTO codes (id, code) VALUES (3, 'X-1')", ddl: false));

        CollectionAssert.AreEqual(new[] { 1L, 2L }, await Ids(database, executor, dbname, "SELECT id FROM codes"));
    }
}
