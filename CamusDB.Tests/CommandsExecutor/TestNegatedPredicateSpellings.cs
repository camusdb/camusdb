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
/// <c>x NOT BETWEEN a AND b</c>, <c>x NOT LIKE p</c> and <c>x NOT ILIKE p</c> parse as NOT over the
/// positive predicate, so each one returns exactly the rows of its <c>NOT (...)</c> form — NULL rows
/// included, which UNKNOWN drops. Before, all three were syntax errors ("expecting TIN").
/// </summary>
public sealed class TestNegatedPredicateSpellings : SharedNodeBaseTest
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

    private static async Task<List<QueryResultRow>> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<List<long>> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        (await Query(database, executor, dbname, sql)).Select(r => r.Row["id"].LongValue).OrderBy(x => x).ToList();

    /// <summary>items: (id, year, name, tag) with NULLs in year and name.</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE items (id int64 primary key, year int64, name string(32), tag int64)", ddl: true);
        await Exec(database, executor, dbname, "CREATE INDEX items_year_idx ON items (year)", ddl: true);
        await Exec(database, executor, dbname,
            "INSERT INTO items (id, year, name, tag) VALUES " +
            "(1, 2019, 'alpha', 1), (2, 2020, 'Alpine', 2), (3, 2021, 'beta', 2), " +
            "(4, 2022, NULL, 1), (5, NULL, 'amber', 3), (6, 2023, 'Gamma', 2)",
            ddl: false);

        return (dbname, database, executor);
    }

    [Test]
    public async Task EachSpelling_MatchesItsParenthesisedNotForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        (string Short, string Long, long[] Expected)[] cases =
        [
            ("year NOT BETWEEN 2020 AND 2021", "NOT (year BETWEEN 2020 AND 2021)", [1, 4, 6]),
            ("name NOT LIKE 'a%'", "NOT (name LIKE 'a%')", [2, 3, 6]),
            ("name NOT ILIKE 'a%'", "NOT (name ILIKE 'a%')", [3, 6]),
            ("year NOT BETWEEN tag + 2018 AND 2022", "NOT (year BETWEEN tag + 2018 AND 2022)", [6]),
            ("year NOT BETWEEN -1 AND - 1", "NOT (year BETWEEN -1 AND -1)", [1, 2, 3, 4, 6]),
        ];

        foreach ((string shortForm, string longForm, long[] expected) in cases)
        {
            List<long> viaShort = await Ids(database, executor, dbname, "SELECT id FROM items WHERE " + shortForm);
            List<long> viaLong = await Ids(database, executor, dbname, "SELECT id FROM items WHERE " + longForm);

            CollectionAssert.AreEqual(expected, viaShort, shortForm);
            CollectionAssert.AreEqual(viaLong, viaShort, shortForm);
        }
    }

    [Test]
    public async Task NotBetween_BindsTighterThanAnd()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // (year NOT BETWEEN 2020 AND 2021) AND tag = 1 — not year NOT BETWEEN 2020 AND (2021 AND tag = 1).
        CollectionAssert.AreEqual(new[] { 1L, 4L }, await Ids(database, executor, dbname,
            "SELECT id FROM items WHERE year NOT BETWEEN 2020 AND 2021 AND tag = 1"));

        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L, 4L, 6L }, await Ids(database, executor, dbname,
            "SELECT id FROM items WHERE name NOT LIKE 'a%' AND tag = 2 OR tag = 1"));
    }

    [Test]
    public async Task InHaving()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT tag, COUNT(*) AS c FROM items GROUP BY tag HAVING COUNT(*) NOT BETWEEN 2 AND 2 ORDER BY tag");

        CollectionAssert.AreEqual(new[] { 2L, 3L }, rows.Select(r => r.Row["tag"].LongValue).ToList());
    }

    [Test]
    public async Task InCheckConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE codes (id int64 primary key, code string(16), n int64)", ddl: true);
        await Exec(database, executor, dbname, "ALTER TABLE codes ADD CONSTRAINT chk_code CHECK (code NOT LIKE 'tmp%')", ddl: true);
        await Exec(database, executor, dbname, "ALTER TABLE codes ADD CONSTRAINT chk_n CHECK (n NOT BETWEEN 10 AND 20)", ddl: true);

        await Exec(database, executor, dbname, "INSERT INTO codes (id, code, n) VALUES (1, 'final', 5), (2, NULL, NULL)", ddl: false);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(database, executor, dbname, "INSERT INTO codes (id, code, n) VALUES (3, 'tmp1', 5)", ddl: false));
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(database, executor, dbname, "INSERT INTO codes (id, code, n) VALUES (4, 'x', 15)", ddl: false));

        CollectionAssert.AreEqual(new[] { 1L, 2L }, await Ids(database, executor, dbname, "SELECT id FROM codes"));
    }

    [Test]
    public async Task InViewBody_QueriesAndRendersAsNotOverThePositiveForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await Exec(database, executor, dbname,
            "CREATE VIEW odd_items AS SELECT id FROM items WHERE year NOT BETWEEN 2020 AND 2021 AND name NOT ILIKE 'g%'", ddl: true);

        CollectionAssert.AreEqual(new[] { 1L }, await Ids(database, executor, dbname, "SELECT id FROM odd_items"));

        string shown = (await Query(database, executor, dbname, "SHOW CREATE VIEW odd_items"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("NOT (year BETWEEN 2020 AND 2021)", shown);
        StringAssert.Contains("NOT (name ILIKE 'g%')", shown);
    }

    [Test]
    public async Task NotLike_OverNullableColumnDoesNotThrow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // Row 4 has a NULL name: UNKNOWN, dropped, no type error.
        CollectionAssert.AreEqual(new[] { 3L, 6L }, await Ids(database, executor, dbname,
            "SELECT id FROM items WHERE name NOT ILIKE 'A%'"));
    }
}
