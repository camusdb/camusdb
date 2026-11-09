
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A correlated subquery under a join runs its body once for each distinct set of outer values, not once for
/// each joined row, and gives the same rows as before. Each expected result was measured on PostgreSQL 16.11
/// with the same rows. The counts come from <see cref="CorrelatedRunProbe"/>, a test-only counter.
///
/// <para>The tables: <c>a(id, k, s)</c> = (1, 1, 'x'), (2, 1, 'X'), (3, 2, 'y'), (4, 2, 'y'), (5, 3, 'z'),
/// (6, NULL, 'z'); <c>b(id, k)</c> = (1, 1), (2, 3). No column of <c>b</c> but its key has an index, so a
/// predicate on <c>b.k</c> scans <c>b</c>. The outer values of <c>a.k</c> are 1, 2, 3 and NULL: 4 distinct
/// values over 6 rows, and over 36 rows of <c>a AS a0 CROSS JOIN a AS a2</c>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCorrelatedSubqueryResultCache : BaseTest
{
    private const string Joined = "FROM a AS a0 CROSS JOIN a AS a2";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE a (id INT64 PRIMARY KEY, k INT64, s STRING)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO a (id, k, s) VALUES (1, 1, 'x'), (2, 1, 'X'), (3, 2, 'y'), (4, 2, 'y'), (5, 3, 'z'), (6, NULL, 'z')");
        await Commit(database, executor, dbname, "CREATE TABLE b (id INT64 PRIMARY KEY, k INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO b (id, k) VALUES (1, 1), (2, 3)");

        return (dbname, database, executor);
    }

    private static async Task<int> Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        int changed = 0;

        if (ddl)
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            changed = (await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null))).ModifiedRows;

        await database.Transactions.CommitAsync(tx);
        return changed;
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        _ => value.StrValue ?? "",
    };

    /// <summary>Each row as its columns joined with <c>|</c>, the rows sorted and joined with <c> ; </c>.</summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return string.Join(" ; ", rows
                .Select(r => string.Join("|", r.Row.Values.Select(Render)))
                .OrderBy(r => r, StringComparer.Ordinal));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    /// <summary>Runs <paramref name="sql"/> and returns its rows and the counts of body runs and kept results.</summary>
    private static async Task<(string Rows, int Runs, int Hits)> Counted(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            string rows = await Query(database, executor, dbname, sql);
            return (rows, probe.Runs, probe.Hits);
        }
    }

    [Test]
    public async Task NotExists_ScanForm_UnderAJoin_RunsOnceForEachOuterValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string rows, int runs, int hits) = await Counted(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a0.k) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("3|6 ; 4|6 ; 6|6"));
        Assert.That(runs, Is.EqualTo(4), "one run for each of the values 1, 2, 3 and NULL");
        Assert.That(hits, Is.EqualTo(32), "the other 32 joined rows reuse a kept answer");
    }

    [Test]
    public async Task Exists_BodyForm_UnderAJoin_RunsOnceForEachOuterValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // GROUP BY and HAVING need the whole pipeline, so the body is not the fast form.
        (string rows, int runs, int hits) = await Counted(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE EXISTS (SELECT b.k FROM b WHERE b.k = a0.k GROUP BY b.k HAVING COUNT(*) > 0) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("1|6 ; 2|6 ; 5|6"));
        Assert.That(runs, Is.EqualTo(4));
        Assert.That(hits, Is.EqualTo(32));
    }

    [Test]
    public async Task ScalarSubquery_InTheSelectList_RunsOnceForEachOuterValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string rows, int runs, int hits) = await Counted(database, executor, dbname,
            $"SELECT a0.id, a2.id, (SELECT MAX(b.id) FROM b WHERE b.k = a0.k) AS m {Joined} WHERE a2.id = 1");

        Assert.That(rows, Is.EqualTo("1|1|1 ; 2|1|1 ; 3|1| ; 4|1| ; 5|1|2 ; 6|1|"));
        Assert.That(runs, Is.EqualTo(4));
        Assert.That(hits, Is.EqualTo(2));
    }

    [Test]
    public async Task InSubquery_KeepsTheList_AndTestsEachRowWithItsOwnValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // The key is a0.k, and the tested value a2.id changes in each joined row.
        (string rows, int runs, int hits) = await Counted(database, executor, dbname,
            $"SELECT a0.id, a2.id {Joined} WHERE a2.id IN (SELECT b.id FROM b WHERE b.k = a0.k)");

        Assert.That(rows, Is.EqualTo("1|1 ; 2|1 ; 5|2"));
        Assert.That(runs, Is.EqualTo(4));
        Assert.That(hits, Is.EqualTo(32));
    }

    [Test]
    public async Task NotInSubquery_KeepsTheList_AndTestsEachRowWithItsOwnValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string rows, int runs, _) = await Counted(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE a2.id NOT IN (SELECT b.id FROM b WHERE b.k = a0.k) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("1|5 ; 2|5 ; 3|6 ; 4|6 ; 5|5 ; 6|6"));
        Assert.That(runs, Is.EqualTo(4));
    }

    [Test]
    public async Task QuantifiedSubquery_KeepsTheValueSet_AndTestsEachRowWithItsOwnValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string rows, int runs, _) = await Counted(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE a2.id > ALL (SELECT b.id FROM b WHERE b.k = a0.k) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("1|5 ; 2|5 ; 3|6 ; 4|6 ; 5|4 ; 6|6"));
        Assert.That(runs, Is.EqualTo(4));
    }

    [Test]
    public async Task VolatileBody_RunsForEachRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string rows, int runs, int hits) = await Counted(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a0.k AND random() >= 0) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("3|6 ; 4|6 ; 6|6"));
        Assert.That(runs, Is.EqualTo(36), "random() can give another value in each run, so no answer is kept");
        Assert.That(hits, Is.EqualTo(0));
    }

    [Test]
    public async Task IndexSeekForm_RunsForEachRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // b.id is the key, so each run is one seek, and the fast form keeps no answer.
        (string rows, int runs, int hits) = await Counted(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a0.k) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("1|6 ; 2|6 ; 3|6 ; 4|6"));
        Assert.That(runs, Is.EqualTo(36));
        Assert.That(hits, Is.EqualTo(0));
    }

    [Test]
    public async Task KeyIsExact_StringsThatDifferInCase_AreTwoKeys()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // 'x' and 'X' are two keys, so each row gets its own value back.
        (string rows, int runs, _) = await Counted(database, executor, dbname,
            "SELECT a0.id, (SELECT a0.s || '!' FROM b WHERE b.id = 1) AS v FROM a AS a0");

        Assert.That(rows, Is.EqualTo("1|x! ; 2|X! ; 3|y! ; 4|y! ; 5|z! ; 6|z!"));
        Assert.That(runs, Is.EqualTo(4), "the values x, X, y and z");
    }

    [Test]
    public async Task Update_WhereNotExists_RunsOnceForEachOuterValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        int changed;
        int runs;

        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            changed = await Commit(database, executor, dbname, "UPDATE a SET s = 'w' WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)");
            runs = probe.Runs;
        }

        Assert.That(changed, Is.EqualTo(3));
        Assert.That(runs, Is.EqualTo(4));
        Assert.That(await Query(database, executor, dbname, "SELECT id, s FROM a"), Is.EqualTo("1|x ; 2|X ; 3|w ; 4|w ; 5|z ; 6|w"));
    }

    [Test]
    public async Task NestedSubquery_ThatReadsTheOuterQuery_GivesThePostgreSqlRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // The inner EXISTS reads a0.k, a name two levels up, so the middle body binds again for each row.
        string rows = await Query(database, executor, dbname,
            $"SELECT a0.id, COUNT(*) {Joined} WHERE EXISTS (SELECT 1 FROM b WHERE b.id <= a2.id " +
            "AND EXISTS (SELECT 1 FROM b AS b2 WHERE b2.id = b.id AND b2.k = a0.k)) GROUP BY a0.id");

        Assert.That(rows, Is.EqualTo("1|6 ; 2|6 ; 5|5"));
    }
}
