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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A subquery can stand in every expression position, at any depth. Each uncorrelated subquery is
/// folded into its value before the statement reads its first row; a correlated EXISTS is evaluated
/// per row in the WHERE of a SELECT, at any depth of the predicate, and refused with
/// <see cref="CamusDBErrorCodes.FeatureNotSupported"/> everywhere else.
///
/// <para>Before this held, a subquery outside a WHERE conjunct of a SELECT reached the per-row
/// evaluator unresolved and failed with the internal code <see cref="CamusDBErrorCodes.InvalidInternalOperation"/>
/// ("EXISTS subquery must be resolved before expression evaluation"): in a JOIN ON clause, a select
/// list, a HAVING, the WHERE of an UPDATE or DELETE, INSERT VALUES, and in a CASE's ELSE or a
/// BETWEEN bound of a SELECT WHERE, which the rewrite walk did not visit. A correlated EXISTS below a
/// comparison or an IS NULL test failed the same way, and one combined with another table's column in
/// a join conjunct was pushed to that table's scan, where its outer columns were missing. These tests
/// drive each position through the SQL entry point and check the rows, the written values and the
/// error codes.</para>
/// </summary>
public sealed class TestSubqueryPositions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 int64, c2 bool, c3 string(32))");
        await ExecDdl(database, executor, dbname, "CREATE TABLE t1 (c0 int64 PRIMARY KEY, c1 int64, c2 bool)");
        await ExecDdl(database, executor, dbname, "CREATE TABLE t2 (c0 int64 PRIMARY KEY, c3 int64)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO t0 (c0, c1, c2, c3) VALUES (1, 10, true, 'a'), (2, 20, false, 'b'), (3, NULL, NULL, NULL)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO t1 (c0, c1, c2) VALUES (1, 10, true), (2, 99, false)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO t2 (c0, c3) VALUES (1, 5)");

        return (dbname, database, executor);
    }

    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    /// <summary>The c0 values of the rows, sorted, so a result compares without an ORDER BY.</summary>
    private static async Task<List<long>> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, sql);
        return rows.Select(r => r.Row["c0"].LongValue).OrderBy(x => x).ToList();
    }

    /// <summary>The value of column <paramref name="column"/> of each row, in c0 order, as text ("NULL" for a NULL).</summary>
    private static async Task<List<string>> Column(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, string column)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, sql);
        return rows.OrderBy(r => r.Row["c0"].LongValue).Select(r => Text(r.Row[column])).ToList();
    }

    private static string Text(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        ColumnType.Integer64 => value.LongValue.ToString(),
        _ => value.ToString() ?? "",
    };

    private static CamusDBException AssertRefused(string expectedCode, string expectedMessagePart, AsyncTestDelegate action)
    {
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(action);
        Assert.IsNotNull(ex);
        Assert.AreEqual(expectedCode, ex!.Code, ex.Message);
        StringAssert.Contains(expectedMessagePart, ex.Message);
        return ex;
    }

    [Test]
    public async Task SelectWhere_UncorrelatedSubqueryInCaseElseAndBetweenBound_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The ELSE result of a CASE and the bounds of a BETWEEN live in child slots the walk once skipped.
        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, await Ids(database, executor, dbname,
            "SELECT c0 FROM t0 WHERE CASE WHEN c2 THEN true ELSE EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) END"));

        CollectionAssert.AreEqual(new List<long> { 1 }, await Ids(database, executor, dbname,
            "SELECT c0 FROM t0 WHERE CASE WHEN c2 THEN true ELSE c1 IN (SELECT c1 FROM t1) END"));

        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, await Ids(database, executor, dbname,
            "SELECT c0 FROM t0 WHERE CASE WHEN c2 THEN 1 ELSE (SELECT COUNT(*) FROM t2) END = 1"));

        CollectionAssert.AreEqual(new List<long> { 1, 2 }, await Ids(database, executor, dbname,
            "SELECT c0 FROM t0 WHERE c1 BETWEEN (SELECT MIN(c1) FROM t1) AND (SELECT MAX(c1) FROM t1)"));
    }

    [Test]
    public async Task SelectWhere_CorrelatedExistsBelowNonBooleanNode_IsEvaluatedPerRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string isNull = "SELECT a0.c0 FROM t0 AS a0 WHERE (EXISTS (SELECT 1 FROM t1 AS s WHERE s.c1 = a0.c1)) IS NULL";
        const string equalsTrue = "SELECT a0.c0 FROM t0 AS a0 WHERE (EXISTS (SELECT 1 FROM t1 AS s WHERE s.c1 = a0.c1)) = true";
        const string caseWhen = "SELECT a0.c0 FROM t0 AS a0 WHERE CASE WHEN EXISTS (SELECT 1 FROM t1 AS s WHERE s.c1 = a0.c1) THEN true ELSE false END";
        const string caseElse = "SELECT a0.c0 FROM t0 AS a0 WHERE CASE WHEN a0.c2 THEN false ELSE NOT EXISTS (SELECT 1 FROM t1 AS s WHERE s.c1 = a0.c1) END";
        const string compared = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c2 = EXISTS (SELECT 1 FROM t1 AS s WHERE s.c1 = a0.c1)";

        // Twice each: the per-row answers go into a copy of the predicate, and the parsed statement
        // is shared by the second execution, so it must come back unchanged.
        for (int round = 0; round < 2; round++)
        {
            CollectionAssert.AreEqual(new List<long>(), await Ids(database, executor, dbname, isNull), isNull);
            CollectionAssert.AreEqual(new List<long> { 1 }, await Ids(database, executor, dbname, equalsTrue), equalsTrue);
            CollectionAssert.AreEqual(new List<long> { 1 }, await Ids(database, executor, dbname, caseWhen), caseWhen);
            CollectionAssert.AreEqual(new List<long> { 2, 3 }, await Ids(database, executor, dbname, caseElse), caseElse);
            CollectionAssert.AreEqual(new List<long> { 1, 2 }, await Ids(database, executor, dbname, compared), compared);
        }
    }

    [Test]
    public async Task SelectWhere_CorrelatedExistsInAJoinConjunctWithAnotherAlias_IsEvaluatedAfterTheJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The conjunct names only a1 in the open, so the pushdown would send it to a1's scan, where
        // a0.c0 (read by the EXISTS) is not present.
        CollectionAssert.AreEqual(new List<long> { 1, 2 }, await Ids(database, executor, dbname,
            "SELECT a0.c0 FROM t0 AS a0 INNER JOIN t1 AS a1 ON a0.c0 = a1.c0 " +
            "WHERE (EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = a0.c0)) = (a1.c1 = 10)"));

        CollectionAssert.AreEqual(new List<long> { 1 }, await Ids(database, executor, dbname,
            "SELECT a0.c0 FROM t0 AS a0 INNER JOIN t1 AS a1 ON a0.c0 = a1.c0 " +
            "WHERE a1.c1 = 10 OR EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = a0.c0)"));

        CollectionAssert.AreEqual(new List<long> { 1, 2 }, await Ids(database, executor, dbname,
            "SELECT a0.c0 FROM t0 AS a0 INNER JOIN t1 AS a1 ON a0.c0 = a1.c0 " +
            "WHERE a1.c1 = 99 OR EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = a0.c0)"));

        CollectionAssert.AreEqual(new List<long> { 1 }, await Ids(database, executor, dbname,
            "SELECT a0.c0 FROM t0 AS a0 LEFT JOIN t1 AS a1 ON a0.c0 = a1.c0 " +
            "WHERE CASE WHEN a1.c1 IS NULL THEN EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = a0.c0) ELSE a1.c1 = 10 END"));
    }

    [Test]
    public async Task JoinOn_UncorrelatedSubquery_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> inner = await ExecQuery(database, executor, dbname,
            "SELECT a0.c0 AS l, a1.c0 AS r FROM t0 AS a0 INNER JOIN t1 AS a1 " +
            "ON (a0.c0 = a1.c0 AND NOT EXISTS (SELECT 1 FROM t2 AS s6 WHERE s6.c3 = 7))");
        CollectionAssert.AreEquivalent(new[] { "1:1", "2:2" }, inner.Select(r => Text(r.Row["l"]) + ":" + Text(r.Row["r"])));

        List<QueryResultRow> left = await ExecQuery(database, executor, dbname,
            "SELECT a0.c0 AS l, a1.c0 AS r FROM t0 AS a0 LEFT JOIN t1 AS a1 " +
            "ON (a0.c0 = a1.c0 AND a1.c1 IN (SELECT c1 FROM t1 WHERE c1 < 50))");
        CollectionAssert.AreEquivalent(new[] { "1:1", "2:NULL", "3:NULL" }, left.Select(r => Text(r.Row["l"]) + ":" + Text(r.Row["r"])));

        List<QueryResultRow> scalar = await ExecQuery(database, executor, dbname,
            "SELECT a0.c0 AS l, a1.c0 AS r FROM t0 AS a0 INNER JOIN t1 AS a1 " +
            "ON (a0.c0 = a1.c0 AND a1.c1 = (SELECT MIN(c1) FROM t1))");
        CollectionAssert.AreEquivalent(new[] { "1:1" }, scalar.Select(r => Text(r.Row["l"]) + ":" + Text(r.Row["r"])));

        // The EXISTS is true, so the ON is false for every pair and each right row is padded.
        List<QueryResultRow> right = await ExecQuery(database, executor, dbname,
            "SELECT a0.c0 AS l, a1.c0 AS r FROM t0 AS a0 RIGHT JOIN t1 AS a1 " +
            "ON (a1.c2 AND (NOT EXISTS (SELECT 1 FROM t2 AS s6 WHERE s6.c3 = s6.c3)))");
        CollectionAssert.AreEquivalent(new[] { "NULL:1", "NULL:2" }, right.Select(r => Text(r.Row["l"]) + ":" + Text(r.Row["r"])));
    }

    [Test]
    public async Task JoinOn_CorrelatedExists_IsRefusedWithASpecificCode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        AssertRefused(CamusDBErrorCodes.FeatureNotSupported, "JOIN ON", async () =>
            await ExecQuery(database, executor, dbname,
                "SELECT a0.c0 FROM t0 AS a0 INNER JOIN t1 AS a1 " +
                "ON (a0.c0 = a1.c0 AND EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = a0.c0))"));
    }

    [Test]
    public async Task SelectList_UncorrelatedSubquery_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        CollectionAssert.AreEqual(new[] { "true", "true", "true" }, await Column(database, executor, dbname,
            "SELECT c0, EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) AS f FROM t0", "f"));

        CollectionAssert.AreEqual(new[] { "1", "1", "1" }, await Column(database, executor, dbname,
            "SELECT c0, CASE WHEN EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) THEN 1 ELSE 0 END AS f FROM t0", "f"));

        // Three-valued: a NULL c1 is neither in nor not in the list.
        CollectionAssert.AreEqual(new[] { "true", "false", "NULL" }, await Column(database, executor, dbname,
            "SELECT c0, c1 IN (SELECT c1 FROM t1) AS f FROM t0", "f"));

        CollectionAssert.AreEqual(new[] { "false", "true", "NULL" }, await Column(database, executor, dbname,
            "SELECT c0, c1 NOT IN (SELECT c1 FROM t1) AS f FROM t0", "f"));

        CollectionAssert.AreEqual(new[] { "true", "false", "NULL" }, await Column(database, executor, dbname,
            "SELECT c0, c1 = ANY (SELECT c1 FROM t1) AS f FROM t0", "f"));

        CollectionAssert.AreEqual(new[] { "1", "1", "1" }, await Column(database, executor, dbname,
            "SELECT c0, (SELECT COUNT(*) FROM t2) AS f FROM t0", "f"));

        CollectionAssert.AreEqual(new[] { "15", "25", "NULL" }, await Column(database, executor, dbname,
            "SELECT c0, (SELECT MAX(c3) FROM t2) + c1 AS f FROM t0", "f"));

        // The same folding serves a derived table's select list and a view body.
        CollectionAssert.AreEqual(new[] { "true", "true", "true" }, await Column(database, executor, dbname,
            "SELECT d.c0, d.f FROM (SELECT c0, EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) AS f FROM t0) AS d", "f"));

        await ExecDdl(database, executor, dbname, "CREATE VIEW v1 AS SELECT c0, EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) AS f FROM t0");
        CollectionAssert.AreEqual(new[] { "true", "true", "true" }, await Column(database, executor, dbname, "SELECT c0, f FROM v1", "f"));

        // EXPLAIN prepares the same plan, so it folds the subquery without failing.
        List<QueryResultRow> plan = await ExecQuery(database, executor, dbname,
            "EXPLAIN SELECT c0, EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) AS f FROM t0");
        Assert.IsTrue(plan.Count > 0);
    }

    [Test]
    public async Task SelectList_CorrelatedExists_IsRefusedWithASpecificCode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        AssertRefused(CamusDBErrorCodes.FeatureNotSupported, "select list", async () =>
            await ExecQuery(database, executor, dbname,
                "SELECT a0.c0, EXISTS (SELECT 1 FROM t1 AS s WHERE s.c1 = a0.c1) AS f FROM t0 AS a0"));
    }

    [Test]
    public async Task Having_UncorrelatedSubquery_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> all = await ExecQuery(database, executor, dbname,
            "SELECT c2, COUNT(*) AS n FROM t0 GROUP BY c2 HAVING EXISTS (SELECT 1 FROM t2 WHERE c3 = 5)");
        Assert.AreEqual(3, all.Count);

        List<QueryResultRow> none = await ExecQuery(database, executor, dbname,
            "SELECT c2, COUNT(*) AS n FROM t0 GROUP BY c2 HAVING COUNT(*) > (SELECT COUNT(*) FROM t2)");
        Assert.AreEqual(0, none.Count);

        List<QueryResultRow> each = await ExecQuery(database, executor, dbname,
            "SELECT c2, COUNT(*) AS n FROM t0 GROUP BY c2 HAVING COUNT(*) >= (SELECT COUNT(*) FROM t2)");
        Assert.AreEqual(3, each.Count);
    }

    [Test]
    public async Task Delete_ExistsSubquery_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.AreEqual(0, await ExecNonQuery(database, executor, dbname,
            "DELETE FROM t2 WHERE EXISTS (SELECT 1 FROM t0 AS s3 WHERE CAST(NULL AS bool))"));
        CollectionAssert.AreEqual(new List<long> { 1 }, await Ids(database, executor, dbname, "SELECT c0 FROM t2"));

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "DELETE FROM t2 WHERE NOT EXISTS (SELECT 1 FROM t0 AS s3 WHERE s3.c0 = 999)"));
        CollectionAssert.AreEqual(new List<long>(), await Ids(database, executor, dbname, "SELECT c0 FROM t2"));
    }

    /// <summary>The locate scan evaluates a correlated EXISTS for each row. PostgreSQL 16.11 deletes the one row.</summary>
    [Test]
    public async Task Delete_CorrelatedExists_IsEvaluatedForEachRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "DELETE FROM t2 WHERE EXISTS (SELECT 1 FROM t0 AS s3 WHERE s3.c0 = t2.c0)"));

        CollectionAssert.AreEqual(new List<long>(), await Ids(database, executor, dbname, "SELECT c0 FROM t2"));
    }

    [Test]
    public async Task Update_ExistsSubqueryInWhereAndSet_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "UPDATE t1 SET c1 = CASE WHEN EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) THEN 7 ELSE 0 END WHERE c0 = 2"));
        CollectionAssert.AreEqual(new[] { "10", "7" }, await Column(database, executor, dbname, "SELECT c0, c1 FROM t1", "c1"));

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "UPDATE t1 SET c2 = NOT EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) WHERE c0 = 2"));
        CollectionAssert.AreEqual(new[] { "true", "false" }, await Column(database, executor, dbname, "SELECT c0, c2 FROM t1", "c2"));

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "UPDATE t1 SET c1 = 5 WHERE EXISTS (SELECT 1 FROM t2 WHERE c3 = 5) AND c0 = 1"));
        CollectionAssert.AreEqual(new[] { "5", "7" }, await Column(database, executor, dbname, "SELECT c0, c1 FROM t1", "c1"));

        // The IN sits in a CASE's ELSE: row 2 (c2 false, c1 7) is the one the membership test accepts.
        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "UPDATE t1 SET c1 = 6 WHERE CASE WHEN c2 THEN false ELSE c1 IN (SELECT c1 FROM t1 WHERE c1 = 7) END"));
        CollectionAssert.AreEqual(new[] { "5", "6" }, await Column(database, executor, dbname, "SELECT c0, c1 FROM t1", "c1"));
    }

    /// <summary>
    /// A correlated EXISTS in the WHERE of an UPDATE is evaluated for each row; PostgreSQL 16.11 updates
    /// row 1. In a SET value it is refused, and the refused statement writes nothing.
    /// </summary>
    [Test]
    public async Task Update_CorrelatedExists_RunsInTheWhere_AndIsRefusedInASetValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        AssertRefused(CamusDBErrorCodes.FeatureNotSupported, "a SET value of an UPDATE", async () =>
            await ExecNonQuery(database, executor, dbname,
                "UPDATE t1 SET c2 = EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = t1.c0) WHERE c0 = 1"));

        CollectionAssert.AreEqual(new[] { "10", "99" }, await Column(database, executor, dbname, "SELECT c0, c1 FROM t1", "c1"));

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "UPDATE t1 SET c1 = 5 WHERE EXISTS (SELECT 1 FROM t2 AS s WHERE s.c0 = t1.c0)"));

        CollectionAssert.AreEqual(new[] { "5", "99" }, await Column(database, executor, dbname, "SELECT c0, c1 FROM t1", "c1"));
    }

    [Test]
    public async Task Insert_SubqueryInValues_IsFolded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.AreEqual(2, await ExecNonQuery(database, executor, dbname,
            "INSERT INTO t2 (c0, c3) VALUES (50, (SELECT COUNT(*) FROM t0)), " +
            "(51, CASE WHEN EXISTS (SELECT 1 FROM t0 WHERE c0 = 999) THEN 1 ELSE 0 END)"));

        CollectionAssert.AreEqual(new[] { "5", "3", "0" }, await Column(database, executor, dbname, "SELECT c0, c3 FROM t2", "c3"));
    }

    [Test]
    public async Task Insert_ManyRowsWithOneSubquery_WalksTheRowSpineWithoutRecursion()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The row spine of a multi-row INSERT is the one tree the parser leaves unbounded; a recursive
        // scan of it overflowed the stack. Subqueries in the first, a middle and the last row.
        const int rows = 3000;
        System.Text.StringBuilder sql = new("INSERT INTO t2 (c0, c3) VALUES ");
        for (int i = 0; i < rows; i++)
        {
            if (i > 0)
                sql.Append(", ");
            long c0 = 100 + i;
            string c3 = i == 0 || i == rows / 2 || i == rows - 1 ? "(SELECT COUNT(*) FROM t0)" : "7";
            sql.Append('(').Append(c0).Append(", ").Append(c3).Append(')');
        }

        Assert.AreEqual(rows, await ExecNonQuery(database, executor, dbname, sql.ToString()));

        List<QueryResultRow> result = await ExecQuery(database, executor, dbname, "SELECT c0, c3 FROM t2 WHERE c3 = 3");
        CollectionAssert.AreEquivalent(new[] { 100L, 100L + rows / 2, 100L + rows - 1 }, result.Select(r => r.Row["c0"].LongValue));

        List<QueryResultRow> plain = await ExecQuery(database, executor, dbname, "SELECT COUNT(*) AS n FROM t2 WHERE c3 = 7");
        Assert.AreEqual(rows - 3, plain[0].Row["n"].LongValue);
    }

    [Test]
    public async Task Upsert_ExistsInDoUpdate_IsFoldedWhenUncorrelatedAndRefusedWhenCorrelated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.AreEqual(1, await ExecNonQuery(database, executor, dbname,
            "INSERT INTO t2 (c0, c3) VALUES (1, 7) ON CONFLICT (c0) DO UPDATE SET c3 = CASE WHEN EXISTS (SELECT 1 FROM t0) THEN 9 ELSE 0 END"));
        CollectionAssert.AreEqual(new[] { "9" }, await Column(database, executor, dbname, "SELECT c0, c3 FROM t2", "c3"));

        AssertRefused(CamusDBErrorCodes.FeatureNotSupported, "ON CONFLICT DO UPDATE", async () =>
            await ExecNonQuery(database, executor, dbname,
                "INSERT INTO t2 (c0, c3) VALUES (1, 7) ON CONFLICT (c0) DO UPDATE SET c3 = CASE WHEN EXISTS (SELECT 1 FROM t0 AS s WHERE s.c0 = t2.c0) THEN 1 ELSE 0 END"));
        CollectionAssert.AreEqual(new[] { "9" }, await Column(database, executor, dbname, "SELECT c0, c3 FROM t2", "c3"));
    }

    [Test]
    public async Task GroupBy_Subquery_IsRefusedWithASpecificCode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        AssertRefused(CamusDBErrorCodes.InvalidInput, "GROUP BY does not support subqueries", async () =>
            await ExecQuery(database, executor, dbname, "SELECT COUNT(*) AS n FROM t0 GROUP BY (SELECT 1)"));
    }
}
