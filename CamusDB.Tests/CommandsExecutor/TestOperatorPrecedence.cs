
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The precedence of the expression operators, through the whole engine. Each expected result in a table was
/// measured on PostgreSQL 16.11 in the C locale with the same rows, so a table holds 749 statements that
/// the two engines agree on. A result renders as <c>t</c> or <c>f</c>, a NULL renders as an empty cell, cells
/// join with <c>|</c> and rows join with <c> ; </c>. An expected value that starts with <c>CADB</c> is the error
/// code of CamusDB, where PostgreSQL also gave an error.
///
/// <para>One test runs a whole table in one database, and a failure names each statement that differs. A test
/// for each statement costs about 5 minutes of suite time, because each test builds a database.</para>
///
/// <para>The SQL fuzzer cannot find this class of defect, because its printer puts parentheses around every
/// operator. These tables and <c>TestSQLParserOperatorPrecedence</c> are the only protection.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOperatorPrecedence : BaseTest
{
    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString("G17", CultureInfo.InvariantCulture),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else if (sql.StartsWith("REFRESH", StringComparison.Ordinal) || sql.StartsWith("INSERT", StringComparison.Ordinal))
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            throw new ArgumentException(sql);

        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>The error code of a statement, or an empty string when it succeeds and commits.</summary>
    private static async Task<string> TryCommit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        try
        {
            await Commit(database, executor, dbname, sql);
            return "";
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
    }

    /// <summary>Runs a statement and rolls back, so a table of statements reads the same rows from start to end.</summary>
    private static async Task<string> Run(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("UPDATE", StringComparison.Ordinal) || sql.StartsWith("DELETE", StringComparison.Ordinal))
                return (await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null))).ModifiedRows.ToString(CultureInfo.InvariantCulture);

            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return string.Join(" ; ", rows.Select(row => string.Join("|", row.Row.Values.Select(Render))));
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, s STRING, b BOOL, n INT64)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE bb (id INT64 PRIMARY KEY, a BOOL, b BOOL)");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s, b, n) VALUES (1, 10, 'a', TRUE, NULL), (2, 20, 'b', FALSE, 5), (3, NULL, NULL, NULL, 7)");
        await Commit(database, executor, dbname, "INSERT INTO bb (id, a, b) VALUES (1, TRUE, TRUE), (2, TRUE, FALSE), (3, TRUE, NULL), (4, FALSE, TRUE), (5, FALSE, FALSE), (6, FALSE, NULL), (7, NULL, TRUE), (8, NULL, FALSE), (9, NULL, NULL)");

        return (dbname, database, executor);
    }

    private async Task Check((string Sql, string Expected)[] rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        List<string> differences = [];

        foreach ((string sql, string expected) in rows)
        {
            string actual = await Run(database, executor, dbname, sql);
            if (actual != expected)
                differences.Add($"{sql}: expected '{expected}', got '{actual}'");
        }

        Assert.That(differences, Is.Empty);
    }

    /// <summary>An IS test reads the whole value expression before it.</summary>
    [Test]
    public Task IsTest_OverAValueOperator() => Check(
    [
        ("SELECT 1 + 1 IS NULL", "f"),
        ("SELECT 1 + 1 IS NOT NULL", "t"),
        ("SELECT 2 - 1 IS NULL", "f"),
        ("SELECT 2 - 1 IS NOT NULL", "t"),
        ("SELECT 2 * 3 IS NULL", "f"),
        ("SELECT 2 * 3 IS NOT NULL", "t"),
        ("SELECT 6 / 2 IS NULL", "f"),
        ("SELECT 6 / 2 IS NOT NULL", "t"),
        ("SELECT 7 % 2 IS NULL", "f"),
        ("SELECT 7 % 2 IS NOT NULL", "t"),
        ("SELECT 2 ^ 2 IS NULL", "f"),
        ("SELECT 2 ^ 2 IS NOT NULL", "t"),
        ("SELECT 'a' || 'b' IS NULL", "f"),
        ("SELECT 'a' || 'b' IS NOT NULL", "t"),
        ("SELECT 1 & 3 IS NULL", "f"),
        ("SELECT 1 & 3 IS NOT NULL", "t"),
        ("SELECT 1 | 2 IS NULL", "f"),
        ("SELECT 1 | 2 IS NOT NULL", "t"),
        ("SELECT 1 # 3 IS NULL", "f"),
        ("SELECT 1 # 3 IS NOT NULL", "t"),
        ("SELECT 1 << 1 IS NULL", "f"),
        ("SELECT 1 << 1 IS NOT NULL", "t"),
        ("SELECT 4 >> 1 IS NULL", "f"),
        ("SELECT 4 >> 1 IS NOT NULL", "t"),
        ("SELECT - 1 IS NULL", "f"),
        ("SELECT - 1 IS NOT NULL", "t"),
        ("SELECT -(1) IS NULL", "f"),
        ("SELECT -(1) IS NOT NULL", "t"),
        ("SELECT ~1 IS NULL", "f"),
        ("SELECT ~1 IS NOT NULL", "t"),
        ("SELECT 1 + 1 IS TRUE", "CADB0400"),
        ("SELECT 'a' || 'b' IS TRUE", "CADB0400"),
        ("SELECT 1 + 1 IS NOT TRUE", "CADB0400"),
        ("SELECT 'a' || 'b' IS NOT TRUE", "CADB0400"),
        ("SELECT 1 + 1 IS FALSE", "CADB0400"),
        ("SELECT 'a' || 'b' IS FALSE", "CADB0400"),
        ("SELECT 1 + 1 IS NOT FALSE", "CADB0400"),
        ("SELECT 'a' || 'b' IS NOT FALSE", "CADB0400"),
        ("SELECT NULL + 1 IS NULL", "t"),
        ("SELECT 1 + NULL IS NULL", "t"),
        ("SELECT NULL * 2 IS NOT NULL", "f"),
        ("SELECT 'a' || NULL IS NULL", "t"),
        ("SELECT NULL || 'a' IS NOT NULL", "f"),
        ("SELECT 1 IS NULL IS NULL", "f"),
        ("SELECT 1 IS NULL IS TRUE", "f"),
        ("SELECT 1 IS NULL IS FALSE", "t"),
        ("SELECT 1 IS NOT NULL IS NOT FALSE", "t"),
        ("SELECT TRUE IS TRUE IS TRUE", "t"),
        ("SELECT NULL IS NULL IS NOT NULL", "t"),
        ("SELECT 1 + 1 IS UNKNOWN", "CADB0406"),
        ("SELECT CASE WHEN 1 + 1 IS NULL THEN 1 ELSE 2 END", "2"),
        ("SELECT CASE 1 + 1 IS NULL WHEN FALSE THEN 1 ELSE 2 END", "1"),
        ("SELECT coalesce(1 + 1 IS NULL, TRUE)", "f"),
        ("SELECT 2 * 3 ^ 2 IS NULL", "f"),
        ("SELECT CAST(1 AS BIGINT) + 1 IS NULL", "f"),
        ("SELECT 1 + 1::BIGINT IS NULL", "f"),
        ("SELECT (1 + 1) IS NULL", "f"),
        ("SELECT 1 + (1 IS NULL)", "CADB0400"),
        ("SELECT EXISTS (SELECT 1) IS TRUE", "t"),
        ("SELECT (SELECT 1) + 1 IS NULL", "f"),
    ]);

    /// <summary>An IS test reads the whole comparison before it. Many of these rows gave a wrong answer with no error when IS bound tighter.</summary>
    [Test]
    public Task IsTest_OverAComparison() => Check(
    [
        ("SELECT 1 = 1 IS NULL", "f"),
        ("SELECT 1 = 1 IS NOT NULL", "t"),
        ("SELECT 1 = 2 IS NULL", "f"),
        ("SELECT 1 = 2 IS NOT NULL", "t"),
        ("SELECT NULL = 1 IS NULL", "t"),
        ("SELECT NULL = 1 IS NOT NULL", "f"),
        ("SELECT 1 = NULL IS NULL", "t"),
        ("SELECT 1 = NULL IS NOT NULL", "f"),
        ("SELECT 1 = 1 IS TRUE", "t"),
        ("SELECT 1 = 1 IS FALSE", "f"),
        ("SELECT 1 = 1 IS NOT TRUE", "f"),
        ("SELECT 1 = 1 IS NOT FALSE", "t"),
        ("SELECT 1 = 2 IS TRUE", "f"),
        ("SELECT 1 = 2 IS FALSE", "t"),
        ("SELECT 1 = 2 IS NOT TRUE", "t"),
        ("SELECT 1 = 2 IS NOT FALSE", "f"),
        ("SELECT NULL = 1 IS TRUE", "f"),
        ("SELECT NULL = 1 IS FALSE", "f"),
        ("SELECT NULL = 1 IS NOT TRUE", "t"),
        ("SELECT NULL = 1 IS NOT FALSE", "t"),
        ("SELECT 1 <> 1 IS NULL", "f"),
        ("SELECT 1 <> 1 IS NOT NULL", "t"),
        ("SELECT 1 <> 2 IS NULL", "f"),
        ("SELECT 1 <> 2 IS NOT NULL", "t"),
        ("SELECT NULL <> 1 IS NULL", "t"),
        ("SELECT NULL <> 1 IS NOT NULL", "f"),
        ("SELECT 1 <> NULL IS NULL", "t"),
        ("SELECT 1 <> NULL IS NOT NULL", "f"),
        ("SELECT 1 <> 1 IS TRUE", "f"),
        ("SELECT 1 <> 1 IS FALSE", "t"),
        ("SELECT 1 <> 1 IS NOT TRUE", "t"),
        ("SELECT 1 <> 1 IS NOT FALSE", "f"),
        ("SELECT 1 <> 2 IS TRUE", "t"),
        ("SELECT 1 <> 2 IS FALSE", "f"),
        ("SELECT 1 <> 2 IS NOT TRUE", "f"),
        ("SELECT 1 <> 2 IS NOT FALSE", "t"),
        ("SELECT NULL <> 1 IS TRUE", "f"),
        ("SELECT NULL <> 1 IS FALSE", "f"),
        ("SELECT NULL <> 1 IS NOT TRUE", "t"),
        ("SELECT NULL <> 1 IS NOT FALSE", "t"),
        ("SELECT 1 < 1 IS NULL", "f"),
        ("SELECT 1 < 1 IS NOT NULL", "t"),
        ("SELECT 1 < 2 IS NULL", "f"),
        ("SELECT 1 < 2 IS NOT NULL", "t"),
        ("SELECT NULL < 1 IS NULL", "t"),
        ("SELECT NULL < 1 IS NOT NULL", "f"),
        ("SELECT 1 < NULL IS NULL", "t"),
        ("SELECT 1 < NULL IS NOT NULL", "f"),
        ("SELECT 1 < 1 IS TRUE", "f"),
        ("SELECT 1 < 1 IS FALSE", "t"),
        ("SELECT 1 < 1 IS NOT TRUE", "t"),
        ("SELECT 1 < 1 IS NOT FALSE", "f"),
        ("SELECT 1 < 2 IS TRUE", "t"),
        ("SELECT 1 < 2 IS FALSE", "f"),
        ("SELECT 1 < 2 IS NOT TRUE", "f"),
        ("SELECT 1 < 2 IS NOT FALSE", "t"),
        ("SELECT NULL < 1 IS TRUE", "f"),
        ("SELECT NULL < 1 IS FALSE", "f"),
        ("SELECT NULL < 1 IS NOT TRUE", "t"),
        ("SELECT NULL < 1 IS NOT FALSE", "t"),
        ("SELECT 1 > 1 IS NULL", "f"),
        ("SELECT 1 > 1 IS NOT NULL", "t"),
        ("SELECT 1 > 2 IS NULL", "f"),
        ("SELECT 1 > 2 IS NOT NULL", "t"),
        ("SELECT NULL > 1 IS NULL", "t"),
        ("SELECT NULL > 1 IS NOT NULL", "f"),
        ("SELECT 1 > NULL IS NULL", "t"),
        ("SELECT 1 > NULL IS NOT NULL", "f"),
        ("SELECT 1 > 1 IS TRUE", "f"),
        ("SELECT 1 > 1 IS FALSE", "t"),
        ("SELECT 1 > 1 IS NOT TRUE", "t"),
        ("SELECT 1 > 1 IS NOT FALSE", "f"),
        ("SELECT 1 > 2 IS TRUE", "f"),
        ("SELECT 1 > 2 IS FALSE", "t"),
        ("SELECT 1 > 2 IS NOT TRUE", "t"),
        ("SELECT 1 > 2 IS NOT FALSE", "f"),
        ("SELECT NULL > 1 IS TRUE", "f"),
        ("SELECT NULL > 1 IS FALSE", "f"),
        ("SELECT NULL > 1 IS NOT TRUE", "t"),
        ("SELECT NULL > 1 IS NOT FALSE", "t"),
        ("SELECT 1 <= 1 IS NULL", "f"),
        ("SELECT 1 <= 1 IS NOT NULL", "t"),
        ("SELECT 1 <= 2 IS NULL", "f"),
        ("SELECT 1 <= 2 IS NOT NULL", "t"),
        ("SELECT NULL <= 1 IS NULL", "t"),
        ("SELECT NULL <= 1 IS NOT NULL", "f"),
        ("SELECT 1 <= NULL IS NULL", "t"),
        ("SELECT 1 <= NULL IS NOT NULL", "f"),
        ("SELECT 1 <= 1 IS TRUE", "t"),
        ("SELECT 1 <= 1 IS FALSE", "f"),
        ("SELECT 1 <= 1 IS NOT TRUE", "f"),
        ("SELECT 1 <= 1 IS NOT FALSE", "t"),
        ("SELECT 1 <= 2 IS TRUE", "t"),
        ("SELECT 1 <= 2 IS FALSE", "f"),
        ("SELECT 1 <= 2 IS NOT TRUE", "f"),
        ("SELECT 1 <= 2 IS NOT FALSE", "t"),
        ("SELECT NULL <= 1 IS TRUE", "f"),
        ("SELECT NULL <= 1 IS FALSE", "f"),
        ("SELECT NULL <= 1 IS NOT TRUE", "t"),
        ("SELECT NULL <= 1 IS NOT FALSE", "t"),
        ("SELECT 1 >= 1 IS NULL", "f"),
        ("SELECT 1 >= 1 IS NOT NULL", "t"),
        ("SELECT 1 >= 2 IS NULL", "f"),
        ("SELECT 1 >= 2 IS NOT NULL", "t"),
        ("SELECT NULL >= 1 IS NULL", "t"),
        ("SELECT NULL >= 1 IS NOT NULL", "f"),
        ("SELECT 1 >= NULL IS NULL", "t"),
        ("SELECT 1 >= NULL IS NOT NULL", "f"),
        ("SELECT 1 >= 1 IS TRUE", "t"),
        ("SELECT 1 >= 1 IS FALSE", "f"),
        ("SELECT 1 >= 1 IS NOT TRUE", "f"),
        ("SELECT 1 >= 1 IS NOT FALSE", "t"),
        ("SELECT 1 >= 2 IS TRUE", "f"),
        ("SELECT 1 >= 2 IS FALSE", "t"),
        ("SELECT 1 >= 2 IS NOT TRUE", "t"),
        ("SELECT 1 >= 2 IS NOT FALSE", "f"),
        ("SELECT NULL >= 1 IS TRUE", "f"),
        ("SELECT NULL >= 1 IS FALSE", "f"),
        ("SELECT NULL >= 1 IS NOT TRUE", "t"),
        ("SELECT NULL >= 1 IS NOT FALSE", "t"),
        ("SELECT TRUE = TRUE IS NULL", "f"),
        ("SELECT TRUE = TRUE IS NOT NULL", "t"),
        ("SELECT TRUE = TRUE IS TRUE", "t"),
        ("SELECT TRUE = TRUE IS NOT TRUE", "f"),
        ("SELECT TRUE = TRUE IS FALSE", "f"),
        ("SELECT TRUE = TRUE IS NOT FALSE", "t"),
        ("SELECT TRUE <> TRUE IS NULL", "f"),
        ("SELECT TRUE <> TRUE IS NOT NULL", "t"),
        ("SELECT TRUE <> TRUE IS TRUE", "f"),
        ("SELECT TRUE <> TRUE IS NOT TRUE", "t"),
        ("SELECT TRUE <> TRUE IS FALSE", "t"),
        ("SELECT TRUE <> TRUE IS NOT FALSE", "f"),
        ("SELECT TRUE = FALSE IS NULL", "f"),
        ("SELECT TRUE = FALSE IS NOT NULL", "t"),
        ("SELECT TRUE = FALSE IS TRUE", "f"),
        ("SELECT TRUE = FALSE IS NOT TRUE", "t"),
        ("SELECT TRUE = FALSE IS FALSE", "t"),
        ("SELECT TRUE = FALSE IS NOT FALSE", "f"),
        ("SELECT TRUE <> FALSE IS NULL", "f"),
        ("SELECT TRUE <> FALSE IS NOT NULL", "t"),
        ("SELECT TRUE <> FALSE IS TRUE", "t"),
        ("SELECT TRUE <> FALSE IS NOT TRUE", "f"),
        ("SELECT TRUE <> FALSE IS FALSE", "f"),
        ("SELECT TRUE <> FALSE IS NOT FALSE", "t"),
        ("SELECT TRUE = NULL IS NULL", "t"),
        ("SELECT TRUE = NULL IS NOT NULL", "f"),
        ("SELECT TRUE = NULL IS TRUE", "f"),
        ("SELECT TRUE = NULL IS NOT TRUE", "t"),
        ("SELECT TRUE = NULL IS FALSE", "f"),
        ("SELECT TRUE = NULL IS NOT FALSE", "t"),
        ("SELECT TRUE <> NULL IS NULL", "t"),
        ("SELECT TRUE <> NULL IS NOT NULL", "f"),
        ("SELECT TRUE <> NULL IS TRUE", "f"),
        ("SELECT TRUE <> NULL IS NOT TRUE", "t"),
        ("SELECT TRUE <> NULL IS FALSE", "f"),
        ("SELECT TRUE <> NULL IS NOT FALSE", "t"),
        ("SELECT FALSE = TRUE IS NULL", "f"),
        ("SELECT FALSE = TRUE IS NOT NULL", "t"),
        ("SELECT FALSE = TRUE IS TRUE", "f"),
        ("SELECT FALSE = TRUE IS NOT TRUE", "t"),
        ("SELECT FALSE = TRUE IS FALSE", "t"),
        ("SELECT FALSE = TRUE IS NOT FALSE", "f"),
        ("SELECT FALSE <> TRUE IS NULL", "f"),
        ("SELECT FALSE <> TRUE IS NOT NULL", "t"),
        ("SELECT FALSE <> TRUE IS TRUE", "t"),
        ("SELECT FALSE <> TRUE IS NOT TRUE", "f"),
        ("SELECT FALSE <> TRUE IS FALSE", "f"),
        ("SELECT FALSE <> TRUE IS NOT FALSE", "t"),
        ("SELECT FALSE = FALSE IS NULL", "f"),
        ("SELECT FALSE = FALSE IS NOT NULL", "t"),
        ("SELECT FALSE = FALSE IS TRUE", "t"),
        ("SELECT FALSE = FALSE IS NOT TRUE", "f"),
        ("SELECT FALSE = FALSE IS FALSE", "f"),
        ("SELECT FALSE = FALSE IS NOT FALSE", "t"),
        ("SELECT FALSE <> FALSE IS NULL", "f"),
        ("SELECT FALSE <> FALSE IS NOT NULL", "t"),
        ("SELECT FALSE <> FALSE IS TRUE", "f"),
        ("SELECT FALSE <> FALSE IS NOT TRUE", "t"),
        ("SELECT FALSE <> FALSE IS FALSE", "t"),
        ("SELECT FALSE <> FALSE IS NOT FALSE", "f"),
        ("SELECT FALSE = NULL IS NULL", "t"),
        ("SELECT FALSE = NULL IS NOT NULL", "f"),
        ("SELECT FALSE = NULL IS TRUE", "f"),
        ("SELECT FALSE = NULL IS NOT TRUE", "t"),
        ("SELECT FALSE = NULL IS FALSE", "f"),
        ("SELECT FALSE = NULL IS NOT FALSE", "t"),
        ("SELECT FALSE <> NULL IS NULL", "t"),
        ("SELECT FALSE <> NULL IS NOT NULL", "f"),
        ("SELECT FALSE <> NULL IS TRUE", "f"),
        ("SELECT FALSE <> NULL IS NOT TRUE", "t"),
        ("SELECT FALSE <> NULL IS FALSE", "f"),
        ("SELECT FALSE <> NULL IS NOT FALSE", "t"),
        ("SELECT NULL = TRUE IS NULL", "t"),
        ("SELECT NULL = TRUE IS NOT NULL", "f"),
        ("SELECT NULL = TRUE IS TRUE", "f"),
        ("SELECT NULL = TRUE IS NOT TRUE", "t"),
        ("SELECT NULL = TRUE IS FALSE", "f"),
        ("SELECT NULL = TRUE IS NOT FALSE", "t"),
        ("SELECT NULL <> TRUE IS NULL", "t"),
        ("SELECT NULL <> TRUE IS NOT NULL", "f"),
        ("SELECT NULL <> TRUE IS TRUE", "f"),
        ("SELECT NULL <> TRUE IS NOT TRUE", "t"),
        ("SELECT NULL <> TRUE IS FALSE", "f"),
        ("SELECT NULL <> TRUE IS NOT FALSE", "t"),
        ("SELECT NULL = FALSE IS NULL", "t"),
        ("SELECT NULL = FALSE IS NOT NULL", "f"),
        ("SELECT NULL = FALSE IS TRUE", "f"),
        ("SELECT NULL = FALSE IS NOT TRUE", "t"),
        ("SELECT NULL = FALSE IS FALSE", "f"),
        ("SELECT NULL = FALSE IS NOT FALSE", "t"),
        ("SELECT NULL <> FALSE IS NULL", "t"),
        ("SELECT NULL <> FALSE IS NOT NULL", "f"),
        ("SELECT NULL <> FALSE IS TRUE", "f"),
        ("SELECT NULL <> FALSE IS NOT TRUE", "t"),
        ("SELECT NULL <> FALSE IS FALSE", "f"),
        ("SELECT NULL <> FALSE IS NOT FALSE", "t"),
        ("SELECT NULL = NULL IS NULL", "t"),
        ("SELECT NULL = NULL IS NOT NULL", "f"),
        ("SELECT NULL = NULL IS TRUE", "f"),
        ("SELECT NULL = NULL IS NOT TRUE", "t"),
        ("SELECT NULL = NULL IS FALSE", "f"),
        ("SELECT NULL = NULL IS NOT FALSE", "t"),
        ("SELECT NULL <> NULL IS NULL", "t"),
        ("SELECT NULL <> NULL IS NOT NULL", "f"),
        ("SELECT NULL <> NULL IS TRUE", "f"),
        ("SELECT NULL <> NULL IS NOT TRUE", "t"),
        ("SELECT NULL <> NULL IS FALSE", "f"),
        ("SELECT NULL <> NULL IS NOT FALSE", "t"),
        ("SELECT 1 IS NULL = FALSE", "t"),
        ("SELECT TRUE = 1 IS NULL", "CADB0400"),
        ("SELECT TRUE = (NULL IS NULL)", "t"),
        ("SELECT (TRUE = NULL) IS NULL", "t"),
    ]);

    /// <summary>AND, OR and NOT bind looser than an IS test.</summary>
    [Test]
    public Task IsTest_WithAndOrNot() => Check(
    [
        ("SELECT TRUE AND TRUE IS NULL", "f"),
        ("SELECT TRUE AND TRUE IS TRUE", "t"),
        ("SELECT TRUE AND TRUE IS FALSE", "f"),
        ("SELECT TRUE AND TRUE IS NOT NULL", "t"),
        ("SELECT TRUE OR TRUE IS NULL", "t"),
        ("SELECT TRUE OR TRUE IS TRUE", "t"),
        ("SELECT TRUE OR TRUE IS FALSE", "t"),
        ("SELECT TRUE OR TRUE IS NOT NULL", "t"),
        ("SELECT TRUE AND FALSE IS NULL", "f"),
        ("SELECT TRUE AND FALSE IS TRUE", "f"),
        ("SELECT TRUE AND FALSE IS FALSE", "t"),
        ("SELECT TRUE AND FALSE IS NOT NULL", "t"),
        ("SELECT TRUE OR FALSE IS NULL", "t"),
        ("SELECT TRUE OR FALSE IS TRUE", "t"),
        ("SELECT TRUE OR FALSE IS FALSE", "t"),
        ("SELECT TRUE OR FALSE IS NOT NULL", "t"),
        ("SELECT TRUE AND NULL IS NULL", "t"),
        ("SELECT TRUE AND NULL IS TRUE", "f"),
        ("SELECT TRUE AND NULL IS FALSE", "f"),
        ("SELECT TRUE AND NULL IS NOT NULL", "f"),
        ("SELECT TRUE OR NULL IS NULL", "t"),
        ("SELECT TRUE OR NULL IS TRUE", "t"),
        ("SELECT TRUE OR NULL IS FALSE", "t"),
        ("SELECT TRUE OR NULL IS NOT NULL", "t"),
        ("SELECT FALSE AND TRUE IS NULL", "f"),
        ("SELECT FALSE AND TRUE IS TRUE", "f"),
        ("SELECT FALSE AND TRUE IS FALSE", "f"),
        ("SELECT FALSE AND TRUE IS NOT NULL", "f"),
        ("SELECT FALSE OR TRUE IS NULL", "f"),
        ("SELECT FALSE OR TRUE IS TRUE", "t"),
        ("SELECT FALSE OR TRUE IS FALSE", "f"),
        ("SELECT FALSE OR TRUE IS NOT NULL", "t"),
        ("SELECT FALSE AND FALSE IS NULL", "f"),
        ("SELECT FALSE AND FALSE IS TRUE", "f"),
        ("SELECT FALSE AND FALSE IS FALSE", "f"),
        ("SELECT FALSE AND FALSE IS NOT NULL", "f"),
        ("SELECT FALSE OR FALSE IS NULL", "f"),
        ("SELECT FALSE OR FALSE IS TRUE", "f"),
        ("SELECT FALSE OR FALSE IS FALSE", "t"),
        ("SELECT FALSE OR FALSE IS NOT NULL", "t"),
        ("SELECT FALSE AND NULL IS NULL", "f"),
        ("SELECT FALSE AND NULL IS TRUE", "f"),
        ("SELECT FALSE AND NULL IS FALSE", "f"),
        ("SELECT FALSE AND NULL IS NOT NULL", "f"),
        ("SELECT FALSE OR NULL IS NULL", "t"),
        ("SELECT FALSE OR NULL IS TRUE", "f"),
        ("SELECT FALSE OR NULL IS FALSE", "f"),
        ("SELECT FALSE OR NULL IS NOT NULL", "f"),
        ("SELECT NULL AND TRUE IS NULL", "f"),
        ("SELECT NULL AND TRUE IS TRUE", ""),
        ("SELECT NULL AND TRUE IS FALSE", "f"),
        ("SELECT NULL AND TRUE IS NOT NULL", ""),
        ("SELECT NULL OR TRUE IS NULL", ""),
        ("SELECT NULL OR TRUE IS TRUE", "t"),
        ("SELECT NULL OR TRUE IS FALSE", ""),
        ("SELECT NULL OR TRUE IS NOT NULL", "t"),
        ("SELECT NULL AND FALSE IS NULL", "f"),
        ("SELECT NULL AND FALSE IS TRUE", "f"),
        ("SELECT NULL AND FALSE IS FALSE", ""),
        ("SELECT NULL AND FALSE IS NOT NULL", ""),
        ("SELECT NULL OR FALSE IS NULL", ""),
        ("SELECT NULL OR FALSE IS TRUE", ""),
        ("SELECT NULL OR FALSE IS FALSE", "t"),
        ("SELECT NULL OR FALSE IS NOT NULL", "t"),
        ("SELECT NULL AND NULL IS NULL", ""),
        ("SELECT NULL AND NULL IS TRUE", "f"),
        ("SELECT NULL AND NULL IS FALSE", "f"),
        ("SELECT NULL AND NULL IS NOT NULL", "f"),
        ("SELECT NULL OR NULL IS NULL", "t"),
        ("SELECT NULL OR NULL IS TRUE", ""),
        ("SELECT NULL OR NULL IS FALSE", ""),
        ("SELECT NULL OR NULL IS NOT NULL", ""),
        ("SELECT NOT TRUE IS NULL", "t"),
        ("SELECT NOT TRUE IS NOT NULL", "f"),
        ("SELECT NOT TRUE IS TRUE", "f"),
        ("SELECT NOT TRUE IS NOT TRUE", "t"),
        ("SELECT NOT TRUE IS FALSE", "t"),
        ("SELECT NOT TRUE IS NOT FALSE", "f"),
        ("SELECT NOT FALSE IS NULL", "t"),
        ("SELECT NOT FALSE IS NOT NULL", "f"),
        ("SELECT NOT FALSE IS TRUE", "t"),
        ("SELECT NOT FALSE IS NOT TRUE", "f"),
        ("SELECT NOT FALSE IS FALSE", "f"),
        ("SELECT NOT FALSE IS NOT FALSE", "t"),
        ("SELECT NOT NULL IS NULL", "f"),
        ("SELECT NOT NULL IS NOT NULL", "t"),
        ("SELECT NOT NULL IS TRUE", "t"),
        ("SELECT NOT NULL IS NOT TRUE", "f"),
        ("SELECT NOT NULL IS FALSE", "t"),
        ("SELECT NOT NULL IS NOT FALSE", "f"),
        ("SELECT NOT 1 + 1 IS NULL", "t"),
        ("SELECT NOT 1 = 2 IS NULL", "t"),
        ("SELECT 1 IS NULL AND TRUE", "f"),
        ("SELECT 1 IS NULL OR TRUE", "t"),
        ("SELECT TRUE AND 1 IS NULL", "f"),
    ]);

    /// <summary>IN binds looser than a value operator and tighter than a comparison.</summary>
    [Test]
    public Task In_OnTheLevelOfLike() => Check(
    [
        ("SELECT NOT 1 IN (2)", "t"),
        ("SELECT NOT 1 NOT IN (1)", "t"),
        ("SELECT 1 IS NULL IN (FALSE)", "t"),
        ("SELECT 1 + 4 IN (5)", "t"),
        ("SELECT 2 * 3 IN (6)", "t"),
        ("SELECT 1 + 4 NOT IN (5)", "f"),
        ("SELECT 'a' || 'b' IN ('ab')", "t"),
        ("SELECT 'a' || 'b' NOT IN ('ab')", "f"),
        ("SELECT - 1 IN (-1)", "t"),
        ("SELECT 1 & 3 IN (1)", "t"),
        ("SELECT 2 ^ 2 IN (4)", "t"),
        ("SELECT 1 IN (1) = TRUE", "t"),
        ("SELECT 1 IN (2) = FALSE", "t"),
        ("SELECT TRUE = 1 IN (1)", "t"),
        ("SELECT FALSE = 1 IN (2)", "t"),
        ("SELECT 1 = 1 IN (TRUE)", "CADB0400"),
        ("SELECT TRUE = TRUE IN (FALSE)", "f"),
        ("SELECT FALSE = TRUE IN (FALSE)", "t"),
        ("SELECT TRUE <> TRUE IN (FALSE)", "t"),
        ("SELECT FALSE < TRUE IN (TRUE)", "t"),
        ("SELECT 1 IN (1) IS NULL", "f"),
        ("SELECT 1 IN (1) IS TRUE", "t"),
        ("SELECT 1 NOT IN (1) IS FALSE", "t"),
        ("SELECT NULL IN (1) IS NULL", "t"),
        ("SELECT 1 IN (1) AND TRUE", "t"),
        ("SELECT TRUE AND 1 IN (2)", "f"),
        ("SELECT 1 IN (1) IN (TRUE)", "t"),
        ("SELECT 1 IN (1) NOT IN (FALSE)", "t"),
        ("SELECT TRUE = 1 NOT IN (2)", "t"),
        ("SELECT FALSE = 1 NOT IN (1)", "t"),
        ("SELECT TRUE <> 1 NOT IN (2)", "f"),
        ("SELECT 1 + 4 IN (SELECT 5)", "t"),
        ("SELECT 1 + 4 NOT IN (SELECT 5)", "f"),
        ("SELECT TRUE = 1 IN (SELECT 1)", "t"),
        ("SELECT 1 IN (SELECT 1) = TRUE", "t"),
        ("SELECT 1 IN (SELECT 1) IS NULL", "f"),
        ("SELECT TRUE = 1 NOT IN (SELECT 1)", "f"),
        ("SELECT id FROM t WHERE id + 4 IN (SELECT 5)", "1"),
        ("SELECT -2 ^ 2 IN (4)", "t"),
        ("SELECT 1 + (SELECT 1) IN (2)", "t"),
    ]);

    /// <summary>BETWEEN binds looser than a value operator and tighter than a comparison.</summary>
    [Test]
    public Task Between_OnTheLevelOfLike() => Check(
    [
        ("SELECT NOT 1 BETWEEN 2 AND 3", "t"),
        ("SELECT NOT 1 NOT BETWEEN 0 AND 2", "t"),
        ("SELECT 1 IS NULL BETWEEN FALSE AND TRUE", "t"),
        ("SELECT 1 + 1 BETWEEN 1 AND 3", "t"),
        ("SELECT 1 + 1 NOT BETWEEN 1 AND 3", "f"),
        ("SELECT 'a' || 'b' BETWEEN 'a' AND 'b'", "t"),
        ("SELECT 1 BETWEEN 1 AND 1 + 1", "t"),
        ("SELECT 1 BETWEEN 0 + 1 AND 2", "t"),
        ("SELECT 'ab' BETWEEN 'a' AND 'a' || 'b'", "t"),
        ("SELECT 1 BETWEEN 0 AND 2 = TRUE", "t"),
        ("SELECT TRUE = 1 BETWEEN 0 AND 2", "t"),
        ("SELECT FALSE = 1 BETWEEN 2 AND 3", "t"),
        ("SELECT TRUE <> 1 BETWEEN 0 AND 2", "f"),
        ("SELECT TRUE = TRUE BETWEEN FALSE AND TRUE", "t"),
        ("SELECT FALSE = TRUE BETWEEN FALSE AND FALSE", "t"),
        ("SELECT FALSE < TRUE BETWEEN TRUE AND TRUE", "t"),
        ("SELECT 1 BETWEEN 0 AND 2 IS NULL", "f"),
        ("SELECT 1 BETWEEN 0 AND 2 IS TRUE", "t"),
        ("SELECT NULL BETWEEN 0 AND 2 IS NULL", "t"),
        ("SELECT 1 NOT BETWEEN 0 AND 2 IS FALSE", "t"),
        ("SELECT 1 BETWEEN 0 AND 2 AND TRUE", "t"),
        ("SELECT TRUE AND 1 BETWEEN 2 AND 3", "f"),
        ("SELECT 1 BETWEEN 0 AND 2 OR FALSE", "t"),
        ("SELECT 1 IN (1) BETWEEN FALSE AND TRUE", "t"),
        ("SELECT 1 BETWEEN 0 AND 2 LIKE 'x'", "CADB0407"),
        ("SELECT TRUE = 1 NOT BETWEEN 2 AND 3", "t"),
        ("SELECT FALSE = 1 NOT BETWEEN 0 AND 2", "t"),
        ("SELECT 'b' BETWEEN 'a' AND 'c' ~ 'c'", "CADB0400"),
        ("SELECT 1 BETWEEN 0 AND 2 ~ 'x'", "CADB0407"),
        ("SELECT 1 BETWEEN 0 AND 2 = 1", "CADB0400"),
        ("SELECT 1 BETWEEN 0 AND 1 + 1 IS NULL", "f"),
        ("SELECT 'a' LIKE 'a' BETWEEN FALSE AND TRUE", "CADB0406"),
        ("SELECT 'a' ILIKE 'a' BETWEEN FALSE AND TRUE", "CADB0406"),
        ("SELECT 'a' NOT LIKE 'a' BETWEEN FALSE AND TRUE", "CADB0406"),
        ("SELECT 'a' NOT ILIKE 'a' BETWEEN FALSE AND TRUE", "CADB0406"),
        ("SELECT 'a' ~ 'a' BETWEEN FALSE AND TRUE", "t"),
        ("SELECT 'a' ~* 'A' BETWEEN FALSE AND TRUE", "t"),
        ("SELECT 'a' !~ 'b' BETWEEN FALSE AND TRUE", "t"),
        ("SELECT 'a' !~* 'B' BETWEEN FALSE AND TRUE", "t"),
    ]);

    /// <summary>LIKE and ILIKE bind looser than || and tighter than a comparison, and they do not chain.</summary>
    [Test]
    public Task Like_OnItsOwnLevel() => Check(
    [
        ("SELECT NOT 'a' LIKE 'b'", "t"),
        ("SELECT NOT 'a' NOT LIKE 'a'", "t"),
        ("SELECT 1 IN (1) LIKE 'x'", "CADB0407"),
        ("SELECT 'a' LIKE 'a' IN (TRUE)", "CADB0406"),
        ("SELECT 'a' LIKE 'a' = TRUE", "t"),
        ("SELECT TRUE = 'a' LIKE 'a'", "t"),
        ("SELECT FALSE = 'a' LIKE 'b'", "t"),
        ("SELECT 'a' LIKE 'a' <> TRUE", "f"),
        ("SELECT 'a' LIKE 'a' IS NULL", "f"),
        ("SELECT 'a' LIKE 'a' IS TRUE", "t"),
        ("SELECT NULL LIKE 'a' IS NULL", "t"),
        ("SELECT 'a' LIKE 'a' ESCAPE '#' = TRUE", "t"),
        ("SELECT 'a' LIKE 'a' ESCAPE '#' IS NULL", "f"),
        ("SELECT TRUE = 'a' LIKE 'a' ESCAPE '#'", "t"),
        ("SELECT 'a' || 'b' LIKE 'ab'", "t"),
        ("SELECT 'ab' LIKE 'a' || 'b'", "t"),
        ("SELECT 'ab' LIKE 'a' || 'b' IS TRUE", "t"),
        ("SELECT 'a' LIKE 'a' AND TRUE", "t"),
        ("SELECT TRUE AND 'a' LIKE 'b'", "f"),
        ("SELECT 'a' LIKE 'a' OR FALSE", "t"),
        ("SELECT 'a' LIKE 'a' LIKE 'a'", "CADB0406"),
        ("SELECT 'a' LIKE 'a' ESCAPE '#' || ''", "t"),
        ("SELECT 'a' IN ('a') LIKE 'x'", "CADB0407"),
        ("SELECT 1 + 1 LIKE '2'", "CADB0407"),
        ("SELECT 'a' < 'b' LIKE 'c'", "CADB0400"),
        ("SELECT 'a' LIKE 'b' < 'c'", "CADB0400"),
        ("SELECT 'a' ILIKE 'a' = TRUE", "t"),
        ("SELECT TRUE = 'a' ILIKE 'a'", "t"),
        ("SELECT FALSE = 'a' ILIKE 'b'", "t"),
        ("SELECT 'a' ILIKE 'a' <> TRUE", "f"),
        ("SELECT 'a' ILIKE 'a' IS NULL", "f"),
        ("SELECT 'a' ILIKE 'a' IS TRUE", "t"),
        ("SELECT NULL ILIKE 'a' IS NULL", "t"),
        ("SELECT 'a' ILIKE 'a' ESCAPE '#' = TRUE", "t"),
        ("SELECT 'a' ILIKE 'a' ESCAPE '#' IS NULL", "f"),
        ("SELECT TRUE = 'a' ILIKE 'a' ESCAPE '#'", "t"),
        ("SELECT 'a' || 'b' ILIKE 'ab'", "t"),
        ("SELECT 'ab' ILIKE 'a' || 'b'", "t"),
        ("SELECT 'ab' ILIKE 'a' || 'b' IS TRUE", "t"),
        ("SELECT 'a' ILIKE 'a' AND TRUE", "t"),
        ("SELECT TRUE AND 'a' ILIKE 'b'", "f"),
        ("SELECT 'a' ILIKE 'a' OR FALSE", "t"),
        ("SELECT 'a' ILIKE 'a' ILIKE 'a'", "CADB0406"),
        ("SELECT 'a' ILIKE 'a' ESCAPE '#' || ''", "t"),
        ("SELECT 'a' ILIKE 'a' IN (TRUE)", "CADB0406"),
        ("SELECT 'a' IN ('a') ILIKE 'x'", "CADB0407"),
        ("SELECT 1 + 1 ILIKE '2'", "CADB0407"),
        ("SELECT 'a' < 'b' ILIKE 'c'", "CADB0400"),
        ("SELECT 'a' ILIKE 'b' < 'c'", "CADB0400"),
        ("SELECT 'a' NOT LIKE 'a' = TRUE", "f"),
        ("SELECT TRUE = 'a' NOT LIKE 'a'", "f"),
        ("SELECT FALSE = 'a' NOT LIKE 'b'", "f"),
        ("SELECT 'a' NOT LIKE 'a' <> TRUE", "t"),
        ("SELECT 'a' NOT LIKE 'a' IS NULL", "f"),
        ("SELECT 'a' NOT LIKE 'a' IS TRUE", "f"),
        ("SELECT NULL NOT LIKE 'a' IS NULL", "t"),
        ("SELECT 'a' NOT LIKE 'a' ESCAPE '#' = TRUE", "f"),
        ("SELECT 'a' NOT LIKE 'a' ESCAPE '#' IS NULL", "f"),
        ("SELECT TRUE = 'a' NOT LIKE 'a' ESCAPE '#'", "f"),
        ("SELECT 'a' || 'b' NOT LIKE 'ab'", "f"),
        ("SELECT 'ab' NOT LIKE 'a' || 'b'", "f"),
        ("SELECT 'ab' NOT LIKE 'a' || 'b' IS TRUE", "f"),
        ("SELECT 'a' NOT LIKE 'a' AND TRUE", "f"),
        ("SELECT TRUE AND 'a' NOT LIKE 'b'", "t"),
        ("SELECT 'a' NOT LIKE 'a' OR FALSE", "f"),
        ("SELECT 'a' NOT LIKE 'a' NOT LIKE 'a'", "CADB0406"),
        ("SELECT 'a' NOT LIKE 'a' ESCAPE '#' || ''", "f"),
        ("SELECT 'a' NOT LIKE 'a' IN (TRUE)", "CADB0406"),
        ("SELECT 'a' IN ('a') NOT LIKE 'x'", "CADB0407"),
        ("SELECT 1 + 1 NOT LIKE '2'", "CADB0407"),
        ("SELECT 'a' < 'b' NOT LIKE 'c'", "CADB0400"),
        ("SELECT 'a' NOT LIKE 'b' < 'c'", "CADB0400"),
        ("SELECT 'a' NOT ILIKE 'a' = TRUE", "f"),
        ("SELECT TRUE = 'a' NOT ILIKE 'a'", "f"),
        ("SELECT FALSE = 'a' NOT ILIKE 'b'", "f"),
        ("SELECT 'a' NOT ILIKE 'a' <> TRUE", "t"),
        ("SELECT 'a' NOT ILIKE 'a' IS NULL", "f"),
        ("SELECT 'a' NOT ILIKE 'a' IS TRUE", "f"),
        ("SELECT NULL NOT ILIKE 'a' IS NULL", "t"),
        ("SELECT 'a' NOT ILIKE 'a' ESCAPE '#' = TRUE", "f"),
        ("SELECT 'a' NOT ILIKE 'a' ESCAPE '#' IS NULL", "f"),
        ("SELECT TRUE = 'a' NOT ILIKE 'a' ESCAPE '#'", "f"),
        ("SELECT 'a' || 'b' NOT ILIKE 'ab'", "f"),
        ("SELECT 'ab' NOT ILIKE 'a' || 'b'", "f"),
        ("SELECT 'ab' NOT ILIKE 'a' || 'b' IS TRUE", "f"),
        ("SELECT 'a' NOT ILIKE 'a' AND TRUE", "f"),
        ("SELECT TRUE AND 'a' NOT ILIKE 'b'", "t"),
        ("SELECT 'a' NOT ILIKE 'a' OR FALSE", "f"),
        ("SELECT 'a' NOT ILIKE 'a' NOT ILIKE 'a'", "CADB0406"),
        ("SELECT 'a' NOT ILIKE 'a' ESCAPE '#' || ''", "f"),
        ("SELECT 'a' NOT ILIKE 'a' IN (TRUE)", "CADB0406"),
        ("SELECT 'a' IN ('a') NOT ILIKE 'x'", "CADB0407"),
        ("SELECT 1 + 1 NOT ILIKE '2'", "CADB0407"),
        ("SELECT 'a' < 'b' NOT ILIKE 'c'", "CADB0400"),
        ("SELECT 'a' NOT ILIKE 'b' < 'c'", "CADB0400"),
        ("SELECT 'a' ~ 'a' LIKE 'x'", "CADB0407"),
        ("SELECT 'a' LIKE 'a' ~ 'a'", "CADB0407"),
        ("SELECT 'a' ~* 'A' LIKE 'x'", "CADB0407"),
        ("SELECT 'a' LIKE 'a' ~* 'A'", "CADB0407"),
        ("SELECT 'a' !~ 'b' LIKE 'x'", "CADB0407"),
        ("SELECT 'a' LIKE 'a' !~ 'b'", "CADB0407"),
        ("SELECT 'a' !~* 'B' LIKE 'x'", "CADB0407"),
        ("SELECT 'a' LIKE 'a' !~* 'B'", "CADB0407"),
    ]);

    /// <summary>The regular expression operators share the level of || and group left to right.</summary>
    [Test]
    public Task Regex_OnTheLevelOfConcat() => Check(
    [
        ("SELECT 'a' ~ 'a' = TRUE", "t"),
        ("SELECT TRUE = 'a' ~ 'a'", "t"),
        ("SELECT 'a' ~ 'a' IS NULL", "f"),
        ("SELECT 'a' ~ 'a' IS TRUE", "t"),
        ("SELECT 'a' || 'b' ~ 'a'", "t"),
        ("SELECT 'ab' ~ 'a' || 'b'", "trueb"),
        ("SELECT 'a' ~ 'a' AND TRUE", "t"),
        ("SELECT NOT 'a' ~ 'a'", "f"),
        ("SELECT 'a' ~ 'a' IN (TRUE)", "t"),
        ("SELECT 'a' ~ 'a' ~ 'a'", "CADB0407"),
        ("SELECT 'a' ~ 'a' < 'b'", "CADB0400"),
        ("SELECT 1 + 1 ~ '2'", "CADB0407"),
        ("SELECT 'a' ~* 'A' = TRUE", "t"),
        ("SELECT TRUE = 'a' ~* 'A'", "t"),
        ("SELECT 'a' ~* 'A' IS NULL", "f"),
        ("SELECT 'a' ~* 'A' IS TRUE", "t"),
        ("SELECT 'a' || 'b' ~* 'A'", "t"),
        ("SELECT 'ab' ~* 'A' || 'b'", "trueb"),
        ("SELECT 'a' ~* 'A' AND TRUE", "t"),
        ("SELECT NOT 'a' ~* 'A'", "f"),
        ("SELECT 'a' ~* 'A' IN (TRUE)", "t"),
        ("SELECT 'a' ~* 'A' ~* 'A'", "CADB0407"),
        ("SELECT 'a' ~* 'a' < 'b'", "CADB0400"),
        ("SELECT 1 + 1 ~* '2'", "CADB0407"),
        ("SELECT 'a' !~ 'b' = TRUE", "t"),
        ("SELECT TRUE = 'a' !~ 'b'", "t"),
        ("SELECT 'a' !~ 'b' IS NULL", "f"),
        ("SELECT 'a' !~ 'b' IS TRUE", "t"),
        ("SELECT 'a' || 'b' !~ 'b'", "f"),
        ("SELECT 'ab' !~ 'b' || 'b'", "falseb"),
        ("SELECT 'a' !~ 'b' AND TRUE", "t"),
        ("SELECT NOT 'a' !~ 'b'", "f"),
        ("SELECT 'a' !~ 'b' IN (TRUE)", "t"),
        ("SELECT 'a' !~ 'b' !~ 'b'", "CADB0407"),
        ("SELECT 'a' !~ 'a' < 'b'", "CADB0400"),
        ("SELECT 1 + 1 !~ '2'", "CADB0407"),
        ("SELECT 'a' !~* 'B' = TRUE", "t"),
        ("SELECT TRUE = 'a' !~* 'B'", "t"),
        ("SELECT 'a' !~* 'B' IS NULL", "f"),
        ("SELECT 'a' !~* 'B' IS TRUE", "t"),
        ("SELECT 'a' || 'b' !~* 'B'", "f"),
        ("SELECT 'ab' !~* 'B' || 'b'", "falseb"),
        ("SELECT 'a' !~* 'B' AND TRUE", "t"),
        ("SELECT NOT 'a' !~* 'B'", "f"),
        ("SELECT 'a' !~* 'B' IN (TRUE)", "t"),
        ("SELECT 'a' !~* 'B' !~* 'B'", "CADB0407"),
        ("SELECT 'a' !~* 'a' < 'b'", "CADB0400"),
        ("SELECT 1 + 1 !~* '2'", "CADB0407"),
    ]);

    /// <summary>One comparison level with no chain.</summary>
    [Test]
    public Task Comparisons_DoNotChain() => Check(
    [
        ("SELECT NOT TRUE = FALSE", "t"),
        ("SELECT 1 = 1 = TRUE", "CADB0406"),
        ("SELECT TRUE = 1 = 1", "CADB0406"),
        ("SELECT 1 < 2 = TRUE", "CADB0406"),
        ("SELECT TRUE = 1 < 2", "CADB0406"),
        ("SELECT 1 < 2 < 3", "CADB0406"),
        ("SELECT 1 < 2 > FALSE", "CADB0406"),
        ("SELECT 1 <> 2 <> FALSE", "CADB0406"),
        ("SELECT 1 = 2 <> TRUE", "CADB0406"),
        ("SELECT TRUE <> 1 = 2", "CADB0406"),
        ("SELECT 1 >= 1 <= TRUE", "CADB0406"),
        ("SELECT FALSE = FALSE = FALSE", "CADB0406"),
        ("SELECT FALSE <> FALSE = FALSE", "CADB0406"),
        ("SELECT 1 & 3 = 1", "t"),
        ("SELECT 1 = 1 & 3", "t"),
        ("SELECT 'a' || 'b' = 'ab'", "t"),
        ("SELECT 'ab' = 'a' || 'b'", "t"),
        ("SELECT 1 + 1 = 2", "t"),
        ("SELECT 2 = 1 + 1", "t"),
        ("SELECT 1 < 1 + 1", "t"),
        ("SELECT 'a' || 'b' < 'b'", "t"),
        ("SELECT 1 + 1 < 3 = TRUE", "CADB0406"),
    ]);

    /// <summary>The jsonb operators share the level of || and group left to right.</summary>
    [Test]
    public Task Jsonb_OnTheLevelOfConcat() => Check(
    [
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' IS NULL", "f"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'z' IS NULL", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb -> 'a' IS NOT NULL", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' = '1'", "t"),
        ("SELECT '1' = '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a'", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' IN ('1')", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' NOT IN ('1')", "f"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' LIKE '1'", "t"),
        ("SELECT '1' LIKE '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a'", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' BETWEEN '0' AND '2'", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' || 'x'", "1x"),
        ("SELECT 'x' || '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a'", "CADB0418"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' ~ '1'", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb -> 'b' -> 0 IS NULL", "f"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb -> 'b' ->> 1 + 0", "2"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb -> 'b' ->> 0 + 1", "2"),
        ("SELECT 1 + '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a'", "CADB0400"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' + 1", "CADB0400"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb @> '{\"a\":1}'::jsonb = TRUE", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb @> '{\"a\":1}'::jsonb IS TRUE", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ? 'a' = TRUE", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ? 'a' IS NULL", "f"),
        ("SELECT TRUE = '{\"a\":1,\"b\":[1,2]}'::jsonb ? 'a'", "t"),
        ("SELECT NOT '{\"a\":1,\"b\":[1,2]}'::jsonb ? 'a'", "f"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ? 'a' AND TRUE", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb #>> '{a}' IS NULL", "f"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb #> '{a}' IS NOT NULL", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb -> 'a' = '1'::jsonb", "t"),
        ("SELECT '{\"a\":1,\"b\":[1,2]}'::jsonb ->> 'a' & 1", "CADB0400"),
        ("SELECT 1 & '{\"a\":1,\"b\":[1,2]}'::jsonb -> 'a'", "CADB0400"),
    ]);

    /// <summary>The same table in each position, on columns and with rows to read.</summary>
    [Test]
    public Task EveryPositionOfAStatement() => Check(
    [
        ("SELECT id FROM t WHERE v + 1 IS NULL", "3"),
        ("SELECT id FROM t WHERE v + 1 IS NOT NULL ORDER BY id", "1 ; 2"),
        ("SELECT id, v + 1 IS NULL FROM t ORDER BY id", "1|f ; 2|f ; 3|t"),
        ("SELECT id, b = TRUE IS NULL FROM t ORDER BY id", "1|f ; 2|f ; 3|t"),
        ("SELECT id, b = n IS NULL FROM t ORDER BY id", "CADB0400"),
        ("SELECT id, s || 'x' IS NULL FROM t ORDER BY id", "1|f ; 2|f ; 3|t"),
        ("SELECT id, NOT b IS NULL FROM t ORDER BY id", "1|t ; 2|t ; 3|f"),
        ("SELECT id, b AND n IS NULL FROM t ORDER BY id", "1|t ; 2|f ; 3|f"),
        ("SELECT id, b OR n IS NULL FROM t ORDER BY id", "1|t ; 2|f ; 3|"),
        ("SELECT id, v > 10 IS TRUE FROM t ORDER BY id", "1|f ; 2|t ; 3|f"),
        ("SELECT id, v > 10 IS NOT TRUE FROM t ORDER BY id", "1|t ; 2|f ; 3|t"),
        ("SELECT id FROM t WHERE id + 4 IN (5, 7) ORDER BY id", "1 ; 3"),
        ("SELECT id FROM t WHERE id + 4 NOT IN (5, 7)", "2"),
        ("SELECT id FROM t WHERE b = id IN (1, 2) ORDER BY id", "1"),
        ("SELECT id FROM t WHERE b = id NOT IN (1, 2) ORDER BY id", "2"),
        ("SELECT id FROM t WHERE b = v BETWEEN 5 AND 15 ORDER BY id", "1 ; 2"),
        ("SELECT id FROM t WHERE b = s LIKE 'a' ORDER BY id", "1 ; 2"),
        ("SELECT id FROM t WHERE s LIKE 'a' = b ORDER BY id", "1 ; 2"),
        ("SELECT id FROM t WHERE s || 'x' LIKE 'ax'", "1"),
        ("SELECT id FROM t WHERE s LIKE 'a' IS TRUE", "1"),
        ("SELECT id FROM t WHERE s NOT LIKE 'a' IS TRUE", "2"),
        ("SELECT id FROM t ORDER BY v + 1 IS NULL, id", "1 ; 2 ; 3"),
        ("SELECT COUNT(*) FROM t WHERE v + 1 IS NULL", "1"),
        ("SELECT v + 1 IS NULL AS x, COUNT(*) FROM t GROUP BY v + 1 IS NULL ORDER BY x", "f|2 ; t|1"),
        ("SELECT COUNT(*) FROM t HAVING COUNT(*) + 1 IS NOT NULL", "3"),
        ("SELECT l.id FROM t l JOIN t r ON l.id + 1 IS NOT NULL AND l.id = r.id ORDER BY l.id", "1 ; 2 ; 3"),
        ("SELECT l.id FROM t l JOIN t r ON l.id + 1 IN (2) AND l.id = r.id", "1"),
        ("SELECT id FROM t WHERE v + 1 IS NULL OR id = 1 ORDER BY id", "1 ; 3"),
        ("SELECT id FROM t WHERE id = 1 AND v + 1 IS NOT NULL", "1"),
        ("UPDATE t SET b = v + 1 IS NULL WHERE id = 1", "1"),
        ("UPDATE t SET b = TRUE WHERE id + 1 IN (2)", "1"),
        ("UPDATE t SET b = v + 1 IN (11) WHERE id = 1", "1"),
        ("DELETE FROM t WHERE v + 1 IS NULL", "1"),
        ("DELETE FROM t WHERE id + 4 IN (5)", "1"),
        ("SELECT id FROM bb WHERE a = b IS NULL ORDER BY id", "3 ; 6 ; 7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a = (b IS NULL) ORDER BY id", "3 ; 4 ; 5"),
        ("SELECT id FROM bb WHERE (a = b) IS NULL ORDER BY id", "3 ; 6 ; 7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a = b IS NOT NULL ORDER BY id", "1 ; 2 ; 4 ; 5"),
        ("SELECT id FROM bb WHERE a = b IS TRUE ORDER BY id", "1 ; 5"),
        ("SELECT id FROM bb WHERE a = b IS FALSE ORDER BY id", "2 ; 4"),
        ("SELECT id FROM bb WHERE a = b IS NOT TRUE ORDER BY id", "2 ; 3 ; 4 ; 6 ; 7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a = b IS NOT FALSE ORDER BY id", "1 ; 3 ; 5 ; 6 ; 7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a <> b IS NULL ORDER BY id", "3 ; 6 ; 7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a <> b IS TRUE ORDER BY id", "2 ; 4"),
        ("SELECT id FROM bb WHERE a < b IS NULL ORDER BY id", "3 ; 6 ; 7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a <= b IS FALSE ORDER BY id", "2"),
        ("SELECT id FROM bb WHERE a = b IN (TRUE) ORDER BY id", "1 ; 5"),
        ("SELECT id FROM bb WHERE a = b NOT IN (TRUE) ORDER BY id", "2 ; 4"),
        ("SELECT id FROM bb WHERE a = b BETWEEN FALSE AND TRUE ORDER BY id", "1 ; 2"),
        ("SELECT id FROM bb WHERE a = b NOT BETWEEN TRUE AND TRUE ORDER BY id", "2 ; 4"),
        ("SELECT id FROM bb WHERE NOT a = b IS NULL ORDER BY id", "1 ; 2 ; 4 ; 5"),
        ("SELECT id FROM bb WHERE NOT a IS NULL ORDER BY id", "1 ; 2 ; 3 ; 4 ; 5 ; 6"),
        ("SELECT id FROM bb WHERE a AND b IS NULL ORDER BY id", "3"),
        ("SELECT id FROM bb WHERE a OR b IS NOT NULL ORDER BY id", "1 ; 2 ; 3 ; 4 ; 5 ; 7 ; 8"),
        ("SELECT id FROM bb WHERE a AND b IS TRUE ORDER BY id", "1"),
        ("SELECT id FROM bb WHERE a OR b IS FALSE ORDER BY id", "1 ; 2 ; 3 ; 5 ; 8"),
        ("SELECT id FROM bb WHERE NOT a AND b ORDER BY id", "4"),
        ("SELECT id FROM bb WHERE NOT a OR b ORDER BY id", "1 ; 4 ; 5 ; 6 ; 7"),
        ("SELECT id FROM bb WHERE a IS NULL = b ORDER BY id", "2 ; 5 ; 7"),
        ("SELECT id FROM bb WHERE a IS TRUE = b ORDER BY id", "1 ; 5 ; 8"),
        ("SELECT id FROM bb WHERE a IN (TRUE) = b ORDER BY id", "1 ; 5"),
        ("SELECT id FROM bb WHERE a IN (TRUE) IS NULL ORDER BY id", "7 ; 8 ; 9"),
        ("SELECT id FROM bb WHERE a BETWEEN FALSE AND TRUE = b ORDER BY id", "1 ; 4"),
        ("SELECT id, a = b IS NULL FROM bb ORDER BY a = b IS NULL, id", "1|f ; 2|f ; 4|f ; 5|f ; 3|t ; 6|t ; 7|t ; 8|t ; 9|t"),
        ("SELECT COUNT(*) FROM bb WHERE a = b IS NOT NULL", "4"),
        ("SELECT a = b IS NULL AS x, COUNT(*) FROM bb GROUP BY a = b IS NULL ORDER BY x", "f|4 ; t|5"),
    ]);

    /// <summary>A constant division by zero under a test is refused as a division.</summary>
    [Test]
    public Task DivisionByZero_UnderATest() => Check(
    [
        ("SELECT 1 / 0 IS NULL", "CADB0400"),
        ("SELECT 1 / 0 IS NOT NULL", "CADB0400"),
        ("SELECT 1 % 0 IS NULL", "CADB0400"),
        ("SELECT 1 / 0 IN (1)", "CADB0400"),
        ("SELECT 1 / 0 BETWEEN 0 AND 1", "CADB0400"),
        ("SELECT id FROM t WHERE 1 / 0 IS NULL", "CADB0400"),
        ("SELECT id FROM em WHERE 1 / 0 IS NULL", "CADB0400"),
        ("SELECT id FROM em WHERE 1 / 0 IN (1)", "CADB0400"),
    ]);

    /// <summary>Parenthesized forms, and the forms around them.</summary>
    [Test]
    public Task OtherForms() => Check(
    [
        ("SELECT NOT NOT TRUE", "t"),
        ("SELECT 1 + 1 & 3", "2"),
        ("SELECT 1 & 3 + 1", "0"),
        ("SELECT POSITION('a' IN 'xa')", "2"),
        ("SELECT POSITION('a' || 'b' IN 'xab')", "2"),
        ("SELECT POSITION(1 + 1 IN '2')", "CADB0400"),
    ]);

    /// <summary>
    /// The error of a division under a test must name the division. Before IS moved, these failed on the type of
    /// the operand, so the message pointed at the wrong mistake.
    /// </summary>
    [TestCase("SELECT 1 / 0 IS NULL")]
    [TestCase("SELECT 1 / 0 IS NOT NULL")]
    [TestCase("SELECT 1 / 0 IN (1)")]
    [TestCase("SELECT 1 / 0 BETWEEN 0 AND 1")]
    [TestCase("SELECT id FROM em WHERE 1 / 0 IS NULL")]
    [TestCase("SELECT id FROM em WHERE 1 / 0 IN (1)")]
    public async Task DivisionByZero_NamesTheDivision(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await cursor.ToListAsync();
        });

        await database.Transactions.RollbackAsync(tx);

        Assert.That(e!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(e.Message, Does.Contain("Division by zero"));
    }

    /// <summary>PostgreSQL does not evaluate a dead CASE branch, and it does not refuse a stored division.</summary>
    [Test]
    public async Task DivisionByZero_FollowsTheRuleOfTheConstantCheck()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, "SELECT CASE WHEN FALSE THEN 1 / 0 IS NULL ELSE TRUE END"), Is.EqualTo("t"));

        Assert.That(await TryCommit(database, executor, dbname, "CREATE VIEW dz AS SELECT 1 / 0 IS NULL AS x"), Is.Empty);
        Assert.That(await Run(database, executor, dbname, "SELECT x FROM dz"), Is.EqualTo(CamusDBErrorCodes.InvalidInput));

        Assert.That(await TryCommit(database, executor, dbname, "CREATE TABLE ckd (id INT64 PRIMARY KEY, v INT64, CHECK (1 / 0 IS NULL))"), Is.Empty);
        Assert.That(await TryCommit(database, executor, dbname, "INSERT INTO ckd (id, v) VALUES (1, 1)"), Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    /// <summary>
    /// The rows that each grouping selects are different, so these results show which grouping a view reads. A
    /// view created before the table changed holds the text <c>a = (b IS NULL)</c> for a statement that wrote
    /// <c>a = b IS NULL</c>, because the renderer writes each grouping out. That text keeps its old meaning.
    /// </summary>
    [Test]
    public async Task View_ReadsTheGroupingInItsStoredText_AcrossAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW written_today AS SELECT id FROM bb WHERE a = b IS NULL");
        await Commit(database, executor, dbname, "CREATE VIEW stored_earlier AS SELECT id FROM bb WHERE a = (b IS NULL)");

        foreach (DatabaseDescriptor current in new[] { database, await Reopen(executor, dbname) })
        {
            Assert.That(await Run(current, executor, dbname, "SELECT id FROM written_today ORDER BY id"), Is.EqualTo("3 ; 6 ; 7 ; 8 ; 9"));
            Assert.That(await Run(current, executor, dbname, "SELECT id FROM stored_earlier ORDER BY id"), Is.EqualTo("3 ; 4 ; 5"));
            Assert.That(await Run(current, executor, dbname, "SHOW CREATE VIEW written_today"), Does.Contain("(a = b) IS NULL"));
            Assert.That(await Run(current, executor, dbname, "SHOW CREATE VIEW stored_earlier"), Does.Contain("a = (b IS NULL)"));
        }
    }

    /// <summary>The refresh parses the stored text again, so a materialized view keeps its grouping too.</summary>
    [Test]
    public async Task MaterializedView_ReadsTheGroupingInItsStoredText_AfterARefresh()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE MATERIALIZED VIEW mv AS SELECT id FROM bb WHERE a = b IS NULL");
        Assert.That(await Run(database, executor, dbname, "SELECT COUNT(*) FROM mv"), Is.EqualTo("5"));

        await Commit(database, executor, dbname, "INSERT INTO bb (id, a, b) VALUES (10, NULL, TRUE)");
        await Commit(database, executor, dbname, "REFRESH MATERIALIZED VIEW mv");
        Assert.That(await Run(database, executor, dbname, "SELECT COUNT(*) FROM mv"), Is.EqualTo("6"));
    }

    /// <summary>
    /// The row <c>(NULL, TRUE)</c> tells the two groupings apart: <c>(a = b) IS NOT NULL</c> is false, so the row
    /// is refused, and <c>a = (b IS NOT NULL)</c> is NULL, so the row passes. Measured on PostgreSQL. A CHECK
    /// created before the table changed holds the second text, and it keeps that meaning.
    /// </summary>
    [Test]
    public async Task Check_ReadsTheGroupingInItsStoredText_AcrossAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE ck_today (id INT64 PRIMARY KEY, a BOOL, b BOOL, CHECK (a = b IS NOT NULL))");
        await Commit(database, executor, dbname, "CREATE TABLE ck_earlier (id INT64 PRIMARY KEY, a BOOL, b BOOL, CHECK (a = (b IS NOT NULL)))");

        DatabaseDescriptor reopened = await Reopen(executor, dbname);

        Assert.That(await Run(reopened, executor, dbname, "SHOW CREATE TABLE ck_today"), Does.Contain("(a = b) IS NOT NULL"));
        Assert.That(await Run(reopened, executor, dbname, "SHOW CREATE TABLE ck_earlier"), Does.Contain("a = (b IS NOT NULL)"));

        Assert.That(await TryCommit(reopened, executor, dbname, "INSERT INTO ck_today (id, a, b) VALUES (1, NULL, TRUE)"), Is.EqualTo(CamusDBErrorCodes.CheckConstraintViolation));
        Assert.That(await TryCommit(reopened, executor, dbname, "INSERT INTO ck_today (id, a, b) VALUES (2, TRUE, TRUE)"), Is.Empty);
        Assert.That(await TryCommit(reopened, executor, dbname, "INSERT INTO ck_earlier (id, a, b) VALUES (1, NULL, TRUE)"), Is.Empty);
        Assert.That(await TryCommit(reopened, executor, dbname, "INSERT INTO ck_earlier (id, a, b) VALUES (2, FALSE, TRUE)"), Is.EqualTo(CamusDBErrorCodes.CheckConstraintViolation));
    }

    private static async Task<DatabaseDescriptor> Reopen(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }
}
