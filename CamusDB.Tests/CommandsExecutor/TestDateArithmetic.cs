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
/// The arithmetic operators on a date, with the PostgreSQL rules: <c>date + int64</c>,
/// <c>int64 + date</c> and <c>date - int64</c> give a date, and <c>date - date</c> gives the number of
/// days as an int64. Every other operator or pair of a date with an integer stays an error, as in
/// PostgreSQL. Each case goes through SQL, so the parser, the static type checks, the derived table
/// types and the evaluator are all on the path. Each expected value was checked against the
/// PostgreSQL rules for <c>date + integer</c> and <c>date - date</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestDateArithmetic : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE ev (id INT64 PRIMARY KEY, d DATE, due DATE, ts DATETIME)");
        await Commit(database, executor, dbname,
            "INSERT INTO ev (id, d, due, ts) VALUES " +
            "(1, '2024-01-31', '2024-03-01', '2024-01-31T10:00:00Z'), " +
            "(2, '2024-02-28', '2024-02-01', '2024-02-28T00:00:00Z'), " +
            "(3, NULL, '2024-12-31', NULL)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, d DATE, ts DATETIME)");

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Date => new DateTime(value.LongValue, DateTimeKind.Utc).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    /// <summary>
    /// The rows of <paramref name="sql"/>: cells join with <c>|</c> and rows with <c> ; </c>, or the error
    /// code. The type of the last cell of each row goes to <paramref name="lastCellTypes"/>.
    /// </summary>
    private static async Task<string> Run(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, List<ColumnType>? lastCellTypes = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            foreach (QueryResultRow row in rows)
                lastCellTypes?.Add(row.Row.Values.Last().Type);

            return string.Join(" ; ", rows.Select(r => string.Join("|", r.Row.Values.Select(Render))));
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

    // ── Values ───────────────────────────────────────────────────────────────────

    [TestCase("CAST('2024-01-31' AS date) + 1", "2024-02-01", ColumnType.Date)]
    [TestCase("1 + CAST('2024-01-31' AS date)", "2024-02-01", ColumnType.Date)]
    [TestCase("CAST('2024-01-01' AS date) - 1", "2023-12-31", ColumnType.Date)]
    [TestCase("CAST('2024-01-10' AS date) + -15", "2023-12-26", ColumnType.Date)]
    [TestCase("CAST('2024-01-10' AS date) - -21", "2024-01-31", ColumnType.Date)]
    [TestCase("CAST('2024-01-10' AS date) + 0", "2024-01-10", ColumnType.Date)]
    [TestCase("CAST('2024-02-28' AS date) + 1", "2024-02-29", ColumnType.Date)]
    [TestCase("CAST('2023-02-28' AS date) + 1", "2023-03-01", ColumnType.Date)]
    [TestCase("CAST('2024-03-01' AS date) - 1", "2024-02-29", ColumnType.Date)]
    [TestCase("CAST('2024-02-29' AS date) + 366", "2025-03-01", ColumnType.Date)]
    [TestCase("CAST('2024-03-01' AS date) - CAST('2024-02-01' AS date)", "29", ColumnType.Integer64)]
    [TestCase("CAST('2023-03-01' AS date) - CAST('2023-02-01' AS date)", "28", ColumnType.Integer64)]
    [TestCase("CAST('2024-02-01' AS date) - CAST('2024-03-01' AS date)", "-29", ColumnType.Integer64)]
    [TestCase("CAST('2024-02-01' AS date) - CAST('2024-02-01' AS date)", "0", ColumnType.Integer64)]
    [TestCase("CAST('2025-01-01' AS date) - CAST('2024-01-01' AS date)", "366", ColumnType.Integer64)]
    [TestCase("CAST('9999-12-31' AS date) - CAST('0001-01-01' AS date)", "3652058", ColumnType.Integer64)]
    [TestCase("CAST('9999-12-30' AS date) + 1", "9999-12-31", ColumnType.Date)]
    [TestCase("CAST('0001-01-02' AS date) - 1", "0001-01-01", ColumnType.Date)]
    [TestCase("CAST('2024-01-31' AS date) + 1 + 1", "2024-02-02", ColumnType.Date)]
    [TestCase("(CAST('2024-01-31' AS date) + 30) - CAST('2024-01-31' AS date)", "30", ColumnType.Integer64)]
    [TestCase("current_date() + 30 - current_date()", "30", ColumnType.Integer64)]
    public async Task AFromlessSelect_GivesThePostgreSqlResult(string expression, string expected, ColumnType type)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected), expression);
            Assert.That(types, Is.EqualTo(new[] { type }), expression);
        });
    }

    [TestCase("CAST(NULL AS date) + 1")]
    [TestCase("1 + CAST(NULL AS date)")]
    [TestCase("CAST('2024-01-01' AS date) - CAST(NULL AS date)")]
    [TestCase("CAST('2024-01-01' AS date) + NULL")]
    public async Task ANullOperand_GivesNull(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(""), expression);
    }

    [Test]
    public async Task ColumnOperands_GiveOneValuePerRow_AndNullForANullDate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string rows = await Run(database, executor, dbname, "SELECT id, d + 7, due - d, 1 + due FROM ev ORDER BY id");

        Assert.That(rows, Is.EqualTo("1|2024-02-07|30|2024-03-02 ; 2|2024-03-06|-27|2024-02-02 ; 3|||2025-01-01"));
    }

    // ── Out of range ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A result before 0001-01-01 or after 9999-12-31 is an error, never a wrapped date. A day count
    /// near the int64 limits must not overflow inside the evaluator first.
    /// </summary>
    [TestCase("CAST('9999-12-31' AS date) + 1")]
    [TestCase("CAST('0001-01-01' AS date) - 1")]
    [TestCase("1 + CAST('9999-12-31' AS date)")]
    [TestCase("CAST('2024-01-01' AS date) + 9223372036854775807")]
    [TestCase("CAST('2024-01-01' AS date) - 9223372036854775807")]
    [TestCase("CAST('2024-01-01' AS date) + -9223372036854775807")]
    [TestCase("CAST('2024-01-01' AS date) + 3652059")]
    public async Task AResultOutsideTheDateRange_IsAnError(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    // ── Pairs that stay errors ───────────────────────────────────────────────────

    /// <summary>
    /// PostgreSQL has no operator for these pairs. A datetime moves only by an interval (see
    /// <c>TestIntervalSql</c>). A quoted string is not read as a date: PostgreSQL reads <c>'2024-01-01' + 1</c> as an integer
    /// sum and refuses the literal.
    /// </summary>
    [TestCase("CAST('2024-01-01' AS date) * 2")]
    [TestCase("CAST('2024-01-01' AS date) / 2")]
    [TestCase("2 - CAST('2024-01-01' AS date)")]
    [TestCase("CAST('2024-01-01' AS date) + CAST('2024-01-01' AS date)")]
    [TestCase("CAST('2024-01-01' AS date) + 1.5")]
    [TestCase("CAST('2024-01-01' AS date) + TRUE")]
    [TestCase("CAST('2024-01-01' AS date) + 'x'")]
    [TestCase("CAST('2024-01-01T00:00:00Z' AS datetime) + 1")]
    [TestCase("CAST('2024-01-01T00:00:00Z' AS datetime) - 1")]
    [TestCase("'2024-01-01' + 1")]
    public async Task APairWithNoDateRule_IsAnError(string expression)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression}"), Is.EqualTo(CamusDBErrorCodes.InvalidInput), expression);
    }

    /// <summary>
    /// The static check runs before any row is read. On an empty table the refused pairs are still
    /// errors, and the date rules are accepted.
    /// </summary>
    [TestCase("d * 2", "CADB0400")]
    [TestCase("ts + 1", "CADB0400")]
    [TestCase("2 - d", "CADB0400")]
    [TestCase("d + 1", "")]
    [TestCase("1 + d", "")]
    [TestCase("d - 1", "")]
    [TestCase("d - d", "")]
    public async Task OnAnEmptyTable_TheStaticCheckAppliesTheSameRule(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, $"SELECT {expression} FROM em"), Is.EqualTo(expected), expression);
    }

    // ── Static types ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A stored column takes the type that the value has: a date for <c>date ± int64</c> and an int64 for
    /// <c>date - date</c>.
    /// </summary>
    [Test]
    public async Task CreateTableAs_StoresTheDateAndTheDayCountTypes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE c AS SELECT id, d + 1 AS nxt, due - d AS span FROM ev");

        TableSchema schema = executor.Catalogs.GetTableSchema(database, "c");
        Dictionary<string, ColumnType> types = schema.Columns!.ToDictionary(column => column.Name, column => column.Type);

        string rows = await Run(database, executor, dbname, "SELECT id, nxt, span FROM c ORDER BY id");

        Assert.Multiple(() =>
        {
            Assert.That(types["nxt"], Is.EqualTo(ColumnType.Date));
            Assert.That(types["span"], Is.EqualTo(ColumnType.Integer64));
            Assert.That(rows, Is.EqualTo("1|2024-02-01|30 ; 2|2024-02-29|-27 ; 3||"));
        });
    }

    /// <summary>
    /// A date expression compares with a date column and with a quoted date literal, and it combines with
    /// a date in a CASE. Each of these needs the static type of the expression to be a date.
    /// </summary>
    [Test]
    public async Task ADateExpression_ComparesAndCombinesAsADate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];

        string addEqualsColumn = await Run(database, executor, dbname, "SELECT id FROM ev WHERE d + 30 = due ORDER BY id");
        string addEqualsLiteral = await Run(database, executor, dbname, "SELECT id FROM ev WHERE d + 1 = '2024-02-29' ORDER BY id");
        string dayCount = await Run(database, executor, dbname, "SELECT id FROM ev WHERE due - d > 0 ORDER BY id");
        string constantBound = await Run(database, executor, dbname, "SELECT id FROM ev WHERE d > CAST('2024-02-28' AS date) - 1 ORDER BY id");
        string caseResult = await Run(database, executor, dbname, "SELECT id, CASE WHEN id = 1 THEN d + 1 ELSE due END FROM ev ORDER BY id", types);

        Assert.Multiple(() =>
        {
            Assert.That(addEqualsColumn, Is.EqualTo("1"));
            Assert.That(addEqualsLiteral, Is.EqualTo("2"));
            Assert.That(dayCount, Is.EqualTo("1"));
            Assert.That(constantBound, Is.EqualTo("2"));
            Assert.That(caseResult, Is.EqualTo("1|2024-02-01 ; 2|2024-02-01 ; 3|2024-12-31"));
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Date, ColumnType.Date, ColumnType.Date }));
        });
    }

    /// <summary>
    /// The date rule does not make a date and an integer share a type elsewhere. CASE, COALESCE and UNION
    /// still refuse the pair, because no common type exists.
    /// </summary>
    [TestCase("SELECT COALESCE(d, 1) FROM ev")]
    [TestCase("SELECT CASE WHEN id = 1 THEN d ELSE 1 END FROM ev")]
    [TestCase("SELECT d FROM ev UNION ALL SELECT id FROM ev")]
    public async Task ADateAndAnInteger_StillHaveNoCommonType(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(CamusDBErrorCodes.InvalidInput), sql);
    }
}
