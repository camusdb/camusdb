/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// LIKE, ILIKE, NOT LIKE, the ESCAPE clause and SHOW ... LIKE follow PostgreSQL, through one matcher. Each
/// expected result in <see cref="Statement"/> was measured on PostgreSQL 16.11 in the C locale with the same
/// tables. A result renders as <c>t</c> or <c>f</c>, a NULL renders as an empty cell, rows join with
/// <c> ; </c>, and an expected value that starts with <c>CADB</c> is an error code where PostgreSQL also
/// gave an error.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestLikeMatcher : BaseTest
{
    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<string> Run(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
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

    /// <summary>An INSERT, or the code of its refusal. Nothing stays, so one table serves several rows.</summary>
    private static async Task<string> TryInsert(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            return "written";
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

        await Commit(database, executor, dbname, "CREATE TABLE lk (id INT64 PRIMARY KEY, s STRING, p STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE esc (id INT64 PRIMARY KEY, escape STRING)");
        await Commit(database, executor, dbname, "CREATE VIEW vl AS SELECT id FROM lk WHERE s LIKE 'a_'");
        await Commit(database, executor, dbname, "CREATE VIEW vl2 AS SELECT id FROM lk WHERE s LIKE 'a#_' ESCAPE '#'");
        await Commit(database, executor, dbname, "INSERT INTO esc (id, escape) VALUES (1, '#')");
        await Commit(database, executor, dbname, @"INSERT INTO lk (id, s, p) VALUES (1, 'ab', 'a_'), (2, 'a_', 'a\_'), (3, 'a%', 'a\%'), (4, NULL, 'a'), (5, 'a\', 'a\\'), (6, '', '%'), (7, '🙂', '_'), (8, 'abc', NULL)");

        return (dbname, database, executor);
    }

    // ── Every measured statement ─────────────────────────────────────────────────────────────────

    [TestCase(@"SELECT 'ab' LIKE 'a_'", @"t")]
    [TestCase(@"SELECT 'a_' LIKE 'a_'", @"t")]
    [TestCase(@"SELECT 'a' LIKE 'a_'", @"f")]
    [TestCase(@"SELECT 'abc' LIKE 'a_'", @"f")]
    [TestCase(@"SELECT '' LIKE '_'", @"f")]
    [TestCase(@"SELECT 'ab' LIKE 'a_%'", @"t")]
    [TestCase(@"SELECT 'a' LIKE 'a_%'", @"f")]
    [TestCase(@"SELECT 'AB' ILIKE 'a_'", @"t")]
    [TestCase(@"SELECT '🙂' LIKE '_'", @"t")]
    [TestCase(@"SELECT '🙂x' LIKE '__'", @"t")]
    [TestCase(@"SELECT '🙂' LIKE '__'", @"f")]
    [TestCase(@"SELECT 'a_' LIKE 'a\_'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a\_'", @"f")]
    [TestCase(@"SELECT 'a%' LIKE 'a\%'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a\%'", @"f")]
    [TestCase(@"SELECT 'a\' LIKE 'a\\'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE '\a\b'", @"t")]
    [TestCase(@"SELECT 'a\' LIKE 'a\'", @"CADB0400")]
    [TestCase(@"SELECT 'a_' LIKE 'a#_' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a#_' ESCAPE '#'", @"f")]
    [TestCase(@"SELECT 'a\_' LIKE 'a\_' ESCAPE ''", @"t")]
    [TestCase(@"SELECT 'a' LIKE 'a' ESCAPE 'xy'", @"CADB0400")]
    [TestCase(@"SELECT E'a\nb' LIKE 'a%b'", @"t")]
    [TestCase(@"SELECT E'ab\n' LIKE 'a%b'", @"f")]
    [TestCase(@"SELECT E'a\nb' LIKE 'a%'", @"t")]
    [TestCase(@"SELECT E'a\nb' ILIKE 'a%b'", @"t")]
    [TestCase(@"SELECT E'x\na' LIKE '%a%'", @"t")]
    [TestCase(@"SELECT E'a\nb' LIKE 'a_b'", @"t")]
    [TestCase(@"SELECT 'a\_'", @"a\_")]
    [TestCase(@"SELECT length('a\_')", @"3")]
    [TestCase(@"SELECT E'a\\_'", @"a\_")]
    [TestCase(@"SELECT length(E'a\\_')", @"3")]
    [TestCase(@"SELECT 'b' LIKE 'a\'", @"f")]
    [TestCase(@"SELECT '' LIKE '\'", @"f")]
    [TestCase(@"SELECT 'a' LIKE '\'", @"CADB0400")]
    [TestCase(@"SELECT 'ab' LIKE 'a%\'", @"CADB0400")]
    [TestCase(@"SELECT 'a' LIKE 'a%\'", @"f")]
    [TestCase(@"SELECT 'a' LIKE '%\'", @"CADB0400")]
    [TestCase(@"SELECT '' LIKE '%\'", @"f")]
    [TestCase(@"SELECT 'ab' LIKE '%b\'", @"f")]
    [TestCase(@"SELECT 'x' LIKE '%b\'", @"f")]
    [TestCase(@"SELECT 'a' LIKE 'a_\'", @"f")]
    [TestCase(@"SELECT 'ab' LIKE 'a_\'", @"f")]
    [TestCase(@"SELECT 'abc' LIKE 'a_\'", @"CADB0400")]
    [TestCase(@"SELECT '' LIKE ''", @"t")]
    [TestCase(@"SELECT 'a' LIKE ''", @"f")]
    [TestCase(@"SELECT '' LIKE '%'", @"t")]
    [TestCase(@"SELECT '' LIKE '%%'", @"t")]
    [TestCase(@"SELECT 'a' LIKE '%_'", @"t")]
    [TestCase(@"SELECT '' LIKE '%_'", @"f")]
    [TestCase(@"SELECT 'a' LIKE '_%'", @"t")]
    [TestCase(@"SELECT '' LIKE '_%'", @"f")]
    [TestCase(@"SELECT 'abc' LIKE '%_%'", @"t")]
    [TestCase(@"SELECT 'abc' LIKE '___'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE '___'", @"f")]
    [TestCase(@"SELECT 'abc' LIKE 'a%c_'", @"f")]
    [TestCase(@"SELECT 'abcd' LIKE 'a%c_'", @"t")]
    [TestCase(@"SELECT 'aXbYc' LIKE 'a%b_c'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE '%_b'", @"t")]
    [TestCase(@"SELECT 'b' LIKE '%_b'", @"f")]
    [TestCase(@"SELECT 'abab' LIKE '%ab'", @"t")]
    [TestCase(@"SELECT 'abcabd' LIKE '%ab_'", @"t")]
    [TestCase(@"SELECT 'abcabd' LIKE 'ab%ab_'", @"t")]
    [TestCase(@"SELECT 'mississippi' LIKE '%iss%ip%'", @"t")]
    [TestCase(@"SELECT 'mississippi' LIKE 'm%iss%_p%i'", @"t")]
    [TestCase(@"SELECT 'a%b' LIKE 'a\%b'", @"t")]
    [TestCase(@"SELECT 'aXb' LIKE 'a\%b'", @"f")]
    [TestCase(@"SELECT 'a%b' LIKE '%\%%'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE '%\%%'", @"f")]
    [TestCase(@"SELECT '%' LIKE '\%'", @"t")]
    [TestCase(@"SELECT '_' LIKE '\_'", @"t")]
    [TestCase(@"SELECT 'x' LIKE '\_'", @"f")]
    [TestCase(@"SELECT '\' LIKE '\\'", @"t")]
    [TestCase(@"SELECT 'a\b' LIKE 'a\\b'", @"t")]
    [TestCase(@"SELECT 'a\b' LIKE 'a\b'", @"f")]
    [TestCase(@"SELECT 'ab' LIKE 'a\b'", @"t")]
    [TestCase(@"SELECT 'a.b' LIKE 'a.b'", @"t")]
    [TestCase(@"SELECT 'aXb' LIKE 'a.b'", @"f")]
    [TestCase(@"SELECT 'a*' LIKE 'a*'", @"t")]
    [TestCase(@"SELECT 'a(' LIKE '%(%'", @"t")]
    [TestCase(@"SELECT '[a]' LIKE '[a]%'", @"t")]
    [TestCase(@"SELECT 'ab' NOT LIKE 'a_'", @"f")]
    [TestCase(@"SELECT 'a_' NOT LIKE 'a\_'", @"f")]
    [TestCase(@"SELECT 'ab' NOT LIKE 'a\_'", @"t")]
    [TestCase(@"SELECT NOT 'ab' LIKE 'a_'", @"f")]
    [TestCase(@"SELECT NULL LIKE 'a_'", @"")]
    [TestCase(@"SELECT 'a' LIKE NULL", @"")]
    [TestCase(@"SELECT 'a' NOT LIKE NULL", @"")]
    [TestCase(@"SELECT NULL LIKE 'a\'", @"")]
    [TestCase(@"SELECT 'aB' ILIKE 'A\B'", @"t")]
    [TestCase(@"SELECT 'A_' ILIKE 'a\_'", @"t")]
    [TestCase(@"SELECT 'AB' ILIKE 'a\_'", @"f")]
    [TestCase(@"SELECT 'a%' ILIKE 'A\%'", @"t")]
    [TestCase(@"SELECT 'a' ILIKE 'a\'", @"f")]
    [TestCase(@"SELECT 'b' ILIKE 'a\'", @"f")]
    [TestCase(@"SELECT 'aBc' ILIKE '%b%'", @"t")]
    [TestCase(@"SELECT 'aXbYc' ILIKE 'A%B_C'", @"t")]
    [TestCase(@"SELECT 'ab' NOT ILIKE 'A_'", @"f")]
    [TestCase(@"SELECT '🙂' ILIKE '_'", @"t")]
    [TestCase(@"SELECT E'a\nb' ILIKE 'a_b'", @"t")]
    [TestCase(@"SELECT '🙂' LIKE '%_'", @"t")]
    [TestCase(@"SELECT '🙂' LIKE '_%'", @"t")]
    [TestCase(@"SELECT '🙂🙂' LIKE '__'", @"t")]
    [TestCase(@"SELECT '🙂🙂' LIKE '_'", @"f")]
    [TestCase(@"SELECT '🙂' LIKE '🙂'", @"t")]
    [TestCase(@"SELECT '🙂a' LIKE '🙂_'", @"t")]
    [TestCase(@"SELECT 'x🙂' LIKE '%🙂'", @"t")]
    [TestCase(@"SELECT '🙂' LIKE '\🙂'", @"t")]
    [TestCase(@"SELECT 'a🙂b' LIKE 'a_b'", @"t")]
    [TestCase(@"SELECT '🙂x' LIKE '%_x'", @"t")]
    [TestCase(@"SELECT '🙂x' LIKE '%__x'", @"f")]
    [TestCase(@"SELECT 'é' LIKE '_'", @"t")]
    [TestCase(@"SELECT 'a#' LIKE 'a##' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a#b' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'a#' LIKE 'a#' ESCAPE '#'", @"CADB0400")]
    [TestCase(@"SELECT 'b' LIKE 'a#' ESCAPE '#'", @"f")]
    [TestCase(@"SELECT 'a%' LIKE 'a#%' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a#%' ESCAPE '#'", @"f")]
    [TestCase(@"SELECT 'a\b' LIKE 'a\b' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a\b' ESCAPE '#'", @"f")]
    [TestCase(@"SELECT 'a\' LIKE 'a\' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'a_b' LIKE 'a\_b' ESCAPE ''", @"f")]
    [TestCase(@"SELECT 'a%' LIKE 'a%%' ESCAPE '%'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a%%' ESCAPE '%'", @"f")]
    [TestCase(@"SELECT 'ab' LIKE 'a%' ESCAPE '%'", @"CADB0400")]
    [TestCase(@"SELECT 'a_' LIKE 'a__' ESCAPE '_'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a__' ESCAPE '_'", @"f")]
    [TestCase(@"SELECT 'a' LIKE 'a' ESCAPE NULL", @"")]
    [TestCase(@"SELECT NULL LIKE 'a' ESCAPE '#'", @"")]
    [TestCase(@"SELECT 'a' LIKE NULL ESCAPE '#'", @"")]
    [TestCase(@"SELECT 'a' LIKE 'a' ESCAPE 1", @"CADB0400")]
    [TestCase(@"SELECT 'a_' NOT LIKE 'a#_' ESCAPE '#'", @"f")]
    [TestCase(@"SELECT 'ab' NOT LIKE 'a#_' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'A_' ILIKE 'a#_' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 'AB' ILIKE 'a#_' ESCAPE '#'", @"f")]
    [TestCase(@"SELECT 'AB' NOT ILIKE 'a#_' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT '🙂' LIKE '🙂🙂' ESCAPE '🙂'", @"t")]
    [TestCase(@"SELECT 'a' LIKE 'a' ESCAPE '🙂🙂'", @"CADB0400")]
    [TestCase(@"SELECT 'a_' LIKE 'a🙂_' ESCAPE '🙂'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a🙂_' ESCAPE '🙂'", @"f")]
    [TestCase(@"SELECT 'x_' LIKE 'x\_' ESCAPE '\'", @"t")]
    [TestCase(@"SELECT 'xy' LIKE 'x\_' ESCAPE '\'", @"f")]
    [TestCase(@"SELECT 'a_' LIKE 'a#_' ESCAPE ('#' || '')", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a_' ESCAPE '#' AND TRUE", @"t")]
    [TestCase(@"SELECT ('a' LIKE 'a' ESCAPE '#') IS NULL", @"f")]
    [TestCase(@"SELECT 'a' || 'b' LIKE 'a_'", @"t")]
    [TestCase(@"SELECT 'ab' LIKE 'a' || '_'", @"t")]
    [TestCase(@"SELECT 'abc' LIKE 'a_' || 'c' ESCAPE '#'", @"t")]
    [TestCase(@"SELECT 1 LIKE 'a'", @"CADB0407")]
    [TestCase(@"SELECT 'a' LIKE 1", @"CADB0407")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE 'a_' ORDER BY id", @"1 ; 2 ; 3 ; 5")]
    [TestCase(@"SELECT id FROM lk WHERE s NOT LIKE 'a_' ORDER BY id", @"6 ; 7 ; 8")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE 'a\_' ORDER BY id", @"2")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE 'a\%' ORDER BY id", @"3")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE 'a#_' ESCAPE '#' ORDER BY id", @"2")]
    [TestCase(@"SELECT id FROM lk WHERE s NOT LIKE 'a#_' ESCAPE '#' ORDER BY id", @"1 ; 3 ; 5 ; 6 ; 7 ; 8")]
    [TestCase(@"SELECT id FROM lk WHERE s ILIKE 'A_' ORDER BY id", @"1 ; 2 ; 3 ; 5")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE '_' ORDER BY id", @"7")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE '%_' ORDER BY id", @"1 ; 2 ; 3 ; 5 ; 7 ; 8")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE '%' ORDER BY id", @"1 ; 2 ; 3 ; 5 ; 6 ; 7 ; 8")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE p ORDER BY id", @"1 ; 2 ; 3 ; 5 ; 6 ; 7")]
    [TestCase(@"SELECT id FROM lk WHERE s NOT LIKE p ORDER BY id", @"")]
    [TestCase(@"SELECT id FROM lk WHERE 'a' LIKE 'a' ESCAPE s ORDER BY id", @"CADB0400")]
    [TestCase(@"SELECT id FROM lk WHERE id = 1 AND 'a' LIKE 'a' ESCAPE s", @"CADB0400")]
    [TestCase(@"SELECT id FROM lk WHERE id = 6 AND 'a' LIKE 'a' ESCAPE s", @"6")]
    [TestCase(@"SELECT id FROM lk WHERE id = 2 AND 'a' LIKE 'a' ESCAPE s", @"CADB0400")]
    [TestCase(@"SELECT id FROM lk WHERE id = 99 AND s LIKE 'a\'", @"")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE 'a\' ORDER BY id", @"CADB0400")]
    [TestCase(@"SELECT id, s LIKE 'a_' FROM lk ORDER BY id", @"1|t ; 2|t ; 3|t ; 4| ; 5|t ; 6|f ; 7|f ; 8|f")]
    [TestCase(@"SELECT id FROM em WHERE s LIKE 'a\'", @"")]
    [TestCase(@"SELECT id FROM em WHERE s LIKE 'a#' ESCAPE '#'", @"")]
    [TestCase(@"SELECT id FROM em WHERE s LIKE 'a' ESCAPE s", @"")]
    [TestCase(@"SELECT id FROM em WHERE s LIKE 'a' ESCAPE 1", @"CADB0400")]
    [TestCase(@"SELECT id FROM em LIMIT 0", @"")]
    [TestCase(@"SELECT CASE WHEN FALSE THEN 'a' LIKE 'a' ESCAPE 'xy' ELSE TRUE END", @"t")]
    [TestCase(@"SELECT CASE WHEN FALSE THEN 'a' LIKE 'a\' ELSE TRUE END", @"t")]
    [TestCase(@"SELECT escape FROM esc", @"#")]
    [TestCase(@"SELECT 1 AS escape", @"1")]
    [TestCase(@"SELECT e.escape FROM esc e", @"#")]
    [TestCase(@"SELECT id FROM esc ORDER BY escape", @"1")]
    [TestCase(@"SELECT 'a' LIKE 'a' ESCAPE escape FROM esc", @"t")]
    [TestCase(@"SELECT id FROM vl ORDER BY id", @"1 ; 2 ; 3 ; 5")]
    [TestCase(@"SELECT id FROM vl2 ORDER BY id", @"2")]
    [TestCase(@"SELECT 'a' LIKE '%_\'", @"CADB0400")]
    [TestCase(@"SELECT 'a' LIKE 'a\'", @"f")]
    [TestCase(@"SELECT 'bb' LIKE '%b\'", @"CADB0400")]
    [TestCase(@"SELECT like_escape('a#_', '#')", @"a\_")]
    [TestCase(@"SELECT 'a_' LIKE like_escape('a#_', '#')", @"t")]
    [TestCase(@"SELECT like_escape('a', NULL)", @"")]
    [TestCase(@"SELECT like_escape(NULL, '#')", @"")]
    [TestCase(@"SELECT like_escape(1, '#')", @"CADB0400")]
    [TestCase(@"SELECT id FROM lk WHERE s LIKE 'a' ESCAPE (s || 'x')", @"CADB0400")]
    [TestCase(@"SELECT id FROM esc WHERE escape LIKE '#'", @"1")]
    [TestCase(@"SELECT id FROM esc WHERE escape LIKE '#' ESCAPE escape", @"CADB0400")]
    [TestCase(@"SELECT 'a' LIKE escape FROM esc", @"f")]
    [TestCase(@"SELECT id AS Escape FROM esc", @"1")]
    public async Task Statement(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    // ── A pattern or an escape that only a value shows ───────────────────────────────────────────

    private static Dictionary<string, ColumnValue> Text(string name, string? value) => new()
    {
        [name] = value is null ? ColumnValue.Null : new ColumnValue(ColumnType.String, value),
    };

    [TestCase("a_", "1 ; 2 ; 3 ; 5")]
    [TestCase(@"a\_", "2")]
    [TestCase(@"a\%", "3")]
    [TestCase("%", "1 ; 2 ; 3 ; 5 ; 6 ; 7 ; 8")]
    [TestCase(@"\", CamusDBErrorCodes.InvalidInput)]
    public async Task Parameter_AsThePattern(string pattern, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM lk WHERE s LIKE @p ORDER BY id", Text("@p", pattern)), Is.EqualTo(expected));
    }

    [TestCase("#", "2")]
    [TestCase("", "")]
    [TestCase(null, "")]
    [TestCase("xy", CamusDBErrorCodes.InvalidInput)]
    public async Task Parameter_AsTheEscape(string? escape, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM lk WHERE s LIKE 'a#_' ESCAPE @e ORDER BY id", Text("@e", escape)), Is.EqualTo(expected));
    }

    [Test]
    public async Task Parameter_WithHalfOfAPair_CountsAsOneCharacter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        foreach ((string text, string pattern, string expected) in new[]
        {
            ("\uD83D", "_", "t"),
            ("\uD83Dx", "__", "t"),
            ("🙂", "__", "f"),
            ("🙂", "_", "t"),
            ("🙂", "%\uDE42", "f"),
            ("🙂", "%\uDE42%", "f"),
            ("\uDE42", "%\uDE42", "t"),
        })
        {
            Dictionary<string, ColumnValue> parameters = new()
            {
                ["@t"] = new ColumnValue(ColumnType.String, text),
                ["@p"] = new ColumnValue(ColumnType.String, pattern),
            };

            Assert.That(await Run(database, executor, dbname, "SELECT @t LIKE @p", parameters), Is.EqualTo(expected));
        }
    }

    // ── An index does not change the answer ──────────────────────────────────────────────────────

    [TestCase("s LIKE 'a_'", "1 ; 2 ; 3 ; 5")]
    [TestCase(@"s LIKE 'a\_'", "2")]
    [TestCase(@"s LIKE 'a\%'", "3")]
    [TestCase("s NOT LIKE 'a_'", "6 ; 7 ; 8")]
    [TestCase("s LIKE 'a#_' ESCAPE '#'", "2")]
    [TestCase("s ILIKE 'A_'", "1 ; 2 ; 3 ; 5")]
    [TestCase("s LIKE '_'", "7")]
    public async Task IndexedColumn_GivesTheSameRows(string condition, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        await Commit(database, executor, dbname, "CREATE INDEX lk_s ON lk (s)");

        Assert.That(await Run(database, executor, dbname, $"SELECT id FROM lk WHERE {condition} ORDER BY id"), Is.EqualTo(expected), "plain");
        Assert.That(await Run(database, executor, dbname, $"SELECT id FROM lk@{{FORCE_INDEX=lk_s}} WHERE {condition} ORDER BY id"), Is.EqualTo(expected), "forced");
    }

    // ── A stored definition ──────────────────────────────────────────────────────────────────────

    [TestCase("s NOT LIKE 'a_'", "ab", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase("s NOT LIKE 'a_'", "a_", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase("s NOT LIKE 'a_'", "xy", "written")]
    [TestCase("s LIKE 'a_'", "ab", "written")]
    [TestCase("s LIKE 'a_'", "a_", "written")]
    [TestCase("s LIKE 'a_'", "xy", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(@"s LIKE 'a\b'", @"a\b", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(@"s LIKE 'a\b'", "ab", "written")]
    [TestCase("s LIKE 'a#_' ESCAPE '#'", "a_", "written")]
    [TestCase("s LIKE 'a#_' ESCAPE '#'", "ab", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(@"s LIKE 'a\'", "a", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(@"s LIKE 'a\'", "b", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(@"s LIKE 'a\'", "ab", CamusDBErrorCodes.InvalidInput)]
    public async Task Check_FollowsTheRule(string condition, string value, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Commit(database, executor, dbname, $"CREATE TABLE ck (id INT64 PRIMARY KEY, s STRING, CHECK ({condition}))");

        Assert.That(await TryInsert(database, executor, dbname, $"INSERT INTO ck (id, s) VALUES (1, '{value}')"), Is.EqualTo(expected));
    }

    [Test]
    public async Task ShowCreateTable_WithEscape_RunsAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Commit(database, executor, dbname, "CREATE TABLE ck (id INT64 PRIMARY KEY, s STRING, CHECK (s LIKE 'a#_' ESCAPE '#'))");

        string shown = await Run(database, executor, dbname, "SHOW CREATE TABLE ck");
        string ddl = shown[shown.IndexOf("CREATE TABLE", StringComparison.Ordinal)..];

        (string other, DatabaseDescriptor otherDatabase, CommandExecutor otherExecutor) = await CreateDatabase();
        await Commit(otherDatabase, otherExecutor, other, ddl);

        Assert.That(await TryInsert(otherDatabase, otherExecutor, other, "INSERT INTO ck (id, s) VALUES (1, 'a_')"), Is.EqualTo("written"), ddl);
        Assert.That(await TryInsert(otherDatabase, otherExecutor, other, "INSERT INTO ck (id, s) VALUES (1, 'ab')"), Is.EqualTo(CamusDBErrorCodes.CheckConstraintViolation), ddl);
    }

    // ── ESCAPE stays a name ──────────────────────────────────────────────────────────────────────

    [TestCase("SELECT escape.id FROM escape", "1")]
    [TestCase("SELECT e.escape FROM escape e", "#")]
    [TestCase("SELECT Escape FROM escape", "#")]
    [TestCase("SELECT id AS escape FROM escape", "1")]
    [TestCase("SELECT id FROM escape AS escape WHERE escape.id = 1", "1")]
    [TestCase("SELECT 'a_' LIKE 'a#_' escape '#'", "t")]
    [TestCase("SELECT 'a_' LIKE 'a#_' Escape '#'", "t")]
    [TestCase("SELECT id FROM escape WHERE 'a_' LIKE 'a#_' ESCAPE escape", "1")]
    public async Task Escape_IsNotReserved(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Commit(database, executor, dbname, "CREATE TABLE escape (id INT64 PRIMARY KEY, escape STRING)");
        await Commit(database, executor, dbname, "INSERT INTO escape (id, escape) VALUES (1, '#')");

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    // ── SHOW ... LIKE ────────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Show_UsesTheOneMatcher()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string table in new[] { "ck", "ck2", "ck_3", "ckx3" })
            await Commit(database, executor, dbname, $"CREATE TABLE {table} (id INT64 PRIMARY KEY)");

        Assert.That(await Run(database, executor, dbname, "SHOW TABLES LIKE 'ck%'"), Is.EqualTo("ck ; ck2 ; ck_3 ; ckx3"));
        Assert.That(await Run(database, executor, dbname, "SHOW TABLES LIKE 'ck_'"), Is.EqualTo("ck2"));
        Assert.That(await Run(database, executor, dbname, "SHOW TABLES LIKE 'CK%'"), Is.EqualTo(""));
        Assert.That(await Run(database, executor, dbname, "SHOW TABLES LIKE 'ck_3'"), Is.EqualTo("ck_3 ; ckx3"));
        Assert.That(await Run(database, executor, dbname, @"SHOW TABLES LIKE 'ck\_3'"), Is.EqualTo("ck_3"));
        Assert.That(await Run(database, executor, dbname, @"SHOW TABLES LIKE 'c\k'"), Is.EqualTo("ck"));
        Assert.That(await Run(database, executor, dbname, @"SHOW TABLES LIKE 'ck\'"), Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(await Run(database, executor, dbname, @"SHOW TABLES LIKE 'zz\'"), Is.EqualTo(""));
    }

    [Test]
    public async Task Show_Variables_EscapeMatchesTheSameRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string wild = await Run(database, executor, dbname, "SHOW VARIABLES LIKE 'ttl_%'");
        Assert.That(wild, Is.Not.Empty);
        Assert.That(await Run(database, executor, dbname, @"SHOW VARIABLES LIKE 'ttl\_%'"), Is.EqualTo(wild));
    }

    // ── The matcher alone ────────────────────────────────────────────────────────────────────────

    [Test]
    public void Matcher_ALongPatternNeedsNoDeepStack()
    {
        string text = new('a', 200_000);
        string pattern = string.Concat(Enumerable.Repeat("%a", 50_000)) + "%";

        Assert.That(LikeMatcher.Matches(text, pattern, ignoreCase: false), Is.True);
        Assert.That(LikeMatcher.Matches(text, pattern + "b", ignoreCase: false), Is.False);
    }

    [Test]
    public void Matcher_APairFoldsAsOneCharacter()
    {
        const string small = "\U00010428";
        const string capital = "\U00010400";
        bool folds = small.Equals(capital, StringComparison.OrdinalIgnoreCase);

        Assert.That(LikeMatcher.Matches(small, capital, ignoreCase: true), Is.EqualTo(folds), "shortcut");
        Assert.That(LikeMatcher.WalkOnly(small, capital, ignoreCase: true), Is.EqualTo(folds), "walk");
        Assert.That(LikeMatcher.Matches("x" + small, "_" + capital, ignoreCase: true), Is.EqualTo(folds), "underscore");
        Assert.That(LikeMatcher.Matches(small, capital, ignoreCase: false), Is.False);
    }

    private static string RandomString(Random random, string[] alphabet, int maxLength)
    {
        StringBuilder builder = new();
        int length = random.Next(maxLength + 1);

        for (int i = 0; i < length; i++)
            builder.Append(alphabet[random.Next(alphabet.Length)]);

        return builder.ToString();
    }

    /// <summary>
    /// A shortcut must not change an answer, so each pattern shape gives the result of the walk. The patterns
    /// hold only the characters that the shortcuts accept, so most cases reach a shortcut.
    /// </summary>
    [Test]
    public void Matcher_TheShortcutsAgreeWithTheWalk()
    {
        Random random = new(32002);
        string[] text = ["a", "b", "A", "B", "é", "É", "🙂", "\n", "x", "\U00010400", "\U00010428", "\uD83D", "\uDE42"];
        string[] pattern = ["a", "b", "A", "é", "🙂", "\n", "\U00010400", "\uD83D", "\uDE42", "%", "%", "%"];

        for (int i = 0; i < 40_000; i++)
        {
            string t = RandomString(random, text, 6);
            string p = RandomString(random, pattern, 5);
            bool ignoreCase = random.Next(2) == 0;

            Assert.That(LikeMatcher.Matches(t, p, ignoreCase), Is.EqualTo(LikeMatcher.WalkOnly(t, p, ignoreCase)), $"text '{t}' pattern '{p}' ignoreCase {ignoreCase}");
        }
    }

    /// <summary>
    /// Half of a pair in a pattern is one character, so it does not match inside a pair of the text. A
    /// retry after a <c>%</c> must also move over the whole pair, or it would stop at the second half.
    /// </summary>
    [Test]
    public void Matcher_HalfOfAPairInThePattern_DoesNotMatchInsideAPair()
    {
        foreach ((string text, string pattern, bool expected) in new[]
        {
            ("🙂", "%\uDE42", false),
            ("x🙂", "%\uDE42", false),
            ("🙂", "%\uDE42%", false),
            ("\uDE42", "%\uDE42", true),
            ("x\uDE42", "%\uDE42", true),
            ("x\uDE42y", "%\uDE42%", true),
        })
        {
            Assert.That(LikeMatcher.Matches(text, pattern, ignoreCase: false), Is.EqualTo(expected), pattern);
            Assert.That(LikeMatcher.WalkOnly(text, pattern, ignoreCase: false), Is.EqualTo(expected), pattern);
        }
    }

    /// <summary>
    /// A second implementation of the case-sensitive rule, as a regular expression with the single-line option.
    /// Its groups are atomic, because a backtrack must not split a pair. A pattern that ends with the escape
    /// character has no regular expression, so it is skipped.
    /// </summary>
    private static string? ToRegex(string pattern)
    {
        StringBuilder regex = new(@"\A");

        for (int i = 0; i < pattern.Length; i++)
        {
            char c = pattern[i];

            if (c == '%')
            {
                regex.Append(@"(?>[\uD800-\uDBFF][\uDC00-\uDFFF]|.)*");
            }
            else if (c == '_')
            {
                regex.Append(@"(?>[\uD800-\uDBFF][\uDC00-\uDFFF]|.)");
            }
            else if (c == '\\' && ++i == pattern.Length)
            {
                return null;
            }
            else
            {
                regex.Append(Regex.Escape(pattern[i].ToString()));
            }
        }

        return regex.Append(@"\z").ToString();
    }

    [Test]
    public void Matcher_AgreesWithASecondImplementation()
    {
        Random random = new(32003);
        string[] text = ["a", "b", "_", "%", "\\", "é", "🙂", "\n", "x"];
        string[] pattern = ["a", "b", "_", "_", "%", "%", "\\", "é", "🙂", "\n"];
        int matched = 0;

        for (int i = 0; i < 20_000; i++)
        {
            string t = RandomString(random, text, 6);
            string p = RandomString(random, pattern, 6);

            if (ToRegex(p) is not { } regex)
                continue;

            bool expected = Regex.IsMatch(t, regex, RegexOptions.Singleline | RegexOptions.CultureInvariant);
            bool? actual = LikeMatcher.WalkOnly(t, p, ignoreCase: false);

            Assert.That(actual, Is.EqualTo(expected), $"text '{t}' pattern '{p}'");
            matched += expected ? 1 : 0;
        }

        Assert.That(matched, Is.GreaterThan(100), "the cases must include matches");
    }

    [TestCase("a#_", "#", @"a\_")]
    [TestCase("a##", "#", @"a\#")]
    [TestCase(@"a\b", "#", @"a\\b")]
    [TestCase(@"a#\b", "#", @"a\\b")]
    [TestCase("a#", "#", @"a\")]
    [TestCase(@"a\_", "", @"a\\_")]
    [TestCase(@"a\_", @"\", @"a\_")]
    [TestCase("a🙂_", "🙂", @"a\_")]
    public void RewriteEscape_GivesTheBackslashForm(string pattern, string escape, string expected) =>
        Assert.That(LikeMatcher.RewriteEscape(pattern, escape), Is.EqualTo(expected));
}
