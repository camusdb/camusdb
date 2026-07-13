
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for the Phase-1 <c>regexp_*</c> scalar functions:
/// regexp_like, regexp_match, regexp_replace, regexp_count, regexp_instr,
/// regexp_substr, regexp_split_to_array.
/// Also asserts that the Phase-2 set-returning functions (regexp_matches,
/// regexp_split_to_table) return a clear "not supported" error.
/// </summary>
[NonParallelizable]
public sealed class TestRegexpFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "strings",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("val", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        string[] values =
        [
            "hello world",
            "HELLO WORLD",
            "foo123bar",
            "abc def ghi",
            "one,two,three",
            "2024-01-15",
        ];

        foreach (string v in values)
        {
            await executor.Insert(new InsertTicket(
                txnState: txnState,
                databaseName: dbname,
                tableName: "strings",
                values: new() {
                    new() {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "val", new(ColumnType.String, v) },
                    }
                }));
        }

        await database.Transactions.CommitAsync(txnState);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<CamusDBException> AssertQueryThrows(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        return Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;
    }

    // ── regexp_like ────────────────────────────────────────────────────────────

    [Test]
    public async Task RegexpLike_Match_ReturnsTrue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_like('hello world', 'world') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Bool, rows[0].Row["0"].Type);
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task RegexpLike_NoMatch_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_like('hello world', 'xyz') FROM strings LIMIT 1");
        Assert.IsFalse(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task RegexpLike_IFlag_CaseInsensitive()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_like('HELLO', 'hello', 'i') FROM strings LIMIT 1");
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task RegexpLike_NullText_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_like(NULL, 'hello') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    [Test]
    public async Task RegexpLike_ParityWithTildeOperator()
    {
        // regexp_like and ~ must agree on the same input.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_like(val, 'hello'), (val ~ 'hello') FROM strings WHERE val = 'hello world' LIMIT 1");
        Assert.AreEqual(rows[0].Row["0"].BoolValue, rows[0].Row["1"].BoolValue);
    }

    // ── regexp_match ──────────────────────────────────────────────────────────

    [Test]
    public async Task RegexpMatch_NoMatch_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_match('hello', 'xyz') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    [Test]
    public async Task RegexpMatch_NoGroup_ReturnsSingleElementArray()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_match('hello world', 'world') FROM strings LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        Assert.AreEqual(ColumnType.Array, cv.Type);
        Assert.AreEqual(1, cv.ArrayValues!.Count);
        Assert.AreEqual("world", cv.ArrayValues[0].StrValue);
    }

    [Test]
    public async Task RegexpMatch_CaptureGroups_ReturnsGroupArray()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_match('foo123bar', '([a-z]+)(\\d+)([a-z]+)') FROM strings LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        Assert.AreEqual(ColumnType.Array, cv.Type);
        Assert.AreEqual(3, cv.ArrayValues!.Count);
        Assert.AreEqual("foo", cv.ArrayValues[0].StrValue);
        Assert.AreEqual("123", cv.ArrayValues[1].StrValue);
        Assert.AreEqual("bar", cv.ArrayValues[2].StrValue);
    }

    [Test]
    public async Task RegexpMatch_NonParticipatingGroup_NullElement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // Pattern has two alternation groups; only one participates.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_match('hello', '(foo)|(hello)') FROM strings LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        Assert.AreEqual(ColumnType.Array, cv.Type);
        Assert.AreEqual(ColumnType.Null, cv.ArrayValues![0].Type);  // group 1 did not participate
        Assert.AreEqual("hello", cv.ArrayValues[1].StrValue);        // group 2 matched
    }

    // ── regexp_replace ────────────────────────────────────────────────────────

    [Test]
    public async Task RegexpReplace_FirstOnly_Default()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace('aabbcc', 'b', 'X') FROM strings LIMIT 1");
        Assert.AreEqual("aaXbcc", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpReplace_GlobalFlag_ReplacesAll()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace('aabbcc', 'b', 'X', 'g') FROM strings LIMIT 1");
        Assert.AreEqual("aaXXcc", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpReplace_BackrefGroup_Translated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // \1 references group 1
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace('2024-01-15', '(\\d{4})-(\\d{2})-(\\d{2})', '\\3/\\2/\\1') FROM strings LIMIT 1");
        Assert.AreEqual("15/01/2024", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpReplace_BackrefWholeMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // \& refers to the whole match
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace('hello', 'ell', '[\\&]') FROM strings LIMIT 1");
        Assert.AreEqual("h[ell]o", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpReplace_LiteralDollar_NotTreatedAsGroup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // '$1' is a literal in a PostgreSQL replacement (only backslash is a back-reference), so it
        // must NOT be substituted as .NET group 1. First match of '(o)' in 'foo' → 'x$1'.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace('foo', '(o)', 'x$1') FROM strings LIMIT 1");
        Assert.AreEqual("fx$1o", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpReplace_MixedBackrefAndLiteralDollar()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // Backslash back-references are honored; a literal '$' is preserved.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace('ab', '(a)(b)', '\\1-$2') FROM strings LIMIT 1");
        Assert.AreEqual("a-$2", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpReplace_NullArg_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_replace(NULL, 'x', 'y') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    // ── regexp_count ──────────────────────────────────────────────────────────

    [Test]
    public async Task RegexpCount_ZeroMatches()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_count('hello', 'xyz') FROM strings LIMIT 1");
        Assert.AreEqual(0L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpCount_MultipleMatches()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_count('ababab', 'ab') FROM strings LIMIT 1");
        Assert.AreEqual(3L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpCount_StartOffset_SkipFirst()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // "abab": start at position 3 (1-based) skips the first "ab"
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_count('abab', 'ab', 3) FROM strings LIMIT 1");
        Assert.AreEqual(1L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpCount_StartLessThanOne_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        CamusDBException ex = await AssertQueryThrows(executor, database, dbname,
            "SELECT regexp_count('hello', 'h', 0) FROM strings LIMIT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public async Task RegexpCount_CaretAnchorsToStringStart_NotToOffset()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // '^a' on 'xax' matches only at the true string start (position 0), where the char is 'x'.
        // With start=2 the search begins past position 0, so '^' can never match → 0.
        // (A slice-based implementation would wrongly re-anchor '^' to the slice and return 1.)
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_count('xax', '^a', 2) FROM strings LIMIT 1");
        Assert.AreEqual(0L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpCount_NullText_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_count(NULL, 'x') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    // ── regexp_instr ──────────────────────────────────────────────────────────

    [Test]
    public async Task RegexpInstr_FirstMatch_Position()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // "hello world": 'world' starts at index 6 (0-based) → 7 (1-based)
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_instr('hello world', 'world') FROM strings LIMIT 1");
        Assert.AreEqual(7L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpInstr_NoMatch_ReturnsZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_instr('hello', 'xyz') FROM strings LIMIT 1");
        Assert.AreEqual(0L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpInstr_EndOption1_PositionAfterEnd()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // 'world' is at 7..11; endoption=1 → 12
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_instr('hello world', 'world', 1, 1, 1) FROM strings LIMIT 1");
        Assert.AreEqual(12L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpInstr_NthMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // "abab": 2nd 'ab' starts at index 2 → 1-based 3
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_instr('abab', 'ab', 1, 2) FROM strings LIMIT 1");
        Assert.AreEqual(3L, rows[0].Row["0"].LongValue);
    }

    // ── regexp_substr ─────────────────────────────────────────────────────────

    [Test]
    public async Task RegexpSubstr_FirstMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_substr('hello world', '\\w+') FROM strings LIMIT 1");
        Assert.AreEqual("hello", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpSubstr_NoMatch_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_substr('hello', 'xyz') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    [Test]
    public async Task RegexpSubstr_NthMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // "abc def ghi": 2nd word is "def"
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_substr('abc def ghi', '\\w+', 1, 2) FROM strings LIMIT 1");
        Assert.AreEqual("def", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task RegexpSubstr_SubexprGroup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // Extract just the year from ISO date
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_substr('2024-01-15', '(\\d{4})-(\\d{2})-(\\d{2})', 1, 1, NULL, 1) FROM strings LIMIT 1");
        Assert.AreEqual("2024", rows[0].Row["0"].StrValue);
    }

    // ── regexp_split_to_array ─────────────────────────────────────────────────

    [Test]
    public async Task RegexpSplitToArray_Normal()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_split_to_array('one,two,three', ',') FROM strings LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        Assert.AreEqual(ColumnType.Array, cv.Type);
        Assert.AreEqual(3, cv.ArrayValues!.Count);
        Assert.AreEqual("one", cv.ArrayValues[0].StrValue);
        Assert.AreEqual("two", cv.ArrayValues[1].StrValue);
        Assert.AreEqual("three", cv.ArrayValues[2].StrValue);
    }

    [Test]
    public async Task RegexpSplitToArray_NoMatch_WholeStringInArray()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_split_to_array('hello', 'xyz') FROM strings LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        Assert.AreEqual(1, cv.ArrayValues!.Count);
        Assert.AreEqual("hello", cv.ArrayValues[0].StrValue);
    }

    [Test]
    public async Task RegexpSplitToArray_EmptyPattern_SplitsToChars()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_split_to_array('abc', '') FROM strings LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        // PostgreSQL: empty pattern splits into individual characters.
        Assert.AreEqual(3, cv.ArrayValues!.Count);
        Assert.AreEqual("a", cv.ArrayValues[0].StrValue);
        Assert.AreEqual("b", cv.ArrayValues[1].StrValue);
        Assert.AreEqual("c", cv.ArrayValues[2].StrValue);
    }

    [Test]
    public async Task RegexpSplitToArray_NullText_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_split_to_array(NULL, ',') FROM strings LIMIT 1");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    // ── Flags ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Flags_UnknownFlag_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        CamusDBException ex = await AssertQueryThrows(executor, database, dbname,
            "SELECT regexp_like('hello', 'h', 'z') FROM strings LIMIT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("'z'"));
    }

    [Test]
    public async Task Flags_M_MultilineAnchors()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        // Insert a row with a real newline via the Insert API (the SQL parser does not support \n literals).
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: dbname,
            tableName: "strings",
            values: new() {
                new() {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "val", new(ColumnType.String, "line1\nline2\nline3") },
                }
            }));
        await database.Transactions.CommitAsync(tx);

        // With 'm', ^ matches at the start of each line — expects 3 matches.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_count(val, '^line', 1, 'm') FROM strings WHERE val ~ 'line1' LIMIT 1");
        Assert.AreEqual(3L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task Flags_S_DotMatchesNewline()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        // Insert a uniquely-named row with a real newline via the Insert API
        // (the SQL parser does not support \n escape sequences in string literals).
        string sentinel = "SINGLELINE_TEST\nNEWLINE";
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: dbname,
            tableName: "strings",
            values: new() {
                new() {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "val", new(ColumnType.String, sentinel) },
                }
            }));
        await database.Transactions.CommitAsync(tx);

        // With 's', '.' matches \n so the pattern 'TEST.NEWLINE' matches the inserted string.
        // Use a unique prefix so the WHERE clause selects only this row.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_like(val, 'TEST.NEWLINE', 's') FROM strings WHERE val ~ 'SINGLELINE' LIMIT 1");
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    // ── Safety ────────────────────────────────────────────────────────────────

    [Test]
    public async Task InvalidPattern_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        CamusDBException ex = await AssertQueryThrows(executor, database, dbname,
            "SELECT regexp_like('hello', '[invalid') FROM strings LIMIT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("Invalid regular expression"));
    }

    [Test]
    public async Task PathologicalPattern_HitsTimeout()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // Catastrophic backtracking against a non-matching subject must hit the shared match
        // timeout and surface as a bounded domain error, not hang unbounded.
        string subject = new string('a', 40) + "!";
        CamusDBException ex = await AssertQueryThrows(executor, database, dbname,
            $"SELECT regexp_like('{subject}', '^(a+)+$') FROM strings LIMIT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("time limit"));
    }

    // ── Integration: array through projection ─────────────────────────────────

    [Test]
    public async Task RegexpMatch_ThroughProjection_ReturnsArray()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT regexp_match(val, '(\\w+)') FROM strings WHERE val = 'hello world' LIMIT 1");
        ColumnValue cv = rows[0].Row["0"];
        Assert.AreEqual(ColumnType.Array, cv.Type);
        Assert.AreEqual("hello", cv.ArrayValues![0].StrValue);
    }

    [Test]
    public async Task RegexpSplitToArray_InDerivedTable_DoesNotCrash()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        // Derived-table typing must tolerate ColumnType.Array from InferReturnType.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT parts FROM (SELECT regexp_split_to_array(val, ',') AS parts FROM strings WHERE val = 'one,two,three' LIMIT 1) x");
        ColumnValue cv = rows[0].Row["parts"];
        Assert.AreEqual(ColumnType.Array, cv.Type);
        Assert.AreEqual(3, cv.ArrayValues!.Count);
    }

    // ── Phase-2 set-returning functions: clear error ──────────────────────────

    [Test]
    public async Task RegexpMatches_NotSupported_ClearError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        CamusDBException ex = await AssertQueryThrows(executor, database, dbname,
            "SELECT regexp_matches('hello', 'h') FROM strings LIMIT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("set-returning function"));
    }

    [Test]
    public async Task RegexpSplitToTable_NotSupported_ClearError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        CamusDBException ex = await AssertQueryThrows(executor, database, dbname,
            "SELECT regexp_split_to_table('a,b', ',') FROM strings LIMIT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("set-returning function"));
    }
}
