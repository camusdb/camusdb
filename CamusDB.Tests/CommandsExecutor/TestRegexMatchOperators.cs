
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
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for the POSIX-style regex match operators: <c>~</c>, <c>~*</c>, <c>!~</c>, <c>!~*</c>.
/// Covers parser, evaluator, CheckEvaluator three-valued NULL semantics,
/// CheckConditionRenderer round-trip, end-to-end WHERE filtering, and CHECK constraint enforcement.
/// </summary>
[NonParallelizable]
public sealed class TestRegexMatchOperators : BaseTest
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task ExecInsert(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task<List<IReadOnlyDictionary<string, ColumnValue>>> ExecQuery(
        CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<IReadOnlyDictionary<string, ColumnValue>> result = [];
        await foreach (QueryResultRow row in rows)
            result.Add(row.Row);
        await db.Transactions.CommitAsync(tx);
        return result;
    }

    // ── parser: each operator parses to the correct NodeType ───────────────────

    [Test]
    public void Parser_RegexMatch_NodeType()
    {
        NodeAst ast = SQLParserProcessor.ParseCondition("name ~ 'foo'");
        Assert.AreEqual(NodeType.ExprRegexMatch, ast.nodeType);
    }

    [Test]
    public void Parser_RegexMatchCi_NodeType()
    {
        NodeAst ast = SQLParserProcessor.ParseCondition("name ~* 'foo'");
        Assert.AreEqual(NodeType.ExprRegexMatchCi, ast.nodeType);
    }

    [Test]
    public void Parser_RegexNotMatch_NodeType()
    {
        NodeAst ast = SQLParserProcessor.ParseCondition("name !~ 'foo'");
        Assert.AreEqual(NodeType.ExprRegexNotMatch, ast.nodeType);
    }

    [Test]
    public void Parser_RegexNotMatchCi_NodeType()
    {
        NodeAst ast = SQLParserProcessor.ParseCondition("name !~* 'foo'");
        Assert.AreEqual(NodeType.ExprRegexNotMatchCi, ast.nodeType);
    }

    [Test]
    public void Parser_RegexMatch_InCheckExpression()
    {
        NodeAst ast = SQLParserProcessor.ParseCondition("username ~ '^[a-zA-Z][a-zA-Z0-9_]{2,29}$'");
        Assert.AreEqual(NodeType.ExprRegexMatch, ast.nodeType);
    }

    [Test]
    public void Parser_RegexNotEquals_Disambiguation()
    {
        // != must parse as not-equals, not a regex operator.
        NodeAst neq = SQLParserProcessor.ParseCondition("a != 'b'");
        Assert.AreEqual(NodeType.ExprNotEquals, neq.nodeType);

        // !~ must parse as regex-not-match.
        NodeAst nrm = SQLParserProcessor.ParseCondition("a !~ 'b'");
        Assert.AreEqual(NodeType.ExprRegexNotMatch, nrm.nodeType);

        // !~* must parse as regex-not-match case-insensitive.
        NodeAst nrmi = SQLParserProcessor.ParseCondition("a !~* 'b'");
        Assert.AreEqual(NodeType.ExprRegexNotMatchCi, nrmi.nodeType);
    }

    [Test]
    public void Parser_RegexIMatchVsMatchStar_Disambiguation()
    {
        // ~* must lex as TREGEXIMATCH (one token), not TREGEXMATCH + TMULT.
        NodeAst ast = SQLParserProcessor.ParseCondition("col ~* 'x'");
        Assert.AreEqual(NodeType.ExprRegexMatchCi, ast.nodeType);
    }

    [Test]
    public void Parser_RegexAndCombined_Precedence()
    {
        // a ~ 'x' AND b ~ 'y' should parse as (a~'x') AND (b~'y').
        NodeAst ast = SQLParserProcessor.ParseCondition("a ~ 'x' AND b ~ 'y'");
        Assert.AreEqual(NodeType.ExprAnd, ast.nodeType);
        Assert.AreEqual(NodeType.ExprRegexMatch, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprRegexMatch, ast.rightAst!.nodeType);
    }

    // ── evaluator: basic match semantics ──────────────────────────────────────

    private static ColumnValue Eval(string exprSql, Dictionary<string, ColumnValue> row)
    {
        NodeAst ast = SQLParserProcessor.ParseCondition(exprSql);
        return SQLExecutorBaseCreator.EvalExpr(ast, row, null, null);
    }

    [Test]
    public void Eval_RegexMatch_True()
    {
        ColumnValue result = Eval("name ~ 'o+b'", new() { ["name"] = new ColumnValue(ColumnType.String, "foobar") });
        Assert.AreEqual(ColumnType.Bool, result.Type);
        Assert.IsTrue(result.BoolValue);
    }

    [Test]
    public void Eval_RegexMatch_False_CaseSensitive()
    {
        ColumnValue result = Eval("name ~ 'foo'", new() { ["name"] = new ColumnValue(ColumnType.String, "FOOBAR") });
        Assert.AreEqual(ColumnType.Bool, result.Type);
        Assert.IsFalse(result.BoolValue);
    }

    [Test]
    public void Eval_RegexMatchCi_True()
    {
        ColumnValue result = Eval("name ~* 'foo'", new() { ["name"] = new ColumnValue(ColumnType.String, "FOOBAR") });
        Assert.AreEqual(ColumnType.Bool, result.Type);
        Assert.IsTrue(result.BoolValue);
    }

    [Test]
    public void Eval_RegexMatch_Unanchored()
    {
        // '^abc' must not match 'xabc'; '^abc$' must match 'abc'.
        ColumnValue anchored = Eval("v ~ '^abc'", new() { ["v"] = new ColumnValue(ColumnType.String, "xabc") });
        Assert.IsFalse(anchored.BoolValue);

        ColumnValue exact = Eval("v ~ '^abc$'", new() { ["v"] = new ColumnValue(ColumnType.String, "abc") });
        Assert.IsTrue(exact.BoolValue);
    }

    [Test]
    public void Eval_RegexNotMatch_IsNegation()
    {
        var row = new Dictionary<string, ColumnValue> { ["v"] = new ColumnValue(ColumnType.String, "foobar") };
        bool matches = Eval("v ~ 'o+'", row).BoolValue;
        bool notMatches = Eval("v !~ 'o+'", row).BoolValue;
        Assert.AreNotEqual(matches, notMatches);
    }

    [Test]
    public void Eval_RegexNotMatchCi_IsNegation()
    {
        var row = new Dictionary<string, ColumnValue> { ["v"] = new ColumnValue(ColumnType.String, "FOOBAR") };
        bool matches = Eval("v ~* 'foo'", row).BoolValue;
        bool notMatches = Eval("v !~* 'foo'", row).BoolValue;
        Assert.AreNotEqual(matches, notMatches);
    }

    [Test]
    public void Eval_NonStringLeft_ThrowsInvalidAst()
    {
        var ex = Assert.Throws<CamusDBException>(() =>
            Eval("price ~ 'foo'", new() { ["price"] = new ColumnValue(ColumnType.Integer64, 42L) }));
        Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, ex!.Code);
    }

    [Test]
    public void Eval_NonStringRight_ThrowsInvalidAst()
    {
        var ex = Assert.Throws<CamusDBException>(() =>
            Eval("name ~ price", new()
            {
                ["name"] = new ColumnValue(ColumnType.String, "abc"),
                ["price"] = new ColumnValue(ColumnType.Integer64, 42L)
            }));
        Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, ex!.Code);
    }

    [Test]
    public void Eval_InvalidPattern_ThrowsDomainException()
    {
        var ex = Assert.Throws<CamusDBException>(() =>
            Eval("v ~ '('", new() { ["v"] = new ColumnValue(ColumnType.String, "abc") }));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("Invalid regular expression", ex.Message);
    }

    [Test]
    public void Eval_PathologicalPattern_HitsTimeout()
    {
        // Catastrophic-backtracking pattern against a non-matching subject must hit the match
        // timeout and surface as a bounded domain error, never hang unbounded.
        string subject = new string('a', 40) + "!";
        var ex = Assert.Throws<CamusDBException>(() =>
            Eval("v ~ '^(a+)+$'", new() { ["v"] = new ColumnValue(ColumnType.String, subject) }));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("time limit", ex.Message);
    }

    [Test]
    public void Eval_LikeMultiWildcard_StillMatches()
    {
        // The multi-wildcard LIKE fallback now routes through RegexMatcher; behavior is unchanged.
        Assert.IsTrue(Eval("v LIKE 'a%b%c'",
            new() { ["v"] = new ColumnValue(ColumnType.String, "axbxc") }).BoolValue);
        Assert.IsFalse(Eval("v LIKE 'a%b%c'",
            new() { ["v"] = new ColumnValue(ColumnType.String, "axc") }).BoolValue);
    }

    // ── CheckEvaluator: three-valued NULL semantics ────────────────────────────

    [Test]
    public void CheckEval_RegexMatch_True()
    {
        NodeAst cond = SQLParserProcessor.ParseCondition("name ~ '^[a-z]+$'");
        bool? result = CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
            { ["name"] = new ColumnValue(ColumnType.String, "alice") });
        Assert.AreEqual(true, result);
    }

    [Test]
    public void CheckEval_RegexMatch_False()
    {
        NodeAst cond = SQLParserProcessor.ParseCondition("name ~ '^[a-z]+$'");
        bool? result = CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
            { ["name"] = new ColumnValue(ColumnType.String, "1abc") });
        Assert.AreEqual(false, result);
    }

    [Test]
    public void CheckEval_RegexMatch_NullOperand_IsUnknown()
    {
        NodeAst cond = SQLParserProcessor.ParseCondition("name ~ '^[a-z]+$'");
        bool? result = CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
            { ["name"] = new ColumnValue(ColumnType.Null, null) });
        Assert.IsNull(result, "NULL operand must yield UNKNOWN so the row passes");
    }

    [Test]
    public void CheckEval_RegexNotMatch_NullOperand_IsUnknown()
    {
        NodeAst cond = SQLParserProcessor.ParseCondition("name !~ '^[a-z]+$'");
        bool? result = CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
            { ["name"] = new ColumnValue(ColumnType.Null, null) });
        Assert.IsNull(result, "NULL operand must yield UNKNOWN even for !~");
    }

    [Test]
    public void CheckEval_RegexMatchCi_NullOperand_IsUnknown()
    {
        NodeAst cond = SQLParserProcessor.ParseCondition("name ~* '^[a-z]+$'");
        bool? result = CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
            { ["name"] = new ColumnValue(ColumnType.Null, null) });
        Assert.IsNull(result);
    }

    [Test]
    public void CheckEval_RegexNotMatchCi_NullOperand_IsUnknown()
    {
        NodeAst cond = SQLParserProcessor.ParseCondition("name !~* '^[a-z]+$'");
        bool? result = CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
            { ["name"] = new ColumnValue(ColumnType.Null, null) });
        Assert.IsNull(result);
    }

    [Test]
    public void CheckEval_InvalidPattern_IsCheckViolation()
    {
        // A malformed regex inside a CHECK must reject the row with a 400-class violation,
        // not escape as a 500 InvalidInput.
        NodeAst cond = SQLParserProcessor.ParseCondition("name ~ '('");
        var ex = Assert.Throws<CamusDBException>(() =>
            CheckEvaluator.Evaluate(cond, new Dictionary<string, ColumnValue>
                { ["name"] = new ColumnValue(ColumnType.String, "abc") }));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(ex.Code));
    }

    // ── CheckConditionRenderer: round-trip ────────────────────────────────────

    private static void AssertRoundTrip(string exprSql)
    {
        NodeAst ast1 = SQLParserProcessor.ParseCondition(exprSql);
        string rendered1 = PlanRenderer.RenderExpr(ast1);
        NodeAst ast2 = SQLParserProcessor.ParseCondition(rendered1);
        string rendered2 = PlanRenderer.RenderExpr(ast2);
        Assert.AreEqual(rendered1, rendered2, $"Round-trip failed for: {exprSql}");
    }

    [Test]
    public void RoundTrip_RegexMatch() => AssertRoundTrip("col ~ 'foo'");

    [Test]
    public void RoundTrip_RegexMatchCi() => AssertRoundTrip("col ~* 'foo'");

    [Test]
    public void RoundTrip_RegexNotMatch() => AssertRoundTrip("col !~ 'foo'");

    [Test]
    public void RoundTrip_RegexNotMatchCi() => AssertRoundTrip("col !~* 'foo'");

    [Test]
    public void RoundTrip_RegexMatch_PatternWithAnchors()
    {
        AssertRoundTrip("username ~ '^[a-zA-Z][a-zA-Z0-9_]{2,29}$'");
    }

    // ── end-to-end WHERE filtering ─────────────────────────────────────────────

    [Test]
    public async Task EndToEnd_WhereRegexMatch_FiltersRows()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE words (id object_id DEFAULT(gen_id()) PRIMARY KEY, word text NOT NULL)");

        foreach (string w in new[] { "alpha", "BETA", "gamma123", "DELTA_9" })
            await ExecInsert(executor, dbname, $"INSERT INTO words (word) VALUES ('{w}')");

        // Only "alpha" consists entirely of lowercase letters.
        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SELECT word FROM words WHERE word ~ '^[a-z]+$'");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("alpha", rows[0]["word"].StrValue);
    }

    [Test]
    public async Task EndToEnd_WhereRegexMatchCi_CaseInsensitive()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id DEFAULT(gen_id()) PRIMARY KEY, name text NOT NULL)");

        foreach (string n in new[] { "Apple", "banana", "Apricot", "cherry" })
            await ExecInsert(executor, dbname, $"INSERT INTO items (name) VALUES ('{n}')");

        // Case-insensitive '^a': Apple and Apricot, not banana/cherry.
        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SELECT name FROM items WHERE name ~* '^a'");

        var names = rows.Select(r => r["name"].StrValue).OrderBy(s => s).ToList();
        Assert.AreEqual(2, names.Count);
        CollectionAssert.Contains(names, "Apple");
        CollectionAssert.Contains(names, "Apricot");
    }

    [Test]
    public async Task EndToEnd_WhereRegexNotMatch_Filters()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE tags (id object_id DEFAULT(gen_id()) PRIMARY KEY, tag text NOT NULL)");

        foreach (string t in new[] { "foo", "bar", "foobar", "baz" })
            await ExecInsert(executor, dbname, $"INSERT INTO tags (tag) VALUES ('{t}')");

        // !~ 'foo' → bar and baz (those not containing 'foo').
        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SELECT tag FROM tags WHERE tag !~ 'foo'");

        var tags = rows.Select(r => r["tag"].StrValue).OrderBy(s => s).ToList();
        Assert.AreEqual(2, tags.Count);
        CollectionAssert.Contains(tags, "bar");
        CollectionAssert.Contains(tags, "baz");
    }

    // ── CHECK constraint enforcement ──────────────────────────────────────────

    [Test]
    public async Task CheckConstraint_RegexMatch_ValidInsert()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname, @"
            CREATE TABLE users (
                id object_id DEFAULT(gen_id()) PRIMARY KEY,
                username text NOT NULL,
                CONSTRAINT username_format CHECK (username ~ '^[a-zA-Z][a-zA-Z0-9_]{2,29}$')
            )");

        await ExecInsert(executor, dbname, "INSERT INTO users (username) VALUES ('alice123')");
        await ExecInsert(executor, dbname, "INSERT INTO users (username) VALUES ('Bob_Smith')");
    }

    [Test]
    public async Task CheckConstraint_RegexMatch_InvalidInsertRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname, @"
            CREATE TABLE users (
                id object_id DEFAULT(gen_id()) PRIMARY KEY,
                username text NOT NULL,
                CONSTRAINT username_format CHECK (username ~ '^[a-zA-Z][a-zA-Z0-9_]{2,29}$')
            )");

        // Starts with digit.
        var ex1 = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecInsert(executor, dbname, "INSERT INTO users (username) VALUES ('1abc')"));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex1!.Code);

        // Too short (< 3 chars after the leading letter).
        var ex2 = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecInsert(executor, dbname, "INSERT INTO users (username) VALUES ('ab')"));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex2!.Code);

        // Contains space.
        var ex3 = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecInsert(executor, dbname, "INSERT INTO users (username) VALUES ('has space')"));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex3!.Code);
    }

    [Test]
    public async Task CheckConstraint_RegexMatch_NullColumnPasses()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname, @"
            CREATE TABLE profiles (
                id object_id DEFAULT(gen_id()) PRIMARY KEY,
                nickname text,
                CONSTRAINT nick_format CHECK (nickname ~ '^[a-z]+$')
            )");

        // NULL nickname: UNKNOWN → row passes.
        await ExecInsert(executor, dbname, "INSERT INTO profiles (nickname) VALUES (null)");

        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await ExecQuery(executor, dbname, "SELECT nickname FROM profiles");
        Assert.AreEqual(1, rows.Count);
    }

    [Test]
    public async Task CheckConstraint_RegexNotMatch_Enforced()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname, @"
            CREATE TABLE products (
                id object_id DEFAULT(gen_id()) PRIMARY KEY,
                sku text NOT NULL,
                CONSTRAINT no_spaces CHECK (sku !~ '\s')
            )");

        // No whitespace — valid.
        await ExecInsert(executor, dbname, "INSERT INTO products (sku) VALUES ('ABC-001')");

        // Contains space — rejected.
        var ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecInsert(executor, dbname, "INSERT INTO products (sku) VALUES ('bad sku')"));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);
    }

    [Test]
    public async Task CheckConstraint_MalformedRegex_RejectedAtDdlTime()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        // An unclosed group '(' is an invalid pattern; it must fail at CREATE TABLE, not lie
        // dormant until the first INSERT.
        var ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDDL(executor, dbname, @"
                CREATE TABLE bad (
                    id object_id DEFAULT(gen_id()) PRIMARY KEY,
                    name text NOT NULL,
                    CONSTRAINT bad_re CHECK (name ~ '(')
                )"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("Invalid regular expression", ex.Message);
    }

    [Test]
    public async Task CheckConstraint_MalformedRegex_RejectedAtAlterTime()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE things (id object_id DEFAULT(gen_id()) PRIMARY KEY, name text NOT NULL)");

        var ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDDL(executor, dbname,
                "ALTER TABLE things ADD CONSTRAINT bad_re CHECK (name ~ '[')"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("Invalid regular expression", ex.Message);
    }

    [Test]
    public async Task CheckConstraint_Persistence_ReopenEnforces()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname, @"
            CREATE TABLE codes (
                id object_id DEFAULT(gen_id()) PRIMARY KEY,
                code text NOT NULL,
                CONSTRAINT code_format CHECK (code ~ '^[A-Z]{3}-[0-9]{3}$')
            )");

        // Valid insert before reopen.
        await ExecInsert(executor, dbname, "INSERT INTO codes (code) VALUES ('ABC-123')");

        // Close and reopen.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        // Constraint re-parses from persisted SQL text and still enforces.
        var ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecInsert(executor, dbname, "INSERT INTO codes (code) VALUES ('invalid')"));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);

        // Valid insert still works.
        await ExecInsert(executor, dbname, "INSERT INTO codes (code) VALUES ('XYZ-999')");
    }
}
