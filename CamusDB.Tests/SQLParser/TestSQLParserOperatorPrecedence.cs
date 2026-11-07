
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The grouping that the precedence table gives. The renderer writes each grouping out with parentheses, so its
/// text shows the tree in one line. It is also the text that a stored view or CHECK holds, so
/// <see cref="StoredText_ParsesBackToTheSameGrouping"/> is the reason a change of this table does not change the
/// meaning of a stored definition.
///
/// <para>The expected groupings follow the table of PostgreSQL. <c>TestOperatorPrecedence</c> holds the results
/// measured on PostgreSQL through the whole engine.</para>
/// </summary>
[TestFixture]
internal sealed class TestSQLParserOperatorPrecedence
{
    private static string Grouping(string expression) =>
        CheckConditionRenderer.Render(SQLParserProcessor.ParseCondition(expression));

    private const string Forms = nameof(GroupingForms);

    private static readonly object[] GroupingForms =
    [
        // IS is below the comparisons and above NOT.
        new object[] { "a + 1 IS NULL", "(a + 1) IS NULL" },
        new object[] { "a || b IS NOT NULL", "(a || b) IS NOT NULL" },
        new object[] { "a = b IS NULL", "(a = b) IS NULL" },
        new object[] { "a <> b IS TRUE", "(a <> b) IS TRUE" },
        new object[] { "a < b IS NOT TRUE", "(a < b) IS NOT TRUE" },
        new object[] { "a >= b IS FALSE", "(a >= b) IS FALSE" },
        new object[] { "a <= b IS NOT FALSE", "(a <= b) IS NOT FALSE" },
        new object[] { "a IS NULL = b", "(a IS NULL) = b" },
        new object[] { "a IS NULL IS NULL", "(a IS NULL) IS NULL" },
        new object[] { "NOT a IS NULL", "NOT (a IS NULL)" },
        new object[] { "NOT a = b IS NULL", "NOT ((a = b) IS NULL)" },
        new object[] { "a AND b IS NULL", "a AND (b IS NULL)" },
        new object[] { "a OR b IS TRUE", "a OR (b IS TRUE)" },
        new object[] { "- a IS NULL", "(-a) IS NULL" },

        // BETWEEN, IN and LIKE are above the comparisons and below every other operator.
        new object[] { "a = b IN (1)", "a = (b IN (1))" },
        new object[] { "a <> b NOT IN (1)", "a <> (b NOT IN (1))" },
        new object[] { "a IN (1) = b", "(a IN (1)) = b" },
        new object[] { "a IN (1) IS NULL", "(a IN (1)) IS NULL" },
        new object[] { "a + 1 IN (2)", "(a + 1) IN (2)" },
        new object[] { "a + 1 NOT IN (2)", "(a + 1) NOT IN (2)" },
        new object[] { "a || b IN ('x')", "(a || b) IN ('x')" },
        new object[] { "NOT a IN (1)", "NOT (a IN (1))" },
        new object[] { "a = b BETWEEN 1 AND 2", "a = (b BETWEEN 1 AND 2)" },
        new object[] { "a = b NOT BETWEEN 1 AND 2", "a = (NOT (b BETWEEN 1 AND 2))" },
        new object[] { "a < b BETWEEN 1 AND 2", "a < (b BETWEEN 1 AND 2)" },
        new object[] { "a BETWEEN 1 AND 2 = b", "(a BETWEEN 1 AND 2) = b" },
        new object[] { "a BETWEEN 1 AND 2 IS NULL", "(a BETWEEN 1 AND 2) IS NULL" },
        new object[] { "a + 1 BETWEEN 1 AND 2", "(a + 1) BETWEEN 1 AND 2" },
        new object[] { "a BETWEEN 1 AND b || c", "a BETWEEN 1 AND (b || c)" },
        new object[] { "a BETWEEN 1 AND b ~ c", "a BETWEEN 1 AND (b ~ c)" },
        new object[] { "a BETWEEN 1 AND b !~* c", "a BETWEEN 1 AND (b !~* c)" },
        new object[] { "a LIKE b = c", "(a LIKE b) = c" },
        new object[] { "a = b LIKE c", "a = (b LIKE c)" },
        new object[] { "a = b NOT LIKE c", "a = (NOT (b LIKE c))" },
        new object[] { "a < b ILIKE c", "a < (b ILIKE c)" },
        new object[] { "a < b NOT ILIKE c", "a < (NOT (b ILIKE c))" },
        new object[] { "a LIKE b IS NULL", "(a LIKE b) IS NULL" },
        new object[] { "a || b LIKE c", "(a || b) LIKE c" },
        new object[] { "a LIKE b || c", "a LIKE (b || c)" },
        new object[] { "NOT a LIKE b", "NOT (a LIKE b)" },
        new object[] { "NOT a NOT LIKE b", "NOT (NOT (a LIKE b))" },
        new object[] { "a AND b NOT LIKE c", "a AND (NOT (b LIKE c))" },
        new object[] { "a LIKE b ESCAPE c = d", "(a LIKE like_escape(b, c)) = d" },
        new object[] { "a = b LIKE c ESCAPE d", "a = (b LIKE like_escape(c, d))" },

        // The regular expression and jsonb operators share the level of || and group to the left.
        new object[] { "a ~ b = c", "(a ~ b) = c" },
        new object[] { "a = b ~ c", "a = (b ~ c)" },
        new object[] { "a ~ b || c", "(a ~ b) || c" },
        new object[] { "a || b ~ c", "(a || b) ~ c" },
        new object[] { "a ~ b LIKE c", "(a ~ b) LIKE c" },
        new object[] { "a LIKE b ~ c", "a LIKE (b ~ c)" },
        new object[] { "a ~* b IS NULL", "(a ~* b) IS NULL" },
        new object[] { "a + 1 ~ b", "(a + 1) ~ b" },
        new object[] { "a !~ b !~* c", "(a !~ b) !~* c" },
        new object[] { "a ->> b IS NULL", "jsonb_object_field_text(a, b) IS NULL" },
        new object[] { "a ->> b = c", "jsonb_object_field_text(a, b) = c" },
        new object[] { "a ->> b IN ('x')", "jsonb_object_field_text(a, b) IN ('x')" },
        new object[] { "a ->> b LIKE c", "jsonb_object_field_text(a, b) LIKE c" },
        new object[] { "a -> 0 + 1", "jsonb_object_field(a, 0 + 1)" },
        new object[] { "a -> b -> c", "jsonb_object_field(jsonb_object_field(a, b), c)" },
        new object[] { "a || b ->> c", "jsonb_object_field_text(a || b, c)" },
        new object[] { "a + b ->> c", "jsonb_object_field_text(a + b, c)" },
        new object[] { "a ->> b || c", "jsonb_object_field_text(a, b) || c" },
        new object[] { "a @> b IS TRUE", "jsonb_contains(a, b) IS TRUE" },
        new object[] { "a ? b = c", "jsonb_exists(a, b) = c" },
        new object[] { "NOT a ? b", "NOT jsonb_exists(a, b)" },

        // The levels that did not move.
        new object[] { "NOT a = b", "NOT (a = b)" },
        new object[] { "NOT a AND b", "(NOT a) AND b" },
        new object[] { "NOT a OR b", "(NOT a) OR b" },
        new object[] { "a OR b AND c", "a OR (b AND c)" },
        new object[] { "a AND b = c", "a AND (b = c)" },
        new object[] { "a + 1 = b", "(a + 1) = b" },
        new object[] { "a || b = c", "(a || b) = c" },
        new object[] { "1 + 2 & 3", "(1 + 2) & 3" },
        new object[] { "a * b + c", "(a * b) + c" },
        new object[] { "a + b * c", "a + (b * c)" },
    ];

    [TestCaseSource(Forms)]
    public void Grouping_FollowsThePostgreSqlTable(string expression, string expected)
    {
        Assert.That(Grouping(expression), Is.EqualTo(expected));
    }

    /// <summary>
    /// A stored view or CHECK holds rendered text. That text writes each grouping out, so it parses back to the
    /// same tree on any precedence table, also a later one.
    /// </summary>
    [TestCaseSource(Forms)]
    public void StoredText_ParsesBackToTheSameGrouping(string expression, string expected)
    {
        Assert.That(Grouping(Grouping(expression)), Is.EqualTo(expected));
    }

    /// <summary>
    /// The text that the old table stored for each form. Each one must keep the grouping it shows, so a definition
    /// created before the table changed reads as it did.
    /// </summary>
    [TestCase("a = (b IS NULL)")]
    [TestCase("a + (b IS NULL)")]
    [TestCase("(a = b) = c")]
    [TestCase("a = (b < c)")]
    [TestCase("a = (b IN (1))")]
    [TestCase("(a = b) IN (1)")]
    [TestCase("(a = b) LIKE c")]
    [TestCase("(a = b) BETWEEN 1 AND 2")]
    [TestCase("a ~ (b || c)")]
    [TestCase("a LIKE (b = c)")]
    public void TextStoredByTheOldTable_KeepsItsGrouping(string stored)
    {
        Assert.That(Grouping(stored), Is.EqualTo(stored));
    }

    /// <summary>PostgreSQL gives a syntax error for each of these, because a reader cannot say what they compare.</summary>
    [TestCase("a = b = c")]
    [TestCase("a <> b <> c")]
    [TestCase("a < b < c")]
    [TestCase("a = b < c")]
    [TestCase("a < b = c")]
    [TestCase("a >= b <= c")]
    [TestCase("a LIKE b LIKE c")]
    [TestCase("a ILIKE b LIKE c")]
    [TestCase("a NOT LIKE b LIKE c")]
    [TestCase("a LIKE b NOT LIKE c")]
    [TestCase("a LIKE b IN (1)")]
    [TestCase("a LIKE b BETWEEN 1 AND 2")]
    [TestCase("a + 1 < b < c")]
    public void Chain_IsASyntaxError(string expression)
    {
        CamusDBException? e = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT " + expression + " FROM t"));
        Assert.That(e!.Code, Is.EqualTo(CamusDBErrorCodes.SqlSyntaxError));
    }

    /// <summary>
    /// Parentheses write either grouping of a chain, so each refused chain has a way to say what it means.
    /// </summary>
    [TestCase("(a = b) = c")]
    [TestCase("a = (b = c)")]
    [TestCase("(a < b) = c")]
    [TestCase("(a LIKE b) LIKE c")]
    public void Chain_WithParentheses_Parses(string expression)
    {
        Assert.That(Grouping(expression), Is.EqualTo(expression));
    }

    /// <summary>
    /// A predicate that ends at its own closing token gives the parser no choice, so non-associativity does not
    /// act on it. PostgreSQL also accepts these two forms.
    /// </summary>
    [TestCase("a IN (1) IN (TRUE)", "(a IN (1)) IN (true)")]
    [TestCase("a IN (1) LIKE b", "(a IN (1)) LIKE b")]
    public void Predicate_ThatEndsAtItsOwnToken_CanBeFollowed(string expression, string expected)
    {
        Assert.That(Grouping(expression), Is.EqualTo(expected));
    }

    /// <summary>NOT and IS NOT still parse where the words have no operator meaning.</summary>
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64 NOT NULL)")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64 DEFAULT (1) NOT NULL)")]
    [TestCase("CREATE TABLE IF NOT EXISTS t (id INT64 PRIMARY KEY)")]
    [TestCase("ALTER TABLE t ALTER COLUMN v SET NOT NULL")]
    public void Not_OutsideAnExpression_StillParses(string sql)
    {
        Assert.That(SQLParserProcessor.Parse(sql), Is.Not.Null);
    }
}
