/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;

using CamusDB.Core;

namespace CamusDB.Tests.SqlFuzz;

/// <summary>
/// One statement where CamusDB answers differently from PostgreSQL on purpose.
/// </summary>
/// <param name="Name">The difference, in a few words.</param>
/// <param name="Setup">Statements that prepare the state. Each entry runs in a database of its own.</param>
/// <param name="Statement">The statement that shows the difference.</param>
/// <param name="CamusDb">An error code, or the first cell of the first row as <c>TestPostgreSqlDivergences</c> renders it.</param>
/// <param name="PostgreSql">What PostgreSQL 16 in the C locale did with the same statement, when it was measured.</param>
/// <param name="Reason">Why the difference stays.</param>
public sealed record DecidedDivergence(
    string Name,
    string[] Setup,
    string Statement,
    string CamusDb,
    string PostgreSql,
    string Reason);

/// <summary>
/// Every difference from PostgreSQL that is a decision and not a defect. <c>docs/postgresql-differences.md</c>
/// is the user-facing form of this list.
///
/// <para><b>Why it is data.</b> A comparison with PostgreSQL reports each of these as a finding on every
/// run, and noise of that kind hides real findings. A harness can read this list, and match a finding
/// against one narrow entry instead of a broad rule. The fuzzer links this file for that reason.</para>
///
/// <para><b>Why a test pins each entry.</b> <c>TestPostgreSqlDivergences</c> runs every statement, so an
/// entry cannot stay here after the behavior changes. Add an entry only for a decision that someone made.
/// A difference that nobody decided is a defect until someone does.</para>
/// </summary>
public static class DecidedDivergences
{
    private static readonly string[] NoSetup = [];

    private static readonly string[] RowTable =
    [
        "CREATE TABLE d (id INT64 PRIMARY KEY, v INT64)",
        "INSERT INTO d (id, v) VALUES (1, 10), (2, 20)",
    ];

    private static readonly string[] CharTable =
    [
        "CREATE TABLE ch (id INT64 PRIMARY KEY, c CHAR(3))",
        "INSERT INTO ch (id, c) VALUES (1, 'ab')",
    ];

    public static IReadOnlyList<DecidedDivergence> All { get; } =
    [
        new("UPDATE without WHERE is refused", RowTable,
            "UPDATE d SET v = 0",
            CamusDBErrorCodes.InvalidInput, "UPDATE 2",
            "A safety rule: one statement cannot change every row by accident. WHERE TRUE does it on purpose."),

        new("DELETE without WHERE is refused", RowTable,
            "DELETE FROM d",
            CamusDBErrorCodes.InvalidInput, "DELETE 2",
            "A safety rule: one statement cannot delete every row by accident. WHERE TRUE does it on purpose."),

        new("A primary key update is refused", RowTable,
            "UPDATE d SET id = 3 WHERE id = 2",
            CamusDBErrorCodes.InvalidInput, "UPDATE 1",
            "Row identity, the row id caches and foreign keys depend on a key that does not change. Delete the row and insert it."),

        new("CHAR(n) does not pad with blanks, so a trailing blank is significant in equality", CharTable,
            "SELECT COUNT(*) FROM ch WHERE c = 'ab '",
            "0", "1",
            "CHAR(n) is a maximum length. Padding would change equality and every index key of the column."),

        new("A trailing blank counts toward the CHAR(n) length", CharTable,
            "INSERT INTO ch (id, c) VALUES (2, 'abc ')",
            CamusDBErrorCodes.ValueTooLong, "INSERT 0 1",
            "CHAR(n) is a maximum length, so nothing is trimmed from a value without an error."),

        new("Strings compare by UTF-16 code unit, not by UTF-8 byte", NoSetup,
            "SELECT '\U0001F600' > '�'",
            "False", "true",
            "The one binary order of the engine. A sort, a top-N, a range scan and every index key use it, and ranged text indexes route by it."),

        new("A COLLATE clause is refused", NoSetup,
            "SELECT 'a' COLLATE \"C\"",
            CamusDBErrorCodes.SqlSyntaxError, "a",
            "Only one collation exists, and a clause that is accepted but ignored would be worse than none."),

        new("An integer literal past INT64 is refused", NoSetup,
            "SELECT 9223372036854775808",
            CamusDBErrorCodes.IntegerValueOutOfRange, "9223372036854775808",
            "An integer literal is INT64, the GoogleSQL rule. CAST('9223372036854775808' AS numeric) writes the larger value."),

        new("A decimal literal is FLOAT64", NoSetup,
            "SELECT 1.0 + 1E-39 = 1.0",
            "True", "false",
            "The GoogleSQL rule. NUMERIC '1.0' writes an exact value."),

        new("A quoted literal does not take the type of the other operand", NoSetup,
            "SELECT 5 = '5'",
            CamusDBErrorCodes.InvalidInput, "true",
            "The GoogleSQL rule: a string and a number have no comparison, typed or not. CAST('5' AS INT64) = 5 writes the conversion."),

        new("NUMERIC has a scale of 9", NoSetup,
            "SELECT CAST('1E-39' AS numeric) = CAST('0' AS numeric)",
            "True", "false",
            "NUMERIC has the fixed precision and scale of GoogleSQL, so its storage size and its arithmetic are bounded."),

        new("log(b, x) is FLOAT64", NoSetup,
            "SELECT log(3, 81) = 4",
            "True", "4.0000000000000000 is NUMERIC",
            "Every math function that is not exact returns FLOAT64, as pow and sqrt do."),

        new("An untyped string is not a boolean condition", NoSetup,
            "SELECT 5 WHERE 'true'",
            CamusDBErrorCodes.InvalidInput, "5",
            "A literal has its own type, and no implicit conversion from string to boolean exists."),

        new("A projection does not run when a FROM-less WHERE drops the row", NoSetup,
            "SELECT 1 / 0 AS x WHERE FALSE",
            "no rows", "division by zero",
            "A value that nothing reads is not computed. The same holds for a table with no matching row."),

        new("A FROM-less SELECT has no aggregates", NoSetup,
            "SELECT COUNT(*) WHERE TRUE",
            CamusDBErrorCodes.InvalidInput, "1",
            "An aggregate needs rows to read, and the forms with and without WHERE follow one rule."),

        new("A FROM-less SELECT has no ORDER BY", NoSetup,
            "SELECT 5 ORDER BY 1",
            CamusDBErrorCodes.InvalidInput, "5",
            "One row has no order, so the clause can only be a mistake."),

        new("A sequence call is refused in a FROM-less SELECT with WHERE", ["CREATE SEQUENCE sq"],
            "SELECT nextval('sq') WHERE TRUE",
            CamusDBErrorCodes.SequenceCallNotAllowedHere, "1",
            "A statement must know how many values it draws before it runs, and the condition decides that only when it runs."),

        new("An aggregate ORDER BY cannot hold a subquery", RowTable,
            "SELECT COUNT(*) FROM d ORDER BY (SELECT 1)",
            CamusDBErrorCodes.InvalidInput, "2",
            "ORDER BY holds no subquery in any SELECT, and an aggregate query follows that rule."),
    ];
}
