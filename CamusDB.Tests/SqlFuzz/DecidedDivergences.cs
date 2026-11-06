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

    private static readonly string[] EmptyTable =
    [
        "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64, s STRING, small SMALLINT)",
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

        new("An untyped string is not a boolean condition on a table", RowTable,
            "SELECT id FROM d WHERE 'true'",
            CamusDBErrorCodes.InvalidInput, "1",
            "The same rule as a FROM-less WHERE: a literal has its own type, and no implicit conversion from string to boolean exists."),

        new("A function result is checked as a condition when a row reaches it", EmptyTable,
            "SELECT id FROM em WHERE length(s)",
            "no rows", "argument of WHERE must be type boolean, not type integer",
            "A function has no static result type in the bind-time check, so only its value shows the type. With a row the statement is CADB0400."),

        new("A function result is checked as an operand when a row reaches it", EmptyTable,
            "SELECT id FROM em WHERE s = length(s)",
            "no rows", "operator does not exist: text = integer",
            "A function has no static result type in the bind-time check, so only its value shows the type. With a row the statement is CADB0400."),

        new("A subquery column that is a function is checked when a value reaches the operator", EmptyTable,
            "SELECT id FROM em WHERE id = (SELECT upper(s) FROM em)",
            "no rows", "operator does not exist: bigint = text",
            "A subquery column has the static type of its expression, and a function has none. With a row the statement is CADB0400."),

        new("A function argument type is checked when a row reaches it", EmptyTable,
            "SELECT length(v) FROM em",
            "no rows", "function length(bigint) does not exist",
            "Functions have no signature table. The name and the argument count are checked at bind time, and each function checks its argument types when it runs."),

        new("A derived string column is checked when a row reaches the operator", EmptyTable,
            "SELECT d.id FROM (SELECT id, s FROM em) AS d WHERE d.id = d.s",
            "no rows", "operator does not exist: bigint = text",
            "A derived table declares String also for a column whose type it does not know, so the bind-time check reads every derived String column as unknown. With a row the statement is CADB0400."),

        new("A function result with a constant zero is checked when a row reaches it", EmptyTable,
            "SELECT length('a') / 0 FROM em",
            "no rows", "division by zero",
            "A function call is not a constant here, because the engine has no list of deterministic functions. With a row the statement is CADB0400."),

        new("A function result of zero as a divisor is checked when a row reaches it", EmptyTable,
            "SELECT 1 / length('') FROM em",
            "no rows", "division by zero",
            "A function call is not a constant here, because the engine has no list of deterministic functions. With a row the statement is CADB0400."),

        new("mod with a constant zero is checked when a row reaches it", EmptyTable,
            "SELECT mod(1, 0) FROM em",
            "no rows", "division by zero",
            "A function call is not a constant here, because the engine has no list of deterministic functions. With a row the statement is CADB0400."),

        new("The percent operator is a call of mod, so it is checked when a row reaches it", EmptyTable,
            "SELECT 1 % 0 FROM em",
            "no rows", "division by zero",
            "The parser builds `a % b` as `mod(a, b)`, so the rule of a function call applies. With a row the statement is CADB0400."),

        new("A subquery in a dead branch still has its constant division checked", EmptyTable,
            "SELECT CASE WHEN FALSE THEN (SELECT 1 / 0 FROM em) ELSE 1 END FROM em",
            CamusDBErrorCodes.InvalidInput, "no rows",
            "A subquery binds before the statement around it, so the check of the subquery runs before anything shows that its branch is dead. Such a branch can never run, so the refusal rejects no useful statement."),

        new("An integer overflow in a constant needs a row", EmptyTable,
            "SELECT 9223372036854775807 + 1 FROM em",
            "no rows", "bigint out of range",
            "A value error is not a statement mistake, and the engine does not evaluate a constant at bind time. With a row the statement is CADB0420."),

        new("A cast of a constant past SMALLINT needs a row", EmptyTable,
            "SELECT CAST(70000 AS SMALLINT) FROM em",
            "no rows", "smallint out of range",
            "A value error is not a statement mistake, and the engine does not evaluate a constant at bind time."),

        new("A constant past the range of a column needs a row", EmptyTable,
            "UPDATE em SET small = 70000 WHERE FALSE",
            "written", "smallint out of range",
            "A value error is not a statement mistake. The declared range is a write rule, so it applies to a row that the statement writes."),

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
