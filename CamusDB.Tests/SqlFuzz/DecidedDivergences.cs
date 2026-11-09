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

    private static readonly string[] ZeroTable =
    [
        "CREATE TABLE z (id INT64 PRIMARY KEY, v INT64, n INT64)",
        "INSERT INTO z (id, v, n) VALUES (1, 0, NULL)",
    ];

    private const string RunsBeforeTheRow =
        "A subquery, an aggregate and a window function are computed before the row is evaluated, so the expression around them cannot stop them.";

    private const string ConstantValueError =
        "A value error is not a statement mistake, and the engine does not evaluate a constant at bind time. An argument that does not run raises nothing, as in a CASE branch.";

    private const string RegexUnits =
        "The regular expression engine matches UTF-16 units and has no code point mode. Translating a pattern needs a parser for its whole syntax. A pattern that names the character matches it.";

    private const string RegexHalfPair =
        "The engine matches UTF-16 units, so the result would hold half of a pair, which has no UTF-8 form. An error is better than a damaged value.";

    private const string NotSupportedYet =
        "CamusDB does not have this form yet. The statement fails with a clear error, and no answer is wrong.";

    private const string UntypedNull =
        "A NULL literal has no type, and it takes the type that the operator needs, the GoogleSQL rule. PostgreSQL gives an output column of NULL the type text.";

    private const string EveryDerivedColumnRuns =
        "A derived table and a view compute every column of each of their rows before the outer query reads or filters that row. PostgreSQL computes only the columns it reads, and can filter first.";

    private const string ReservedPerCallSite =
        "A FROM-less SELECT reserves one value for each call site before it evaluates. An unused value is a gap, which a sequence is allowed to have, and no value repeats.";

    private const string NegativeCountAtBind =
        "A negative LIMIT or OFFSET literal is a statement mistake, so it is refused when the statement binds, before any row is read. PostgreSQL reads the clause only when its node runs.";

    private const string ExpressionColumnName =
        "An expression column that has no alias takes its position in the select list as its name, starting at 0. PostgreSQL takes the name of the function, or ?column?. Write AS to give the column a name.";

    private static readonly string[] TwoRowZeroTable =
    [
        "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)",
        "INSERT INTO t (id, v) VALUES (1, 0), (2, 2)",
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

        new("A regular expression matches UTF-16 units, so a dot counts each half of a pair", NoSetup,
            "SELECT regexp_count('\U0001F642', '.')",
            "2", "1",
            RegexUnits),

        new("A regular expression dot does not match a pair as one character", NoSetup,
            "SELECT regexp_like('\U0001F642', '^.$')",
            "False", "true",
            RegexUnits),

        new("The regex operator matches UTF-16 units too", NoSetup,
            "SELECT '\U0001F642' ~ '^.$'",
            "False", "true",
            RegexUnits),

        new("A global replace counts each half of a pair", NoSetup,
            "SELECT regexp_replace('\U0001F642', '.', 'x', 'g') = 'xx'",
            "True", "false",
            RegexUnits),

        new("A split counts each half of a pair as one match", NoSetup,
            "SELECT cardinality(regexp_split_to_array('\U0001F642', '.'))",
            "3", "2",
            RegexUnits),

        new("A regular expression that would return half of a pair is refused", NoSetup,
            "SELECT regexp_substr('\U0001F642x', '.')",
            CamusDBErrorCodes.InvalidInput, "\U0001F642",
            RegexHalfPair),

        new("A replace that would keep half of a pair is refused", NoSetup,
            "SELECT regexp_replace('\U0001F642', '.', 'x')",
            CamusDBErrorCodes.InvalidInput, "x",
            RegexHalfPair),

        new("A position inside a pair is refused", NoSetup,
            "SELECT regexp_instr('\U0001F642x', '.', 1, 2)",
            CamusDBErrorCodes.InvalidInput, "2",
            RegexHalfPair),

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

        new("A function argument type in a discarded EXISTS output is never checked", RowTable,
            "SELECT id FROM d WHERE EXISTS (SELECT length(v) FROM d) ORDER BY id",
            "1", "function length(bigint) does not exist",
            "Functions have no signature table, so only a call shows a wrong argument type, and EXISTS does not evaluate its output."),

        new("An ESCAPE string of two characters needs a row", EmptyTable,
            "SELECT id FROM em WHERE s LIKE 'a' ESCAPE 'xy'",
            "no rows", "invalid escape string",
            "A value error is not a statement mistake, and the engine does not evaluate a constant at bind time."),

        new("ILIKE folds non-ASCII letters", NoSetup,
            "SELECT 'É' ILIKE 'é'",
            "True", "false",
            "ILIKE uses ordinal case folding, which covers every cased letter. PostgreSQL in the C locale folds only ASCII, and a linguistic locale gives the same answer as CamusDB."),

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

        new("A FROM-less subquery has no aggregates", NoSetup,
            "SELECT (SELECT COUNT(*))",
            CamusDBErrorCodes.InvalidInput, "1",
            "A subquery and a view body follow the rule of the statement, so one form of the rule covers each position."),

        new("A FROM-less SELECT has no ORDER BY", NoSetup,
            "SELECT 5 ORDER BY 1",
            CamusDBErrorCodes.InvalidInput, "5",
            "One row has no order, so the clause can only be a mistake."),

        new("A FROM-less subquery has no ORDER BY", NoSetup,
            "SELECT (SELECT 1 ORDER BY 1)",
            CamusDBErrorCodes.InvalidInput, "1",
            "A subquery and a view body follow the rule of the statement, so one form of the rule covers each position."),

        new("A sequence call is refused in a FROM-less subquery", ["CREATE SEQUENCE sq"],
            "SELECT (SELECT nextval('sq'))",
            CamusDBErrorCodes.SequenceCallNotAllowedHere, "1",
            "A statement must know how many values it draws before it runs, and a nested LIMIT 0, WHERE FALSE or discarded EXISTS list decides that only when it runs."),

        new("A sequence call is refused in a FROM-less SELECT with WHERE", ["CREATE SEQUENCE sq"],
            "SELECT nextval('sq') WHERE TRUE",
            CamusDBErrorCodes.SequenceCallNotAllowedHere, "1",
            "A statement must know how many values it draws before it runs, and the condition decides that only when it runs."),

        new("A call with an argument whose type the statement does not show has no static type", RowTable,
            "SELECT COALESCE(1, upper(x)) FROM (SELECT CAST(v AS STRING) AS x FROM d) q",
            "1", "COALESCE types integer and text cannot be matched",
            "A function call shows its declared type only when the type of each argument is known. A derived STRING column, like a parameter, shows its type only through its value, so the call is checked when it returns one, as a CASE branch is."),

        new("A subquery in a COALESCE argument runs before the row", ZeroTable,
            "SELECT COALESCE(id, (SELECT 1 / b.v FROM z b)) FROM z",
            CamusDBErrorCodes.InvalidInput, "1",
            RunsBeforeTheRow),

        new("A subquery in a COALESCE argument can return too many rows", RowTable,
            "SELECT COALESCE(1, (SELECT id FROM d))",
            CamusDBErrorCodes.InvalidInput, "1",
            RunsBeforeTheRow),

        new("A subquery in a CASE branch runs before the row", ZeroTable,
            "SELECT CASE WHEN TRUE THEN 1 ELSE (SELECT 1 / b.v FROM z b) END FROM z",
            CamusDBErrorCodes.InvalidInput, "1",
            RunsBeforeTheRow),

        new("An aggregate in a COALESCE argument runs before the row", ZeroTable,
            "SELECT COALESCE(1, SUM(1 / v)) FROM z",
            CamusDBErrorCodes.InvalidInput, "1",
            RunsBeforeTheRow),

        new("A window function in a COALESCE argument runs before the row", ZeroTable,
            "SELECT COALESCE(1, SUM(1 / v) OVER (ORDER BY id)) FROM z",
            CamusDBErrorCodes.InvalidInput, "1",
            RunsBeforeTheRow),

        new("A sequence call after an argument that must run reserves a value",
            ["CREATE SEQUENCE sq", "CREATE TABLE w (id INT64 PRIMARY KEY, c INT64)", "INSERT INTO w (id, c) VALUES (1, COALESCE(nextval('sq'), nextval('sq')))"],
            "SELECT nextval('sq')",
            "3", "2",
            "A statement reserves its values before it runs, and only a literal or a parameter shows its value then. An unused value is a gap, which a sequence is allowed to have. The result of the statement stays correct."),

        new("A sequence call over a relation is refused when a row reaches it",
            ["CREATE SEQUENCE sq", "CREATE TABLE z (id INT64 PRIMARY KEY, v INT64, n INT64)", "INSERT INTO z (id, v, n) VALUES (1, 0, NULL)"],
            "SELECT COALESCE(n, nextval('sq')) FROM z",
            CamusDBErrorCodes.SequenceCallNotAllowedHere, "1",
            "A statement over a relation cannot know how many values it draws before it runs. A call that no row reaches draws nothing, so it needs no refusal, as in a CASE branch."),

        new("An integer overflow in a constant COALESCE argument needs that argument to run", ZeroTable,
            "SELECT COALESCE(v, 9223372036854775807 + 1) FROM z",
            "0", "bigint out of range",
            ConstantValueError),

        new("A bad cast of a constant in a COALESCE argument needs that argument to run", ZeroTable,
            "SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM z",
            "1", "invalid input syntax for type bigint: \"x\"",
            ConstantValueError),

        new("A domain error of a function in a COALESCE argument needs that argument to run", ZeroTable,
            "SELECT COALESCE(v, sqrt(-1.0)) FROM z",
            "0", "cannot take square root of a negative number",
            ConstantValueError),

        new("The percent operator in a COALESCE argument needs that argument to run", ZeroTable,
            "SELECT COALESCE(v, 1 % 0) FROM z",
            "0", "division by zero",
            "The parser builds `a % b` as `mod(a, b)`, so the rule of a function call applies, and the call runs only when an earlier argument is NULL."),

        new("A second predicate after BETWEEN reads left to right", NoSetup,
            "SELECT 1 BETWEEN 0 AND 2 BETWEEN FALSE AND TRUE",
            "True", "syntax error at or near \"BETWEEN\"",
            "The form has one reading, and a refusal needs grammar rules that exist only to refuse it."),

        new("IN after BETWEEN reads left to right", NoSetup,
            "SELECT 1 BETWEEN 0 AND 2 IN (TRUE)",
            "True", "syntax error at or near \"IN\"",
            "The same reason."),

        new("An aggregate ORDER BY cannot hold a subquery", RowTable,
            "SELECT COUNT(*) FROM d ORDER BY (SELECT 1)",
            CamusDBErrorCodes.InvalidInput, "2",
            "ORDER BY holds no subquery in any SELECT, and an aggregate query follows that rule."),

        new("An expression over a group key is not supported", RowTable,
            "SELECT v + 1 FROM d GROUP BY v",
            CamusDBErrorCodes.InvalidInput, "11",
            NotSupportedYet),

        new("A column that the primary key fixes must be in GROUP BY", RowTable,
            "SELECT id, v FROM d GROUP BY id",
            CamusDBErrorCodes.InvalidInput, "1",
            NotSupportedYet),

        new("GROUP BY cannot name an output alias", RowTable,
            "SELECT v AS q FROM d GROUP BY q",
            CamusDBErrorCodes.UnknownColumn, "10",
            NotSupportedYet),

        new("A grouped select list cannot hold an uncorrelated scalar subquery", RowTable,
            "SELECT id, (SELECT 5) FROM d GROUP BY id ORDER BY id",
            CamusDBErrorCodes.InvalidInput, "1",
            NotSupportedYet),

        new("ORDER BY cannot hold an aggregate that the select list does not hold", RowTable,
            "SELECT v FROM d GROUP BY v ORDER BY COUNT(*), v",
            CamusDBErrorCodes.InvalidInput, "10",
            NotSupportedYet),

        new("HAVING needs GROUP BY or an aggregate in the select list", EmptyTable,
            "SELECT 1 FROM em HAVING COUNT(*) = 0",
            CamusDBErrorCodes.InvalidInput, "1",
            NotSupportedYet),

        new("LIMIT NULL is not supported", RowTable,
            "SELECT id FROM d LIMIT NULL",
            CamusDBErrorCodes.SqlSyntaxError, "1",
            NotSupportedYet),

        new("A qualified star is not supported", RowTable,
            "SELECT d.* FROM d",
            CamusDBErrorCodes.SqlSyntaxError, "1",
            NotSupportedYet),

        new("WITH is not supported", NoSetup,
            "WITH c AS (SELECT 5 AS x) SELECT x FROM c",
            CamusDBErrorCodes.SqlSyntaxError, "5",
            NotSupportedYet),

        new("IS UNKNOWN is not supported", NoSetup,
            "SELECT NULL IS UNKNOWN",
            CamusDBErrorCodes.SqlSyntaxError, "true",
            NotSupportedYet),

        new("IS DISTINCT FROM is not supported", NoSetup,
            "SELECT 1 IS DISTINCT FROM 2",
            CamusDBErrorCodes.SqlSyntaxError, "true",
            NotSupportedYet),

        new("ISNULL and NOTNULL are not supported", NoSetup,
            "SELECT 1 ISNULL",
            CamusDBErrorCodes.SqlSyntaxError, "false",
            NotSupportedYet),

        new("An aggregate FILTER clause is not supported", RowTable,
            "SELECT COUNT(*) FILTER (WHERE v > 10) FROM d",
            CamusDBErrorCodes.SqlSyntaxError, "1",
            NotSupportedYet),

        new("The haystack of POSITION cannot hold a binary operator", NoSetup,
            "SELECT POSITION('a' IN 'x' || 'a')",
            CamusDBErrorCodes.SqlSyntaxError, "2",
            NotSupportedYet),

        new("A DEFAULT needs parentheses", NoSetup,
            "CREATE TABLE dd (id INT64 PRIMARY KEY, v INT64 DEFAULT 1)",
            CamusDBErrorCodes.SqlSyntaxError, "CREATE TABLE",
            NotSupportedYet),

        new("A set-returning function is not supported in a select list", RowTable,
            "SELECT generate_series(1, 2) FROM d",
            CamusDBErrorCodes.FeatureNotSupported, "1",
            NotSupportedYet),

        new("A view column needs a name", NoSetup,
            "CREATE VIEW vv AS SELECT 1",
            CamusDBErrorCodes.InvalidInput, "CREATE VIEW",
            NotSupportedYet),

        new("FOR UPDATE is not supported on a derived table", NoSetup,
            "SELECT x FROM (SELECT 5 AS x) d FOR UPDATE",
            CamusDBErrorCodes.InvalidInput, "5",
            NotSupportedYet),

        new("FOR UPDATE is not supported in a subquery", NoSetup,
            "SELECT (SELECT 1 FOR UPDATE)",
            CamusDBErrorCodes.SqlSyntaxError, "1",
            NotSupportedYet),

        new("A subquery is not supported in RETURNING", EmptyTable,
            "INSERT INTO em (id, v) VALUES (1, 1) RETURNING (SELECT 7)",
            CamusDBErrorCodes.InvalidInput, "7",
            NotSupportedYet),

        new("A NULL subquery result takes the type of the other operand", NoSetup,
            "SELECT (SELECT NULL) + 1",
            "NULL", "operator does not exist: text + integer",
            UntypedNull),

        new("A NULL derived column takes the type of the other operand", NoSetup,
            "SELECT x + 1 FROM (SELECT NULL AS x) d",
            "NULL", "operator does not exist: text + integer",
            UntypedNull),

        new("IN over a NULL subquery result has a NULL answer", NoSetup,
            "SELECT 1 IN (SELECT NULL)",
            "NULL", "operator does not exist: integer = text",
            UntypedNull),

        new("IN over a NULL subquery result selects no row", RowTable,
            "SELECT id FROM d WHERE id IN (SELECT NULL)",
            "no rows", "operator does not exist: bigint = text",
            UntypedNull),

        new("NOT IN over a NULL subquery result selects no row", RowTable,
            "SELECT id FROM d WHERE id NOT IN (SELECT NULL)",
            "no rows", "operator does not exist: bigint = text",
            UntypedNull),

        new("A NULL subquery result is a NULL condition", RowTable,
            "SELECT id FROM d WHERE (SELECT NULL)",
            "no rows", "argument of WHERE must be type boolean, not type text",
            UntypedNull),

        new("A COALESCE of NULL literals takes the type of the next argument", ["CREATE SEQUENCE sq"],
            "SELECT COALESCE(COALESCE(NULL, NULL), nextval('sq'))",
            "1", "COALESCE types text and bigint cannot be matched",
            UntypedNull),

        new("An integer overflow in a CASE branch that does not run raises nothing", ZeroTable,
            "SELECT CASE WHEN id > 0 THEN 1 ELSE 9223372036854775807 + 1 END FROM z",
            "1", "bigint out of range",
            ConstantValueError),

        new("A bad cast in a CASE branch that does not run raises nothing", ZeroTable,
            "SELECT CASE WHEN id > 0 THEN 1 ELSE CAST('x' AS BIGINT) END FROM z",
            "1", "invalid input syntax for type bigint: \"x\"",
            ConstantValueError),

        new("A domain error in a CASE branch that does not run raises nothing", ZeroTable,
            "SELECT CASE WHEN id > 0 THEN 1 ELSE sqrt(-1.0) END FROM z",
            "1", "cannot take square root of a negative number",
            ConstantValueError),

        new("A subscript applies to an array constructor without parentheses", NoSetup,
            "SELECT ARRAY[1, 2][1] + 1 IS NULL",
            "False", "syntax error at or near \"[\"",
            "The form has one reading, and PostgreSQL needs (ARRAY[1, 2])[1] for it. A refusal needs grammar rules that exist only to refuse it."),

        new("A derived column that the outer query does not read is computed", ZeroTable,
            "SELECT 1 FROM (SELECT v / 0 AS q FROM z) d",
            CamusDBErrorCodes.InvalidInput, "1",
            EveryDerivedColumnRuns),

        new("A derived column is computed before the outer WHERE filters its row", TwoRowZeroTable,
            "SELECT x FROM (SELECT id, 1 / v AS x FROM t) d WHERE id = 2",
            CamusDBErrorCodes.InvalidInput, "0",
            EveryDerivedColumnRuns),

        new("A view column is computed before the outer WHERE filters its row",
            [.. TwoRowZeroTable, "CREATE VIEW c AS SELECT id, 1 / v AS x FROM t"],
            "SELECT x FROM c WHERE id = 2",
            CamusDBErrorCodes.InvalidInput, "0",
            EveryDerivedColumnRuns),

        new("A view column that the outer query does not read is computed",
            [.. TwoRowZeroTable, "CREATE VIEW c AS SELECT id, (SELECT 1 / v FROM t WHERE id = 1) AS x FROM t"],
            "SELECT id FROM c ORDER BY id",
            CamusDBErrorCodes.InvalidInput, "1",
            EveryDerivedColumnRuns),

        new("A FROM-less SELECT under LIMIT 0 reserves a sequence value", ["CREATE SEQUENCE sq", "SELECT nextval('sq') LIMIT 0"],
            "SELECT nextval('sq')",
            "2", "1",
            ReservedPerCallSite),

        new("A sequence call in a CASE branch that does not run reserves a value", ["CREATE SEQUENCE sq", "SELECT CASE WHEN FALSE THEN nextval('sq') ELSE 1 END"],
            "SELECT nextval('sq')",
            "2", "1",
            ReservedPerCallSite),

        new("setval is refused inside COALESCE", ["CREATE SEQUENCE sq"],
            "SELECT COALESCE(1, setval('sq', 100))",
            CamusDBErrorCodes.SequenceCallNotAllowedHere, "1",
            "setval moves a counter, so it is allowed only as a whole projection of a FROM-less SELECT, where the statement applies it once before it evaluates."),

        new("A negative LIMIT in a view body is refused at CREATE VIEW", RowTable,
            "CREATE VIEW c AS SELECT id FROM d LIMIT -1",
            CamusDBErrorCodes.InvalidInput, "CREATE VIEW",
            NegativeCountAtBind),

        new("A negative LIMIT in a correlated EXISTS is refused also when no outer row reaches it", EmptyTable,
            "SELECT id FROM em o WHERE EXISTS (SELECT 1 FROM em i WHERE i.id = o.id LIMIT -1)",
            CamusDBErrorCodes.InvalidInput, "no rows",
            NegativeCountAtBind),

        new("An aggregate column has its position as its name", RowTable,
            "SELECT max FROM (SELECT MAX(v) FROM d) q",
            CamusDBErrorCodes.UnknownColumn, "20",
            ExpressionColumnName),

        new("A function column has its position as its name", RowTable,
            "SELECT abs FROM (SELECT abs(v) FROM d) q ORDER BY 1",
            CamusDBErrorCodes.UnknownColumn, "10",
            ExpressionColumnName),

        new("A scalar subquery over an expression has its position as its name", RowTable,
            "SELECT id, (SELECT MAX(v) FROM d) FROM d ORDER BY max, id",
            CamusDBErrorCodes.UnknownColumn, "1",
            ExpressionColumnName),
    ];
}
