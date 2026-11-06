# PostgreSQL expression syntax

CamusDB accepts these PostgreSQL expression forms, which data-generation and migration scripts use
often:

| Form | Example | Meaning |
|------|---------|---------|
| Digit separators | `200_000` | The number 200000 |
| `%` | `i % 3` | The remainder of `i / 3`, the same as `mod(i, 3)` |
| `::` | `x::text` | The same as `CAST(x AS text)` |
| Subscript | `tags[1]` | The first element of an array |
| Quantified comparison | `'news' = ANY (tags)` | `true` when `tags` holds `'news'` |
| Quantified comparison | `score > ALL (limits)` | `true` when `score` is above every element of `limits` |
| Unary minus | `-total` | The negative of `total` |
| Negated predicates | `year NOT BETWEEN 2020 AND 2021` | The same as `NOT (year BETWEEN 2020 AND 2021)` |
| Concatenation | `first_name \|\| ' ' \|\| last_name` | The two strings joined; `NULL` if one side is `NULL` |
| Bitwise operators | `flags & 4`, `a \| b`, `a # b`, `~a`, `1 << n`, `a >> n` | AND, OR, XOR, NOT and shifts on `int64` and `bytes` |
| Power | `2 ^ 10` | The same as `pow(2, 10)`; a `float64` |
| Window functions | `row_number() OVER (PARTITION BY g ORDER BY t)` | A value per row computed from related rows; see [window-functions.md](./window-functions.md) |

Together they run a statement such as:

```sql
INSERT INTO orders
  SELECT i, 1 + (i % 200_000), (ARRAY['paid', 'shipped', 'delivered'])[1 + i % 3]
  FROM series;
```

## Characters outside a token

A statement can hold only the characters that the SQL grammar uses. Inside a string, a comment or a
quoted identifier, any character can appear, with the limits below. Anywhere else, an unknown
character is a syntax error that names the character and its position (`CADB0406`):

```text
(line 2, col 3) Syntax error, unexpected character '$'
(line 1, col 10) Syntax error, unexpected character U+00A0
```

- Whitespace is the space, the tab, the line break, the form feed and the vertical tab. A no-break
  space (U+00A0) and a byte order mark (U+FEFF) are not whitespace. Text that you copy from a web page
  can hold the first one.
- A `;` can close a statement, and it can repeat. It cannot appear anywhere else, and a second
  statement cannot follow it, because the server takes one statement per request.
- A string literal or a quoted identifier cannot hold a raw line break or a raw tab. Use an `E'…'`
  string with `\n` or `\t`.
- An identifier without quotes holds only ASCII letters, digits and `_`.

## Digit separators

A numeric literal can have one underscore between two digits, to group the digits. The underscores
have no effect on the value.

```sql
SELECT 200_000;        -- 200000
SELECT 1_000.000_5;    -- 1000.0005
SELECT 1e1_0;          -- 1e10
SELECT id FROM t LIMIT 1_000;
```

The rule applies to every place a number is written: values, `LIMIT` and `OFFSET`, and sizes such as
`string(1_0)`.

An underscore that is not between two digits is a syntax error (`CADB0406`, "Invalid numeric
literal"). These are all errors: `200_`, `2__0`, `1_.5`, `1.5_`, `1_e5`.

A name that starts with an underscore, such as `_000`, is still an identifier.

Hexadecimal integers (`0xFF`) do not accept separators.

## The `%` operator

`a % b` is the remainder of `a / b`. It has the same precedence as `*` and `/`, and it groups from
left to right:

```sql
SELECT 1 + 7 % 3 * 2;   -- 3, that is 1 + ((7 % 3) * 2)
SELECT 17 % 5 % 3;      -- 2, that is (17 % 5) % 3
```

`%` is a call to the `mod` function, so the two spellings always agree:

- Two `int64` operands give an `int64`. Any other numeric operand gives a `float64`.
- The result has the sign of the dividend: `-7 % 3` is `-1`.
- A `NULL` operand gives `NULL`.
- A zero divisor is an error.

Because `%` is stored as a call, `SHOW CREATE VIEW`, a CHECK constraint and `EXPLAIN` show
`mod(a, b)` where you wrote `a % b`. The two forms are equivalent.

A `%` inside a string, such as a `LIKE` pattern, is not an operator.

## NULL operands

A `NULL` operand gives `NULL` for every arithmetic operator, not only for `%`:

```sql
SELECT NULL + 1;      -- NULL
SELECT total * 2;     -- NULL for each row where total is NULL
SELECT NULL / 0;      -- NULL: the NULL check comes before the zero check
```

A non-numeric operand, such as `'a' + 1`, is still an error. So is a zero divisor with a
non-`NULL` dividend.

`LIKE`, `ILIKE`, `~`, `~*`, `!~` and `!~*` follow the same rule. A `NULL` value or a `NULL` pattern
makes the result UNKNOWN, so `WHERE name LIKE 'a%'` skips the rows where `name` is `NULL`.
`name !~ 'x'` is also UNKNOWN for those rows, not `true`.

## Integer overflow

`+`, `-`, `*` and `/` with two `int64` operands never wrap. A result outside the `int64` range fails
with `CADB0420`, as PostgreSQL fails with `bigint out of range`:

```sql
SELECT 9223372036854775807 + 1;       -- error CADB0420
SELECT -9223372036854775808 / -1;     -- error CADB0420
SELECT 9223372036854775807 + 0;       -- 9223372036854775807
```

`SUM`, literals and conversions follow the same rule. See
[INT64 overflow](data-types.md#int64-overflow).

## Float overflow, Infinity and NaN

A `float64` or `float32` result that overflows or underflows from finite operands fails with
`CADB0421`, as PostgreSQL fails with `value out of range: overflow` or `underflow`. It never becomes an
infinity or a zero. `'Infinity'`, `'-Infinity'` and `'NaN'` written on purpose are values, and an
operation on them gives the IEEE result:

```sql
SELECT 1e308 * 10;                          -- error CADB0421 (overflow)
SELECT 1e-300 * 1e-300;                     -- error CADB0421 (underflow)
SELECT 1e400;                               -- error CADB0421: the literal is past the range
SELECT CAST('Infinity' AS float64) + 1;     -- Infinity
SELECT CAST('NaN' AS float64) = CAST('NaN' AS float64);   -- true
```

`NaN` equals `NaN` and sorts above every number, and `-0` equals `0`. See
[Float values](data-types.md#float-values-infinity-nan-and-overflow).

## Unary minus

`-x` negates any numeric expression: a column, a parameter, a call or a parenthesised expression.
It binds tighter than `*` and `/`, so `-a * b` is `(-a) * b`. A cast binds tighter still, so
`-x::int` negates the cast value.

```sql
SELECT -total, -(price * qty), 3 - -4;   -- 3 - -4 is 7
```

- The result has the type of the operand.
- `-NULL` is `NULL`.
- A non-numeric operand is an error.
- Negating the smallest `int64` value is an overflow error (`CADB0420`).

A minus sign directly before a number, as in `-5` or `- 5`, gives a negative constant, not a
negation. The planner can then use it as an index bound.

## `LIKE` and `ILIKE` patterns

A pattern follows the rule of PostgreSQL:

| In the pattern | What it matches |
|---|---|
| `%` | Any run of characters, also an empty one |
| `_` | One character. It counts a code point, so `'🙂' LIKE '_'` is true |
| `\` and the character after it | That character as a literal, so `'a%' LIKE 'a\%'` is true and `'ab' LIKE 'a\%'` is false |
| Any other character | Itself. `LIKE` compares it exactly, and `ILIKE` ignores case |

A newline is one character like any other. `SHOW ... LIKE` uses the same rule.

A plain string literal keeps a backslash, so `'a\_'` holds three characters. In an `E''` literal, write
`E'a\\_'` for the same value.

A pattern that ends with the escape character fails with `CADB0400`, but only when the match reaches that
character. So `'b' LIKE 'a\'` is `false`, and `'a\' LIKE 'a\'` fails. This is also the rule of PostgreSQL.

### The `ESCAPE` clause

`ESCAPE` selects another escape character:

```sql
SELECT 'a_' LIKE 'a#_' ESCAPE '#';    -- true
SELECT 'ab' LIKE 'a#_' ESCAPE '#';    -- false
SELECT 'a\b' LIKE 'a\b' ESCAPE '#';   -- true: a backslash is a literal here
SELECT 'a\b' LIKE 'a\b' ESCAPE '';    -- true: an empty string means no escape character
```

The clause applies to `LIKE`, `ILIKE`, `NOT LIKE` and `NOT ILIKE`. Its value can be a column or a parameter. A
`NULL` value makes the result UNKNOWN, and a string of more than one character fails with `CADB0400`. `ESCAPE` is
not a reserved word, so a column can have that name.

`x LIKE p ESCAPE e` is the same as `x LIKE like_escape(p, e)`, as in PostgreSQL. `SHOW CREATE VIEW`, a CHECK
constraint and `EXPLAIN` show that form. `like_escape` returns the pattern in its backslash form, and it can be
called directly.

### Definitions stored by an older build

An older build read `_` and a backslash as literals. Nothing rewrites a stored definition, so each one takes the
rule above when it next runs:

- A CHECK constraint can refuse a row that it accepted before. `CHECK (s NOT LIKE 'a_')` accepted `'ab'`, and now
  refuses it. An old row stays stored, and the next write that checks it fails with `CADB0303`.
- A view takes the new rule at once, so its rows can change.
- A materialized view keeps its old rows until `REFRESH MATERIALIZED VIEW`.
- During a rolling upgrade, two nodes can give different answers for the same pattern.

To keep the old meaning, write `\_` for a literal underscore and `\\` for a literal backslash. Use
`SHOW CREATE TABLE` and `SHOW CREATE VIEW` to find a pattern that holds either character.

## `NOT BETWEEN`, `NOT LIKE` and `NOT ILIKE`

These spellings are the same as `NOT` over the positive predicate:

```sql
SELECT id FROM orders WHERE year NOT BETWEEN 2020 AND 2021;
SELECT id FROM fruits WHERE name NOT LIKE 'a%';
SELECT id FROM fruits WHERE name NOT ILIKE 'a%';
```

A `NULL` operand makes the positive predicate UNKNOWN, and `NOT` over UNKNOWN is still UNKNOWN, so a
row with a `NULL` year or name is not returned. `NOT BETWEEN` binds tighter than `AND`, the same as
`BETWEEN`. `SHOW CREATE VIEW`, a CHECK constraint and `EXPLAIN` show the stored form,
`NOT (year BETWEEN 2020 AND 2021)`.

## Concatenation: `||`

`a || b` joins two values. `||` is concatenation, as in PostgreSQL. It is not the logical OR that
MySQL reads by default. Use `OR` for a logical OR.

| Left | Right | Result |
|------|-------|--------|
| `string` | `string` | `string` |
| `bytes` | `bytes` | `bytes` |
| `string` | any other scalar | `string` |
| any other scalar | `string` | `string` |

```sql
SELECT first_name || ' ' || last_name FROM users;   -- 'Ada Lovelace'
SELECT X'01' || X'02';                              -- X'0102'
SELECT 'n=' || 5;                                   -- 'n=5'
SELECT 'v=' || X'01ff';                             -- 'v=\x01ff'
```

- When one side is a string, the other side converts to text. The conversion is the same as
  `CAST(x AS string)` and `concat()`: a date is `2026-10-12`, bytes are `\x` and lowercase hex, and
  a float is the shortest text that gives the same value.
- A `NULL` operand gives `NULL`: `'a' || NULL` is `NULL`. This is the difference from `concat()`,
  which skips a `NULL` argument: `concat('a', NULL)` is `'a'`.
- Two operands that are not strings, such as `1 || 2` or `X'01' || 2`, are an error.
- An array operand is an error. CamusDB has no array concatenation.

## Bitwise operators

| Operator | Meaning | Operand types |
|----------|---------|---------------|
| `a & b` | AND | two `int64`, or two `bytes` of equal length |
| `a \| b` | OR | two `int64`, or two `bytes` of equal length |
| `a # b` | XOR | two `int64`, or two `bytes` of equal length |
| `~a` | NOT | `int64` or `bytes` |
| `a << n` | shift left | two `int64` |
| `a >> n` | shift right | two `int64` |

```sql
SELECT 12 & 10;            -- 8
SELECT 12 | 10;            -- 14
SELECT 12 # 10;            -- 6
SELECT ~5;                 -- -6
SELECT 1 << 4;             -- 16
SELECT -16 >> 2;           -- -4
SELECT X'F0F0' & X'FF00';  -- X'F000'
SELECT ~X'F00F';           -- X'0FF0'
SELECT id FROM items WHERE flags & 4 = 4;
```

- A `NULL` operand gives `NULL`.
- `>>` is an arithmetic shift. It keeps the sign, so `-16 >> 2` is `-4`.
- A shift uses only the low six bits of the count. Thus the count is taken modulo 64:
  `1 << 64` is `1`, and `1 << -1` is the smallest `int64`. PostgreSQL gives the same results on
  x86-64 and ARM64.
- Two `bytes` operands of different lengths are an error, as for PostgreSQL `bit` strings.
- A `float64`, `float32`, `numeric`, `bool` or `string` operand is an error. There is no implicit
  conversion to `int64`.
- `#` is XOR, as in PostgreSQL. `^` is [power](#power-), also as in PostgreSQL, and not XOR as in
  MySQL and GoogleSQL.
- `#-` with no space between the two characters is the jsonb delete-path operator, as in PostgreSQL
  (see [json.md](json.md#lexer-notes)). `x #-1` is therefore not `x XOR -1`; write `x # -1`.

### Power `^`

`a ^ b` is the same call as `pow(a, b)`, so the two cannot differ on an error, on overflow or on a
special value. The result is a `float64`, for every numeric operand type.

```sql
SELECT 2 ^ 10;             -- 1024
SELECT id FROM points WHERE x ^ 2 + y ^ 2 < 100;
```

The precedence and the grouping are those of PostgreSQL, and they are not those of mathematics:

| Expression | Value | Why |
|------------|-------|-----|
| `2 * 3 ^ 2` | `18` | `^` binds tighter than `*`, `/` and `%` |
| `-2 ^ 2` | `4` | Unary minus binds tighter than `^`, so this is `(-2) ^ 2` |
| `2 ^ 3 ^ 2` | `64` | `^` groups to the left, so this is `(2 ^ 3) ^ 2` |

Write `-(2 ^ 2)` for `-4`, and `2 ^ (3 ^ 2)` for `512`.

A `NULL` operand gives `NULL`. For the errors and the special values, see
[Non-finite and out-of-range floats](data-types.md).

### Prefix `~` and regex match `~`

`~` has two meanings. Where an operand starts, `~` is bitwise NOT. After an operand, `~` is the
regex match. Thus `name ~ '^A'` is a regex match, `~flags` is a bitwise NOT, and `a ~ ~b` is a regex
match of `a` against `~b`.

Prefix `~` binds as tightly as unary minus, so `~a + 1` is `(~a) + 1`. This is the C and MySQL
reading. PostgreSQL reads `~a + 1` as `~(a + 1)`, because it gives a prefix operator the lower
"any other operator" level. Write the parentheses when a statement must give the same result on
both systems.

## Operator precedence

The table lists the operators from the tightest binding to the loosest. Operators on one row have the
same precedence. A left-associative row groups from left to right: `a - b - c` is `(a - b) - c`.

| Operators | Associativity |
|-----------|---------------|
| `::` (cast), `[ ]` (subscript) | left |
| unary `-`, prefix `~` | right |
| `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?\|`, `?&`, `#-` (jsonb, see [json.md](json.md#precedence)) | left |
| `IS`, `IN`, `.` (qualified name) | left |
| `*`, `/`, `%` | left |
| `+`, `-` | left |
| `\|\|`, `&`, `\|`, `#`, `<<`, `>>` | left |
| `<`, `>`, `<=`, `>=` | left |
| `=`, `<>`, `!=`, `BETWEEN` | left |
| `LIKE`, `ILIKE`, `~`, `~*`, `!~`, `!~*` | left |
| `NOT` | right |
| `AND` | left |
| `OR` | left |

Some results of this table:

```sql
SELECT 1 + 2 & 3;        -- 3, that is (1 + 2) & 3
SELECT 6 | 1 & 3;        -- 3, that is (6 | 1) & 3
SELECT 1 << 1 + 1;       -- 4, that is 1 << (1 + 1)
SELECT 'a' || 'b' = 'ab';  -- true, that is ('a' || 'b') = 'ab'
```

`||` and the bitwise operators share one level, as in PostgreSQL. Thus `6 | 1 & 3` is `(6 | 1) & 3`.
In C, `&` binds tighter than `|`, and the result is 7.

## Several aggregates without `GROUP BY`

A `SELECT` without `GROUP BY` can hold any number of aggregates. They all reduce the same single
group, in one pass over the rows:

```sql
SELECT COUNT(*) AS total, COUNT(amount) AS present, MIN(amount), MAX(amount) FROM sales;
```

A bare column next to an aggregate is still an error without `GROUP BY`, because there is no group
for the column to take its value from.

## The `::` cast

`x::type` is a shorter way to write `CAST(x AS type)`. It accepts the same type names as `CAST`,
including `text`, `int`, `integer` and `double`.

```sql
SELECT '42'::int + 1;         -- 43
SELECT random()::text;
SELECT @p::int64;             -- casts a bound parameter
SELECT '42'::text::int64;     -- casts can be chained
```

`::` binds tighter than every other operator, as in PostgreSQL. In `a + b::int`, only `b` is cast.
To cast a larger expression, put it in parentheses: `(a + b)::text`.

A minus sign written directly before a number is part of the number, so `-1::text` is the string
`'-1'`. (PostgreSQL applies the cast first and then rejects the minus on a string.)

`SHOW CREATE VIEW` and a CHECK constraint show the `CAST(x AS type)` form.

## Array subscripts

`value[n]` reads element `n` of an array. The first element is `1`.

```sql
SELECT tags[1] FROM t;
SELECT (ARRAY['paid', 'shipped', 'delivered'])[1 + i % 3] FROM series;
SELECT ARRAY['a', 'b'][2];    -- 'b'
```

These rules follow PostgreSQL:

- An index that is less than 1 or greater than the array length gives `NULL`, not an error.
- A `NULL` array or a `NULL` index gives `NULL`.
- An index that is not an integer is an error, even `1.0`.
- A subscript on a value that is not an array is an error.

A subscript can appear anywhere an expression can: the select list, `WHERE`, `ORDER BY`, a CHECK
constraint and a view body.

The result column of `tags[1]` has the element type of `tags`. For a subscript on an `ARRAY[...]`
literal, the type is the type of its first non-`NULL` element.

Not supported: slices (`tags[1:2]`) and assignment to an element (`UPDATE t SET tags[1] = 'x'`).

## Array elements

Each element of `ARRAY[...]` is a full expression, as in PostgreSQL:

```sql
SELECT ARRAY[n::string, 'hello'] FROM t;
SELECT ARRAY[o.name, t.n::string] FROM t JOIN o ON t.id = o.id;
SELECT ARRAY[min(n), max(n)] FROM t;
```

Two rules differ from PostgreSQL:

- All non-`NULL` elements must have the same type. PostgreSQL finds a common type, so it accepts
  `ARRAY[1, 2.5]` and `ARRAY[1, '2']`. CamusDB rejects both. Cast the elements to one type.
- A scalar subquery in an element must be uncorrelated, as every subquery outside a `WHERE` must
  be. See [Subqueries in expressions](#subqueries-in-expressions).

See also [Data types → Arrays](data-types.md#arrays).

## IN lists

An item of `x IN (…)` or `x NOT IN (…)` is a literal, a parameter or a column name. A column item is
read from the same row as `x`, so `WHERE a.x IN (a.y, 5)` is `a.x = a.y OR a.x = 5` with the usual
three-valued rules: a match is TRUE, no match next to a NULL item is UNKNOWN, and `NOT IN` negates the
definite answers. A column item has a value per row, so the planner cannot turn the list into index
seeks; a list of literals or parameters on an indexed column still can. An item that is an expression
(`CAST(…)`, `y + 1`) is a syntax error; write it as a comparison instead. In a `HAVING`, the tested
value and the items follow the HAVING scope rule: a group key, a select item or an aggregate.

## Subqueries in expressions

A subquery can stand anywhere an expression can: a `WHERE`, a select list, a `HAVING`, a `JOIN ON`
predicate, the `WHERE` and `SET` values of `UPDATE` and `DELETE`, the `VALUES` of an `INSERT`, and
`ON CONFLICT DO UPDATE`. Inside an expression it can sit at any depth: a `CASE` branch, a
comparison operand, an `IS NULL` test, a `BETWEEN` bound, a function argument.

```sql
SELECT id, EXISTS (SELECT 1 FROM audit WHERE level = 'error') AS flagged FROM users;
SELECT id, (SELECT MAX(score) FROM scores) - score AS gap FROM players;
SELECT u.id FROM users u JOIN roles r ON u.role = r.id AND r.name IN (SELECT name FROM admins);
UPDATE orders SET total = (SELECT SUM(amount) FROM lines WHERE lines.order_id = 7) WHERE id = 7;
DELETE FROM sessions WHERE NOT EXISTS (SELECT 1 FROM users WHERE active);
INSERT INTO counters (name, value) VALUES ('users', (SELECT COUNT(*) FROM users));
SELECT * FROM t WHERE CASE WHEN kind = 1 THEN x IN (SELECT x FROM allowed) ELSE x > 0 END;
```

The rule that decides what runs where is **correlation**: a subquery is correlated when its `WHERE`
reads a column of the enclosing query through a qualifier that is not one of its own sources
(`outer.id` in `EXISTS (SELECT 1 FROM inner WHERE inner.id = outer.id)`).

- An **uncorrelated** subquery runs once, before the statement reads its first row, and its result
  takes its place: a scalar subquery becomes its value, `EXISTS` becomes `true` or `false`, and
  `IN`, `NOT IN` and a quantified comparison become a membership test over the rows it returned.
  This holds in every position listed above.
- A **correlated `EXISTS`** is evaluated once per row. CamusDB supports it in the `WHERE` of a
  `SELECT` only, at any depth of the predicate. In every other position it is refused with
  `CADB0533` (`FeatureNotSupported`), because the position has no row to evaluate it against.
- A **correlated `IN`**, `NOT IN` or quantified comparison is refused everywhere, as the
  [quantified comparison](#quantified-comparisons-any-some-and-all) rule below says. A correlated
  scalar subquery is refused as an unknown alias.

Two positions do not take a subquery at all: `ORDER BY` and `GROUP BY` refuse one with `CADB0400`.

`EXPLAIN` runs the uncorrelated subqueries of a statement, because folding them is part of
preparing the plan; see [EXPLAIN](./explain.md).

## Quantified comparisons: `ANY`, `SOME` and `ALL`

A quantified comparison compares a value with each element of an array, or with each row of a
subquery. CamusDB accepts every comparison operator with every quantifier:

| Form | Meaning |
|------|---------|
| `x op ANY (a)` | `true` when `x op element` holds for one element of `a` |
| `x op SOME (a)` | A synonym of `ANY` |
| `x op ALL (a)` | `true` when `x op element` holds for every element of `a` |

`op` is `=`, `<>`, `<`, `<=`, `>` or `>=`. `!=` is the same as `<>`. `ANY`, `SOME` and `ALL` are not
case-sensitive.

The right operand can be an array literal, an array column, an array parameter, any other expression
that gives an array, or a subquery:

```sql
SELECT * FROM posts WHERE 'news' = ANY (tags);
SELECT * FROM users WHERE id = ANY (ARRAY[1, 2, 3]);
SELECT * FROM users WHERE id = ANY (@ids);                 -- an array parameter
SELECT * FROM t WHERE x = ANY (SELECT y FROM u);
SELECT * FROM t WHERE x <> ALL (SELECT y FROM u);
SELECT * FROM runs WHERE duration > ALL (ARRAY[10, 20]);
SELECT * FROM runs WHERE duration <= ANY (SELECT budget FROM limits);
CREATE TABLE posts (id int64 PRIMARY KEY, tags array(string) CHECK ('banned' <> ALL (tags)));
CREATE TABLE runs (id int64 PRIMARY KEY, scores array(int64) CHECK (0 < ALL (scores)));
```

A subquery on the right must not reference a column of the outer query. CamusDB runs the subquery
once, before the outer scan starts, so an outer column has no value to read. This is the rule that
`IN (SELECT ...)` already follows.

A subquery on the right is not an array, and the rule that an array holds one element type does not
apply to it. The rows it returns can hold more than one type. Each row is compared to the left
operand on its own, by the same rule the operator uses outside a quantifier. So
`SELECT 0 < ANY (SELECT CASE WHEN id = 1 THEN 1 ELSE 2.5 END FROM t)` compares `0` to an `int64`
value and then to a `float64` value, and gives `true`. The same values written as
`ARRAY[1, 2.5]` are still rejected, because an array literal does have one element type.

### NULL rules

The result is the result of the expansion the form stands for, with three-valued `OR` and `AND`:

- `x op ANY (a1, a2)` is `(x op a1) OR (x op a2)`.
- `x op ALL (a1, a2)` is `(x op a1) AND (x op a2)`.

Every rule below follows from that expansion:

- `ANY` is `true` when one comparison is `true`. `ALL` is `false` when one comparison is `false`.
  A decided answer wins, even when another element is `NULL`.
- With no decided answer, a `NULL` element makes the result `NULL`.
- A `NULL` `x` gives `NULL`, when the set is not empty.
- An empty array, or a subquery with no rows, gives `false` for `ANY` and `true` for `ALL`. This is
  also true for a `NULL` `x`, because no comparison runs.
- A `NULL` array gives `NULL`. A `NULL` array is not an empty array.

One element comparison means exactly what the same operator means outside a quantifier. A mixed
numeric pair, such as an `int64` value against a `float64` element, compares by value. A pair that
the operator cannot compare, such as a number against a string, is the same error it is outside a
quantifier.

A `WHERE` clause keeps a row only when the result is `true`. A CHECK constraint rejects a row only
when the result is `false`. So `CHECK ('banned' <> ALL (tags))` accepts a `NULL` array, and it also
accepts an array that holds a `NULL` element and no `'banned'`.

### How CamusDB stores each form

Three forms are membership tests, and the parser changes each one into the equivalent `IN`,
`NOT IN` or `array_contains` expression. `SHOW CREATE VIEW`, `SHOW CREATE TABLE` (for a CHECK
constraint) and `EXPLAIN` then show the changed form, not the text that you wrote:

| You write | CamusDB shows |
|-----------|---------------|
| `x = ANY (ARRAY[1, 2])` | `x IN (1, 2)` |
| `x <> ALL (ARRAY[1, 2])` | `x NOT IN (1, 2)` |
| `x = ANY (tags)` | `array_contains(tags, x)` |
| `x <> ALL (tags)` | `NOT array_contains(tags, x)` |
| `x = ANY (SELECT y FROM u)` | `x IN (SELECT y FROM u)` |
| `x <> ALL (SELECT y FROM u)` | `x NOT IN (SELECT y FROM u)` |

`= SOME` is shown as `= ANY` is shown, because the two are one form.

Every other operator and quantifier pair keeps its own form, and is shown as you wrote it. Only the
subquery gains a pair of parentheses:

| You write | CamusDB shows |
|-----------|---------------|
| `x < ALL (ARRAY[1, 2])` | `x < ALL (ARRAY[1, 2])` |
| `x >= ANY (tags)` | `x >= ANY (tags)` |
| `x <> ANY (SELECT y FROM u)` | `x <> ANY ((SELECT y FROM u))` |

### Index use

An `ARRAY[...]` literal becomes an `IN` list only for a membership form, and only when each element
is a constant or a parameter. Then the planner can use an index on `x`, as it does for
`x IN (1, 2)`. Every other array becomes an `array_contains` call, which the query evaluates for
each row. This includes an empty `ARRAY[]` and an `ARRAY[...]` with an element such as a column or a
calculation.

`x = ANY (@ids)` and `x = ANY (tags)` do not use an index. No form with an ordered operator uses an
index; each one is evaluated for each row.

### Not supported

- `ANY`, `SOME` or `ALL` with no argument or with more than one argument. The error is `CADB0406`.
- The quantifier on the left side, as in `ANY (tags) = x`, or a quantifier in any other position, as
  in `SELECT any(tags)`. The error is "ANY/SOME/ALL is valid only as the right operand of a
  comparison".
- An array text literal on the right, such as `x = ANY ('{a,b}')`. CamusDB has no array text literal.

PostgreSQL rejects `x = (ANY (tags))`. CamusDB accepts it and reads it as `x = ANY (tags)`.

A column can still have the name `any`, `some` or `all`. The words are a quantifier only when a
parenthesized argument follows them on the right side of a comparison.
