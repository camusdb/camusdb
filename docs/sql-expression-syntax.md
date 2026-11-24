# PostgreSQL expression syntax

CamusDB supports the PostgreSQL expression forms in the table below. Data-generation and migration
scripts use them often.

| Form | Example | Meaning |
|------|---------|---------|
| Digit separators | `200_000` | The number 200000 |
| `%` | `i % 3` | The remainder of `i / 3`, the same as `mod(i, 3)` |
| `::` | `x::text` | The same as `CAST(x AS text)` |
| Subscript | `tags[1]` | The first element of an array |
| Quantified comparison | `'news' = ANY (tags)` | `true` when `tags` contains `'news'` |
| Quantified comparison | `score > ALL (limits)` | `true` when `score` is above each element of `limits` |
| Unary minus | `-total` | The negative of `total` |
| Negated predicates | `year NOT BETWEEN 2020 AND 2021` | The same as `NOT (year BETWEEN 2020 AND 2021)` |
| Concatenation | `first_name \|\| ' ' \|\| last_name` | The two strings joined; `NULL` if one side is `NULL` |
| Bitwise operators | `flags & 4`, `a \| b`, `a # b`, `~a`, `1 << n`, `a >> n` | AND, OR, XOR, NOT and shifts on `int64` and `bytes` |
| Power | `2 ^ 10` | The same as `pow(2, 10)`; a `float64` |
| Window functions | `row_number() OVER (PARTITION BY g ORDER BY t)` | A value per row computed from related rows; see [window-functions.md](./window-functions.md) |

A single statement can combine several of these forms.

```sql
INSERT INTO orders
  SELECT i, 1 + (i % 200_000), (ARRAY['paid', 'shipped', 'delivered'])[1 + i % 3]
  FROM series;
```

## Characters outside a token

A statement can contain only the characters that the SQL grammar uses. In a string, a comment or a
quoted identifier, any character can appear, with the limits below. Anywhere else, an unknown
character is a syntax error (`CADB0406`). The error message names the character and its position.

```text
(line 2, col 3) Syntax error, unexpected character '$'
(line 1, col 10) Syntax error, unexpected character U+00A0
```

- Whitespace is the space, the tab, the line break, the form feed and the vertical tab. A no-break
  space (U+00A0) and a byte order mark (U+FEFF) are not whitespace. Text that you copy from a web page
  can hold the first one.
- A `;` can close a statement, and it can repeat. It cannot appear anywhere else, and a second
  statement cannot follow it. The server takes one statement per request.
- A string literal or a quoted identifier cannot hold a raw line break or a raw tab. Use an `E'…'`
  string with `\n` or `\t`.
- An unquoted identifier can contain only ASCII letters, digits and `_`.

## Digit separators

A numeric literal can have one underscore between two digits, to group the digits. The underscores
have no effect on the value.

```sql
SELECT 200_000;        -- 200000
SELECT 1_000.000_5;    -- 1000.0005
SELECT 1e1_0;          -- 1e10
SELECT id FROM t LIMIT 1_000;
```

The rule applies wherever a number is written: values, `LIMIT` and `OFFSET`, and sizes such as
`string(1_0)`.

An underscore that is not between two digits is a syntax error (`CADB0406`, "Invalid numeric
literal"). These are all errors: `200_`, `2__0`, `1_.5`, `1.5_`, `1_e5`.

A name that starts with an underscore, such as `_000`, is an identifier.

Hexadecimal integers (`0xFF`) do not accept separators.

## The `%` operator

`a % b` is the remainder of `a / b`. It has the same precedence as `*` and `/`, and it groups from
left to right.

```sql
SELECT 1 + 7 % 3 * 2;   -- 3, that is 1 + ((7 % 3) * 2)
SELECT 17 % 5 % 3;      -- 2, that is (17 % 5) % 3
```

`%` is a call to the `mod` function. This means that the two spellings always give the same result.

- Two `int64` operands give an `int64`. Any other numeric operand gives a `float64`.
- The result has the sign of the dividend: `-7 % 3` is `-1`.
- A `NULL` operand gives `NULL`.
- A zero divisor is an error.

Because `%` is stored as a call, `SHOW CREATE VIEW`, a CHECK constraint and `EXPLAIN` show
`mod(a, b)` where you wrote `a % b`. The two forms are equivalent.

A `%` in a string, such as a `LIKE` pattern, is not an operator.

## NULL operands

A `NULL` operand gives `NULL` for each arithmetic operator.

```sql
SELECT NULL + 1;      -- NULL
SELECT total * 2;     -- NULL for each row where total is NULL
SELECT NULL / 0;      -- NULL: the NULL check comes before the zero check
```

However, a non-numeric operand, such as `'a' + 1`, is an error. So is a zero divisor with a
non-`NULL` dividend.

`LIKE`, `ILIKE`, `~`, `~*`, `!~` and `!~*` follow the same rule. A `NULL` value or a `NULL` pattern
makes the result UNKNOWN. This means that `WHERE name LIKE 'a%'` skips the rows where `name` is
`NULL`. For those rows `name !~ 'x'` is also UNKNOWN, and a `WHERE` clause with it skips them too.

## Integer overflow

`+`, `-`, `*` and `/` with two `int64` operands never wrap. A result outside the `int64` range is an
error (`CADB0420`), as it is in PostgreSQL (`bigint out of range`).

```sql
SELECT 9223372036854775807 + 1;       -- error CADB0420
SELECT -9223372036854775808 / -1;     -- error CADB0420
SELECT 9223372036854775807 + 0;       -- 9223372036854775807
```

`SUM`, literals and conversions follow the same rule. See
[INT64 overflow](data-types.md#int64-overflow).

## Float overflow, Infinity and NaN

A `float64` or `float32` result that overflows or underflows from finite operands is an error
(`CADB0421`), as it is in PostgreSQL (`value out of range: overflow` or `underflow`). The result does
not become an infinity or a zero. `'Infinity'`, `'-Infinity'` and `'NaN'` written on purpose are
values, and an operation on them gives the IEEE result.

```sql
SELECT 1e308 * 10;                          -- error CADB0421 (overflow)
SELECT 1e-300 * 1e-300;                     -- error CADB0421 (underflow)
SELECT 1e400;                               -- error CADB0421: the literal is past the range
SELECT CAST('Infinity' AS float64) + 1;     -- Infinity
SELECT CAST('NaN' AS float64) = CAST('NaN' AS float64);   -- true
```

`NaN` equals `NaN` and sorts above all numbers, and `-0` equals `0`. See
[Float values](data-types.md#float-values-infinity-nan-and-overflow).

## Unary minus

`-x` negates any numeric expression: a column, a parameter, a call or a parenthesised expression.
It binds tighter than `*` and `/`: `-a * b` is `(-a) * b`. A cast binds tighter than unary minus,
and `-x::int` negates the cast value.

```sql
SELECT -total, -(price * qty), 3 - -4;   -- 3 - -4 is 7
```

- The result has the type of the operand.
- `-NULL` is `NULL`.
- A non-numeric operand is an error.
- Negating the smallest `int64` value is an overflow error (`CADB0420`).

A minus sign directly before a number, as in `-5` or `- 5`, is part of a negative constant. The
planner can then use the constant as an index bound.

## `LIKE` and `ILIKE` patterns

A pattern follows the rule of PostgreSQL. Each character in the pattern matches as the table shows.

| In the pattern | What it matches |
|---|---|
| `%` | Any run of characters, also an empty one |
| `_` | One character. It counts a code point: `'🙂' LIKE '_'` is true |
| `\` and the character after it | That character as a literal: `'a%' LIKE 'a\%'` is true and `'ab' LIKE 'a\%'` is false |
| Any other character | Itself. `LIKE` compares it exactly, and `ILIKE` ignores case |

A newline is one character like any other. `SHOW ... LIKE` uses the same rule.

In a standard string literal, a backslash is an ordinary character: `'a\_'` is three characters long.
In an `E''` literal, write `E'a\\_'` for the same value.

A pattern that ends with the escape character returns an error (`CADB0400`) only if the match reaches that
character. For example, `'b' LIKE 'a\'` is `false` and `'a\' LIKE 'a\'` returns the error. This is
also the rule of PostgreSQL.

### The `ESCAPE` clause

`ESCAPE` selects another escape character. With `ESCAPE '#'`, for example, a backslash in the pattern
is a literal character.

```sql
SELECT 'a_' LIKE 'a#_' ESCAPE '#';    -- true
SELECT 'ab' LIKE 'a#_' ESCAPE '#';    -- false
SELECT 'a\b' LIKE 'a\b' ESCAPE '#';   -- true: a backslash is a literal here
SELECT 'a\b' LIKE 'a\b' ESCAPE '';    -- true: an empty string means no escape character
```

The clause applies to `LIKE`, `ILIKE`, `NOT LIKE` and `NOT ILIKE`. Its value can be a column or a parameter. A
`NULL` value makes the result UNKNOWN, and a string of more than one character is an error (`CADB0400`).
`ESCAPE` is not a reserved word. A column can have that name.

`x LIKE p ESCAPE e` is the same as `x LIKE like_escape(p, e)`, as in PostgreSQL. `SHOW CREATE VIEW`, a CHECK
constraint and `EXPLAIN` show that form. `like_escape` returns the pattern in its backslash form, and it can be
called directly.

### Definitions stored by an older build

An older build read `_` and a backslash as literals. A stored definition is not rewritten. Each one
uses the rule above the next time it is evaluated, with these effects.

- A CHECK constraint can block a row that it allowed before. `CHECK (s NOT LIKE 'a_')` allowed `'ab'`, and
  now blocks it. An old row is not removed, and the next write that checks it returns an error (`CADB0303`).
- A view uses the new rule at once. Its rows can change.
- A materialized view returns its old rows until `REFRESH MATERIALIZED VIEW`.
- During a rolling upgrade, two nodes can give different answers for the same pattern.

To keep the old meaning, write `\_` for a literal underscore and `\\` for a literal backslash. Use
`SHOW CREATE TABLE` and `SHOW CREATE VIEW` to find a pattern that contains either character.

## `NOT BETWEEN`, `NOT LIKE` and `NOT ILIKE`

These spellings are the same as `NOT` over the positive predicate.

```sql
SELECT id FROM orders WHERE year NOT BETWEEN 2020 AND 2021;
SELECT id FROM fruits WHERE name NOT LIKE 'a%';
SELECT id FROM fruits WHERE name NOT ILIKE 'a%';
```

A `NULL` operand makes the positive predicate UNKNOWN, and `NOT` over UNKNOWN is also UNKNOWN. This
means that a row with a `NULL` year or name is not returned. `NOT BETWEEN` binds tighter than `AND`, the same as
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
- An array operand is an error. Array concatenation is not supported.

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
- `>>` is an arithmetic shift. The sign of the result is the sign of the operand: `-16 >> 2` is `-4`.
- A shift uses only the low six bits of the count. So the count is taken modulo 64:
  `1 << 64` is `1`, and `1 << -1` is the smallest `int64`. PostgreSQL gives the same results on
  x86-64 and ARM64.
- Two `bytes` operands of different lengths are an error, as for PostgreSQL `bit` strings.
- A `float64`, `float32`, `numeric`, `bool` or `string` operand is an error. There is no implicit
  conversion to `int64`.
- `#` is XOR, as in PostgreSQL. `^` is [power](#power-), also as in PostgreSQL. In MySQL and
  GoogleSQL, `^` is XOR.
- `#-` written as two adjacent characters is the jsonb delete-path operator, as in PostgreSQL
  (see [json.md](json.md#lexer-notes)). So `x #-1` does not mean `x XOR -1`; write `x # -1`.

### Power `^`

`a ^ b` is the same call as `pow(a, b)`. This means that the two give the same errors (overflow
included) and the same special values. The result is a `float64` for all numeric operand types.

```sql
SELECT 2 ^ 10;             -- 1024
SELECT id FROM points WHERE x ^ 2 + y ^ 2 < 100;
```

The precedence and the grouping are those of PostgreSQL. They differ from those of mathematics, as
the examples in the table show.

| Expression | Value | Why |
|------------|-------|-----|
| `2 * 3 ^ 2` | `18` | `^` binds tighter than `*`, `/` and `%` |
| `-2 ^ 2` | `4` | Unary minus binds tighter than `^`. This is `(-2) ^ 2` |
| `2 ^ 3 ^ 2` | `64` | `^` groups to the left. This is `(2 ^ 3) ^ 2` |

Write `-(2 ^ 2)` for `-4`, and `2 ^ (3 ^ 2)` for `512`.

A `NULL` operand gives `NULL`. For the errors and the special values, see
[Non-finite and out-of-range floats](data-types.md#float-values-infinity-nan-and-overflow).

### Prefix `~` and regex match `~`

`~` has two meanings. Where an operand starts, `~` is bitwise NOT. After an operand, `~` is the
regex match. So `name ~ '^A'` is a regex match, `~flags` is a bitwise NOT, and `a ~ ~b` is a regex
match of `a` against `~b`.

Prefix `~` binds as tightly as unary minus: `~a + 1` is `(~a) + 1`. This is the C and MySQL
reading. PostgreSQL gives a prefix operator the lower "any other operator" level, and it reads
`~a + 1` as `~(a + 1)`. Write the parentheses when a statement must give the same result on
both systems.

## Operator precedence

The table lists the operators from the tightest binding to the loosest. Operators on one row have the
same precedence. A left-associative row groups from left to right: `a - b - c` is `(a - b) - c`. The
table is the table of PostgreSQL. The one exception is prefix `~` (see [above](#bitwise-operators)).

| Operators | Associativity |
|-----------|---------------|
| `::` (cast), `[ ]` (subscript) | left |
| unary `-`, prefix `~` | right |
| `.` (qualified name) | left |
| `^` | left |
| `*`, `/`, `%` | left |
| `+`, `-` | left |
| `\|\|`, `&`, `\|`, `#`, `<<`, `>>`, `~`, `~*`, `!~`, `!~*` and the jsonb operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?\|`, `?&`, `#-`) | left |
| `BETWEEN`, `IN`, `LIKE`, `ILIKE` and their `NOT` forms | no chain |
| `<`, `>`, `<=`, `>=`, `=`, `<>`, `!=` | no chain |
| `IS` | no chain |
| `NOT` | right |
| `AND` | left |
| `OR` | left |

The comment on each statement below shows the grouping that the table gives.

```sql
SELECT 1 + 2 & 3;            -- 3, that is (1 + 2) & 3
SELECT 6 | 1 & 3;            -- 3, that is (6 | 1) & 3
SELECT 1 << 1 + 1;           -- 4, that is 1 << (1 + 1)
SELECT 'a' || 'b' = 'ab';    -- true, that is ('a' || 'b') = 'ab'
SELECT 1 + 1 IS NULL;        -- false, that is (1 + 1) IS NULL
SELECT FALSE = NULL IS NULL; -- true, that is (FALSE = NULL) IS NULL
SELECT 1 + 4 IN (5);         -- true, that is (1 + 4) IN (5)
SELECT 'a' LIKE 'a' = TRUE;  -- true, that is ('a' LIKE 'a') = TRUE
SELECT TRUE = 1 NOT IN (2);  -- true, that is TRUE = (1 NOT IN (2))
SELECT NOT 1 + 1 IS NULL;    -- true, that is NOT ((1 + 1) IS NULL)
```

`||` and the bitwise operators share one level, as in PostgreSQL. So `6 | 1 & 3` is `(6 | 1) & 3`.
In C, `&` binds tighter than `|`, and the result is 7.

### No chain

A chain of two comparisons is a syntax error, as in PostgreSQL. Also, a `LIKE` cannot directly follow
another `LIKE`, and `IN` or `BETWEEN` cannot directly follow a `LIKE`. Use parentheses to write the grouping
that you mean.

```sql
SELECT a = b = c;     -- CADB0406
SELECT (a = b) = c;
SELECT a < b < c;     -- CADB0406
SELECT a < b AND b < c;
```

A predicate that ends with a closing token can have another predicate after it, also as in PostgreSQL:
`x IN (1) IN (TRUE)` is `(x IN (1)) IN (TRUE)`. `BETWEEN` follows this rule too. PostgreSQL does not allow
that form. See [the differences from PostgreSQL](postgresql-differences.md#operator-precedence).

### Stored definitions

A change of this table does not change a stored definition. A view body and a `CHECK` condition are stored
as text that writes each grouping out with parentheses. For example, `SHOW CREATE VIEW` prints `(a = b) IS NULL`.

Before this table followed PostgreSQL, `IS` and `IN` bound tighter than all binary operators. A view or a
`CHECK` from that time uses the grouping of that time. For example, a view that was created with
`a = b IS NULL` is stored as the text `a = (b IS NULL)`, and it continues to read that. To get the PostgreSQL
reading, create the view again. Use `SHOW CREATE VIEW` and `SHOW CREATE TABLE` to read a stored definition.

## Several aggregates without `GROUP BY`

A `SELECT` that omits `GROUP BY` can contain any number of aggregates. They all reduce the same single
group, in one pass over the rows.

```sql
SELECT COUNT(*) AS total, COUNT(amount) AS present, MIN(amount), MAX(amount) FROM sales;
```

However, a bare column next to an aggregate is an error when the query omits `GROUP BY`. There is no
group for the column to take its value from.

## The `::` cast

`x::type` is a shorter way to write `CAST(x AS type)`. You can use the same type names as with `CAST`,
including `text`, `int`, `integer` and `double`.

```sql
SELECT '42'::int + 1;         -- 43
SELECT random()::text;
SELECT @p::int64;             -- casts a bound parameter
SELECT '42'::text::int64;     -- casts can be chained
```

`::` binds tighter than all other operators, as in PostgreSQL. In `a + b::int`, only `b` is cast.
To cast a larger expression, put it in parentheses: `(a + b)::text`.

A minus sign written directly before a number is part of the number. This means that `-1::text` is
the string `'-1'`. (PostgreSQL applies the cast first, and the minus on a string is then an error.)

`SHOW CREATE VIEW` and a CHECK constraint show the `CAST(x AS type)` form.

## Array subscripts

`value[n]` reads element `n` of an array. The first element is `1`.

```sql
SELECT tags[1] FROM t;
SELECT (ARRAY['paid', 'shipped', 'delivered'])[1 + i % 3] FROM series;
SELECT ARRAY['a', 'b'][2];    -- 'b'
```

Subscripts follow the rules of PostgreSQL.

- An index that is less than 1 or greater than the array length gives `NULL`.
- A `NULL` array or a `NULL` index gives `NULL`.
- An index that is not an integer is an error, even `1.0`.
- A subscript on a value that is not an array is an error.

A subscript can appear anywhere an expression can: the select list, `WHERE`, `ORDER BY`, a CHECK
constraint and a view body.

The result column of `tags[1]` has the element type of `tags`. For a subscript on an `ARRAY[...]`
literal, the type is the element type of the literal (see [Array elements](#array-elements)).

Not supported: slices (`tags[1:2]`) and assignment to an element (`UPDATE t SET tags[1] = 'x'`).

## Array elements

Each element of `ARRAY[...]` is a full expression, as in PostgreSQL.

```sql
SELECT ARRAY[n::string, 'hello'] FROM t;
SELECT ARRAY[o.name, t.n::string] FROM t JOIN o ON t.id = o.id;
SELECT ARRAY[min(n), max(n)] FROM t;
```

The element type is the common type of the elements, by the rule `CASE` and `COALESCE` use (see
[Data types → Result types of CASE, COALESCE, IFNULL and IF](data-types.md#result-types-of-case-coalesce-ifnull-and-if)),
and every element is converted to it:

| Expression | Element type |
|---|---|
| `ARRAY[1, 2.5]` | `float64` |
| `ARRAY[1, CAST(2.5 AS float32)]` | `float32` |
| `ARRAY[1, NUMERIC '2.5']` | `numeric` |
| `ARRAY[date_col, '2026-01-01']` | `date`; the literal is parsed |
| `ARRAY[n::string, 'hello']` | `string` |
| `ARRAY[1, true]` | error `CADB0400` |
| `ARRAY[1, '2']` | error `CADB0400` |

The error names both types:

```
No common type for the elements of ARRAY: Integer64 and Bool; write a CAST to make the types agree
```

The element type comes from the element expressions, so every row of a statement gets the same one,
in a `SELECT`, an `UPDATE`, a `DELETE`, a CHECK constraint, an expression index and a view's
`WITH CHECK OPTION` alike. `ARRAY[int_col, float_col]` is
an `array(float64)` also in a row where `float_col` is `NULL`. Two
elements with no common type fail when the statement is bound, before any row is read, also over an
empty table. An element whose type only its value shows, such as a parameter, is checked when the
array is built: `ARRAY[1, @p]` is an `array(float64)` when `@p` is a `float64`, and fails when `@p` is
a `bool`. `NULL` elements, the empty `ARRAY[]` and the rejection of nested arrays do not change.

These rules differ from PostgreSQL:

- A quoted literal does not become a number. PostgreSQL reads `'2'` in `ARRAY[1, '2']` as an
  integer. CamusDB keeps the rule it uses for `CASE`: a quoted literal is a `string`, and becomes only
  a `date` or a `datetime`. Write `ARRAY[1, CAST('2' AS int64)]`.
- `ARRAY[1, 2.5]` is an `array(float64)`, where PostgreSQL gives `numeric[]`, because CamusDB reads
  `2.5` as a `float64`. Write `ARRAY[1, NUMERIC '2.5']` for a `numeric` array.
- A scalar subquery in an element follows the rule of its clause: it can be correlated where a
  correlated subquery is supported. See [Subqueries in expressions](#subqueries-in-expressions).

See also [Data types → Arrays](data-types.md#arrays).

## IN lists

An item of `x IN (…)` or `x NOT IN (…)` is a literal, a parameter or a column name. A column item is
read from the same row as `x`. This means that `WHERE a.x IN (a.y, 5)` is `a.x = a.y OR a.x = 5` with
the usual three-valued rules. A match is TRUE, no match next to a NULL item is UNKNOWN, and `NOT IN`
negates the definite answers. A column item has a value per row, and the planner cannot turn such a
list into index seeks. A list of literals or parameters on an indexed column can use index seeks. An item that is an expression
(`CAST(…)`, `y + 1`) is a syntax error; write it as a comparison instead. In a `HAVING`, the tested
value and the items follow the HAVING scope rule: a group key, a select item or an aggregate.

## Subqueries in expressions

A subquery can stand anywhere an expression can: a `WHERE`, a select list, a `HAVING`, a `JOIN ON`
predicate, the `WHERE` and `SET` values of `UPDATE` and `DELETE`, the `VALUES` of an `INSERT`, and
`ON CONFLICT DO UPDATE`. In an expression, it can be at any depth: a `CASE` branch, a comparison
operand, an `IS NULL` test, a `BETWEEN` bound or a function argument.

```sql
SELECT id, EXISTS (SELECT 1 FROM audit WHERE level = 'error') AS flagged FROM users;
SELECT id, (SELECT MAX(score) FROM scores) - score AS gap FROM players;
SELECT u.id FROM users u JOIN roles r ON u.role = r.id AND r.name IN (SELECT name FROM admins);
UPDATE orders SET total = (SELECT SUM(amount) FROM lines WHERE lines.order_id = 7) WHERE id = 7;
DELETE FROM sessions WHERE NOT EXISTS (SELECT 1 FROM users WHERE active);
INSERT INTO counters (name, value) VALUES ('users', (SELECT COUNT(*) FROM users));
SELECT * FROM t WHERE CASE WHEN kind = 1 THEN x IN (SELECT x FROM allowed) ELSE x > 0 END;
```

How a subquery is evaluated depends on **correlation**: a subquery is correlated when a name in it
resolves to the enclosing query. As in PostgreSQL, a name resolves to the nearest query that has it.

- A qualified name resolves to the nearest query whose `FROM` declares its qualifier. For example,
  `outer.id` in `EXISTS (SELECT 1 FROM inner WHERE inner.id = outer.id)` reads the enclosing query.
- A bare name resolves to the nearest query that has a column with that name. For example, `v` in
  `SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w = v)` reads `r.v` if `r2` doesn't have a column `v`.
  A column that both queries have is the column of the subquery. It is not an ambiguous name.
- A name that no query has is an unknown column (`CADB0404`), as before.

The names are read in all clauses of the subquery and in its nested subqueries. Its derived tables
and set operations follow the same rule. A derived table cannot read the other items of the `FROM` that contains it.
However, it can read the enclosing query, as in PostgreSQL. A `LATERAL` derived table can also read
the items to its left in the same `FROM` (see [LATERAL](./joins.md#lateral)).

- An **uncorrelated** subquery is evaluated once, before the statement reads its first row, and its
  result takes its place. For example, a scalar subquery becomes its value, and `EXISTS` becomes
  `true` or `false`. `IN`, `NOT IN` and a quantified comparison become a membership test over the
  rows that the subquery returned. This applies in all the positions listed above.
- A **correlated** subquery is evaluated once per row. This applies to `EXISTS`, a scalar subquery,
  `IN`, `NOT IN` and a quantified comparison. It is supported at any depth of an expression in
  three places.
  - the `WHERE` and the select list of a `SELECT`
  - the `WHERE` of an `UPDATE` or a `DELETE`
  - the `SET` values of an `UPDATE`

  The body can be any query: a join, a derived table or a view, no `FROM`, an aggregate, `GROUP BY`,
  `HAVING`, `OFFSET`, a window function, or another subquery. A correlated `EXISTS` whose body
  reads one table and needs only its `FROM` and `WHERE` reads the inner table directly. It uses an
  index seek when one fits. Any other body is executed as a complete query for each outer row.
- Within one statement, an outer row with the same outer values as an earlier row gets the earlier
  result, and the body is not evaluated again. This means that a correlated subquery under a join,
  which sees each value of the outer table many times, is evaluated once for each distinct value. The
  values compare exactly: `'A'` and `'a'` are two values, and so are `0.0` and `-0.0`. A body with a
  volatile function such as `random()` or `nextval` is evaluated for each row. So is a body that reads
  a value of a query two or more levels up, and a fast-form `EXISTS` with an index seek. An error is
  not cached, and the cached results have a fixed size limit for each subquery.
- A scalar subquery gives NULL for no row and returns an error (`CADB0400`) for a second row. `IN`,
  `NOT IN` and a quantified comparison follow the same three-valued rules as their uncorrelated forms.
- In an `UPDATE` or `DELETE`, the subquery reads the rows as the statement found them, also when it
  reads the target table, as in PostgreSQL. A subquery of a `SET` value is evaluated when the
  statement finds the row, before the first write.
- A subquery in a `CASE` branch that is not taken, or in a `COALESCE` argument after the first value,
  is evaluated anyway. As in PostgreSQL, its error does not appear. However, it costs time.
- A correlated subquery is not supported in the `HAVING`, a `JOIN ON`, the select list of a query that
  groups or aggregates, or `ON CONFLICT DO UPDATE`. These clauses have no stage that is executed once
  for each row, and the statement returns `CADB0533` (`FeatureNotSupported`). The message names the
  clause and the outer name that the subquery reads. PostgreSQL supports these forms. For more
  information, see [differences from PostgreSQL](postgresql-differences.md#correlated-subqueries).

Two positions do not take a subquery at all: a subquery in `ORDER BY` or `GROUP BY` is an error
(`CADB0400`).

A scalar subquery in a select list names its output column after its single inner column, as in
PostgreSQL. That name is the alias or the column name. It can also be the name of a nested scalar
subquery. For example, `SELECT (SELECT 5 AS id)` has the column `id`. When the inner column is another expression, the
subquery column has its position as its name, as any expression column has. PostgreSQL names it
`?column?`, or after its function.

`EXPLAIN` executes the uncorrelated subqueries of a statement. Folding them is part of preparing the
plan. For more information, see [EXPLAIN](./explain.md).

## Names in ORDER BY

A bare name in `ORDER BY` matches the output names of the select list first, as in PostgreSQL. It
matches a column of the `FROM` clause if no output has that name. An output name is an alias or the
name of a scalar subquery column. A column reference also gives an output name: its column name with
the table name removed. For example, `SELECT v AS id FROM r ORDER BY id` sorts by `v`, and
`SELECT a.id FROM r a JOIN r b ON a.id = b.id ORDER BY id` sorts by `a.id`.

- A name that two outputs with different expressions have is an error (`CADB0400`,
  `ORDER BY "id" is ambiguous`). One example is `SELECT id, v AS id FROM r ORDER BY id`. Another is
  `SELECT *, v AS id FROM r ORDER BY id`, where `*` puts out `id` too. So is
  `SELECT id, (SELECT 5 AS id) FROM r ORDER BY id`.
- Two outputs with the same expression are one meaning: `SELECT id, id FROM r ORDER BY id` and
  `SELECT r.id, id FROM r ORDER BY id` sort by `id`.
- A position (`ORDER BY 2`), a qualified name (`ORDER BY r.id`) and an expression (`ORDER BY id + 0`)
  are not output names. This means that they cannot be ambiguous.

## Quantified comparisons: `ANY`, `SOME` and `ALL`

A quantified comparison compares a value with each element of an array, or with each row of a
subquery. All comparison operators work with all three quantifiers.

| Form | Meaning |
|------|---------|
| `x op ANY (a)` | `true` when `x op element` is true for one element of `a` |
| `x op SOME (a)` | A synonym of `ANY` |
| `x op ALL (a)` | `true` when `x op element` is true for each element of `a` |

`op` is `=`, `<>`, `<`, `<=`, `>` or `>=`. `!=` is the same as `<>`. `ANY`, `SOME` and `ALL` are not
case-sensitive.

The right operand can be any expression that gives an array, such as an array literal or an array
parameter. It can also be a subquery.

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

A subquery on the right must not reference a column of the outer query. The subquery is evaluated
once, before the outer scan starts. At that point, an outer column does not have a value to read.
`IN (SELECT ...)` follows the same rule.

A subquery on the right is not an array, and its rows are not converted to one type. The rows it
returns can have more than one type. Each row is compared to the left operand
separately, by the same rule the operator uses outside a quantifier. For example,
`SELECT 0 < ANY (SELECT CASE WHEN id = 1 THEN 1 ELSE 2.5 END FROM t)` compares `0` to an `int64`
value and then to a `float64` value, and gives `true`. The same values written as `ARRAY[1, 2.5]`
form an `array(float64)` (see [Array elements](#array-elements)), and give the same answer.

### NULL rules

The result is the result of the expansion that the form stands for, with three-valued `OR` and `AND`.

- `x op ANY (a1, a2)` is `(x op a1) OR (x op a2)`.
- `x op ALL (a1, a2)` is `(x op a1) AND (x op a2)`.

The rules below follow from that expansion.

- `ANY` is `true` when one comparison is `true`. `ALL` is `false` when one comparison is `false`.
  A decided answer wins, even when another element is `NULL`.
- If no comparison decides the answer, a `NULL` element makes the result `NULL`.
- A `NULL` `x` gives `NULL`, when the set is not empty.
- An empty array, or a subquery that returns zero rows, gives `false` for `ANY` and `true` for `ALL`.
  This is also true for a `NULL` `x`. No comparison is made.
- A `NULL` array gives `NULL`. A `NULL` array is not an empty array.

One element comparison means exactly what the same operator means outside a quantifier. A mixed
numeric pair, such as an `int64` value against a `float64` element, compares by value. A pair that
the operator cannot compare, such as a number against a string, is the same error it is outside a
quantifier.

A `WHERE` clause returns a row when the result is `true`, and skips it when the result is `false` or
`NULL`. A CHECK constraint blocks a row when the result is `false`, and allows it when the result is
`true` or `NULL`. This means that `CHECK ('banned' <> ALL (tags))` allows a `NULL` array. It also allows an
array with a `NULL` element and no `'banned'` element.

### How CamusDB stores each form

Three forms are membership tests, and the parser changes each one into the equivalent `IN`,
`NOT IN` or `array_contains` expression. `SHOW CREATE VIEW`, `SHOW CREATE TABLE` (for a CHECK
constraint) and `EXPLAIN` then show the changed form in place of the text that you wrote.

| You write | CamusDB shows |
|-----------|---------------|
| `x = ANY (ARRAY[1, 2])` | `x IN (1, 2)` |
| `x <> ALL (ARRAY[1, 2])` | `x NOT IN (1, 2)` |
| `x = ANY (tags)` | `array_contains(tags, x)` |
| `x <> ALL (tags)` | `NOT array_contains(tags, x)` |
| `x = ANY (SELECT y FROM u)` | `x IN (SELECT y FROM u)` |
| `x <> ALL (SELECT y FROM u)` | `x NOT IN (SELECT y FROM u)` |

`= SOME` is shown as `= ANY` is shown. The two are one form.

Other operator and quantifier pairs are shown as you wrote them. A subquery gains a pair of
parentheses, and the rest of the text is unchanged.

| You write | CamusDB shows |
|-----------|---------------|
| `x < ALL (ARRAY[1, 2])` | `x < ALL (ARRAY[1, 2])` |
| `x >= ANY (tags)` | `x >= ANY (tags)` |
| `x <> ANY (SELECT y FROM u)` | `x <> ANY ((SELECT y FROM u))` |

### Index use

An `ARRAY[...]` literal becomes an `IN` list if two conditions are true: the form is a membership
form, and each element is a constant or a parameter. Then the planner can use an index on `x`, as it
does for `x IN (1, 2)`. Any other array becomes an `array_contains` call, which the query evaluates
for each row. This includes an empty `ARRAY[]` and an `ARRAY[...]` with an element such as a column or a
calculation.

`x = ANY (@ids)` and `x = ANY (tags)` do not use an index. No form with an ordered operator uses an
index; each one is evaluated for each row.

### Not supported

- `ANY`, `SOME` or `ALL` with zero arguments or with more than one argument. The error is `CADB0406`.
- The quantifier on the left side (`ANY (tags) = x`) or in another position (`SELECT any(tags)`). The
  error is "ANY/SOME/ALL is valid only as the right operand of a comparison".
- An array text literal on the right, such as `x = ANY ('{a,b}')`. Array text literals are not
  supported.

`x = (ANY (tags))` is an error in PostgreSQL. CamusDB reads it as `x = ANY (tags)`.

A column can have the name `any`, `some` or `all`. The words are a quantifier if a parenthesized
argument follows them on the right side of a comparison, and an ordinary name otherwise.
