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
| Unary minus | `-total` | The negative of `total` |
| Negated predicates | `year NOT BETWEEN 2020 AND 2021` | The same as `NOT (year BETWEEN 2020 AND 2021)` |

Together they run a statement such as:

```sql
INSERT INTO orders
  SELECT i, 1 + (i % 200_000), (ARRAY['paid', 'shipped', 'delivered'])[1 + i % 3]
  FROM series;
```

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
- Negating the smallest `int64` value is an overflow error.

A minus sign directly before a number, as in `-5` or `- 5`, gives a negative constant, not a
negation. The planner can then use it as an index bound.

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
- A scalar subquery in an element works in `WHERE`, but not in the select list of a query with a
  `FROM`. This limit applies to every scalar subquery in a select list, not only to arrays.

See also [Data types → Arrays](data-types.md#arrays).

## `= ANY`, `= SOME` and `<> ALL`

A quantified comparison compares a value with each element of an array or each row of a subquery.
CamusDB accepts the three forms that are membership tests:

| Form | Meaning | The same as |
|------|---------|-------------|
| `x = ANY (a)` | `x` is equal to one element of `a` | `x IN (...)` over the elements of `a` |
| `x = SOME (a)` | A synonym of `= ANY` | `x IN (...)` |
| `x <> ALL (a)` | `x` is not equal to any element of `a` | `x NOT IN (...)` |

`!=` is the same as `<>`. `ANY`, `SOME` and `ALL` are not case-sensitive.

The right operand can be an array literal, an array column, an array parameter, any other expression
that gives an array, or a subquery:

```sql
SELECT * FROM posts WHERE 'news' = ANY (tags);
SELECT * FROM users WHERE id = ANY (ARRAY[1, 2, 3]);
SELECT * FROM users WHERE id = ANY (@ids);                 -- an array parameter
SELECT * FROM t WHERE x = ANY (SELECT y FROM u);
SELECT * FROM t WHERE x <> ALL (SELECT y FROM u);
CREATE TABLE posts (id int64 PRIMARY KEY, tags array(string) CHECK ('banned' <> ALL (tags)));
```

### NULL rules

The rules are the rules of `IN` and `NOT IN`:

- A match makes `= ANY` `true` and `<> ALL` `false`, even when the array also holds a `NULL`.
- A `NULL` `x` gives `NULL`, when the array is not empty.
- No match gives `NULL` when the array holds a `NULL` element, and otherwise `false` for `= ANY` and
  `true` for `<> ALL`.
- An empty array, or a subquery with no rows, gives `false` for `= ANY` and `true` for `<> ALL`. This
  is also true for a `NULL` `x`.
- A `NULL` array gives `NULL`.

A `WHERE` clause keeps a row only when the result is `true`. A CHECK constraint rejects a row only
when the result is `false`. So `CHECK ('banned' <> ALL (tags))` accepts a `NULL` array, and it also
accepts an array that holds a `NULL` element and no `'banned'`.

### How CamusDB stores each form

The parser changes each form into an equivalent expression. `SHOW CREATE VIEW`, `SHOW CREATE TABLE`
(for a CHECK constraint) and `EXPLAIN` show the changed form, not the text that you wrote:

| You write | CamusDB shows |
|-----------|---------------|
| `x = ANY (ARRAY[1, 2])` | `x IN (1, 2)` |
| `x <> ALL (ARRAY[1, 2])` | `x NOT IN (1, 2)` |
| `x = ANY (tags)` | `array_contains(tags, x)` |
| `x <> ALL (tags)` | `NOT array_contains(tags, x)` |
| `x = ANY (SELECT y FROM u)` | `x IN (SELECT y FROM u)` |
| `x <> ALL (SELECT y FROM u)` | `x NOT IN (SELECT y FROM u)` |

An `ARRAY[...]` literal becomes an `IN` list only when each element is a constant or a parameter.
Then the planner can use an index on `x`, as it does for `x IN (1, 2)`. Every other array becomes an
`array_contains` call, which the query evaluates for each row. This includes an empty `ARRAY[]` and an
`ARRAY[...]` with an element such as a column or a calculation.

`x = ANY (@ids)` and `x = ANY (tags)` do not use an index.

### Not supported

- The other operators with a quantifier: `< ANY`, `<= ALL`, `> SOME`, `>= ANY`, `= ALL` and `<> ANY`.
  They give the error `CADB0533` "only = ANY, = SOME and <> ALL are supported".
- `ANY`, `SOME` or `ALL` with no argument or with more than one argument. The error is `CADB0406`.
- The quantifier on the left side, as in `ANY (tags) = x`, or a quantifier in any other position, as in
  `SELECT any(tags)`. The error is "ANY/SOME/ALL is valid only as the right operand of = or <>".
- An array text literal on the right, such as `x = ANY ('{a,b}')`. CamusDB has no array text literal.

PostgreSQL rejects `x = (ANY (tags))`. CamusDB accepts it and reads it as `x = ANY (tags)`.

A column can still have the name `any`, `some` or `all`. The words are a quantifier only when a
parenthesized argument follows them on the right side of a comparison.
