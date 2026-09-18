# PostgreSQL expression syntax

CamusDB accepts four PostgreSQL expression forms that data-generation and migration scripts use
often:

| Form | Example | Meaning |
|------|---------|---------|
| Digit separators | `200_000` | The number 200000 |
| `%` | `i % 3` | The remainder of `i / 3`, the same as `mod(i, 3)` |
| `::` | `x::text` | The same as `CAST(x AS text)` |
| Subscript | `tags[1]` | The first element of an array |

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
