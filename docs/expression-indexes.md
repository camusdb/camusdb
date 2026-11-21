# Expression Indexes

An expression index stores the value of an expression, not the value of a column, in each entry.
A query that filters or sorts on the same expression can then read the index instead of the table.

```sql
CREATE INDEX users_email ON users ((doc->>'email'));
CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)));

SELECT * FROM users WHERE doc->>'email' = 'a@example.com';   -- uses users_email
SELECT * FROM users WHERE lower(email) = 'a@example.com';    -- uses users_lower_email
```

PostgreSQL and CockroachDB accept the same form. ORMs and migration tools emit it for
case-insensitive lookups and for fields of a JSON document.

## Contents

- [Syntax](#syntax)
- [Allowed expressions](#allowed-expressions)
- [Refused expressions](#refused-expressions)
- [Result type](#result-type)
- [How the planner uses an expression index](#how-the-planner-uses-an-expression-index)
- [Writes and the backfill](#writes-and-the-backfill)
- [Unique expression indexes](#unique-expression-indexes)
- [Stored form](#stored-form)
- [Schema changes](#schema-changes)
- [SHOW CREATE TABLE, SHOW INDEXES and camus-dump](#show-create-table-show-indexes-and-camus-dump)
- [Cluster mode](#cluster-mode)
- [Limits](#limits)
- [Code map](#code-map)

## Syntax

A key part of an ordered index is a column, a parenthesized expression, or a function call. Each
part takes `ASC` or `DESC`, and expressions and columns can be mixed in one key.

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON t ((expr) [ASC | DESC], col [ASC | DESC], ...) [INCLUDE (...)];
CREATE INDEX users_upper ON users (upper(email));            -- a call needs no extra parentheses
ALTER TABLE users ADD INDEX users_len ((length(email)));
CREATE TABLE users (id oid PRIMARY KEY, email string, UNIQUE KEY users_lower_email ((lower(email))));
```

`((col))` is the plain column `col`, not an expression. The primary key, the `INCLUDE` list and a GIN
index take columns only.

`DESC` follows the column rule: a `string` or `bytes` expression cannot be descending, because those
types have no descending key encoding.

## Allowed expressions

The value of the expression goes into every entry, so the expression must give the same value for the
same row each time it runs, on every node and in every build. An index expression can hold:

- literals, and columns of the indexed table (with or without the table name as a qualifier);
- arithmetic, string, bit, comparison and boolean operators, `LIKE`, the regular-expression operators,
  `IS NULL`, `IN` with a list of values, `BETWEEN`, subscripts, `CASE` and `IF`;
- `CAST` to a type without a size, and the `::` form of it;
- calls of the immutable built-in functions.

The immutable built-in functions are listed in `ScalarFunctionRegistry.ImmutableFunctionNames`. The
list is opt-in: a function that is not on it is refused. A test fails when a new built-in function is
on neither that list nor `NotImmutableFunctionNames`, so each new function gets a decision. A function
that reads the clock for one of its forms, such as `age` or `unix_timestamp`, is not on the list.
`lower` and `upper` use the invariant culture, and no session time zone exists, so the date and text
functions are immutable.

## Refused expressions

Each of these gives `CADB0423` (`InvalidIndexExpression`; HTTP 400, gRPC `INVALID_ARGUMENT`):

| Expression | Reason |
|------------|--------|
| `now()`, `current_date()`, `clock_timestamp()` | reads the clock |
| `random()`, `gen_id()`, `gen_uuid_v4()` | gives a new value each time |
| `nextval('s')` and the other sequence functions | reads a sequence |
| `current_user()` and the other session functions | reads the session |
| `count(x)` and every aggregate | an aggregate has no value for one row |
| `(SELECT ...)`, `EXISTS`, `IN (SELECT ...)`, `x = ANY (...)` | a subquery |
| `row_number() OVER ()` | a window function |
| `lower(@p)` | a parameter |
| `DEFAULT`, `*` | not a value |
| `lower(missing)`, `lower(other.email)` | not a public column of the indexed table |
| an extension or inline function | not an immutable built-in |
| `doc->'a'`, an array, an interval | no ordered key encoding |
| `json_value(doc, '$.a')` | the type is known only from the value |
| `CAST(email AS string(10))` | a sized cast target |

A sized cast is refused because the stored text of the expression keeps the target type but not its
size. Cast to a type without a size, or index the column.

## Result type

The type of the expression is computed from the statement when the index is created, by the same rules
that type a column of `CREATE TABLE AS SELECT`. It must be known before a row is read and must have an
ordered key encoding: `id`, `int64`, `string`, `bool`, `float64`, `float32`, `bytes`, `date`,
`datetime`, `uuid` or `numeric`. Use `->>` to index a text field of a `jsonb` document, and a cast to
index it as another type:

```sql
CREATE INDEX docs_v ON docs (((doc->>'v')::int64));
SELECT id FROM docs WHERE (doc->>'v')::int64 = 42;   -- uses docs_v
```

The type is stored with the index. A later build that types a function another way does not change how
an existing index encodes its keys.

## How the planner uses an expression index

The predicate analysis recognizes `column op constant`. A WHERE term over an expression first stays in
the residual conjuncts. `ExpressionIndexPredicates.Match` then looks at each residual conjunct of one of
these shapes:

- `expr op constant` and `constant op expr`, with `=`, `<`, `<=`, `>` or `>=`;
- `expr BETWEEN a AND b`;
- `expr IN (v1, v2, ...)`;
- `expr = a OR expr = b ...`.

It renders `expr` in its canonical text: names in lower case, the table qualifier removed, function
names in lower case. Parentheses never reach the syntax tree, so they do not change the text. String
literals keep their case. When an index expression has the same canonical text, the term becomes a
comparison on the hidden key name of that expression. From there the index selector, the bound
absorption, the constant coercion and the residual filter treat it like a column comparison.

```sql
-- all three use users_lower_email:
WHERE lower(email) = 'a@x'
WHERE LOWER( email ) = 'a@x'
WHERE 'a@x' = lower(u.email)     -- FROM users u

-- a different expression, so it does not:
WHERE upper(email) = 'A@X'
WHERE doc->>'EMAIL' = 'a@x'      -- the literal differs in case
```

A term that the scan absorbs leaves the residual filter: each entry key is the value of the expression
for its row, computed by the same evaluator. A term that no scan absorbs is evaluated on each row as it
was written.

`ORDER BY expr` reads the index order when `expr` matches the expression at that key position, and the
plan then has no sort. An equality on a leading expression lets the next key order the rows:
`WHERE lower(email) = 'a@x' ORDER BY score` uses an index on `((lower(email)), score)` with no sort.

EXPLAIN names the expressions of an expression index in the `detail` column:

```text
node           detail
index-lookup   index=users_lower_email, expressions=[lower(`email`)], key='a@x'
```

## Writes and the backfill

At an expression position, the in-memory index holds a hidden key name (`#expr:` and a hash of the
stored text and type). No SQL identifier can contain `#`, so no column, alias or parameter has that name.
`IndexKeyRows.For` gives each write path a copy of the row with the value of each expression under its
hidden name, and the key builders read it like a column. The copy is for key building only. It is never
encoded, returned or given to a trigger.

- `INSERT`, `INSERT ... SELECT` and the insert of `ON CONFLICT` evaluate each expression for the new
  row. A column the statement leaves out reads as NULL.
- `UPDATE` compares the columns each expression reads between the old and the new row. When none of
  them changed, the index is not touched and the expression is not evaluated. Otherwise the old and the
  new key are compared, so `UPDATE users SET email = 'ANN@x'` on `'ann@x'` keeps the entry of
  `lower(email)`.
- `DELETE` and the row-level TTL sweep evaluate the expression on the old row to find its entry.
- The backfill of `CREATE INDEX` (`IndexBackfiller`) decodes the columns the expressions read and
  evaluates them the same way. The standalone and the cluster add paths share it, with concurrent
  writers.

An error that the expression raises for a row fails the statement, as the same error in a `SET` or a
`CHECK` does. A value whose type differs from the stored result type also fails the statement, except
an `int64` value of a `float64` expression, which is widened.

## Unique expression indexes

Duplicate detection uses the unique-index storage semantics and gives `DuplicateUniqueKeyValue`. A row
whose expression is NULL has no entry, so several such rows can exist, as for a unique column.
`INSERT ... ON CONFLICT DO NOTHING` without a target, and `ON CONFLICT ON CONSTRAINT name`, treat a unique
expression index as an arbiter. A column list target (`ON CONFLICT (email)`) matches only an index on
those columns.

## Stored form

`TableIndexSchema.KeyExpressions` holds one `IndexKeyExpression` for each expression position:

| Field | Value |
|-------|-------|
| `Position` | the key position |
| `Sql` | the canonical text, in which each column is a `__camus_col_{id}` token (`StoredColumnRef`, the form view bodies use) |
| `Type` | the result type |
| `ColumnIds` | the ids of the columns the expression reads |

At that position `ColumnIds` holds the marker `~expr`, which matches no column. An index without
expressions has no `KeyExpressions` field, so its stored form is the same as before.

The text is stored in place of the syntax tree because the tree's wire codec writes node types as
implicit enum numbers, and a node type added in the middle of the enum would change them.

`TableOpener` parses the text again when it opens the table, replaces each token with the current column
name, and builds the in-memory `BoundIndexExpression`: the expression for evaluation, the display text,
the canonical text and the hidden key name. `TableDescriptor.ExpressionKeys` maps each hidden key name
to its expression; the planner reads its type there.

## Schema changes

- `RENAME COLUMN` needs no change to the index, because the stored text names the column by id.
  SHOW CREATE TABLE and EXPLAIN show the new name.
- `DROP COLUMN` on a column that an index expression reads is refused, as it is for a key column. Drop
  the index first.
- `DROP INDEX` and `RENAME INDEX` work as for any index.

## SHOW CREATE TABLE, SHOW INDEXES and camus-dump

SHOW CREATE TABLE prints an expression index inline, with each column name quoted:

```sql
UNIQUE KEY `users_lower_email` ((lower(`email`))),
KEY `users_mixed` (`score` DESC, (jsonb_object_field_text(`doc`, 'email'))),
```

`doc->>'email'` prints as `jsonb_object_field_text(doc, 'email')`, the call the operator parses to. The
statement parses again to the same index.

SHOW INDEXES writes an expression position of `Columns` as `(expr)`. An expression can hold commas, so
`Columns` cannot be split to rebuild an expression index. The `Key` field holds the key list exactly as
`CREATE INDEX` takes it:

```text
Columns:    score,(jsonb_object_field_text(`doc`, 'email'))
Directions: DESC,ASC
Key:        `score` DESC, (jsonb_object_field_text(`doc`, 'email'))
```

camus-dump writes `Key` for an index whose `Columns` holds an expression. An older camus-dump refuses
such an index, because an expression is no identifier, and stops with an error.

## Cluster mode

`CREATE INDEX` on a cluster runs through the schema-change coordinator, as for any index: the index goes
from `DeleteOnly` to `WriteOnly`, the backfill runs, and the index becomes `Public`. `IndexBuildInfo`
and the persisted coordinator job carry the stored expressions, so a job that a new leader resumes
builds the same keys. A follower forwards the statement to the schema leader with the expression text
in `ColumnIndexInfoRequest.Expression`.

Every node of a cluster runs the same build. A build that does not know expression indexes reads the
`~expr` marker as an unknown column and fails on the first write to the index, instead of writing
entries over the wrong values.

## Limits

- No statistics are collected for an expression key: `ANALYZE` skips expression indexes, and the cost
  model prices an expression comparison with its default selectivity.
- A join leaf, and a query whose rows are keyed by qualified names, does not match an expression index.
  The term is evaluated on each row.
- A `GROUP BY` or `DISTINCT` on an expression does not stream from an expression index. It is grouped by
  hashing, with the same result.
- A range on a leading `DESC` key part uses no index scan, as a range on a leading `DESC` column does
  not. An equality uses the index.
- Partial indexes (`WHERE` on `CREATE INDEX`), expression GIN indexes and computed columns are separate
  features.

## Code map

| Concern | File |
|---------|------|
| Grammar (`index_element_list`) | `CamusDB.Core/SQLParser/SQLParser.Language.grammar.y` |
| Ticket text of an expression part | `Controllers/DDL/SQLExecutorAlterIndexCreator.ExpressionKeyPart`, `Models/ColumnIndexInfo.cs` |
| Validation, stored form, binding, canonical text | `Controllers/DDL/IndexExpressionBinder.cs` |
| Immutable function list | `Controllers/Functions/ScalarFunctionRegistry.cs` |
| Persisted and in-memory models | `Catalogs/Models/IndexKeyExpression.cs`, `BoundIndexExpression.cs`, `TableIndexSchema.cs` |
| Standalone add path | `Controllers/DDL/TableIndexAdder.cs` |
| Cluster add path and resume | `Controllers/DDL/SchemaDdlService.cs`, `Catalogs/SchemaChangeCoordinator.cs` |
| Inline `KEY` of `CREATE TABLE` | `Catalogs/Replication/SchemaChangeEntryFactory.BuildInlineIndexes` |
| Key values for the write paths | `Controllers/IndexKeyRows.cs` |
| WHERE matching | `Controllers/Queries/ExpressionIndexPredicates.cs` |
| ORDER BY matching | `Controllers/Queries/IndexScanSelector.OrderKeyMatches` |
| Rendering | `Controllers/SchemaQuerier.cs`, `Controllers/Queries/PlanRenderer.cs` |
| Tests | `CamusDB.Tests/CommandsExecutor/TestExpressionIndexes.cs`, `TestExpressionIndexesCluster.cs`, `TestOnlineBackfillUnderWrites.cs`, `CamusDB.Tests/Queries/TestImmutableFunctionList.cs` |
