# Expression Indexes

Each entry of an expression index stores the value of an expression computed from its row.
A query that filters or sorts on the same expression can then use the index to find its rows.

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

`((col))` is read as the column `col`. The primary key, the `INCLUDE` list and a GIN
index take columns only.

`DESC` follows the column rule: a `string` or `bytes` expression cannot be descending. Those types do
not have a descending key encoding.

## Allowed expressions

The value of the expression is written into the index entry. This means that the expression must give
the same value for the same row each time it is evaluated, on any node and in any build. An index
expression can contain only the elements below.

- literals, and columns of the indexed table (the table name as a qualifier is optional);
- arithmetic, string, bit, comparison and boolean operators, `LIKE`, the regular-expression operators,
  `IS NULL`, `IN` with a list of values, `BETWEEN`, subscripts, `CASE` and `IF`;
- `CAST` to an unsized type, and the `::` form of it;
- calls of the immutable built-in functions.

The immutable built-in functions are listed in `ScalarFunctionRegistry.ImmutableFunctionNames`. The
list is opt-in: a function that is not on it is refused. A test fails when a new built-in function is
on neither that list nor `NotImmutableFunctionNames`. This means that each new function gets a decision. A function
that reads the clock for one of its forms, such as `age` or `unix_timestamp`, is not on the list.
`lower` and `upper` use the invariant culture, and no session time zone exists. For this reason, the
date and text functions are immutable. The forms of `date_trunc`, `date_part` and `to_char` that take a
time zone argument are refused. Their result depends on the time zone database that ships with the build,
and a later build could compute another key for the same row.

## Refused expressions

Each expression in the following table returns the error `CADB0423` (`InvalidIndexExpression`; HTTP 400,
gRPC `INVALID_ARGUMENT`).

| Expression | Reason |
|------------|--------|
| `now()`, `current_date()`, `clock_timestamp()` | reads the clock |
| `random()`, `gen_id()`, `gen_uuid_v4()` | gives a new value each time |
| `nextval('s')` and the other sequence functions | reads a sequence |
| `current_user()` and the other session functions | reads the session |
| `count(x)` and the other aggregates | an aggregate gives one value for a group of rows |
| `(SELECT ...)`, `EXISTS`, `IN (SELECT ...)`, `x = ANY (...)` | a subquery |
| `row_number() OVER ()` | a window function |
| `lower(@p)` | a parameter |
| `DEFAULT`, `*` | not a value |
| `lower(missing)`, `lower(other.email)` | not a public column of the indexed table |
| an extension or inline function | not an immutable built-in |
| `date_trunc('day', ts, 'America/New_York')` | a time zone argument: the result depends on the build |
| `doc->'a'`, an array, an interval | no ordered key encoding |
| `json_value(doc, '$.a')` | the type is known only from the value |
| `CAST(email AS string(10))` | a sized cast target |

A sized cast is refused because the size of the target type is not saved in the stored text of the
expression. Cast to the unsized type, or index the column.

## Result type

The type of the expression is computed from the statement when the index is created, by the same rules
that type a column of `CREATE TABLE AS SELECT`. It must be known before a row is read and must have an
ordered key encoding: `id`, `int64`, `string`, `bool`, `float64`, `float32`, `bytes`, `date`,
`datetime`, `uuid` or `numeric`. Use `->>` to index a text field of a `jsonb` document, and add a cast
to index the field as another type. The following index stores the field `v` as an `int64`.

```sql
CREATE INDEX docs_v ON docs (((doc->>'v')::int64));
SELECT id FROM docs WHERE (doc->>'v')::int64 = 42;   -- uses docs_v
```

The type is stored with the index. A later build that types a function another way does not change how
an existing index encodes its keys.

The value of each row is computed with the types of the columns, as a `SELECT` of the same expression
computes it. A `CASE`, `COALESCE`, `IFNULL`, `IF`, `GREATEST`, `LEAST` or `ARRAY[...]` over columns of
different types has their common type in every row, also in a row where a `NULL` hides the type of a
column. With an `int64` column `i` and a `float64` column `f`, an index on `(COALESCE(i, f) / 4)` stores
`0.5` for the row `i = 2, f = NULL`, which is what `SELECT COALESCE(i, f) / 4` gives. An index on such an
expression that an earlier build created can hold keys computed from the values of each row (`0` in
that example). Rebuild it with `DROP INDEX` and `CREATE INDEX`.

## How the planner uses an expression index

The predicate analysis recognizes `column op constant`. A WHERE term over an expression is first left in
the residual conjuncts. `ExpressionIndexPredicates.Match` then examines each residual conjunct that has
one of the four shapes below.

- `expr op constant` and `constant op expr`, with `=`, `<`, `<=`, `>` or `>=`;
- `expr BETWEEN a AND b`;
- `expr IN (v1, v2, ...)`;
- `expr = a OR expr = b ...`.

It renders `expr` in its canonical text: names in lower case, the table qualifier removed, function
names in lower case. Parentheses are dropped by the parser and do not change the text. String
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

A constant has one value for the duration of a statement. A literal, a parameter, a statement-clock function
such as `now()`, and an immutable function of these are constants. An operand that calls a function
that can give a new value on each call, such as `random()`, `gen_id()`, `nextval()` or
`clock_timestamp()`, is no constant. Its term is left in the residual filter and evaluated for each
row, and it cannot become a seek bound. The same rule applies to a comparison on a column
(`PredicateAnalyzer.TryGetConstant`).

`ORDER BY expr` reads the index order when `expr` matches the expression at that key position, and the
plan then skips the sort step. An equality on a leading expression lets the next key order the rows:
`WHERE lower(email) = 'a@x' ORDER BY score` uses an index on `((lower(email)), score)` and needs no sort.

EXPLAIN lists the expressions of an expression index in the `detail` column of the plan.

```text
node           detail
index-lookup   index=users_lower_email, expressions=[lower(`email`)], key='a@x'
```

## Writes and the backfill

The in-memory index uses a hidden key name at an expression position (`#expr:` and a hash of the
stored text and type). No SQL identifier can contain `#`. This means that no column, alias or parameter
has that name.
`IndexKeyRows.For` gives each write path a small map of the key positions: the column value at a column
position, and the expression value under its hidden name at an expression position. The key builders
read it like a row. The map is for key building only. It is not encoded, returned or given to a
trigger. INCLUDE values come from the row itself.

- `INSERT`, `INSERT ... SELECT` and the insert of `ON CONFLICT` evaluate each expression for the new
  row. A column the statement leaves out reads as NULL.
- `UPDATE` compares the columns each expression reads between the old and the new row. When all of
  them have identical values in both rows, the index is not touched and the expression is not evaluated.
  Otherwise the old and the new key are compared. For example, `UPDATE users SET email = 'ANN@x'` on
  `'ann@x'` leaves the entry of `lower(email)` unchanged. The input test compares the stored values for
  identity (`IndexKeyRows.Identical`), which is stricter than SQL equality. For example, `0.0 = -0.0`, but `atan2(v, -1.0)` gives pi for one and
  -pi for the other, and the jsonb documents `{"a": 1}` and `{"a": 1.0}` are equal, but `doc->>'a'`
  gives `1` for one and `1.0` for the other. The INCLUDE values of a covering index use the same test.
- `DELETE` and the row-level TTL sweep evaluate the expression on the old row to find its entry.
- The backfill of `CREATE INDEX` (`IndexBackfiller`) decodes the columns the expressions read and
  evaluates them the same way. The standalone and the cluster add paths share it, with concurrent
  writers.

An error that the expression raises for a row fails the statement, as the same error in a `SET` or a
`CHECK` does. A value whose type differs from the stored result type also fails the statement, except
an `int64` value of a `float64` expression, which is widened.

## Unique expression indexes

Duplicate detection uses the unique-index storage semantics and gives `DuplicateUniqueKeyValue`. A row
whose expression is NULL does not get an entry. This means that several such rows can exist, as for a
unique column. `INSERT ... ON CONFLICT DO NOTHING` with the conflict target left out, and `ON CONFLICT ON CONSTRAINT name`, treat a unique
expression index as an arbiter. A column list target (`ON CONFLICT (email)`) matches only an index on
those columns.

## Stored form

Each expression position is stored as one `IndexKeyExpression` in `TableIndexSchema.KeyExpressions`,
with the four fields in the following table.

| Field | Value |
|-------|-------|
| `Position` | the key position |
| `Sql` | the canonical text, in which each column is a `__camus_col_{id}` token (`StoredColumnRef`, the form view bodies use) |
| `Type` | the result type |
| `ColumnIds` | the ids of the columns the expression reads |

At that position, the marker `~expr` is written to `ColumnIds`. It matches no column. The
`KeyExpressions` field is omitted for an index that contains only columns. This means that the stored
form of such an index is the same as before.

The text is stored in place of the syntax tree because the tree's wire codec writes node types as
implicit enum numbers, and a node type added in the middle of the enum would change them.

`TableOpener` parses the text again when it opens the table, replaces each token with the current column
name, and builds the in-memory `BoundIndexExpression`: the expression for evaluation, the display text,
the canonical text and the hidden key name.

A scan decodes index keys with the types of the index that its plan captured
(`TableDescriptor.IndexKeyType`). This means that a `DROP INDEX` that commits after planning does not
change how an open scan decodes the entries in its snapshot. `TableDescriptor.ExpressionKeys` maps each
hidden key name to its expression, for a planner pass that has only the name. The map is grow-only: a
`DROP INDEX` leaves its entry in place. The name is a hash of the stored text and the type. This means
that one name always has one type. The planner matches WHERE terms against the indexes that the table
has now. It does not match them against this map.

## Schema changes

- `RENAME COLUMN` needs no change to the index. The stored text names the column by id, and
  SHOW CREATE TABLE and EXPLAIN show the new name.
- `DROP COLUMN` on a column that an index expression reads is refused, as it is for a key column. Drop
  the index first.
- `DROP INDEX` and `RENAME INDEX` work as for any index.

## SHOW CREATE TABLE, SHOW INDEXES and camus-dump

SHOW CREATE TABLE prints an expression index inline, with each column name quoted.

```sql
UNIQUE KEY `users_lower_email` ((lower(`email`))),
KEY `users_mixed` (`score` DESC, (jsonb_object_field_text(`doc`, 'email'))),
```

`doc->>'email'` prints as `jsonb_object_field_text(doc, 'email')`, the call the operator parses to. The
statement parses again to the same index.

SHOW INDEXES writes an expression position of `Columns` as `(expr)`. An expression can include commas.
This means that `Columns` cannot be split to rebuild an expression index. The `Key` field gives the key
list in the exact form that `CREATE INDEX` takes.

```text
Columns:    score,(jsonb_object_field_text(`doc`, 'email'))
Directions: DESC,ASC
Key:        `score` DESC, (jsonb_object_field_text(`doc`, 'email'))
```

camus-dump writes `Key` for an index whose `Columns` value includes an expression. An older camus-dump
cannot read an expression as an identifier. It stops with an error on such an index.

## Cluster mode

On a cluster, `CREATE INDEX` goes through the schema-change coordinator, as for any index. The index
moves from `DeleteOnly` to `WriteOnly`, the backfill is done, and then the index becomes `Public`.
`IndexBuildInfo` and the persisted coordinator job carry the stored expressions. This means that a job
that a new leader resumes builds the same keys. A follower forwards the statement to the schema leader with the expression text
in `ColumnIndexInfoRequest.Expression`.

The definition that the coordinator stages is built on the table opened under the DDL gate. A
`DROP COLUMN` can commit after the statement first read the table and before it entered the gate. The
index is then refused with the error for a missing column, and no definition is staged. An index that
reads a column id the table does not have is also refused when the `AddIndex` delta is applied
(`IndexDeltaApplier`). The check is made on the proposer and on all nodes that apply the delta. This
means that no path can stage such an index.

The nodes of a cluster are expected to run the same build. A build that does not know expression indexes
reads the `~expr` marker as an unknown column and fails on the first write to the index. It does not
write entries over the wrong values.

## Limits

- No statistics are collected for an expression key: `ANALYZE` skips expression indexes, and the cost
  model prices an expression comparison with its default selectivity.
- A join leaf, and a query whose rows are keyed by qualified names, does not match an expression index.
  The term is evaluated on each row.
- A `GROUP BY` or `DISTINCT` on an expression does not stream from an expression index. It is grouped by
  hashing, with the same result.
- A range on a leading `DESC` key part uses no index scan, as a range on a leading `DESC` column does
  not. An equality uses the index.
- A read through an expression index that fetches the row also decodes the columns the expression
  reads, and evaluates the expression again, to check the entry against its row (`IndexEntryCheck`).
  A large value stored out of line, such as a big jsonb document, is fetched for that check even when
  the query does not return it. The cost model does not price the check. Each other plan for the same
  query evaluates the expression on at least the same rows. This means that the check does not change
  which plan is cheaper. A covering index, with an `INCLUDE` of all the columns the query reads, reads no
  row and makes no check.
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
