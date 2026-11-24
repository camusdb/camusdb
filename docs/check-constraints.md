# CHECK & Named NOT NULL Constraints — Concepts & Developer Guide

> **Audience:** developers writing SQL against CamusDB who want to know exactly how `CHECK`
> and named `NOT NULL` behave, and engineers maintaining or extending the catalog/DDL, write,
> and replication layers.
> **Scope:** the SQL surface, the load-bearing NULL semantics, how a condition is stored and
> re-parsed, where it is enforced on writes, how `ALTER` validates existing data, and how
> constraints replicate across a cluster.

CamusDB supports `CHECK` — the most general single-row integrity constraint — as both a
column-level and a table-level constraint, addable and droppable via `ALTER TABLE`. The same
`ALTER … DROP CONSTRAINT` machinery also names and drops `NOT NULL` constraints. This means that a
previously permanent `NOT NULL` can be introspected and removed.

---

## Part I — Using CHECK constraints

### What a CHECK is

A `CHECK` is a boolean predicate over the columns of a **single row**. It is evaluated on each
`INSERT` and `UPDATE`, against the fully-built row (defaults applied, values coerced), just
before the row is written. **The write returns an error only when the predicate evaluates to
`FALSE`.** `DELETE` is unaffected.

```sql
-- column-level: the check is attached to one column but may reference others
CREATE TABLE products (
  product_no integer,
  name       string,
  price      float64 CHECK (price > 0)
);

-- table-level, named: the name is what you later DROP
CREATE TABLE products (
  product_no       integer,
  price            float64,
  discounted_price float64,
  CONSTRAINT valid_discount CHECK (price > discounted_price)
);

-- table-level, unnamed (auto-named)
CREATE TABLE t (x int64, y int64, CHECK (x + y > 0));

-- add / drop after the fact
ALTER TABLE employees ADD CONSTRAINT positive_salary CHECK (salary > 0);
ALTER TABLE employees DROP CONSTRAINT positive_salary;
```

A column check may reference **other** columns — `joined_date date CHECK (joined_date > birth_date)`
is legal; it is a table check written at the column.

### Constraint names

Each check has a name. `DROP CONSTRAINT` uses it, and `SHOW CREATE TABLE` shows it. The name
depends on how the check is declared.

| Declaration                              | Name                        |
| ---------------------------------------- | --------------------------- |
| Column-level `col … CHECK (…)`           | `{table}_{col}_check`       |
| Table-level `CONSTRAINT name CHECK (…)`  | `name` (as written)         |
| Table-level unnamed `CHECK (…)`          | `{table}_check{N}`          |

Names must be **unique per table** across the CHECK, named NOT NULL and FOREIGN KEY constraints.
This is because `DROP CONSTRAINT` finds a constraint by name across all three kinds. Auto-naming skips
any slot a user name already claimed, and a genuine duplicate returns `CADB0400` at
`CREATE`/`ALTER`. That includes a CHECK that takes the name of a foreign key, and a `SET NOT NULL` whose
generated name is the name of a CHECK or a foreign key. The comparison ignores case.

> Note: a *column-level named* check (`col … CONSTRAINT name CHECK (…)`) is **not** in the grammar.
> Use the table-level form when you need to choose the name.

### What a condition may contain

A check must be a **deterministic, single-row** predicate. The conditions in this list return an
error at DDL-validate time, for both `CREATE` and `ALTER ADD`.

- **subqueries** (scalar, `IN (SELECT …)`, `EXISTS`),
- **aggregate functions** (`count`, `sum`, `avg`, …),
- **volatile / non-deterministic functions** (`now`, `gen_uuid_v4`/`v7`, `gen_id`, …),
- references to **columns that don't exist** on the table,
- **functions that don't exist**, and a wrong argument count,
- a mistake in types that the statement shows, such as `CHECK (name > 5)` on a `string` column,
- a condition that is **not a `bool`**, such as `CHECK (a)` on an `int64` column or `CHECK (1)`. The
  value of such a condition would read as unknown. This means that it could not cause a write error.

These are the checks that any statement gets. See
[Errors that do not depend on the rows](data-types.md#errors-that-do-not-depend-on-the-rows). A stored
condition is not validated when it loads. This means that a constraint stored before these checks
existed is not affected.

Anything else that the WHERE grammar supports is allowed in a check. For example, a check can use
comparisons, arithmetic, `BETWEEN`, `LIKE`/`ILIKE` and the regex operators (see below). It can also
use `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `IN (literal list)`, deterministic function calls and `CAST`.

### The NULL rule (three-valued logic) — read this

SQL evaluates a CHECK with **three-valued logic**. A constraint is violated **only when it is
`FALSE`**. `TRUE` and `UNKNOWN` both pass. A comparison with a `NULL` operand is `UNKNOWN`.

This means that **NULL is allowed in a nullable column that has a check.**

```sql
CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0));

INSERT INTO products (id, price) VALUES (gen_id(), 10);   -- price > 0  → TRUE      → accepted
INSERT INTO products (id, price) VALUES (gen_id(), -5);   -- price > 0  → FALSE     → REJECTED
INSERT INTO products (id)        VALUES (gen_id());        -- NULL > 0   → UNKNOWN   → accepted
```

The combinators follow the standard three-valued truth tables.

- `UNKNOWN AND FALSE = FALSE`, `UNKNOWN AND TRUE = UNKNOWN`, `UNKNOWN AND UNKNOWN = UNKNOWN`
- `UNKNOWN OR TRUE = TRUE`, `UNKNOWN OR FALSE = UNKNOWN`, `UNKNOWN OR UNKNOWN = UNKNOWN`
- `NOT UNKNOWN = UNKNOWN`

If you want to *forbid* NULL, add `NOT NULL` or write `… IS NOT NULL AND …` in the check. That is a
deliberate, separate decision.

### Regex match operators (`~`, `~*`, `!~`, `!~*`)

PostgreSQL-style regular-expression operators are available anywhere a boolean expression is (a
CHECK, a `WHERE`, a `HAVING`). Both operands must be text.

| Operator | Meaning                       | Case         |
|----------|-------------------------------|--------------|
| `~`      | matches regex                 | sensitive    |
| `~*`     | matches regex                 | insensitive  |
| `!~`     | does **not** match regex      | sensitive    |
| `!~*`    | does **not** match regex      | insensitive  |

```sql
CREATE TABLE users (
    username text NOT NULL,
    CONSTRAINT username_format
        CHECK (username ~ '^[a-zA-Z][a-zA-Z0-9_]{2,29}$')
);
```

The regex operators work as described in this list.

- **Unanchored.** The pattern matches if it occurs *anywhere* in the value — anchor explicitly with
  `^` / `$` (as above) to require a full-string match.
- **.NET regex flavor.** The patterns use .NET regular expressions, which differ from POSIX ERE. For
  common constructs (character classes, quantifiers, anchors, alternation, groups), this flavor is a
  superset of POSIX ERE. POSIX named classes like `[[:alpha:]]` are **not**
  supported — use `\p{L}` or `[a-zA-Z]`. Case-insensitivity (`~*` / `!~*`) is locale-invariant.
- **Three-valued NULL** applies as in any other comparison. If the subject (or pattern) is
  `NULL`, the result is `UNKNOWN`. This means that `NULL` is allowed in a nullable column with a
  regex CHECK. The negated forms behave the same way: `!~` on a `NULL` is `UNKNOWN`.
- **Malformed patterns fail early.** A literal pattern that isn't a valid regex returns an error at
  `CREATE` / `ALTER` time. The error doesn't wait for the first `INSERT`. A malformed or timed-out pattern
  encountered *during* a CHECK evaluation surfaces as a check-constraint violation (HTTP 400).
- **ReDoS guard.** Each match has a bounded timeout
  (`CamusDBOptions.RegexMatchTimeoutMs`, default 250 ms). A pathological pattern fails when the
  timeout expires. Compiled patterns are cached (`CamusDBOptions.RegexCacheMaxEntries`).

### Array columns

With `cardinality` and `array_contains`, a check tests the full value of an `array(T)` column.
`cardinality` returns its length, and `array_contains` tests whether the array contains a value. A
subscript tests one position.

```sql
CREATE TABLE posts (
  id   int64 PRIMARY KEY,
  tags array(string) CHECK (cardinality(tags) <= 3 AND NOT array_contains(tags, 'banned'))
);
```

Both functions give NULL for a NULL array. This means that a NULL array passes the check.

For a length rule, use `cardinality`. `array_length(tags, 1)` is NULL for an empty array, as in
PostgreSQL. This means that `CHECK (array_length(tags, 1) >= 1)` does **not** stop an empty array:
the condition is UNKNOWN and the check passes. With `CHECK (cardinality(tags) >= 1)`, an empty array
returns an error. `array_contains` follows the `IN` rule. An array with a NULL element and no match
gives UNKNOWN, and the row passes a check on `NOT array_contains(...)` — the same as a check on
`NOT IN`. See
[Data types → Arrays](data-types.md#arrays).

The PostgreSQL forms `'banned' <> ALL (tags)` and `'news' = ANY (tags)` also work in a check, with
the same rules. CamusDB stores them as `NOT array_contains(tags, 'banned')` and
`array_contains(tags, 'news')`, and `SHOW CREATE TABLE` shows that form. See
[`= ANY`, `= SOME` and `<> ALL`](sql-expression-syntax.md#-any--some-and--all).

### Type coercion in a check

Two implicit coercions make the natural SQL work, exactly as in the WHERE path. An explicit `CAST`
is not needed.

- **Numeric widening.** An integer literal compares against a floating-point column. For example,
  `price float64 CHECK (price > 0)` works: the `0` (an `Integer64` literal) is compared numerically
  to the `Float64` value. Integer literals are likewise allowed for float columns on `INSERT`
  (`VALUES (100)` into a `float64` column stores `100.0`).
- **String→typed literal.** A bare string literal is coerced to `Uuid` / `Id` / `Date` / `DateTime`
  when the other operand is that type.

  ```sql
  CREATE TABLE people (id object_id PRIMARY KEY, birth_date date CHECK (birth_date > '1900-01-01'));
  ```

A comparison between incompatible types that neither coercion covers (e.g. a `string` column vs a
numeric literal) returns `CADB0400` when the constraint is declared. When only a value shows the
type, as for a function result (`CHECK (name > length(name))`), the write returns a check violation
for the row (`CADB0303`, "compares incompatible types", HTTP 400).

### ALTER adds validate existing data

`ALTER TABLE … ADD CONSTRAINT … CHECK` **scans the table** and evaluates the new check against
each existing row. If any row violates it, the `ALTER` returns `CADB0303`. The constraint is then
removed again. This matches PostgreSQL. `NOT VALID`, which adds a check and skips the scan, is not
yet supported.

An `ALTER` that fails for any other reason (for example, a timeout while it waits for the other nodes)
also removes the constraint. A failed `ALTER` therefore leaves no constraint, and you can run the same
statement again.

The check is **enforced before the scan starts**. During the scan, an INSERT or UPDATE that breaks
the new check returns `CADB0303`, even if the `ALTER` fails later. This order makes the result
correct. A write that commits during the scan was checked by the statement that made it. This means
that no violating row can appear behind the scan. A transaction that wrote the table before the
`ALTER` started gets the retryable `CADB0502` at commit.

### Error code

| Condition                              | Code       | HTTP |
| -------------------------------------- | ---------- | ---- |
| Row violates a CHECK                   | `CADB0303` | 400  |
| Incompatible types in a CHECK          | `CADB0303` | 400  |
| Invalid check at DDL (subquery/…)      | `CADB0400` | —    |

The message names the violated constraint: `new row violates check constraint "positive_price"`.

---

## Part II — Named & droppable NOT NULL

Historically `NOT NULL` in CamusDB was an anonymous boolean flag that could not be removed. It now
has an **identity** and **ALTER operations**. Its enforcement is the same fast boolean check, and it
is **not** rerouted through the CHECK evaluator.

```sql
-- named at creation
CREATE TABLE products (
  id   object_id PRIMARY KEY,
  name string CONSTRAINT products_name_not_null NOT NULL
);

-- add / remove after the fact
ALTER TABLE products ALTER COLUMN name SET NOT NULL;   -- scans rows; rejects if any is NULL
ALTER TABLE products ALTER COLUMN name DROP NOT NULL;  -- unconditional

-- drop by name (resolves against checks AND named NOT NULLs)
ALTER TABLE products DROP CONSTRAINT products_name_not_null;
```

Named NOT NULL constraints follow these rules.

- Each `NOT NULL` gets a name: the explicit one, or an auto-name `{table}_{col}_not_null`. This means
  that `DROP CONSTRAINT` and introspection work uniformly.
- **`SET NOT NULL`** scans existing rows and returns an error (`CADB0301` / `NotNullViolation`) if
  any value is NULL, exactly like ADD CHECK's existing-data scan. Writing a NULL returns an error from
  the start of the scan, and the column is nullable again if the scan fails. On a column that is
  already `NOT NULL`, it only sets the generated name and reads no row.
- **`DROP NOT NULL`** is unconditional — relaxing a constraint needs no scan.
- **`DROP CONSTRAINT <name>`** resolves the name against the table's check constraints, then each
  column's NOT NULL constraint name, then the table's foreign keys; not found in any →
  "constraint '…' does not exist".

---

## Part III — How it works (maintainer guide)

### Data flow at a glance

```
SQL text
  │  parser (grammar + lexer, regenerated)
  ▼
NodeAst  ── ConstraintCheck / CreateTableConstraintCheck / AlterTableAddConstraintCheck /
  │           AlterTableDropConstraint / ConstraintNotNullNamed / AlterTableSet|DropNotNull
  │  SQLExecutorCreateTableCreator / SQLExecutorAlterConstraintCreator
  ▼           · desugar column checks → named table checks
  │           · DDL validation (no subquery/aggregate/volatile/unknown-column)
  │           · render condition → SQL text  (CheckConditionRenderer)
  │           · extract referenced columns
  ▼
Ticket  ── CreateTableTicket.CheckConstraints[] / AlterConstraintTicket
  │  CommandExecutor → CatalogsManager
  ▼
TableSchema.CheckConstraints : List<CheckConstraintSchema>   (source of truth, in-memory + persisted)
  │            · Expression  = stored SQL text
  │            · ParsedCondition = NodeAst  (transient, rebuilt at load — NOT persisted)
  ▼
Enforcement on write:  RowInserter.Validate / RowUpdater.FlushUpdateChunk
                         → CheckEnforcer.EnforceOnRow → CheckEvaluator.Evaluate (three-valued)
```

### Grammar / AST

New tokens `TCHECK` and `TSET`; the `condition` non-terminal is reused verbatim (no new expression
grammar). New `NodeType`s: `ConstraintCheck` (column), `CreateTableConstraintCheck` (table, named or
unnamed), `AlterTableAddConstraintCheck`, `AlterTableDropConstraint`, `ConstraintNotNullNamed`,
`AlterTableSetNotNull`, `AlterTableDropNotNull`. The parser/scanner are **regenerated** from
`SQLParser.Language.grammar.y` / `.analyzer.lex` — do not hand-edit the generated files.

### Column check desugaring

`SQLExecutorCreateTableCreator.CollectCheckConstraints` walks the CREATE AST and turns **each**
check — column-level and table-level — into a `CheckConstraintInfo { Name, Expression,
ReferencedColumns }` on `CreateTableTicket.CheckConstraints`. There is exactly **one internal
representation**: a named check with a condition. (Column checks are *not* carried on `ColumnInfo`;
that path was removed as dead.)

### Store as text, re-parse at load — and why there are two renderers

The persisted form of a condition is **human-readable SQL text** (good for `SHOW CREATE TABLE`). At
table-open, `CatalogsManager.ParseCheckConstraintAsts` / `LoadCheckConstraintAsts` parse each
`Expression` back into a `NodeAst` and cache it on `CheckConstraintSchema.ParsedCondition`
(`[JsonIgnore]`, not persisted). **Enforcement always uses this re-parsed AST**, on all nodes.

That makes round-trip fidelity load-bearing: *render → persist → parse* must reproduce the **same
predicate**. `PlanRenderer.RenderExpr` is intentionally lossy. It targets EXPLAIN display. It drops parentheses,
collapses IN-lists to `(...)`, and abbreviates CAST targets. Using it would silently
enforce a *different* predicate after reload (`(a OR b) AND c` → `a OR b AND c` → `a OR (b AND c)`).

For this reason, checks use a dedicated **`CheckConditionRenderer`**. It differs from
`PlanRenderer` in these ways.

- It parenthesizes each **compound** sub-expression when it is an operand. This means that
  precedence can never be regrouped. Atomic/self-delimiting operands are left unwrapped: `price > 0`
  is rendered as `price > 0`, and `(a OR b) AND c` is rendered with its parentheses.
- It renders IN-lists and function arguments in full, and does the same for CAST target types.
- It **throws** for anything it cannot represent faithfully. The DDL layer then returns an error for
  the constraint, and a string that would re-parse differently is not persisted.

`PlanRenderer` is unchanged and is used for EXPLAIN. The guard is a *semantic* round-trip test:
render → parse → render must be a fixed point. Render stability alone is not what it tests.

### Schema model

- **`CheckConstraintSchema`** (`Catalogs/Models/`): `{ Name, Expression, ReferencedColumns,
  [JsonIgnore] ParsedCondition }`. `ReferencedColumns` is computed once from the AST for existence
  validation and the UPDATE skip-optimization.
- **`TableSchema.CheckConstraints`** (`List<CheckConstraintSchema>?`) — the source of truth,
  parallel to `Indexes`; null/empty for a table that has no checks (backward-compatible).
- **`TableColumnSchema.NotNullConstraintName`** (`string?`) — the identity of a named NOT NULL;
  the `NotNull` bool is unchanged and remains the enforcement signal.

### Enforcement

`CheckEnforcer.EnforceOnRow(table, row)` calls `CheckEvaluator.Evaluate(check.ParsedCondition, row)`
for each entry of `table.Schema.CheckConstraints`. It throws `CADB0303` on a `false` result. It is
called from the INSERT and UPDATE paths.

- **INSERT** — in `RowInserter.Validate`, in the per-row loop, after NOT NULL / length checks,
  before the row is written. (Rows are already fully built with defaults at ticket-creation time.)
- **UPDATE** — in `RowUpdater.FlushUpdateChunk`, on the new row, after coercion / NOT NULL
  checks and before `RowEncoder.Encode`.

**`CheckEvaluator`** is the three-valued core. It is deliberately isolated from the WHERE evaluator,
which collapses NULL → false.

- comparison nodes evaluate each operand via the leaf evaluator; if **either operand is `Null`** →
  `UNKNOWN`; otherwise compare;
- `AND`/`OR`/`NOT` implement the three-valued tables above;
- `IS [NOT] NULL` are always definite; `BETWEEN`/`LIKE`/`IN` return `UNKNOWN` when a relevant operand
  is NULL;
- **leaf evaluation delegates to `SQLExecutorBaseCreator.EvalExpr`** so coercion (String→Date/Uuid/Id,
  arithmetic, function calls, CAST) is identical to WHERE. A missing column (omitted from an INSERT)
  is treated as `NULL`.
- `CompareValues` widens mixed integer/float operands to `double`, coerces a bare string operand to
  `Uuid`/`Id`/`Date`/`DateTime`. It turns a genuinely incomparable pair into a `CamusDBException`
  (a `CheckConstraintViolation`). A raw `ArgumentException` from `ColumnValue.CompareTo` can't
  escape. The WHERE evaluator (`SQLExecutorBaseCreator.CompareValues`) applies the same numeric
  widening. This means that `WHERE price > 0` and `CHECK (price > 0)` behave consistently.

`NOT NULL` is **not** evaluated here. It is a fast boolean check in
`RowInserter.Validate` / `RowUpdater.CheckForNotNulls`.

### ALTER execution

The ALTER operations (`AddCheck`, `DropConstraint`, `SetNotNull`, `DropNotNull`, `SetStorage`) are
in `TableConstraintAlterer`.

**ADD CHECK and SET NOT NULL enforce the constraint before they scan** (`RowConstraintValidationPass`).
The scan takes no lock. This means that it doesn't see a write that commits after the scan passed its
row. The constraint is enforced before the scan, and the write-shape fence
([distributed-schema-architecture.md](distributed-schema-architecture.md) §9.1) settles each write
that was planned before the constraint existed. As a result, the statement that made each such write
checked it. In the opposite order, a short autocommit `UPDATE` can commit
a violating value between the scan and the start of enforcement, and the constraint is then
published over a row that breaks it. Both operations enter the DDL gate for their table, as a
foreign-key ADD does. In a cluster, the gate's semaphore is released during the scan, and the table is
held (`SchemaChangeCoordinator.ValidateRowConstraintAsync`). This means that DDL on other tables
of the database can proceed, and DDL on this table waits for the scan. A standalone node scans under
the semaphore.

`isClusterMode` selects one of two paths.

- **Standalone:** under `Schema.Semaphore`, publish a new constraint list or column list on
  `table.Schema`. The lists are copy-on-write and are not changed in place. DML iterates
  both lists with no lock. Next, `DatabaseDescriptor.FenceWritersAndWaitAsync` makes the later commit
  of each earlier writer fail, and waits for the commits in flight. After that, scan, persist in a DDL
  `KvTransaction`, and commit. Any failure before the commit **reverts** the in-memory change
  (`RevertChecksAsync` / `RevertColumnAsync`). This means that the node can't enforce a constraint
  that is not proven and durable. A
  crash before the persist leaves no constraint.
- **Cluster:** record a coordinator job (`SchemaElementKind.Check` / `SchemaElementKind.NotNull`,
  keyed by the constraint name) and replicate the delta (below). After that,
  `SchemaChangeCoordinator.ValidateRowConstraintAsync` waits until the earlier writers of each live
  node are settled. It then scans and deletes the job. A failure can be a violation or a timeout, for
  example. It can also be a lost leadership, or a replication that committed but whose
  acknowledgements timed out. On any failure, the coordinator replicates the removal
  (`DropCheckConstraint`, or `SetColumnNotNull` with `NotNull = false`) and then deletes the job
  (`SchemaChangeCoordinator.TakeBackRowConstraintAsync`). The job is left in place only when the
  removal also fails. A new schema leader that finds the job validates again
  (`ResumeJobsAsync`). This means that a constraint that is enforced but not proven is not left in
  place after a crash. A job that spent `MaxResumeAttempts` is abandoned, and its constraint is removed. A
  CHECK or NOT NULL doesn't have an element state that would show that it was not proven. While the
  job is active, the constraint is visible in `SHOW CREATE TABLE`.

### Cluster / replication

Check and NOT NULL DDL are replicated schema-log operations, exactly like `AddIndex`. The full
definition travels in the payload, and each node builds an **identical** AST. Enforcement is local to
each writer.

- **`SchemaOp`** (append-only; existing values must not be renumbered): `AddCheckConstraint = 10`, `DropCheckConstraint =
  11`, `SetColumnNotNull = 12`.
- **Payloads** (`SchemaChangePayloads.cs`, registered in `MetaJsonContext`): `SchemaCreateTablePayload`
  carries a `CheckConstraints` array; `SchemaCheckConstraintPayload { TableName, ConstraintName,
  Expression }` for ALTER add/drop; `SchemaSetColumnNotNullPayload { TableName, ColumnName, NotNull,
  ConstraintName }`.
- **Apply** (`CatalogsManager`): `ApplyCreateTable` folds `payload.CheckConstraints` and rebuilds the
  AST cache; `ApplyAddCheckConstraint`/`ApplyDropCheckConstraint`/`ApplySetColumnNotNull` mutate the
  in-memory schema. These apply callbacks are **idempotent** (add = replace, drop = remove-if-present,
  set = rebuild column) so Raft redelivery and WAL replay are safe, and they must **not** persist
  from the commit pipeline.
- **Follower forwarding:** a follower serializes the ticket to the leader's
  `/internal/schema-ddl/{create-table,alter-constraint}` endpoint. The forwarding DTOs
  (`SchemaDdlForwardRequests.cs`) carry the full data. This includes
  `ForwardCreateTableRequest.CheckConstraints` and `ColumnInfoRequest.NotNullConstraintName`/`DefaultFunction`.
  It also includes `ForwardAlterConstraintRequest.ColumnName` for the SET/DROP NOT NULL target. **Omitting any of
  these silently drops the constraint on the leader** — the wire form must stay in sync with the
  ticket; the DTO round-trip is covered by tests.

### Schema version

Check and NOT NULL changes are **validation-only** — they do not change row encoding — so they do
**not** bump `TableSchema.Version` (matching index-state changes). This is deliberate; getting it
wrong interacts with the row-decoder catch-up fence.

### AST cache invariant

`CheckEnforcer` **skips** any constraint whose `ParsedCondition` is null (defensive). Therefore each
path that makes a `TableSchema` live must rebuild the cache.

- fresh CREATE
- apply-from-replication
- standalone ALTER
- **load-from-disk**

`ParseCheckConstraintAsts` is invoked on all of them. A new path that populates `CheckConstraints`
and doesn't parse them would silently disable enforcement.

---

## Part IV — Key files

| File | Responsibility |
| ---- | -------------- |
| `SQLParser/*.grammar.y`, `*.analyzer.lex` | tokens + productions (regenerate the parser/scanner) |
| `Commands/Executor/Controllers/DDL/SQLExecutorCreateTableCreator.cs` | column-check desugaring, auto-naming, DDL validation, referenced-column extraction |
| `Commands/Executor/Controllers/DDL/SQLExecutorAlterConstraintCreator.cs` | builds the `AlterConstraintTicket` from ALTER AST |
| `Commands/Executor/Controllers/CheckConditionRenderer.cs` | faithful, round-trip-safe condition → SQL text |
| `Commands/Executor/Controllers/CheckEvaluator.cs` | three-valued evaluation + type coercion |
| `Commands/Executor/Controllers/CheckEnforcer.cs` | iterate + throw `CADB0303` on `false` |
| `Commands/Executor/Controllers/DDL/TableConstraintAlterer.cs` | ADD/DROP CHECK, SET/DROP NOT NULL, existing-row scans, revert-on-failure |
| `Catalogs/Models/CheckConstraintSchema.cs`, `TableSchema.cs`, `TableColumnSchema.cs` | schema model |
| `Catalogs/Models/SchemaChangePayloads.cs`, `SchemaOp.cs`, `Catalogs/MetaJsonContext.cs` | replicated payloads + ops |
| `Catalogs/CatalogsManager.cs` | fold/apply/persist, load-time AST re-parse |
| `Commands/Executor/Models/SchemaDdlForwardRequests.cs`, `HttpSchemaDdlForwarder.cs`, `App/Controllers/SchemaDdlForwardController.cs` | follower→leader forwarding wire form |
| `Commands/Executor/Controllers/RowInserter.cs`, `RowUpdater.cs` | enforcement call sites |
| `Commands/Executor/Controllers/SchemaQuerier.cs` | `SHOW CREATE TABLE` rendering |

---

## Part V — Limitations & non-goals

- **Foreign keys** are a separate constraint, described in [foreign-keys.md](foreign-keys.md). They
  share the constraint name space with CHECK and named NOT NULL (see "Constraint names").
- **No `NOT VALID` / deferred / `NOT ENFORCED`** checks — ADD always full-scans existing rows.
- **No cross-row / subquery / aggregate / non-deterministic** conditions — a check is a pure function
  of one row.
- **No column-level *named* CHECK** in the grammar — use the table-level `CONSTRAINT name CHECK (…)`
  form to control the name.
- The WHERE evaluator's `NULL → false` behavior is **unchanged**; the three-valued logic is
  CHECK-local.

---

## Part VI — Testing map

| Area | Tests |
| ---- | ----- |
| Parse (all declaration forms) | `SQLParser/TestSQLParserCheckConstraints.cs` |
| Model / desugaring / DDL validation | `CommandsExecutor/TestCheckConstraintsDDL.cs` |
| Three-valued evaluation + INSERT/UPDATE enforcement | `CommandsExecutor/TestCheckConstraintsEnforcement.cs` |
| Persistence, reopen, render→parse round-trip | `CommandsExecutor/TestCheckConstraintsPersistence.cs` |
| ALTER ADD/DROP + existing-row validation | `CommandsExecutor/TestCheckConstraintsAlter.cs` |
| Enforce-before-scan order, writes during the scan, removal on violation, job resume | `CommandsExecutor/TestRowConstraintValidationOrder.cs` |
| Named / droppable NOT NULL | `CommandsExecutor/TestNotNullConstraints.cs` |
| `SHOW CREATE TABLE` rendering | `CommandsExecutor/TestShowCreateTableRoundTrip.cs` |
| NOT NULL reopen, date/type coercion, precedence & IN-list fidelity, per-operator NULL end-to-end, ALTER-reject-leaves-unchanged, forwarding-DTO round-trip | `CommandsExecutor/TestCheckConstraintsFixes.cs` |

Changes to NULL semantics or to the condition renderer have a broad blast radius. The same applies to
the persist/reparse path and the forwarding DTOs. After such a change, run the check + notnull +
parser suites and capture results to a TRX.
