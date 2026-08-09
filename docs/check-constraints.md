# CHECK & Named NOT NULL Constraints — Concepts & Developer Guide

> **Audience:** developers writing SQL against CamusDB who want to know exactly how `CHECK`
> and named `NOT NULL` behave, and engineers maintaining or extending the catalog/DDL, write,
> and replication layers.
> **Scope:** the SQL surface, the load-bearing NULL semantics, how a condition is stored and
> re-parsed, where it is enforced on writes, how `ALTER` validates existing data, and how the
> whole thing replicates across a cluster.

CamusDB supports `CHECK` — the most general single-row integrity constraint — as both a
column-level and a table-level constraint, addable and droppable via `ALTER TABLE`. The same
`ALTER … DROP CONSTRAINT` machinery also names and drops `NOT NULL` constraints, so a
previously permanent `NOT NULL` can be introspected and removed.

---

## Part I — Using CHECK constraints

### What a CHECK is

A `CHECK` is a boolean predicate over the columns of a **single row**. It is evaluated on every
`INSERT` and every `UPDATE`, against the fully-built row (defaults applied, values coerced), just
before the row is written. **The write is rejected only when the predicate evaluates to `FALSE`.**
`DELETE` is unaffected.

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
is legal; it is simply a table check written at the column.

### Constraint names

Every check has a name, used by `DROP CONSTRAINT` and shown in `SHOW CREATE TABLE`:

| Declaration                              | Name                        |
| ---------------------------------------- | --------------------------- |
| Column-level `col … CHECK (…)`           | `{table}_{col}_check`       |
| Table-level `CONSTRAINT name CHECK (…)`  | `name` (as written)         |
| Table-level unnamed `CHECK (…)`          | `{table}_check{N}`          |

Names must be **unique per table**. Auto-naming skips any slot a user name already claimed, and a
genuine duplicate (two identically-named constraints) is rejected at `CREATE`/`ALTER`.

> Note: a *column-level named* check (`col … CONSTRAINT name CHECK (…)`) is **not** in the grammar.
> Use the table-level form when you need to choose the name.

### What a condition may contain

A check must be a **deterministic, single-row** predicate. Rejected at DDL-validate time (both
`CREATE` and `ALTER ADD`):

- **subqueries** (scalar, `IN (SELECT …)`, `EXISTS`),
- **aggregate functions** (`count`, `sum`, `avg`, …),
- **volatile / non-deterministic functions** (`now`, `gen_uuid_v4`/`v7`, `gen_id`, …),
- references to **columns that don't exist** on the table.

Everything the WHERE grammar supports otherwise is allowed: comparisons, `AND`/`OR`/`NOT`,
arithmetic, `BETWEEN`, `LIKE`/`ILIKE`, the regex operators `~` / `~*` / `!~` / `!~*` (see below),
`IS [NOT] NULL`, `IN (literal list)`, deterministic function calls, and `CAST`.

### The NULL rule (three-valued logic) — read this

SQL evaluates a CHECK with **three-valued logic**, and a constraint is violated **only when it is
`FALSE`**. `TRUE` and `UNKNOWN` both pass. A comparison with a `NULL` operand is `UNKNOWN`.

The practical consequence: **a nullable column with a check still accepts NULL.**

```sql
CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0));

INSERT INTO products (id, price) VALUES (gen_id(), 10);   -- price > 0  → TRUE      → accepted
INSERT INTO products (id, price) VALUES (gen_id(), -5);   -- price > 0  → FALSE     → REJECTED
INSERT INTO products (id)        VALUES (gen_id());        -- NULL > 0   → UNKNOWN   → accepted
```

The combinators follow standard three-valued truth tables:

- `UNKNOWN AND FALSE = FALSE`, `UNKNOWN AND TRUE = UNKNOWN`, `UNKNOWN AND UNKNOWN = UNKNOWN`
- `UNKNOWN OR TRUE = TRUE`, `UNKNOWN OR FALSE = UNKNOWN`, `UNKNOWN OR UNKNOWN = UNKNOWN`
- `NOT UNKNOWN = UNKNOWN`

If you want to *forbid* NULL, add `NOT NULL` (or `… IS NOT NULL AND …` inside the check) — that is a
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

Key points:

- **Unanchored.** The pattern matches if it occurs *anywhere* in the value — anchor explicitly with
  `^` / `$` (as above) to require a full-string match.
- **.NET regex flavor, not POSIX ERE.** It is a superset for common constructs (character classes,
  quantifiers, anchors, alternation, groups). POSIX named classes like `[[:alpha:]]` are **not**
  supported — use `\p{L}` or `[a-zA-Z]`. Case-insensitivity (`~*` / `!~*`) is locale-invariant.
- **Three-valued NULL** applies just like any other comparison: if the subject (or pattern) is
  `NULL` the result is `UNKNOWN`, so a nullable column with a regex CHECK still accepts `NULL`. This
  holds for the negated forms too (`!~` on a `NULL` is `UNKNOWN`, not "true").
- **Malformed patterns fail early.** A literal pattern that isn't a valid regex is rejected at
  `CREATE` / `ALTER` time, not deferred to the first `INSERT`. A malformed or timed-out pattern
  encountered *during* a CHECK evaluation surfaces as a check-constraint violation (HTTP 400).
- **ReDoS guard.** Every match runs under a bounded timeout
  (`CamusDBConfig.RegexMatchTimeoutMs`, default 250 ms); a pathological pattern fails rather than
  hanging. Compiled patterns are cached (`CamusDBConfig.RegexCacheMaxEntries`).

### Type coercion in a check

Two coercions make the natural SQL work without explicit `CAST`, exactly as the WHERE path does:

- **Numeric widening.** An integer literal compares against a floating-point column, so
  `price float64 CHECK (price > 0)` works — the `0` (an `Integer64` literal) is compared numerically
  to the `Float64` value. Integer literals are likewise accepted for float columns on `INSERT`
  (`VALUES (100)` into a `float64` column stores `100.0`).
- **String→typed literal.** A bare string literal is coerced to `Uuid` / `Id` / `Date` / `DateTime`
  when the other operand is that type:

  ```sql
  CREATE TABLE people (id object_id PRIMARY KEY, birth_date date CHECK (birth_date > '1900-01-01'));
  ```

A comparison between genuinely incompatible types with no such coercion (e.g. a `string` column vs a
numeric literal) rejects the row at write time as a check violation (`CADB0303`, "compares
incompatible types", HTTP 400) — not a raw crash / 500.

### ALTER adds validate existing data

`ALTER TABLE … ADD CONSTRAINT … CHECK` **scans the whole table** and evaluates the new check against
every existing row. If any row violates it, the `ALTER` is rejected (with `CADB0303`) and the schema
is left unchanged. This matches PostgreSQL. `NOT VALID` (add without scanning) is a deferred
follow-up, not yet supported.

### Error code

| Condition                              | Code       | HTTP |
| -------------------------------------- | ---------- | ---- |
| Row violates a CHECK                   | `CADB0303` | 400  |
| Incompatible types inside a CHECK      | `CADB0303` | 400  |
| Invalid check at DDL (subquery/…)      | `CADB0400` | —    |

The message names the violated constraint: `new row violates check constraint "positive_price"`.

---

## Part II — Named & droppable NOT NULL

Historically `NOT NULL` in CamusDB was an anonymous boolean flag that could never be removed. It now
gains an **identity** and **ALTER operations**, while keeping its fast boolean enforcement — it is
**not** rerouted through the CHECK evaluator.

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

Semantics:

- Every `NOT NULL` gets a name — the explicit one, or an auto-name `{table}_{col}_not_null` — so
  `DROP CONSTRAINT` and introspection work uniformly.
- **`SET NOT NULL`** scans existing rows and is rejected (`CADB0301` / `NotNullViolation`) if any
  value is NULL, exactly like ADD CHECK's existing-data scan.
- **`DROP NOT NULL`** is unconditional — relaxing a constraint needs no scan.
- **`DROP CONSTRAINT <name>`** resolves the name against **both** the table's check constraints
  **and** each column's NOT NULL constraint name; not found in either → "constraint '…' does not
  exist".

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

`SQLExecutorCreateTableCreator.CollectCheckConstraints` walks the CREATE AST and turns **every**
check — column-level and table-level — into a `CheckConstraintInfo { Name, Expression,
ReferencedColumns }` on `CreateTableTicket.CheckConstraints`. There is exactly **one internal
representation**: a named check with a condition. (Column checks are *not* carried on `ColumnInfo`;
that path was removed as dead.)

### Store as text, re-parse at load — and why there are two renderers

The persisted form of a condition is **human-readable SQL text** (good for `SHOW CREATE TABLE`). At
table-open, `CatalogsManager.ParseCheckConstraintAsts` / `LoadCheckConstraintAsts` parse each
`Expression` back into a `NodeAst` and cache it on `CheckConstraintSchema.ParsedCondition`
(`[JsonIgnore]`, never persisted). **Enforcement always runs against this re-parsed AST**, on every
node.

That makes round-trip fidelity load-bearing: *render → persist → parse* must reproduce the **same
predicate**. `PlanRenderer.RenderExpr` is intentionally lossy — it targets EXPLAIN display and drops
parentheses, collapses IN-lists to `(...)`, and abbreviates CAST targets. Using it would silently
enforce a *different* predicate after reload (`(a OR b) AND c` → `a OR b AND c` → `a OR (b AND c)`).

So checks use a dedicated **`CheckConditionRenderer`**:

- parenthesizes every **compound** sub-expression when it is an operand (so precedence can never be
  regrouped), while leaving atomic/self-delimiting operands unwrapped — `price > 0` stays `price >
  0`, but `(a OR b) AND c` keeps its grouping;
- renders IN-lists, CAST target types, and function arguments in full;
- **throws** for anything it cannot represent faithfully, so the DDL layer rejects the constraint
  rather than persisting a string that would re-parse differently.

`PlanRenderer` is unchanged and still used for EXPLAIN. The guard is a *semantic* round-trip test
(render → parse → render is a fixed point), not a render-stability test.

### Schema model

- **`CheckConstraintSchema`** (`Catalogs/Models/`): `{ Name, Expression, ReferencedColumns,
  [JsonIgnore] ParsedCondition }`. `ReferencedColumns` is computed once from the AST for existence
  validation and the UPDATE skip-optimization.
- **`TableSchema.CheckConstraints`** (`List<CheckConstraintSchema>?`) — the source of truth,
  parallel to `Indexes`; null/empty for tables without checks (backward-compatible).
- **`TableColumnSchema.NotNullConstraintName`** (`string?`) — the identity of a named NOT NULL;
  the `NotNull` bool is unchanged and remains the enforcement signal.

### Enforcement

`CheckEnforcer.EnforceOnRow(table, row)` iterates `table.Schema.CheckConstraints`, runs
`CheckEvaluator.Evaluate(check.ParsedCondition, row)`, and throws `CADB0303` on a `false` result. It
is called:

- **INSERT** — inside `RowInserter.Validate`, in the per-row loop, after NOT NULL / length checks,
  before the row is written. (Rows are already fully built with defaults at ticket-creation time.)
- **UPDATE** — inside `RowUpdater.FlushUpdateChunk`, on the new row, after coercion / NOT NULL
  checks and before `RowEncoder.Encode`.

**`CheckEvaluator`** is the three-valued core, deliberately isolated from the WHERE evaluator (which
collapses NULL → false):

- comparison nodes evaluate each operand via the leaf evaluator; if **either operand is `Null`** →
  `UNKNOWN`; otherwise compare;
- `AND`/`OR`/`NOT` implement the three-valued tables above;
- `IS [NOT] NULL` are always definite; `BETWEEN`/`LIKE`/`IN` return `UNKNOWN` when a relevant operand
  is NULL;
- **leaf evaluation delegates to `SQLExecutorBaseCreator.EvalExpr`** so coercion (String→Date/Uuid/Id,
  arithmetic, function calls, CAST) is identical to WHERE. A missing column (omitted from an INSERT)
  is treated as `NULL`.
- `CompareValues` widens mixed integer/float operands to `double`, coerces a bare string operand to
  `Uuid`/`Id`/`Date`/`DateTime`, and turns a genuinely incomparable pair into a `CamusDBException`
  (a `CheckConstraintViolation`) instead of letting `ColumnValue.CompareTo` escape as a raw
  `ArgumentException`. The WHERE evaluator (`SQLExecutorBaseCreator.CompareValues`) applies the same
  numeric widening, so `WHERE price > 0` and `CHECK (price > 0)` behave consistently.

`NOT NULL` is **not** evaluated here — it stays a fast boolean check in
`RowInserter.Validate` / `RowUpdater.CheckForNotNulls`.

### ALTER execution

`TableConstraintAlterer` runs the four ALTER operations (`AddCheck`, `DropConstraint`, `SetNotNull`,
`DropNotNull`). ADD CHECK and SET NOT NULL scan existing rows first
(`ScanAndValidateExistingRowsAsync` / `ScanAndValidateNotNullAsync`) and reject before touching the
schema.

Two persistence paths, selected by `isClusterMode`:

- **Standalone:** apply the delta to `table.Schema` under `Schema.Semaphore`, then persist inside a
  DDL `KvTransaction` and commit. Because persist serializes the in-memory schema, the mutation must
  precede persist; if persist/commit then fails, the in-memory change is **reverted**
  (`RevertChecksAsync` / `RevertColumnAsync`) so the node never enforces a constraint that didn't
  become durable.
- **Cluster:** replicated as a schema-log op (below).

### Cluster / replication

Check and NOT NULL DDL are replicated schema-log operations, exactly like `AddIndex`. The full
definition travels in the payload so every node builds an **identical** AST; enforcement is local on
every writer.

- **`SchemaOp`** (append-only, never renumber): `AddCheckConstraint = 10`, `DropCheckConstraint =
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
  (`SchemaDdlForwardRequests.cs`) carry the full data — `ForwardCreateTableRequest.CheckConstraints`,
  `ColumnInfoRequest.NotNullConstraintName`/`DefaultFunction`, and
  `ForwardAlterConstraintRequest.ColumnName` for the SET/DROP NOT NULL target. **Omitting any of
  these silently drops the constraint on the leader** — the wire form must stay in sync with the
  ticket; the DTO round-trip is covered by tests.

### Schema version

Check and NOT NULL changes are **validation-only** — they do not change row encoding — so they do
**not** bump `TableSchema.Version` (matching index-state changes). This is deliberate; getting it
wrong interacts with the row-decoder catch-up fence.

### AST cache invariant

`CheckEnforcer` **skips** any constraint whose `ParsedCondition` is null (defensive). Therefore every
path that makes a `TableSchema` live must rebuild the cache: fresh CREATE, apply-from-replication,
standalone ALTER, and **load-from-disk**. `ParseCheckConstraintAsts` is invoked on all of them; a new
path that populates `CheckConstraints` without parsing would silently disable enforcement.

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

- **No `FOREIGN KEY`** — a separate constraint, and not part of this feature.
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
| Named / droppable NOT NULL | `CommandsExecutor/TestNotNullConstraints.cs` |
| `SHOW CREATE TABLE` rendering | `CommandsExecutor/TestShowCreateTableRoundTrip.cs` |
| Date/type coercion, precedence & IN-list fidelity, per-operator NULL end-to-end, ALTER-reject-leaves-unchanged, NOT NULL reopen, forwarding-DTO round-trip | `CommandsExecutor/TestCheckConstraintsFixes.cs` |

When editing NULL semantics, the condition renderer, the persist/reparse path, or the forwarding
DTOs, run the check + notnull + parser suites (broad blast radius) and capture results to a TRX.
