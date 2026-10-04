# Foreign keys

> **Audience:** developers who write SQL against CamusDB, and engineers who maintain the DDL, DML,
> storage and replication layers.
> **Scope:** the SQL surface, what is checked and when, how a check meets a concurrent writer, the
> index that each constraint needs, schema changes, database branches, the cost, the limits, and
> every error code.

A foreign key makes sure that a value in one table (the **child**, or referencing table) exists in
another table (the **parent**, or referenced table). CamusDB follows the PostgreSQL surface for the
forms it supports. Each check runs inside the transaction of the statement that writes, at every
isolation level and in both locking modes.

---

## 1. Syntax

### A column reference

```sql
CREATE TABLE cities (
    name       string(80) NOT NULL,
    population int64,
    PRIMARY KEY (name)
);

CREATE TABLE weather (
    id      oid NOT NULL DEFAULT (gen_id()),
    city    string(80) REFERENCES cities (name),
    temp_lo int64,
    day     date,
    PRIMARY KEY (id)
);
```

`weather.city` must hold a `name` that exists in `cities`, or NULL.

```sql
INSERT INTO cities (name, population) VALUES ('San Francisco', 808000);

INSERT INTO weather (city, temp_lo, day) VALUES ('San Francisco', 46, '1994-11-27');  -- accepted
INSERT INTO weather (city, temp_lo, day) VALUES (NULL, 50, '1994-11-29');             -- accepted: NULL
INSERT INTO weather (city, temp_lo, day) VALUES ('Berkeley', 45, '1994-11-28');       -- CADB0304

DELETE FROM cities WHERE name = 'San Francisco';                          -- CADB0305
UPDATE cities SET population = 815000 WHERE name = 'San Francisco';       -- accepted: no key change
```

The refused INSERT reports:

```text
Insert or update on table 'weather' violates foreign key constraint 'weather_city_fkey':
key (city)=(Berkeley) is not present in table 'cities'
```

### A table constraint, with a composite key and actions

```sql
CREATE TABLE regions (
    country string(2) NOT NULL,
    code    int64 NOT NULL,
    name    string(80),
    PRIMARY KEY (country, code)
);

CREATE TABLE stores (
    id      oid NOT NULL DEFAULT (gen_id()),
    country string(2),
    region  int64,
    PRIMARY KEY (id),
    CONSTRAINT stores_region_fk FOREIGN KEY (country, region)
        REFERENCES regions (country, code)
        ON DELETE RESTRICT ON UPDATE NO ACTION
);

INSERT INTO regions (country, code, name) VALUES ('us', 6, 'California');
INSERT INTO stores (country, region) VALUES ('us', 6);      -- accepted
INSERT INTO stores (country, region) VALUES ('us', NULL);   -- accepted: one column is NULL
INSERT INTO stores (country, region) VALUES ('us', 7);      -- CADB0304
```

### A self-reference, to the primary key

With no column list, `REFERENCES t` references the primary key of `t`.

```sql
CREATE TABLE employees (
    id         int64 NOT NULL,
    manager_id int64 REFERENCES employees,
    PRIMARY KEY (id)
);

INSERT INTO employees (id, manager_id) VALUES (2, 1), (1, NULL);  -- accepted: one statement
DELETE FROM employees WHERE id = 1;                               -- CADB0305: employee 2 remains
DELETE FROM employees WHERE id > 0;                               -- accepted: the whole tree
```

### ALTER TABLE

```sql
ALTER TABLE weather DROP CONSTRAINT weather_city_fkey;
ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name);
```

`ADD CONSTRAINT` reads every existing row first. If a row has no parent, the statement fails with
`CADB0304`, names the first such key, and leaves nothing behind. See section 6.

### Names

A constraint without `CONSTRAINT name` gets the PostgreSQL default name
`{table}_{column}[_{column}…]_fkey`, for example `weather_city_fkey`. When that name is taken, a
number is added at the end.

CHECK, named NOT NULL and FOREIGN KEY constraints share **one name space** per table, because
`DROP CONSTRAINT name` finds a constraint by name across the three kinds. Each of these is refused
with `CADB0400` (`InvalidInput`):

- a foreign key with the name of a CHECK or of a named NOT NULL constraint;
- a CHECK with the name of a foreign key;
- `ALTER COLUMN ... SET NOT NULL` when its generated name, `{table}_{column}_not_null`, is the name
  of a CHECK or a foreign key.

The comparison ignores case.

### What is accepted, and what is refused

| Form | Result |
| --- | --- |
| Inline `REFERENCES tbl [(col)]`, with or without `CONSTRAINT name` | Accepted |
| Table-level `[CONSTRAINT name] FOREIGN KEY (cols) REFERENCES tbl [(cols)]` | Accepted |
| `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY ...` and `DROP CONSTRAINT name` | Accepted. ADD validates the existing rows. |
| A composite key; a self-reference | Accepted |
| `ON DELETE` / `ON UPDATE` with `NO ACTION` (the default) or `RESTRICT` | Accepted |
| `CASCADE`, `SET NULL`, `SET DEFAULT` | Refused, `CADB0533`. The action is stored, so a later version can run it without a migration. |
| `MATCH SIMPLE` | Accepted. It is the default and the only match type. |
| `MATCH FULL`, `MATCH PARTIAL` | Refused, `CADB0533` |
| `NOT DEFERRABLE`, `INITIALLY IMMEDIATE` | Accepted |
| `DEFERRABLE`, `INITIALLY DEFERRED` | Refused, `CADB0533` |
| `NOT VALID`, `VALIDATE CONSTRAINT` | Not supported |
| A cycle of two or more tables | Refused, `CADB0416`. PostgreSQL permits it. A self-reference is accepted. |
| A table in another database, or a branch that references its source database | Refused, `CADB0415`. A reference names a table in the same database only. |
| A view or a materialized view as the parent or the child | Refused, `CADB0415` |
| A parent with row-level TTL | Refused, `CADB0533` (section 7) |

The referenced columns must be exactly the columns of the parent's primary key or of one of its
public unique indexes, in any order. Each pair of columns must have the same type. `string(N)`
lengths can be different.

---

## 2. What is checked, and when

### At the end of each statement

Each statement checks its constraints after its last write, in its own transaction. The order of
the rows inside one statement does not matter:

- an INSERT can write a child before its parent, as the `employees` example does;
- a DELETE can remove a whole self-referencing tree;
- an `INSERT ... SELECT` that writes in pages is checked once, after the last page.

A statement inside an explicit transaction also sees the earlier writes of that transaction. A
parent inserted earlier in the transaction satisfies a later child. A parent deleted earlier in the
transaction does not.

### The rules

- **Child side.** An INSERT, or an UPDATE that sets a referencing column, needs the parent key.
  If the key is missing, the statement fails with `CADB0304` (`ForeignKeyViolation`).
- **MATCH SIMPLE.** A NULL in **any** referencing column satisfies the constraint, as the
  `('us', NULL)` store shows. No parent is read for such a row.
- **Parent side.** A DELETE that removes a referenced key, or an UPDATE that changes one, fails
  when a child row still holds the key: `CADB0305` (`ForeignKeyRestrictDelete`) or `CADB0306`
  (`ForeignKeyRestrictUpdate`).
- **An UPDATE that does not change a referenced key** does no foreign-key work. It does not read
  a child, and it never waits for a child transaction.
- **NO ACTION and RESTRICT.** Under `ON UPDATE NO ACTION`, an UPDATE that removes a key, while
  another row of the same statement takes the same key, is accepted: at the end of the statement
  the key exists again. `RESTRICT` refuses the same statement. A DELETE cannot bring a key back, so
  for DELETE the two actions behave the same.

### After a violation in an explicit transaction

A violation is found after the statement wrote its rows, and the engine does not undo the failed
statement. Its rows stay staged in the transaction.

- **HTTP and gRPC** roll back the whole transaction when a statement fails, so a client of those
  transports cannot commit the rows.
- **A caller of the embedded engine API** that catches the error and commits anyway stores the
  rows, orphans included. Roll the transaction back after a foreign-key error. CamusDB has no
  savepoints.

An autocommit statement always rolls back.

---

## 3. Concurrent writers: the lock rendezvous

A child writer and a parent writer meet on one key: the **parent's unique-index entry for the
referenced key**. That entry exists while the parent row holds the key.

- A **child** writer takes a **shared lock** on that entry, and then reads it. The lock is a
  point lock, held to the end of the transaction. It is taken at every isolation level and in
  both locking modes.
- A **parent** DELETE deletes the entry, and a key UPDATE deletes the old entry. Then the parent
  reads the child's index for the key.
- A parent UPDATE of other columns does not touch the entry. That is why it never waits.

The result for each order:

| Who comes first | What happens |
| --- | --- |
| The child holds the lock | The parent's write of the entry waits for the child. After the child commits, the parent's read sees the new child row and fails with `CADB0305` or `CADB0306`. If the child is still open after `lock_wait_deadline_ms` (500 ms by default), the parent fails with `CADB0504` (`TransactionMustRetry`) or `CADB0502` (`TransactionConflict`). PostgreSQL waits with no limit. |
| The parent's write is pending | The child cannot take the lock until the parent ends. Then it reads the entry as gone and fails with `CADB0304`. |

Both retry codes mean "run the transaction again"; see
[serializable-retry-contract.md](serializable-retry-contract.md).

Notes:

- **An optimistic parent** writer stages its delete without a write intent, so it also takes an
  exclusive lock on each removed key before its read. A child that holds the shared lock makes it
  wait. A child that comes later is refused.
- **An optimistic child** that holds the lock while a pessimistic parent waits for it is aborted
  at commit with `CADB0502`. This is the usual optimistic trade. The parent then goes on.
- **The locks never escalate** to a lock on the whole parent index. An escalated lock would block
  every new parent INSERT until the child transaction ends. One child transaction holds at most
  one lock per distinct parent key.
- **The parent reads the child's index without a lock.** No new child can take the shared lock
  while the parent's write is pending, so the read sees every child there will be.

See [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) §6.5 for how this
lock relates to the other locks.

---

## 4. The index on the referencing columns

A parent DELETE must find a child row by its key without a scan of the child table. Each
constraint therefore has a **backing index** on the child, whose leading columns are the
referencing columns, in any order.

- CamusDB **reuses** a public index of the child when its leading columns are the referencing
  columns. It prefers an index of exactly that width, then the shortest.
- A **unique** index qualifies only when it has exactly the referencing columns, or when it is the
  primary key. A unique index stores no entry for a row with a NULL in any of its columns. A wider
  unique index would therefore hide a child whose referencing columns are set and whose extra column
  is NULL, and a parent DELETE would not see that child.
- When no index qualifies, CamusDB **creates** a non-unique index named `~fk_{constraint name}`.
  The constraint owns it. `SHOW INDEXES` lists it. `SHOW CREATE TABLE` does not, because the
  constraint creates it again.
- An index name that starts with `~` belongs to the engine. SQL cannot name it, so the owned index
  cannot be dropped alone. `DROP CONSTRAINT` drops it.
- `DROP CONSTRAINT` keeps a reused index. `DROP INDEX` of a reused index is refused while the
  constraint exists (section 7).

PostgreSQL does not create this index. MySQL InnoDB does.

---

## 5. Introspection, transports and privileges

**SHOW CREATE TABLE** renders each public constraint as a table constraint, with the current names
of the tables and columns after a rename. `ON DELETE` and `ON UPDATE` appear only when the action is
not `NO ACTION`:

```sql
CONSTRAINT `weather_city_fkey` FOREIGN KEY (`city`) REFERENCES `cities` (`name`)
```

A constraint that is still being added (`WriteOnly`, section 6) is not rendered. The
`SHOW CREATE TABLE ... WITHOUT INDEXES` form keeps the foreign keys. The output can be run again as
SQL.

**HTTP.** `POST /create-table` takes a `foreignKeys` list. Each entry has `name` (required),
`columns`, `referencedTable`, `referencedColumns` (empty means the primary key), `onDelete` and
`onUpdate` (`"no action"` or `"restrict"`). A violation returns HTTP 409, a bad definition 400. See
the error table in section 10.

**gRPC.** A violation and a dependent-object refusal return `FailedPrecondition`. A bad definition
or a cycle returns `InvalidArgument`. The code is in the `camus-error-code` trailer.

**Privileges.** A constraint needs `SELECT` on the parent table, for CREATE TABLE and for ALTER
TABLE ADD CONSTRAINT. A child INSERT tells its writer whether a parent key exists, so a constraint
on a table that the caller cannot read would let the caller read it. Without the privilege the
statement fails with `CADB0517` (`InsufficientPrivilege`). A self-reference needs no extra grant.
There is no `REFERENCES` privilege.

---

## 6. Adding and dropping a constraint

### ADD CONSTRAINT validates the existing rows

`ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY` runs in these steps:

1. It resolves the definition and refuses a cycle (`CADB0416`), before it does any work.
2. If no index qualifies, it builds the owned `~fk_` index, with the same staged backfill as
   `CREATE INDEX`.
3. It adds the constraint in the **WriteOnly** state. From this point every INSERT, UPDATE and
   DELETE checks the constraint.
4. It waits until every live node acknowledges the WriteOnly state. A node acknowledges only when
   no commit that wrote the child or the parent before step 3 is still in flight on it.
5. It reads the backing index in key order, in pages, and checks the distinct keys of each page
   against the parent in one batched read.
6. When every row has its parent, the constraint becomes **Public**.

If a row has no parent, the statement fails with `CADB0304` and names the first such key in index
order. The constraint is removed, and so is the owned index. A key found missing is read again,
together with its parent, at one snapshot before it is reported. A child deleted, or a parent
inserted, after the page read therefore does not make the statement fail.

**Why WriteOnly comes before the validation.** While a change rolls out, two schema versions exist
at the same time. A node on the old version enforces nothing, and it can delete a parent while
another node inserts its child. The validation starts only after every node enforces the
constraint, so it sees any orphan from that window, and no new orphan can form after it.

**A leader change.** If the leader stops after step 3, the ALTER fails, and the constraint stays
WriteOnly: it is enforced but not yet trusted. The new leader finishes the validation and makes it
Public.

**A transaction that was open during the ADD.** A transaction that wrote the child table or the
parent table before step 3 ran no check: the constraint did not exist yet. The validation in step 5
reads committed rows only, so it cannot see a write that is still staged. The engine therefore
does not let such a write commit after the validation:

- A commit that starts after step 3 is refused with `CADB0502` (`TransactionConflict`), and the
  transaction is rolled back. Run the transaction again; the new statements check the constraint.
  An autocommit statement is retried for you.
- A commit that was already in flight at step 3 cannot be refused any more. Step 4 waits for its
  outcome, so step 5 sees its rows. If the commit made an orphan, the ADD fails with `CADB0304`.

The ADD never waits for an idle open transaction, only for a commit in flight. Both the child and
the parent are covered: a `DELETE` of a parent row that was staged before the ADD is refused at
commit in the same way. `CREATE TABLE` with a foreign key protects its parents by the same rule. A transaction that only read the tables, or that writes them after step 3,
is not affected. `CREATE INDEX` uses the same rule, so an index never misses a row.

In standalone mode the same steps run on the one node. `CREATE TABLE` with a constraint goes
through the same states in a cluster, and is Public at once in standalone mode.

### DROP CONSTRAINT

`DROP CONSTRAINT name` stops the enforcement in one step, because an early stop is always safe.
Then it drops the owned index, if there is one.

### Element states

| State | Child side enforced | Parent side enforced | Shown in SHOW CREATE TABLE |
| --- | --- | --- | --- |
| Absent | No | No | No |
| WriteOnly | Yes | Yes | No |
| Public | Yes | Yes | Yes |

---

## 7. Schema changes that a constraint blocks

| Operation | Result |
| --- | --- |
| `DROP TABLE` of a table that another table references (also `FORCE`) | Refused, `CADB0530` (`DependentObjectsExist`). Drop the child, or its constraint, first. |
| `DROP TABLE` of a child | Accepted. Its constraints go with it. |
| `DROP TABLE` of a table that references only itself | Accepted. No row is checked. |
| `TRUNCATE` of a table that another table references | Refused, `CADB0530`, as in PostgreSQL |
| `TRUNCATE` of a child, or of a table that references only itself | Accepted |
| `DROP COLUMN` of a referencing or a referenced column | Refused, `CADB0530` |
| `DROP INDEX` of the referenced index, or of a reused backing index | Refused, `CADB0530` |
| `RENAME` of a table, a column or an index | Accepted. A constraint stores ids, not names. |
| Row-level TTL on a table that a constraint references | Refused, `CADB0533`, in both orders: TTL on a parent, and a constraint to a TTL table. The TTL sweep deletes rows on a path that does no parent-side check. |
| `RELINK` of a dropped child table | The table comes back **without** its constraints, because its parents can be gone since the drop. The statement returns a warning that names each dropped constraint. Add each one again with `ALTER TABLE ... ADD CONSTRAINT`, which validates the restored rows. A formerly owned index stays as an ordinary index. |
| `DROP DATABASE` | No constraint checks |
| `CREATE TABLE ... AS SELECT` | The new table gets no constraint |

These rules also run when a change is applied on each node, in log order. Two conflicting changes
sent to two nodes at the same time therefore cannot both succeed. For example, an `ADD CONSTRAINT`
on one node and a `DROP TABLE` of its parent on another: exactly one of them wins.

---

## 8. Database branches

- **A branch inherits every constraint.** A fork keeps the table, column and index ids, and a
  constraint stores only ids, so each constraint resolves in the branch with no change.
- **A check stays inside the branch.** It reads the branch's own rows merged with its ancestors
  as of the fork. A row that the branch deleted is hidden, and a row that the branch inherited
  counts. A write in the source database after the fork never changes a check in the branch.
- **The rendezvous key is the branch's own key**, so the lock rules of section 3 hold in a branch.
- **A fork is refused while a constraint is WriteOnly** (`CADB0400`). The branch would inherit a
  constraint that nothing validates. Wait until the `ADD CONSTRAINT` finishes.
- `ADD CONSTRAINT` on a branch validates the merged view, so an orphan that only an ancestor holds
  is found.
- A branch cannot reference a table in its source database.

See [database-branching.md](database-branching.md).

---

## 9. Cost

**No constraint in the database:** one memory read per DML statement, no allocation and no extra
round trip. A test measures zero allocated bytes on this path.

**A statement that changes no constrained column** (for example an UPDATE of other columns): no
foreign-key work.

**Child side:** one lock round trip per distinct parent key per transaction, and one batched read of
the parent keys per statement. A key that the transaction already locked costs nothing. On a branch,
the read adds one batch per ancestry level.

**Parent side:** one probe of the child's index per distinct removed key, per referencing
constraint. A probe reads at most one index entry. Up to 16 probes run at the same time.

**ADD CONSTRAINT:** one parent read per distinct key, in batches. 10,000 children of 50 parents
cost 50 key checks.

Measured on one embedded node (Apple M3, in-memory storage, 1 partition, standalone). "Before"
is the release before foreign keys.

| Statement | Before | No constraint | With constraint |
| --- | ---: | ---: | ---: |
| INSERT 1 child | 1.728 ms | 1.723 ms | 1.768 ms |
| INSERT 100 children | 19.86 ms | 19.76 ms | 19.88 ms |
| INSERT 1000 children | 226.4 ms | 237.5 ms | 221.1 ms |
| DELETE 1 parent | 3.761 ms | 3.165 ms | 3.417 ms |
| DELETE 100 parents | 37.30 ms | 42.44 ms | 41.32 ms |
| UPDATE 100 children, other column | 15.70 ms | 18.60 ms | 18.80 ms |
| UPDATE 3 parents, other column | 3.458 ms | 3.476 ms | 3.249 ms |

The children reference 3 parents. The differences are in the noise of the measurement.

**One-phase commit.** A pessimistic child write keeps Kahuna's one-phase commit, also when the parent
is on another partition: a lock is not a written key, so it does not add a commit participant. An
**optimistic** child loses one-phase commit. It validates its reads at commit, and the rendezvous
lock is a range lock over one key, which the commit path treats as a predicate read on any
partition.

**Metrics.** With `ServerDiagnostics` enabled, the counter `camus.foreign_key.operations` counts the
work, tagged by `kind`: `lock_acquired`, `lock_covered`, `parent_lock`, `child_probe_batch`,
`child_probe_key`, `ancestor_probe_batch`, `parent_probe`, `validation_key` and `violation`.

---

## 10. Error codes

| Code | Name | When | HTTP | gRPC |
| --- | --- | --- | --- | --- |
| `CADB0304` | `ForeignKeyViolation` | A child INSERT or UPDATE has no parent. ADD CONSTRAINT finds a row without a parent. | 409 | `FailedPrecondition` |
| `CADB0305` | `ForeignKeyRestrictDelete` | A DELETE removes a parent key that a child still holds. | 409 | `FailedPrecondition` |
| `CADB0306` | `ForeignKeyRestrictUpdate` | An UPDATE changes a parent key that a child still holds. | 409 | `FailedPrecondition` |
| `CADB0415` | `InvalidForeignKeyDefinition` | A count or type mismatch; no primary key or unique index over exactly the referenced columns; a view or a materialized view; another database. | 400 | `InvalidArgument` |
| `CADB0416` | `ForeignKeyCycle` | ADD CONSTRAINT would close a cycle of two or more tables. | 400 | `InvalidArgument` |
| `CADB0011` | `TableDoesntExist` | The referenced table does not exist. | 500 | `NotFound` |
| `CADB0400` | `InvalidInput` | A name that another constraint of the table uses; a fork while a constraint is WriteOnly. | 400 | `InvalidArgument` |
| `CADB0517` | `InsufficientPrivilege` | No `SELECT` on the parent table. | 403 | `PermissionDenied` |
| `CADB0530` | `DependentObjectsExist` | A schema change that section 7 refuses. | 409 | `FailedPrecondition` |
| `CADB0533` | `FeatureNotSupported` | An action other than NO ACTION and RESTRICT; MATCH FULL or PARTIAL; DEFERRABLE; row-level TTL on a parent. | 501 | `Internal` |
| `CADB0504` | `TransactionMustRetry` | A parent write waited for a child transaction longer than `lock_wait_deadline_ms`. | 500 | `Aborted` |
| `CADB0502` | `TransactionConflict` | An optimistic transaction lost the rendezvous at commit; or the transaction wrote the child or the parent before an `ADD CONSTRAINT` and commits after it started (section 6). | 500 | `Aborted` |

Each violation message names the constraint, the child table, the parent table and the key values.

A code with no HTTP mapping returns 500. A code with no gRPC mapping returns `Internal`. Read the
code itself, not the status, to decide what to do; the retry codes say "run the transaction
again".

---

## 11. Limitations

**A lagging node in a cluster.** Step 4 of the ADD waits for every *live* node. A node that the
schema leader has not heard from for `schema_ack_live_node_lease_ms` (30 s by default) is treated
as dead and is not waited for. If that node is alive, cut off from the schema leader only, and still
able to commit to the data partitions, a commit of a write that it planned before the constraint
can land after the validation. Set the lease to `-1` to make the ADD wait for every configured
node, at the price of a schema change that stalls while a node is down.

**A commit whose outcome is unknown.** A commit that returned `CADB0509` stays in flight until
the client resolves it. While it is in flight on a table, an `ADD CONSTRAINT` or `CREATE INDEX` on
that table waits, and fails after `schema_ack_wait_timeout_ms`. Retry the COMMIT of that transaction
until it gives an outcome, then run the schema change again. If the client is gone, the engine stops
the wait by itself when the transaction is older than the longest life of a session (330 s by
default); until then that node acknowledges no schema change of the database.

---

## 12. Maintainer notes

### Key files

| Area | Location |
| --- | --- |
| Model | `Catalogs/Models/ForeignKeySchema.cs`, `ForeignKeyAction`, `ForeignKeyMatch`, `TableIndexSchema.OwnerConstraintId` |
| Published graph and plans | `Catalogs/Models/ForeignKeyGraph.cs`, `ForeignKeyPlan`; read through `Schema.ForeignKeys` |
| Definition and backing index | `Catalogs/Replication/ForeignKeyDefinitionBuilder.cs`, `Catalogs/Apply/ForeignKeyDefinitionRules.cs` |
| Apply of ADD | `Catalogs/Apply/ForeignKeyDeltaApplier.cs` (`SchemaOp.AddForeignKey`); DROP is `SetElementState(Absent)` |
| DDL guards | `Catalogs/Apply/ForeignKeyDependencyRules.cs` |
| Constraint names | `Catalogs/Apply/ConstraintNameRules.cs` |
| Statement checks | `Commands/Executor/Controllers/ForeignKeyStatementChecker.cs`, called by `RowInserter`, `RowInsertSelector`, `RowDeleter`, `RowUpdater` |
| Locks and probes | `Storage/Kv/KvRangeLockManager.AcquireForeignKeyLocksAsync` and `AcquireForeignKeyExclusiveLocksAsync`; `Storage/Kv/KvIndexAccessor.LockAndLookupUniqueManyAsync` and `IndexPrefixExistsAsync` |
| Validation pass | `Commands/Executor/Controllers/DDL/ForeignKeyValidationPass.cs` |
| ALTER flow | `Commands/Executor/Controllers/DDL/SchemaDdlService.ForeignKeys.cs`, `Catalogs/SchemaChangeCoordinator.cs` |
| SQL | `SQLParser/SQLParser.Language.grammar.y`, `Commands/Executor/Controllers/DDL/ForeignKeyAstReader.cs` |
| SHOW and HTTP | `Commands/Executor/Controllers/SchemaQuerier.cs`, `CamusDB/App/Controllers/CreateTableController.cs` |

### Invariants

- **A check runs in the statement's own transaction.** A separate transaction would read a stale
  state and could leave an orphan.
- **The parent-side read runs after the parent's write.** Kahuna grants the exclusive key lock
  without a check of shared range locks; only the write meets the child's lock. A read before the
  write can miss a child that commits a moment later.
- **The graph, not the open tables.** The parent side finds its children through the published
  graph, which is built from every table of the schema. A child table that is not open is still
  checked.
- **Ids only.** A constraint stores no table or column name except its own name. Names come from the
  graph when SHOW or an error message needs them.
- **Every related table is opened and pinned before the first write**, by id, without the caller's
  privilege check. A rename between the open and the write cannot swap a table.
- **No full-table scan** on any enforcement path. The counter `camus.kv.scan_entries` proves it in
  the tests.

### Where each rule is tested

Every scenario runs on a standalone engine and on a cluster-mode engine.

| Rule | Tests |
| --- | --- |
| The examples on this page | `TestForeignKeyAcceptance.TheDocumentedExamplesBehaveAsDocumented` |
| CREATE TABLE forms, names, refused definitions, reused and owned indexes | `TestForeignKeyCreateTable` |
| The SQL forms; MATCH, DEFERRABLE and action clauses refused by name | `SQLParser/TestForeignKeyParsing` |
| Apply-time rules: definitions, cycles, the DDL guards, the claim of a reused index | `TestForeignKeyDefinitionRules` |
| Child side, statement end, INSERT ... SELECT, explicit transaction after a violation | `TestForeignKeyInsert` |
| Parent DELETE, the wait and its limit, the optimistic fence | `TestForeignKeyDelete` |
| UPDATE on both sides, NO ACTION and RESTRICT, no wait for a non-key UPDATE | `TestForeignKeyUpdate` |
| Schema changes that a constraint blocks, RELINK, TTL | `TestForeignKeyDdlGuards`, `TestForeignKeyAcceptance` |
| ADD and DROP CONSTRAINT, cycles, the rollout | `TestForeignKeyAlter`, `TestForeignKeyRollout`, `CamusDB.Cluster.Tests/TestClusterForeignKeyAlter` |
| Backing-index coverage, descending pages, the snapshot recheck, the name space | `TestForeignKeyIntegrityEdges` |
| Branches | `TestForeignKeyBranch` |
| SHOW CREATE TABLE and SHOW INDEXES | `TestShowCreateTableRoundTrip` |
| HTTP and gRPC codes, privileges | `TestForeignKeyTransports`, `TestForeignKeyPrivileges` |
| Cost budgets | `TestForeignKeyBudgets`, `CamusDB.Cluster.Tests/TestClusterForeignKeyOnePhaseCommit`, `CamusDB.MicroBenchmarks/ForeignKeyBenchmarks.cs` |
| Transactions that span an ADD CONSTRAINT or a CREATE INDEX | `TestSchemaChangeSpanningTransactions`, `CamusDB.Cluster.Tests/TestClusterSchemaChangeSpanningTransactions` |
