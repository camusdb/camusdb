# `COMMENT ON` — descriptions for tables, columns, indexes, and databases

CamusDB lets you attach a free-text description to a database object, such as a table or one of its
columns, and read it back through introspection. Comments are pure metadata. They don't affect
planning or row encoding, and they don't change query results.

```sql
COMMENT ON TABLE    users           IS 'Application users';
COMMENT ON COLUMN   users.id        IS 'Internal user identifier';
COMMENT ON COLUMN   users.email     IS 'Unique login email address';
COMMENT ON INDEX    users.email_idx IS 'Lookup by login email';
COMMENT ON DATABASE app             IS 'Primary application database';
```

A comment on a table, a column or an index can be set inline when the object is created, or later
with `COMMENT ON`. Both forms write the same schema field. `CREATE DATABASE` doesn't take a comment. A
database comment can be set only with `COMMENT ON DATABASE`.

## Removing a comment: `IS NULL` vs `IS ''`

```sql
COMMENT ON COLUMN users.email IS NULL;   -- removes the comment
COMMENT ON COLUMN users.email IS '';     -- stores a comment that is present but empty
```

These are **different states**, as in PostgreSQL. You can see the difference in the output.
`SHOW CREATE TABLE` omits the clause entirely for a removed comment and emits `COMMENT ''` for an
empty one. `SHOW INDEXES` shows a removed index comment as `NULL` and an empty one as `''`.

An inline `COMMENT` clause can only set a value. To remove a comment, use `COMMENT ON … IS NULL`.

## Inline comments in `CREATE TABLE`

In `CREATE TABLE`, you can put a comment on the table itself and on each column. You can also put
one on each inline `KEY` / `UNIQUE KEY`.

```sql
CREATE TABLE users (
    id    oid    PRIMARY KEY NOT NULL COMMENT 'Internal user identifier',
    email string NOT NULL             COMMENT 'Unique login email address',
    KEY email_idx (email)             COMMENT 'Lookup by login email'
) COMMENT 'Application users';
```

The same column-level clause is allowed in `ALTER TABLE … ADD COLUMN`.

```sql
ALTER TABLE users ADD COLUMN nickname string NULL COMMENT 'Display name';
```

The syntax is `COMMENT '<text>'`, and it doesn't use **`=`** in either the parsed or the emitted
form. This is cosmetically unlike MySQL's `COMMENT='…'`. The two forms are not interchangeable here.

To include a single quote in the text, write it twice, as in any string literal.

```sql
COMMENT ON COLUMN users.email IS 'The user''s email';   -- stores: The user's email
```

### Characters a comment can contain

A comment can contain any character. `SHOW CREATE TABLE` must emit DDL that parses back to the
identical comment. Each value has a literal form, which means that any comment that can be stored
can also be re-emitted. Quotes and backslashes round-trip, and so do control characters such as
newlines, tabs and NUL.

```sql
COMMENT ON TABLE t IS 'C:\Users\data';          -- backslashes are ordinary characters
COMMENT ON TABLE t IS E'line1\nline2';          -- E'…' for control characters
COMMENT ON TABLE t IS 'x''); DROP TABLE t; --'; -- stored and re-emitted as inert text
```

A comment containing a control character is re-emitted in the `E'…'` form. Any other comment comes
back in the ordinary quoted form, exactly as it reads. See [string literals](data-types.md#string-literals) for both
forms.

The same applies to string `DEFAULT` values, which previously had no guard at all and could produce
`SHOW CREATE TABLE` output that did not re-parse.

## Reading comments back

### `SHOW CREATE TABLE`

This statement shows the comments of the table and of its columns and indexes. The emitted DDL
**re-executes to an identical schema**. If you capture it, drop the table and replay it, the comments
come back byte for byte.

```
CREATE TABLE `users` ( `id` OID NOT NULL COMMENT 'Internal user identifier',
 `email` STRING NOT NULL COMMENT 'Unique login email address',
 PRIMARY KEY (`id`), KEY `email_idx` (`email`) COMMENT 'Lookup by login email')
 COMMENT 'Application users';
```

### `SHOW INDEXES`

The `Comment` field contains the index comment. It is `NULL` when the index doesn't have a comment,
and `''` when the comment was set with `IS ''`. This means that a tool that rebuilds indexes from this
listing can preserve an empty comment. The primary key (`~pk`) cannot carry a comment. Its `Comment`
is always `NULL`.

```sql
CREATE INDEX a_idx ON t (a);
CREATE INDEX b_idx ON t (b);
COMMENT ON INDEX t.b_idx IS '';
SHOW INDEXES FROM t;   -- ~pk: NULL, a_idx: NULL, b_idx: ''
```

Earlier versions reported `''` for both states.

### `SHOW DATABASE`

This statement returns the name and comment of the current database.

```sql
SHOW DATABASE;
```

| database | comment                      |
|----------|------------------------------|
| app      | Primary application database |

Unlike `SHOW CREATE TABLE` and `SHOW INDEXES`, this surface cannot distinguish an unset comment from
an empty one — both render as an empty string.

`SHOW COLUMNS` is deliberately unchanged; its row shape (Field/Type/Null/Key/Default/Extra) is
part of the wire contract.

## Differences from PostgreSQL

**Indexes are table-qualified.** PostgreSQL indexes live in a schema-global namespace. This means
that `COMMENT ON INDEX idx` is unambiguous there. CamusDB indexes are per-table and are referenced by
name within a table (`DROP INDEX … ON t`, `ALTER TABLE t RENAME INDEX …`). There isn't a global
index registry that could resolve a bare name. `COMMENT ON INDEX` therefore requires the
table-qualified form, and a bare index name returns an error.

```sql
COMMENT ON INDEX users.email_idx IS 'Lookup by login email';   -- required
COMMENT ON INDEX email_idx       IS '…';                       -- rejected
```

`COMMENT ON COLUMN` is qualified the same way, which matches PostgreSQL.

**The primary key index cannot carry a comment.** `SHOW CREATE TABLE` renders it as a bare
`PRIMARY KEY (...)` line. That line can't contain an inline `COMMENT` clause, which means that
`SHOW CREATE TABLE` would have no way to show a comment stored there. Comment the table instead.

**Only some object kinds are supported.** In addition to the four kinds above, you can comment on
the objects in this table.

| Object | Statement | Where the comment is shown |
|--------|-----------|----------------------------|
| Sequence | `COMMENT ON SEQUENCE s IS '…'` | See [sequences](sequences.md#inspecting-sequences) |
| View | `COMMENT ON VIEW v IS '…'` | `SHOW VIEWS`, see [views](views.md#introspection) |
| Materialized view | `COMMENT ON MATERIALIZED VIEW m IS '…'` | `SHOW MATERIALIZED VIEWS`, see [views](views.md#introspection) |

A materialized view is a relation. This means that `COMMENT ON TABLE` sets the same comment.
Constraints, branches and functions can't be commented on. CamusDB doesn't provide a
`pg_description` / `information_schema` catalog to query.

## `comment` is a reserved word

`COMMENT` is a keyword. A table or column literally named `comment` must be written with backticks.

```sql
SELECT `comment` FROM posts;      -- required
SELECT comment FROM posts;        -- syntax error
```

Longer identifiers, such as `comments` and `commented_at`, are not affected and parse bare. Schemas
created before `COMMENT` became a keyword also round-trip. This is because `SHOW CREATE TABLE`
already puts backticks around each identifier that it emits.

## Limits and errors

| Situation                                                | Error                                  |
|----------------------------------------------------------|----------------------------------------|
| Comment longer than 65 535 characters                     | `CADB0511` `CommentTooLong` (HTTP 400)  |
| Table does not exist                                      | `TableDoesntExist`                      |
| Column does not exist on the table                        | `UnknownColumn`                         |
| Index does not exist on the table                         | `InvalidInput`                          |
| Database is not registered                                | `DatabaseDoesntExist`                   |
| Unqualified `COMMENT ON COLUMN` / `COMMENT ON INDEX`      | `InvalidInput`                          |
| `COMMENT ON INDEX` targeting the primary key              | `InvalidInput`                          |
| `E'…'` comment literal with a truncated `\x`/`\u` escape, an out-of-range `\U`, or an unpaired surrogate | `InvalidInput` |

The length bound exists because comments are stored in the replicated per-table metadata blob. An
unbounded comment would inflate each schema checkpoint and each schema-log entry.

## Endpoints

All four forms, and `COMMENT ON SEQUENCE`, are accepted on the DDL and non-query SQL endpoints, over
both REST and gRPC.
`COMMENT ON DATABASE` needs no context database — it names its target in the statement — and starts
no transaction, like `CREATE`/`DROP`/`RENAME DATABASE`. The other forms require a context database.

## Versioning and replication

Setting a comment advances the **database** schema version. It doesn't advance
`TableSchema.Version`, the column-layout version used for row MVCC decoding. Comments don't affect
how stored bytes are interpreted, which means that no row needs re-decoding. Indexes and CHECK
constraints behave the same way, as do table settings.

In cluster mode a comment change is replicated as a schema-log delta and applied on all nodes. It is
safe to replay: applying the same delta twice overwrites, and a delta whose target column or
index has since been dropped is a no-op. A `COMMENT ON` issued on a follower is
forwarded to the schema leader.

### Cross-node visibility of database comments

A database comment is stored in the cross-database **registry** entry. It doesn't travel through
Raft schema replication with the per-database schema log. Setting one advances the shared registry
generation, which makes the other nodes revalidate their cached entries on the next lookup. However, a
node that already has a cached entry can serve the previous comment in the brief window before it
reconciles. This is the same behavior as `RENAME DATABASE`, and it is acceptable for metadata — but
do not rely on a database comment being visible cluster-wide the instant the statement returns.
