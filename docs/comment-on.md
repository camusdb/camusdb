# `COMMENT ON` — descriptions for tables, columns, indexes, and databases

CamusDB lets you attach a free-text description to a table, one of its columns, one of its indexes,
or a database, and read it back through introspection. Comments are pure metadata: they never affect
planning, row encoding, or query results.

```sql
COMMENT ON TABLE    users           IS 'Application users';
COMMENT ON COLUMN   users.id        IS 'Internal user identifier';
COMMENT ON COLUMN   users.email     IS 'Unique login email address';
COMMENT ON INDEX    users.email_idx IS 'Lookup by login email';
COMMENT ON DATABASE app             IS 'Primary application database';
```

There are two ways to attach a table, column, or index comment: inline when the object is created,
and after the fact with `COMMENT ON`. Both write the same schema field. Database comments have no
`CREATE` surface and are set only with `COMMENT ON DATABASE`.

## Removing a comment: `IS NULL` vs `IS ''`

```sql
COMMENT ON COLUMN users.email IS NULL;   -- removes the comment
COMMENT ON COLUMN users.email IS '';     -- stores a comment that is present but empty
```

These are **different states**, matching PostgreSQL, and the difference is observable:
`SHOW CREATE TABLE` omits the clause entirely for a removed comment and emits `COMMENT ''` for an
empty one. There is no inline form for removal — an inline `COMMENT` clause only ever sets a value,
so clearing one always goes through `COMMENT ON … IS NULL`.

## Inline comments in `CREATE TABLE`

`CREATE TABLE` accepts a comment on the table itself, on each column, and on each inline
`KEY` / `UNIQUE KEY`:

```sql
CREATE TABLE users (
    id    oid    PRIMARY KEY NOT NULL COMMENT 'Internal user identifier',
    email string NOT NULL             COMMENT 'Unique login email address',
    KEY email_idx (email)             COMMENT 'Lookup by login email'
) COMMENT 'Application users';
```

`ALTER TABLE … ADD COLUMN` accepts the same column-level clause:

```sql
ALTER TABLE users ADD COLUMN nickname string NULL COMMENT 'Display name';
```

The syntax is `COMMENT '<text>'` with **no `=`**, in both the parsed and the emitted form. This is
cosmetically unlike MySQL's `COMMENT='…'`; the two forms are not interchangeable here.

Single quotes inside the text are escaped by doubling them, as in any string literal:

```sql
COMMENT ON COLUMN users.email IS 'The user''s email';   -- stores: The user's email
```

### Characters a comment cannot contain

`SHOW CREATE TABLE` must emit DDL that parses back to the identical comment, and CamusDB's string
literals have no backslash-escape decoding — the lexer treats a backslash plus the next character as
one unit and passes both through unchanged. Two shapes therefore have no representation and are
rejected when the comment is set:

- a backslash immediately before a quote, or at the very end of the comment (it would escape the
  closing quote and the literal would never terminate);
- raw control characters such as a newline or tab.

Quotes themselves are fine anywhere — they are doubled on output and undoubled on input, so a comment
like `x'); DROP TABLE t; --` is stored and re-emitted as ordinary text. Backslashes are fine too, as
long as they are not adjacent to a quote: `C:\Users\data` is accepted.

The same limitation applies to string `DEFAULT` values, which are not currently guarded — a default
containing a newline or a trailing backslash produces `SHOW CREATE TABLE` output that does not
re-parse.

## Reading comments back

### `SHOW CREATE TABLE`

Renders the table, column, and index comments, and the emitted DDL **re-executes to an identical
schema** — capture it, drop the table, replay it, and the comments come back byte for byte.

```
CREATE TABLE `users` ( `id` OID NOT NULL COMMENT 'Internal user identifier',
 `email` STRING NOT NULL COMMENT 'Unique login email address',
 PRIMARY KEY (`id`), KEY `email_idx` (`email`) COMMENT 'Lookup by login email')
 COMMENT 'Application users';
```

### `SHOW DATABASE`

Returns the current database's name and comment:

```sql
SHOW DATABASE;
```

| database | comment                      |
|----------|------------------------------|
| app      | Primary application database |

Unlike `SHOW CREATE TABLE`, this surface cannot distinguish an unset comment from an empty one —
both render as an empty string.

`SHOW COLUMNS` is deliberately unchanged; its row shape (Field/Type/Null/Key/Default/Extra) is
part of the wire contract.

## Differences from PostgreSQL

**Indexes are table-qualified.** PostgreSQL indexes live in a schema-global namespace, so
`COMMENT ON INDEX idx` is unambiguous there. CamusDB indexes are per-table and are referenced by
name within a table (`ALTER TABLE t DROP INDEX …`, `ALTER TABLE t RENAME INDEX …`), and there is no
global index registry to resolve a bare name against. `COMMENT ON INDEX` therefore requires the
table-qualified form:

```sql
COMMENT ON INDEX users.email_idx IS 'Lookup by login email';   -- required
COMMENT ON INDEX email_idx       IS '…';                       -- rejected
```

`COMMENT ON COLUMN` is qualified the same way, which matches PostgreSQL.

**The primary key index cannot carry a comment.** `SHOW CREATE TABLE` renders it as a bare
`PRIMARY KEY (...)` line, which has no inline `COMMENT` form to round-trip through, so a comment
stored there could never be shown. Comment the table instead.

**Only these four object kinds are supported.** Constraints, branches, sequences, and functions have
no `COMMENT ON` form, and there is no `pg_description` / `information_schema` catalog to query.

## `comment` is a reserved word

Introducing the statement makes `COMMENT` a keyword, so a table or column literally named `comment`
must now be written with backticks:

```sql
SELECT `comment` FROM posts;      -- required
SELECT comment FROM posts;        -- syntax error
```

Longer identifiers are unaffected — `comments`, `commented_at`, and so on still parse bare. Schemas
created before this change still round-trip, because `SHOW CREATE TABLE` already backticks every
identifier it emits.

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
| Comment with a control character, or a backslash before a quote / at the end | `InvalidInput`      |

The length bound exists because comments ride the replicated per-table metadata blob; an unbounded
comment would inflate every schema checkpoint and every schema-log entry.

## Endpoints

All four forms are accepted on the DDL and non-query SQL endpoints, over both REST and gRPC.
`COMMENT ON DATABASE` needs no context database — it names its target in the statement — and starts
no transaction, like `CREATE`/`DROP`/`RENAME DATABASE`. The other three require a context database.

## Versioning and replication

Setting a comment advances the **database** schema version but never `TableSchema.Version`, the
column-layout version used for row MVCC decoding — comments do not affect how stored bytes are
interpreted, so no row needs re-decoding. This matches how indexes, CHECK constraints, and table
settings behave.

In cluster mode a comment change is replicated as a schema-log delta and applied on every node. It is
safe to replay: applying the same delta twice simply overwrites, and a delta whose target column or
index has since been dropped is a no-op rather than a failure. A `COMMENT ON` issued on a follower is
forwarded to the schema leader.

### Cross-node visibility of database comments

A database comment lives on the cross-database **registry** entry, not in the per-database schema
log, so it does not travel through Raft schema replication. Setting one advances the shared registry
generation, which makes every other node revalidate its cached entry on the next lookup. A node that
already holds a cached entry can still serve the previous comment in the brief window before it
reconciles. This is the same behavior as `RENAME DATABASE`, and it is acceptable for metadata — but
do not rely on a database comment being visible cluster-wide the instant the statement returns.
