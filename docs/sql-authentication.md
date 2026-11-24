# SQL Authentication & Authorization

> **Audience:** operators enabling authentication on a CamusDB deployment, and developers
> using the authenticated API.
> **Scope:** how to turn on authentication, provision users and grant privileges. It also covers how
> to obtain and use access tokens, and what the engine enforces.

> **Overview.** Authentication is **off by default**. A fresh install behaves exactly as an
> unauthenticated CamusDB, and there is **no default user or password**. When you opt in, the server
> is fail-closed. It does not start if no bootstrap administrator is configured, and it refuses
> plaintext connections. A request that lacks a valid token or the required privilege returns an
> error. Passwords are stored only as salted PBKDF2-HMAC-SHA256 verifiers. Ordinary requests carry a
> short-lived opaque bearer token in place of a password.

---

## 1. Enabling authentication

Authentication configuration comes from the **environment / secret provider**. It is not read from
**`config.yml`**, because a plaintext key or password in a config file is a leak. Set these variables
before you start the server.

| Variable | Meaning |
| --- | --- |
| `CAMUSDB_AUTH_ENABLED` | `true` turns authentication and enforcement on. Anything else = off (default). |
| `CAMUSDB_AUTH_TOKEN_KEY` | Server-side key used to HMAC token secrets at rest. Required when auth is on; **must be identical on all cluster nodes**. At least 32 bytes — with a shorter key, the server does not start. Use a random value from your secret manager, e.g. `openssl rand -hex 32`. |
| `CAMUSDB_BOOTSTRAP_USER` | Name of the first superuser to create when the catalog is empty. |
| `CAMUSDB_BOOTSTRAP_PASSWORD` | That superuser's initial password. Read once at first start, used to hash, then dropped from process memory. |

When the server starts with auth enabled, it checks its secrets and seeds the catalog.

- Both operator-supplied secrets — `CAMUSDB_AUTH_TOKEN_KEY` and, when set, `CAMUSDB_NODE_SECRET` —
  must be **at least 32 bytes**. If one is shorter, the server does not start and names the variable
  that failed. A short token key defeats the hashing of tokens at rest: anyone who obtains the catalog
  can then forge sessions offline. An unset node secret is a valid single-node configuration. In that
  case, the peer routes are not available on the node.
- If the user catalog is **empty**, the server creates exactly one **superuser** from the bootstrap
  values. This is a transactional create-if-absent. This means that concurrent cluster startups yield
  one winner, and a password is not overwritten.
- If the catalog is empty **and no bootstrap secret is set**, the server **does not start**. It will
  not open an unauthenticated administration window.
- Once any user exists, the bootstrap values are ignored. Your operators' accounts are not
  overwritten.

There is deliberately **no default account** and no "change on first login" default. Those are the
most-exploited database misconfigurations. Security is opt-in and seeded from a secret you supply.

## 2. The login / token flow

A password is verified **once**, at `/login`, which returns a short-lived opaque bearer token.
Ordinary requests present that token. They don't re-send the password, and the (expensive) password
hash is not computed again.

```http
POST /login
Content-Type: application/json

{ "user": "admin", "password": "…" }
```
```json
{
  "status": "ok",
  "token": "camus_<id>.<secret>",
  "expiresAtUnixMs": 1785270000000,
  "expiresInSeconds": 900
}
```

A client sends the token in the `Authorization` header of each later request.

```http
POST /execute-sql-query
Authorization: Bearer camus_<id>.<secret>
Content-Type: application/json

{ "databaseName": "app", "sql": "SELECT * FROM orders" }
```

`POST /logout` (with the `Authorization` header) revokes the current token.

- Tokens have a short absolute lifetime (default 15 minutes). There is no refresh token; **log in
  again** to get a new token.
- The token secret is not stored. The catalog contains only `HMAC(serverKey, token)`. This means that
  a catalog leak does not yield usable tokens.
- A password rotation (`ALTER USER`), `DROP USER`, or `/logout` invalidates outstanding tokens.
- **gRPC** carries the same token in the `authorization` request metadata. Errors map to
  `UNAUTHENTICATED` / `PERMISSION_DENIED` / `RESOURCE_EXHAUSTED`. The token itself is obtained from the
  `CamusAuth` service (`Login` / `Logout`), which mirrors these routes. A gRPC-only deployment does not
  need the HTTP port open. For more information, see `docs/grpc-client-protocol.md` §5b.
- **The reply gives the time when the token expires**: `expiresAtUnixMs` (absolute, UTC epoch ms) and
  `expiresInSeconds` (the same deadline as a server-measured duration). Renew against those
  values. `AccessTokenTtl` is configurable. If you hard-code a lifetime, your client will break when an
  operator shortens it.

### TLS

When auth is enabled, credential-bearing requests over a **plaintext** connection are refused
(`RequireTlsWhenAuthEnabled`, default on). Terminate TLS in front of the API. A **loopback** peer is
exempted. This means that single-host development works before you set up certificates. gRPC should also be deployed over
TLS.

If TLS terminates *in front of* the node — an ingress, sidecar, or service mesh — the node sees only
plaintext on the inside hop. It would refuse each forwarded request. For that topology, turn the
requirement off in `config.yml`.

```yml
require_tls_when_auth_enabled: false
```

You can also turn it off on the command line. The command-line value overrides the YAML value.

```sh
camusdb --require-tls-when-auth-enabled false
```

This setting carries no secret. For this reason, it is a normal config setting, and the token key and
the bootstrap credentials are kept in the environment. Turning it off relaxes **only** the transport
check: requests need a valid bearer token as before, and all grants are enforced. Do not turn it off on
a node that clients can reach directly — a bearer token in the clear is easy to steal.

## 3. Managing users

All user and grant statements are **server-level** (no `databaseName` context needed) and require the
**superuser** attribute.

```sql
-- create a user (password bound as a parameter is preferred over an inline literal)
CREATE USER myapp IDENTIFIED WITH sha256_password BY 'app-password';
CREATE USER myapp IDENTIFIED BY 'app-password';        -- plugin defaults to sha256_password
CREATE USER IF NOT EXISTS myapp IDENTIFIED BY '…';
CREATE USER grant_target;                               -- no password: a grant target that cannot log in

-- change your own password: the current one must be presented with REPLACE
ALTER USER admin IDENTIFIED BY 'new-strong-password' REPLACE 'current-password';
ALTER USER admin IDENTIFIED WITH sha256_password BY 'new-strong-password' REPLACE 'current-password';

-- a superuser resets SOMEONE ELSE's password without knowing the old one
ALTER USER myapp IDENTIFIED BY 'new-strong-password';

-- remove a user and all its grants
DROP USER myapp;
DROP USER IF EXISTS myapp;
```

Only `sha256_password` is allowed; any other plugin returns an error. Passwords are capped at 1 KiB.
Network clients should **bind the password as a query parameter** and not put it inline in SQL. This
means that the password does not leak into client history, tracing, or query logs. Both literals of a
`REPLACE` form are redacted from logs.

A rotation invalidates that user's existing tokens, including the one that made the request.

### Why `REPLACE` is required for your own password

Changing your own password proves you know the current one. If this rule did not exist, anyone who has
a live bearer token could take the account permanently: they set a new password and log back in with
it. The credential epoch does invalidate the stolen token. However, the attacker chose the
replacement, and the real owner is locked out.

The rule applies to a superuser changing their own password too. That is the account most worth
taking over, and exempting it would leave the largest hole open.

A superuser changing **another** user's password presents no old secret. That is the recovery path,
for the case where nobody knows the old password.

### Listing the accounts

```sql
SHOW USERS;
SHOW USERS LIKE 'app_%';
```

The statement returns one row per account, ordered by name.

| column | type | meaning |
| --- | --- | --- |
| `user` | string | the account name, in the case it was created with |
| `id` | string | the account's immutable id; `NULL` on an account created before ids existed |
| `superuser` | bool | whether the account bypasses all privilege checks |
| `has_password` | bool | whether the account can log in at all |
| `grants` | int64 | how many grant records the account has |
| `created_at` | string | UTC ISO-8601, to the millisecond |

**No column carries any part of a credential**: the hash and the salt are left out, as are the
iteration count and the algorithm. `has_password` is a boolean because the useful question about a
restored account is whether it can log in. The two internal revocation epochs are omitted for the same
reason.

The statement requires a **superuser**. Its output names all the accounts on the server, and no
per-database grant narrows it. For this reason, it is held to the bar `SHOW GRANTS FOR <other>` is
held to.

The listing is read from storage and bypasses any cache. It is complete or it fails. An operator
taking an inventory would believe a short list. For this reason, a short list is not returned.

### Listing every grant

```sql
SHOW GRANTS FOR *;
```

The statement returns the same three columns as `SHOW GRANTS` — `user`, `object`, `privileges` — for
all accounts. The rows are ordered by account name and then by object. This means that the output is
stable across calls. An account with zero grants produces no row; `SHOW USERS` is where its
existence shows. Superuser only, for the same reason.

The spelling is `*` and not `ALL` on purpose. `all` is a normal identifier. This means that
`SHOW GRANTS FOR all` already means "the account named `all`". Giving that text a second meaning would
change behaviour for anyone who has such an account.

## 4. Granting privileges

```sql
GRANT SELECT, INSERT, ALTER, CREATE TABLE ON my_database.* TO myapp;   -- whole database
GRANT SELECT ON my_database.orders TO reader;                          -- one table
GRANT ALL PRIVILEGES ON my_database.* TO poweruser;                    -- every privilege at that scope
REVOKE INSERT ON my_database.* FROM myapp;

SHOW GRANTS FOR myapp;                                                  -- one row per (object, privileges)
```

Grants are server-level. This means that `SHOW GRANTS` needs no database in scope: send it with the
database field empty, or with any database — the answer is the same either way. `SHOW GRANTS` that
omits `FOR` returns the authenticated caller's grants. For this reason, it requires an authenticated
session. With authentication off, name the user explicitly.

**Reading another account's grants requires a superuser.** With authentication on, `SHOW GRANTS FOR
<other>` is refused with `InsufficientPrivilege`. The refusal is the same whether or not that account
exists. This means that it cannot be used to enumerate the catalog. Your own grants are always
readable, by either spelling. The output is the full privilege layout of the account named: which
databases and tables it reaches, and whether it is a superuser. `SHOW VARIABLES` and
`SHOW ENGINE STATS` are held to the same bar for the same reason, and so are `SHOW PARTITIONS` and
`SHOW SLOW QUERIES`.

The privileges fall into four groups.

- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.
- DDL: `CREATE TABLE`, `DROP`, `ALTER`, `INDEX`, `CREATE`.
- `ALL [PRIVILEGES]`: the union at grant time. It does **not** silently widen when new privileges are
  added later.
- `MONITOR`: server-level, see below.

**`MONITOR`** lets an account that is not a superuser read the node's disk, CPU and memory through the
node stats call (gRPC `GetNodeStats`, REST `GET /v1/admin/stats`). For more information, see
[node-stats.md](node-stats.md).

```sql
GRANT MONITOR ON *.* TO monitoring;
REVOKE MONITOR ON *.* FROM monitoring;
```

- It is valid on `*.*` only. `GRANT` or `REVOKE` of `MONITOR` on `db.*` or `db.table` is refused with
  `InvalidPrivilege` (`CADB0515`).
- It is not part of `ALL PRIVILEGES`. Grant it by name. `REVOKE ALL PRIVILEGES ON *.*` leaves it in
  place, and `REVOKE MONITOR ON *.*` removes it.
- It gives no data access and no catalog visibility. An account that has only `MONITOR` cannot read
  a table, and `SHOW DATABASES` and `SHOW TABLES` list no objects for it.
- `SHOW GRANTS` shows it as `MONITOR`, or as `ALL PRIVILEGES, MONITOR` next to a full grant.

**Scopes** go from broadest to narrowest, with precedence `*.* ⊃ db.* ⊃ db.table`.

- `*.*` — all databases and tables.
- `my_database.*` — all tables in that database.
- `my_database.orders` — one table.

Grants follow three rules.

- **Additive and idempotent** — granting a privilege the user already has is a no-op; `REVOKE`
  subtracts.
- **Bound to immutable object ids** — a dropped-and-recreated table does **not** inherit a prior
  grant, and a renamed table retains it. Names are not used.
- The user and the target database/table must exist. `GRANT` does not create a user.

The **superuser** is a separate attribute set only at bootstrap. It bypasses all checks and is the
only identity allowed to administer users/grants. It cannot be conferred by `GRANT` (there is no
`ALL`-grant shortcut to superuser).

## 5. What the engine enforces

With auth enabled, each statement is checked before it is executed.

- **Authentication** — a missing/invalid/expired token is `401 Unauthenticated`. All authentication
  failures (unknown user, wrong password, bad token) return the same shape to avoid account
  enumeration. Logins are rate-limited per account.
- **Authorization** — the statement's privilege is checked against **each table it touches**, by
  immutable id, at the table-resolution chokepoint. This means that joins, subqueries, semi-joins and
  `EXPLAIN` require the privilege on **each** referenced table. A `db.orders`-only grant authorizes
  `orders` and no other table. A broader `db.*` or global grant, or superuser, satisfies any table in
  scope. A denial is `403 Insufficient privilege`.
- **`INSERT … RETURNING`** needs `SELECT` on the target table in addition to `INSERT`, as in
  PostgreSQL. The RETURNING list reads stored values, defaults and sequence draws. A user with
  `INSERT` only can execute `INSERT` statements that omit RETURNING. The count-only request flag does
  not lift the requirement.
- **`INSERT … ON CONFLICT DO UPDATE`** and **`UPSERT`** need `UPDATE` on the target table in addition
  to `INSERT`. They can change a stored row. `ON CONFLICT DO NOTHING` needs `INSERT` only. For more
  information, see [insert-on-conflict.md](insert-on-conflict.md).
- **The typed gRPC rows API** (`CamusRows`) is checked the same way, on the table the request names.

  | Call | Privilege |
  | --- | --- |
  | `InsertRow` | `INSERT` |
  | `Query`, `QueryById` | `SELECT` |
  | `UpdateRows`, `UpdateById` | `UPDATE` |
  | `DeleteRows`, `DeleteById` | `DELETE` |

  A filtered update or delete needs only the write privilege, as `UPDATE … WHERE` does in SQL.
- **Each table access must declare its privilege.** A request that reaches a table and has not declared a
  privilege is refused with `403 Insufficient privilege`, superusers included. If you see that error
  with the message "the request did not declare which privilege it needs", it is a server defect. A
  grant is not missing. Please report it.
- **C# extensions** — `CREATE TRIGGER` and `DROP TRIGGER` need `ALTER` on the table, the same as
  `ALTER TABLE`. `CALL` requires a superuser, because procedures do not have a grant model yet. SQL
  that a trigger or a procedure executes is checked with the privileges of the user whose statement
  fired or called it. `SHOW EXTENSIONS` and `CREATE`, `ALTER` and `DROP EXTENSION` require a
  superuser. An installed extension decides which operator-deployed code the statements of the
  database may execute. For more information, see [csharp-extensions.md](csharp-extensions.md).
- **User / database administration** — `CREATE/ALTER/DROP USER`, `GRANT`/`REVOKE`, and database
  lifecycle DDL require superuser. So do the HTTP routes `/create-db`, `/drop-db` and `/close-db`.
  Closing is in that list because it rolls back all transactions that are active in that database on
  the node.
- **Cluster routes** — `/v1/cluster/health` answers when no credential is sent. This lets an
  orchestrator probe work. However, only a superuser sees its `localRole`, `hostedPartitions` and
  `stalledPartitions` fields. `/v1/cluster/membership`, `/v1/cluster/placement`,
  `/v1/cluster/backfill-status` and `/v1/cluster/snapshot-status` require a superuser. They name peer
  endpoints, partitions and leaders. `/v1/cluster/leave` and `/v1/cluster/replication-factor` require
  a superuser, and are loopback-only when authentication is off.
- **Database visibility** — a database you have no grant on is reported as `DatabaseDoesntExist`
  (`CADB0010`, HTTP 404, gRPC `NotFound`), with exactly the message an unregistered name gets. This
  applies to all statements executed in that database, `FROM`-less `SELECT` and `SHOW TABLES`
  included. It also applies to all transports: SQL over HTTP and gRPC, the typed rows API, and the
  batch stream. Any difference between the two answers would let a caller test whether a database
  exists. A grant reaches into the database when it is global, on the database, or on any single table
  in it. With such a grant, a statement that needs more than it gives is refused with
  `403 Insufficient privilege` as usual. Statements that ignore the current database (`SHOW DATABASES`,
  `SHOW TABLES FROM db`, `SHOW GRANTS`, `CREATE USER`, …) are not affected by it.
- **Database-level reads** — `SHOW DATABASE` (name and comment) and `SHOW ORPHAN TABLES` (the tables
  dropped from the database) open no table. For this reason, they need `SELECT` at database scope: a
  `db.*` or `*.*` grant, or superuser. A grant on one table makes the database visible but does not
  satisfy this. `SHOW CREATE MATERIALIZED VIEW` needs `SELECT` on the materialized view, as reading it
  would.
- **Catalog visibility** — some listing statements are *filtered*, and do not return an error. These
  are `SHOW TABLES` (with or without `FROM db`) and `SHOW DATABASES`, as well as `SHOW BRANCHES`,
  `SHOW ANCESTORS` and `SHOW ORPHAN DATABASES`. They list only the objects the caller can reach. This
  means that a name is not disclosed to someone who doesn't have a grant on it. `SHOW DATABASES` and
  `SHOW ORPHAN DATABASES` give an empty result to a caller that has zero grants. They do not return
  an error in that case. A
  dropped database is listed by `SHOW ORPHAN DATABASES` under the same rule as a live one, by its id.
  - A **table** is listed when the caller has *any* privilege on it — `INSERT`-only is enough to see
    the name. A `db.*` or `*.*` grant lists all tables in scope; superuser lists all objects.
  - A **database** is listed when the caller has a grant reaching into it: global, on the database,
    or on any single table in it. Seeing the database name does not widen what `SHOW TABLES` then
    lists for it.
  - A `LIKE` pattern and the visibility filter both apply. The pattern does not widen the result.
  - `SHOW TABLES FROM db` (or `IN db`) lists the tables of the named database. It does not use the
    current database. This means that it works when the current database is missing, invisible or not
    set. A **target** the caller cannot see is reported as `DatabaseDoesntExist`, with the same message
    as a name that is not registered. The rows are then filtered exactly as `SHOW TABLES` executed in
    that database filters them: a caller with a grant on one table sees that table only.
  - `SHOW BRANCHES FROM db` / `SHOW ANCESTORS FROM db` name an arbitrary database. For this reason, a
    **target** the caller cannot see is reported as `DatabaseDoesntExist`, the same error as an
    unregistered name. A privilege error would confirm that the database exists. The listed rows are
    then filtered the same way. A non-visible descendant or ancestor is omitted, and `SHOW BRANCHES`
    blanks the `parent` column when the parent database is not visible. Depths show the true position
    in the real branch tree. This means that a filtered level shows up as a gap in the sequence, and
    the remaining rows have their original numbers.
  - With authentication disabled, all of these statements list all objects, as before.

### Known behaviors and limitations (all fail-closed)

- An `UPDATE` / `DELETE` whose subquery reads another table currently requires the **write** privilege
  on that read table in place of `SELECT`. This is over-restrictive and is never over-permissive.
- A `FROM`-less `SELECT` reads no table and needs no table grant. However, it is executed in the current
  database, and its caller needs some grant in that database (see "Database visibility").
- Delegated administration (`GRANT OPTION`-style) is not implemented — administration is superuser-only.

### Reporting the session in SQL

Four functions return information about the session of the statement.

| Function | Returns |
| --- | --- |
| `current_database()` | The database of the statement, or `NULL` for a server-level statement that does not use one (`CREATE DATABASE`, `CREATE USER`, …). |
| `current_user()` | The authenticated user name, or `NULL` when authentication is disabled — there is no identity the server verified. |
| `current_role()` | The same value as `current_user()`. Privileges are granted directly to users. This means that a session does not have a separate role identity; the function exists so that SQL written for engines that separate the two works. |
| `is_superuser()` | `true`/`false` for the **calling** session, or `NULL` when authentication is disabled. It shows the status of the caller only, and needs no privilege: a caller can already learn the same bit by executing a superuser-only statement and seeing whether it is refused. |

```sql
SELECT current_database(), current_user();          -- no FROM needed
SELECT name FROM audit WHERE actor = current_user();
INSERT INTO audit (id, actor) VALUES (gen_id(), current_user());
```

They may be used anywhere an expression may, such as projections or `WHERE`. This includes
`INSERT … VALUES`, `UPDATE … SET` and view bodies. A view returns the session that queries it, and not
the one that created it. They may **not** back a column `DEFAULT` or appear in a `CHECK` condition.
Both are stored and replayed by later inserts, which carry no session. For this reason, the DDL
returns an error for them. Such a schema element could only fail later.

A query that names one of them bypasses the query result cache. The cache is per node and shared
between callers. If the query were cached, a cached `SELECT current_user()` would answer one caller
with another's identity.

## 5a. When a privilege change takes effect

```sql
FLUSH PRIVILEGES;   -- make an authorization change apply now
FLUSH SESSIONS;     -- end every login session, so every client must log in again
```

Both require a **superuser**, and both return no rows.

### The bound, without a flush

Each node caches the authorization it resolved for a token, for at most
`authentication_cache_ttl` (default 1 s). The effect of a change in that window depends on the node.

- **On the node where the change was made, it applies to the very next request.** A `GRANT`, a
  `REVOKE`, a `DROP USER` or a password rotation advances that account's epoch. A cached authorization
  decision built before it is discarded on sight. The change does not wait for the cache to expire.
- **On another node, it applies within `authentication_cache_ttl`.** That node learns of the change
  when a cached decision expires and it reads the catalog again. Setting the knob to 0 makes a
  cross-node change immediate, at the cost of a catalog lookup on each request.

This bound covers all transports. A long-lived gRPC batch stream is included. It re-resolves its
authorization on the same schedule. This means that it does not retain what was true when the stream
opened.

### A gRPC batch stream and the lifetime of its token

A `BatchExecute` stream presents a token once, in the metadata it opens with, and is meant to be
open for a full client session. A token lives 15 minutes by default. This rule reconciles the two:

> **A token's lifetime bounds how long it can be presented. It does not bound a stream that presented
> it while it was valid.**

- **An ordinary expiry does not end the stream.** At the instant its token expires, the stream looks
  its session record up. A record that is present and unrevoked means the session ended by the clock
  alone. From then on, the stream resolves its authorization from the *account* it authenticated as,
  on the same `authentication_cache_ttl` schedule. This means that a `GRANT` or a `REVOKE` reaches its
  next operation.
- **Anything that ends a session early also ends the stream.** Examples are `/logout`,
  `FLUSH SESSIONS`, a password change and `DROP USER`. A user that is dropped and created again under
  the same name does not inherit the stream. The stream is bound to the account's immutable id. During
  the token's lifetime, the stream notices on its next operation. An idle stream notices at the expiry
  at the latest, and the server then closes it.
- **A stream that ends this way ends with status `UNAUTHENTICATED`** and the `CADB0516` code. This is
  the signal for a client to discard its token, log in again, and open a new stream.

The session sweep retains an expired record for `expired_session_retention_ms` (default five minutes)
before it deletes it. The record is the only evidence that separates an expiry from a logout, because
a logout deletes it early. The stream does its lookup at the expiry. This means that the window only
has to cover clock skew between nodes and a late timer. A stream that misses the window is ended:
missing evidence is read as a revocation.

A new stream always needs a token that resolves. For this reason, a client must renew its token before
`expiresAtUnixMs` for the streams it opens later, and for each unary call.

### What `FLUSH PRIVILEGES` adds

It forces this node to re-read the user and grant catalog from storage. It also discards all the
authorization decisions the node had cached, whether or not anything it tracks says something changed.
It also advances the durable coherence generation, which is replicated. The other nodes drop what they
derived from the older one as soon as they read the catalog again — within `authentication_cache_ttl`,
as above. There is no node-to-node call, and it does not matter which node you run it on.

Use it when a node's view is suspect and the ordinary path did not correct it. It is not needed
after an ordinary `GRANT` or `REVOKE`.

**It does not revoke anything.** A client whose grants did not change can continue to work across it,
and does not need to log in again.

### What `FLUSH SESSIONS` costs

It deletes **all** stored sessions. This means that all clients of all nodes must log in again. Use it
when the staleness is somewhere this server cannot reach, such as a client that cached a refusal. It
is deliberately not a side effect of `FLUSH PRIVILEGES`. An operator who wants one grant believed does
not want a fleet logged out.

A fresh login works immediately afterwards. On another node, an authorization decision cached before
the flush can be used until it expires. This is the same `authentication_cache_ttl` bound that
applies to all revocations.

## 6. Configuration knobs

In addition to the environment variables in §1, these settings tune the security/performance
trade-off. The table shows their defaults.

| Setting | Default | Purpose |
| --- | --- | --- |
| `AccessTokenTtl` | 15 min | Absolute token lifetime. |
| `AuthenticationCacheTtl` | 1 s | Max staleness of a per-node authorization cache hit; a cross-node revoke takes effect within this window (§5a). A change made on the node itself applies to the next request regardless. Set to 0 for immediate cross-node revocation at a per-request lookup cost. |
| `ExpiredSessionRetentionMs` | 5 min | How long the session sweep retains an expired session record. A live gRPC batch stream uses the record at the moment of expiry to tell an expired session from a revoked one (§5a). Keep it well above inter-node clock skew. |
| `PasswordHashIterations` | 600,000 | PBKDF2-HMAC-SHA256 work factor (stored per credential, so raising it does not break existing hashes). |
| `LoginKdfMaxConcurrency` | 8 | Cap on concurrent password verifications. A login flood cannot exhaust CPU. |
| `LoginMaxAttemptsPerMinute` | 20 | Per-account login rate limit (`429` on exceed). |
| `RequireTlsWhenAuthEnabled` | true | Refuse plaintext credential-bearing requests (loopback exempt). Settable via `require_tls_when_auth_enabled` in `config.yml` or `--require-tls-when-auth-enabled true\|false`. |

## 7. Quick start (single host)

```sh
export CAMUSDB_AUTH_ENABLED=true
export CAMUSDB_AUTH_TOKEN_KEY="$(openssl rand -hex 32)"
export CAMUSDB_BOOTSTRAP_USER=admin
export CAMUSDB_BOOTSTRAP_PASSWORD="$(openssl rand -base64 24)"
# start the server, then:

#   POST /login {admin, <password>}                 -> token
#   CREATE USER app IDENTIFIED BY '…';              (as admin)
#   GRANT SELECT, INSERT ON app_db.* TO app;
#   POST /login {app, …}                            -> token
#   POST /execute-sql-query  (Bearer <app token>)   -> enforced per table
```

## 8. Migrating an existing unauthenticated installation

You do not need to export, reload, or rewrite anything. Authentication is a startup switch plus a
one-time bootstrap seed. It does not change your databases, tables, rows, indexes or backups.

An existing install converts cleanly because the user catalog lives in a separate reserved
`_system/auth/` key prefix, apart from all user databases. An installation that never had
authentication on has an **empty** user catalog, which is the state §1 seeds from. At the next
start, the server finds zero users and creates the bootstrap superuser. No existing data is migrated
or rewritten, because no existing data lives in that prefix.

### 8.1 Single host

1. Set the secrets in the environment. Do not put them in `config.yml`.

   ```sh
   export CAMUSDB_AUTH_ENABLED=true
   export CAMUSDB_AUTH_TOKEN_KEY="$(openssl rand -hex 32)"
   export CAMUSDB_BOOTSTRAP_USER=admin
   export CAMUSDB_BOOTSTRAP_PASSWORD="$(openssl rand -base64 24)"
   ```

2. Restart the server. `authentication_enabled` is a restart-class setting. By design, there is no
   `SET` that flips it on a running node, because a live flip would change enforcement mid-statement.
3. Log in at `POST /login` as the bootstrap superuser and keep the token.
4. Create one user per application, as that superuser:
   `CREATE USER app IDENTIFIED BY '…';`
5. Grant each application exactly the privileges it needs:
   `GRANT SELECT, INSERT ON app_db.* TO app;`
6. Point each client at `/login`, then have it send `Authorization: Bearer <token>` on each request.

### 8.2 Grant before you cut the clients over

Skipping this step turns a migration into an outage. Before the restart, all callers had full
access. After it, **only the superuser does**. The superuser bypasses all checks, but all other
identities start with zero privileges. This means that an application that worked yesterday gets
`403 Insufficient privilege` on its first statement.

Do steps 4 and 5 while the old clients are running unauthenticated, or during a planned window,
before you switch them.

Two properties of grants matter while you plan the migration.

- A grant binds to the object's **immutable id**. It does not bind to the name. A table you drop and
  recreate does not inherit its old grants; a renamed table retains them. Re-grant after any recreate.
- The privilege is checked against **all** tables a statement touches. A join, a subquery or a
  semi-join needs the privilege on each referenced table.

Audit what each application reads and writes before you write the `GRANT` statements. A
`SHOW GRANTS FOR <user>` afterwards confirms what landed.

### 8.3 Clusters

- **The token key must be identical on all nodes.** A node started with a different
  `CAMUSDB_AUTH_TOKEN_KEY` cannot verify a token another node issued. Requests then fail depending on
  which node they reach.
- **A rolling restart leaves an unauthenticated window.** Each node enforces its local flag. A node
  can join a cluster whose other nodes disagree. Until a node restarts with
  `CAMUSDB_AUTH_ENABLED=true`, it serves unauthenticated requests on its ports. Either restart the
  fleet together, or block client traffic to the not-yet-restarted nodes for the duration. Treat a
  half-migrated cluster as unauthenticated.
- **Peers need certificates.** Raft and Kahuna traffic between nodes carries no user token. With
  authentication on, a node admits that traffic only from a peer that shows a client certificate
  signed by the cluster CA. Set `raft_mutual_tls_enabled: true` and `raft_peer_ca` on all nodes. A
  node with authentication on and peers does not start if they are missing. For more information, see
  [Node-to-node authentication](configuration.md#node-to-node-authentication).
- **Concurrent startups are safe.** The bootstrap is a transactional create-if-absent: several nodes
  starting at once yield one winner, and a node that loses does not overwrite the winner's password.
- **Set `require_tls_when_auth_enabled` deliberately** (§2, TLS) — a cluster usually sits behind an
  ingress or a mesh that terminates TLS in front of the node.

### 8.4 Behavior that changes on the day

You should expect the following changes. They are not signs of breakage.

- **Operator dashboard.** With authentication off, it serves loopback only. There is no principal to
  gate its panels on. With authentication on, it moves to a session cookie on dedicated routes. This
  means that your access path to it changes. For more information, see `docs/operator-dashboard.md`.
- **Catalog listings become filtered.** `SHOW TABLES` and `SHOW DATABASES` list only what the caller
  can reach. The same applies to `SHOW TABLES FROM db`, `SHOW BRANCHES` and `SHOW ANCESTORS`.
  `SHOW DATABASES` gives a caller that has zero grants an empty result. It does not return an error. An empty
  list after the migration means "no grants yet". It does not mean "no databases".
- **A database you have no grant on reads as missing.** A statement in such a database returns error
  `DatabaseDoesntExist`, the same error an unregistered name gets. After the
  migration, that error for a database you know exists means "no grant on it yet".
- **`current_user()` and `is_superuser()` stop returning `NULL`.** They return `NULL` only while
  authentication is off. SQL or a view that tests them for `NULL` changes behavior.
- **gRPC clients need the same token**, in the `authorization` request metadata, obtained from the
  `CamusAuth` service. An HTTP-only migration plan misses them.

### 8.5 Rolling back

Unset `CAMUSDB_AUTH_ENABLED` and restart. Enforcement stops on all nodes.

The users, password verifiers and grants remain in the catalog. The rollback does not delete them. A
later re-enable therefore finds a **non-empty** catalog and skips the bootstrap entirely. The accounts
you already created are kept. The bootstrap values are ignored from that point on. This means that a
forgotten superuser password is *not* recoverable by running the bootstrap again. Fix it with
`ALTER USER` from a session that has superuser, before you roll back.
