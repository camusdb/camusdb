# SQL Authentication & Authorization

> **Audience:** operators enabling authentication on a CamusDB deployment, and developers
> using the authenticated API.
> **Scope:** how to turn on authentication, provision users, grant privileges, obtain and use
> access tokens, and what the engine enforces.

> **Overview.** Authentication is **off by default** — a fresh install behaves exactly as an
> unauthenticated CamusDB, and there is **no default user or password**. When you opt in, the server
> is fail-closed: it refuses to start without a bootstrap administrator, refuses plaintext connections,
> and rejects any request that lacks a valid token or the required privilege. Passwords are stored only
> as salted PBKDF2-HMAC-SHA256 verifiers; ordinary requests carry a short-lived opaque bearer token,
> not a password.

---

## 1. Enabling authentication

Authentication configuration comes from the **environment / secret provider — never `config.yml`**
(a plaintext key or password in a config file is a leak). Set these before starting the server:

| Variable | Meaning |
| --- | --- |
| `CAMUSDB_AUTH_ENABLED` | `true` turns authentication and enforcement on. Anything else = off (default). |
| `CAMUSDB_AUTH_TOKEN_KEY` | Server-side key used to HMAC token secrets at rest. Required when auth is on; **must be identical on every cluster node**. Use a long random value from your secret manager. |
| `CAMUSDB_BOOTSTRAP_USER` | Name of the first superuser to create when the catalog is empty. |
| `CAMUSDB_BOOTSTRAP_PASSWORD` | That superuser's initial password. Read once at first start, used to hash, then dropped from process memory. |

On startup with auth enabled:

- If the user catalog is **empty**, the server creates exactly one **superuser** from the bootstrap
  values (a transactional create-if-absent, so concurrent cluster startups yield one winner without
  overwriting a password).
- If the catalog is empty **and no bootstrap secret is set**, the server **refuses to start** — it will
  never open an unauthenticated administration window.
- Once any user exists, the bootstrap values are ignored (your operators' own accounts are never
  overwritten).

There is deliberately **no default account** and no "change on first login" default — those are the
most-exploited database misconfigurations. Security is opt-in and seeded from a secret you supply.

## 2. The login / token flow

A password is verified **once**, at `/login`, which returns a short-lived opaque bearer token.
Ordinary requests present that token; they never re-send the password and never re-run the (expensive)
password hash.

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

Send the token on every subsequent request:

```http
POST /execute-sql-query
Authorization: Bearer camus_<id>.<secret>
Content-Type: application/json

{ "databaseName": "app", "sql": "SELECT * FROM orders" }
```

`POST /logout` (with the `Authorization` header) revokes the current token.

- Tokens have a short absolute lifetime (default 15 minutes) — there is no refresh token; **re-login**
  is the refresh.
- The token secret is never stored: the catalog holds only `HMAC(serverKey, token)`, so a catalog leak
  does not yield usable tokens.
- A password rotation (`ALTER USER`), `DROP USER`, or `/logout` invalidates outstanding tokens.
- **gRPC** carries the same token in the `authorization` request metadata; errors map to
  `UNAUTHENTICATED` / `PERMISSION_DENIED` / `RESOURCE_EXHAUSTED`. The token itself is obtained from the
  `CamusAuth` service (`Login` / `Logout`), which mirrors these routes — a gRPC-only deployment does not
  need the HTTP port open. See `docs/grpc-client-protocol.md` §5b.
- **The reply reports when the token expires**: `expiresAtUnixMs` (absolute, UTC epoch ms) and
  `expiresInSeconds` (the same deadline as a server-measured duration). Renew against those rather than
  assuming a lifetime — `AccessTokenTtl` is configurable, so a hard-coded assumption breaks the moment an
  operator shortens it.

### TLS

When auth is enabled, credential-bearing requests over a **plaintext** connection are refused
(`RequireTlsWhenAuthEnabled`, default on) — terminate TLS in front of the API. A **loopback** peer is
exempted so single-host development works without certificates. gRPC should likewise be deployed over
TLS.

If TLS terminates *in front of* the node — an ingress, sidecar, or service mesh — the node itself only
ever sees plaintext on the inside hop and would reject every forwarded request. Turn the requirement
off for that topology, in `config.yml`:

```yml
require_tls_when_auth_enabled: false
```

or on the command line, which overrides the YAML value:

```sh
camusdb --require-tls-when-auth-enabled false
```

Unlike the token key and bootstrap credentials, this carries no secret, so it is a normal config
setting rather than an environment variable. Turning it off relaxes **only** the transport check:
requests still need a valid bearer token, and every grant is still enforced. Do not turn it off on a
node that is directly reachable by clients — a bearer token in the clear is trivially stolen.

## 3. Managing users

All user and grant statements are **server-level** (no `databaseName` context needed) and require the
**superuser** attribute.

```sql
-- create a user (password bound as a parameter is preferred over an inline literal)
CREATE USER myapp IDENTIFIED WITH sha256_password BY 'app-password';
CREATE USER myapp IDENTIFIED BY 'app-password';        -- plugin defaults to sha256_password
CREATE USER IF NOT EXISTS myapp IDENTIFIED BY '…';
CREATE USER grant_target;                               -- no password: a grant target that cannot log in

-- rotate a password (invalidates that user's existing tokens)
ALTER USER admin IDENTIFIED WITH sha256_password BY 'new-strong-password';

-- remove a user and all its grants
DROP USER myapp;
DROP USER IF EXISTS myapp;
```

Only `sha256_password` is accepted; any other plugin is rejected. Passwords are capped at 1 KiB.
Network clients should **bind the password as a query parameter** rather than inline it in SQL, so it
does not leak into client history, tracing, or query logs.

## 4. Granting privileges

```sql
GRANT SELECT, INSERT, ALTER, CREATE TABLE ON my_database.* TO myapp;   -- whole database
GRANT SELECT ON my_database.orders TO reader;                          -- one table
GRANT ALL PRIVILEGES ON my_database.* TO poweruser;                    -- every privilege at that scope
REVOKE INSERT ON my_database.* FROM myapp;

SHOW GRANTS FOR myapp;                                                  -- one row per (object, privileges)
```

Privileges: `SELECT`, `INSERT`, `UPDATE`, `DELETE` (DML); `CREATE TABLE`, `DROP`, `ALTER`, `INDEX`,
`CREATE` (DDL); `ALL [PRIVILEGES]` (the union at grant time — it does **not** silently widen when new
privileges are added later).

**Scopes**, broadest to narrowest, with precedence `*.* ⊃ db.* ⊃ db.table`:

- `*.*` — every database and table.
- `my_database.*` — every table in that database.
- `my_database.orders` — one table.

Grants are:

- **Additive and idempotent** — granting a held privilege is a no-op; `REVOKE` subtracts.
- **Bound to immutable object ids**, not names — a dropped-and-recreated table does **not** inherit a
  prior grant, and a rename keeps it.
- The user and the target database/table must exist; `GRANT` never creates a user.

The **superuser** is a separate attribute set only at bootstrap; it bypasses every check and is the
only identity allowed to administer users/grants. It cannot be conferred by `GRANT` (there is no
`ALL`-grant shortcut to superuser).

## 5. What the engine enforces

With auth enabled, every statement is checked before it runs:

- **Authentication** — a missing/invalid/expired token is `401 Unauthenticated`. All authentication
  failures (unknown user, wrong password, bad token) return the same shape to avoid account
  enumeration; logins are rate-limited per account.
- **Authorization** — the statement's privilege is checked against **every table it touches**, by
  immutable id, at the table-resolution chokepoint. This means joins, subqueries, semi-joins, and
  `EXPLAIN` all require the privilege on **each** referenced table — a `db.orders`-only grant authorizes
  `orders` and nothing else. A broader `db.*` or global grant, or superuser, satisfies any table in
  scope. A denial is `403 Insufficient privilege`.
- **User / database administration** — `CREATE/ALTER/DROP USER`, `GRANT`/`REVOKE`, and database
  lifecycle DDL require superuser.
- **Catalog visibility** — `SHOW TABLES`, `SHOW DATABASES`, `SHOW BRANCHES` and `SHOW ANCESTORS` are
  *filtered*, not rejected: they list only the objects the caller can reach, so a name is never
  disclosed to someone who holds no grant on it. A caller with no grants gets an empty result rather
  than an error (erroring would itself confirm that an object exists).
  - A **table** is listed when the caller holds *any* privilege on it — `INSERT`-only is enough to see
    the name. A `db.*` or `*.*` grant lists every table in scope; superuser lists everything.
  - A **database** is listed when the caller holds a grant reaching into it: global, on the database,
    or on any single table inside it. Seeing the database name does not widen what `SHOW TABLES` then
    lists inside it.
  - A `LIKE` pattern and the visibility filter both apply — the pattern never widens the result.
  - `SHOW BRANCHES FROM db` / `SHOW ANCESTORS FROM db` name an arbitrary database, so a **target** the
    caller cannot see is reported as `DatabaseDoesntExist` — the same error as a name that really is
    unregistered, rather than a privilege error that would confirm it exists. The listed rows are then
    filtered the same way: a non-visible descendant or ancestor is omitted, and `SHOW BRANCHES` blanks
    the `parent` column when the parent database is not visible. Depths stay the true position in the
    real branch tree, so a filtered level shows up as a gap in the sequence rather than renumbering the
    surviving rows.
  - With authentication disabled all four statements list everything, as before.

### Known behaviors and limitations (all fail-closed)

- An `UPDATE` / `DELETE` whose subquery reads another table currently requires the **write** privilege
  on that read table rather than `SELECT` — over-restrictive, never over-permissive.
- `SHOW DATABASE` (the singular, current-database form), `SHOW ORPHAN DATABASES`,
  `SHOW ORPHAN TABLES`, and a `FROM`-less `SELECT` open no table and are still allowed to any
  authenticated caller, unfiltered (they expose only names/existence; `SHOW COLUMNS` /
  `SHOW CREATE TABLE` still require `SELECT` on the specific table).
- Delegated administration (`GRANT OPTION`-style) is not implemented — administration is superuser-only.

### Reporting the session in SQL

These functions report the session the statement runs in rather than anything about the rows:

| Function | Returns |
| --- | --- |
| `current_database()` | The database the statement runs against, or `NULL` for a server-level statement that has none (`CREATE DATABASE`, `CREATE USER`, …). |
| `current_user()` | The authenticated user name, or `NULL` when authentication is disabled — there is no identity the server verified. |
| `current_role()` | The same value as `current_user()`. Privileges are granted directly to users, so a session has no role identity of its own; the function exists so SQL written for engines that separate the two still runs. |
| `is_superuser()` | `true`/`false` for the **calling** session, or `NULL` when authentication is disabled. It reports no one else's status, and needs no privilege: a caller can already learn the same bit by running a superuser-only statement and seeing whether it is refused. |

```sql
SELECT current_database(), current_user();          -- no FROM needed
SELECT name FROM audit WHERE actor = current_user();
INSERT INTO audit (id, actor) VALUES (gen_id(), current_user());
```

They may be used anywhere an expression may: projections, `WHERE`, `INSERT … VALUES`, `UPDATE … SET`,
and view bodies (a view reports the session querying it, not the one that created it). They may **not**
back a column `DEFAULT` or appear in a `CHECK` condition — both are stored and replayed by later
inserts, which carry no session, so the DDL rejects them rather than accept a schema element that could
only fail later.

A query naming one of them bypasses the query result cache: the cache is per node and shared between
callers, so a cached `SELECT current_user()` would otherwise answer one caller with another's identity.

## 6. Configuration knobs

Beyond the environment variables in §1, these tune the security/performance trade-off (defaults shown):

| Setting | Default | Purpose |
| --- | --- | --- |
| `AccessTokenTtl` | 15 min | Absolute token lifetime. |
| `AuthenticationCacheTtl` | 1 s | Max staleness of a per-node authorization cache hit; a cross-node revoke takes effect within this window. Set to 0 for immediate revocation at a per-request lookup cost. |
| `PasswordHashIterations` | 600,000 | PBKDF2-HMAC-SHA256 work factor (stored per credential, so raising it never breaks existing hashes). |
| `LoginKdfMaxConcurrency` | 8 | Cap on concurrent password verifications, so a login flood cannot exhaust CPU. |
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
