# SHOW VARIABLES

Inspect the configuration a running CamusDB node is using, from a SQL prompt.

```sql
SHOW VARIABLES;
SHOW VARIABLES LIKE '%cache%';
SHOW VARIABLES LIKE 'ttl_%';
```

```
variable                          value   type   default  source   mutability  scope
────────────────────────────────  ──────  ─────  ───────  ───────  ──────────  ───────
ttl_default_delete_batch_size     100     int    100      default  runtime     cluster
ttl_default_job_cron              @daily  string @daily   default  runtime     cluster
ttl_enabled                       false   bool   true     config   runtime     cluster
ttl_span_lease_ms                 30000   int    30000    default  runtime     cluster
```

## Effective values, not file contents

The rows come from the configuration object the engine was **constructed with**. The statement does
not re-read `config.yml`. A value overridden by an environment
variable or a command-line flag after the file was read differs from what the file says, and the
engine obeys the resolved value.

A key you commented out in `config.yml` shows its built-in default. A key an environment variable
overrode shows the override. What you see is what the node is running.

## Columns

| column     | meaning |
|------------|---------|
| `variable` | The `snake_case` key you would write in `config.yml`. Keys of the nested `kahuna:` section appear in dotted form, e.g. `kahuna.wal_sync_writes`. |
| `value`    | The effective value. SQL `NULL` when the setting is unset — distinct from an empty string. |
| `type`     | The value type. It is one of `bool`, `int`, `long`, `double`, `string`, `enum`, `duration_ms` and `list`. |
| `default`  | The built-in value of the setting, used when no layer overrides it. A value that differs from `default` is one somebody configured. |
| `source`   | The layer that supplied the value. The precedence order is `cluster` > `cli` > `env` > `config` > `default`. |
| `mutability` | `runtime` if a new value takes effect while the node is running, `restart` if the component that reads it latches the value when it is constructed. This describes how the reader behaves. A `restart` key can be changed, but the running node will use the old value until it is restarted. |
| `scope`    | `cluster` when all nodes must agree. A per-node disagreement would change user-visible transaction behavior. `node` when per-node divergence is the point, such as tracing or local cache sizes. |

Values are rendered the way `config.yml` spells them. This means that you can paste a value read here
back into a file unchanged.

- Booleans are lowercase.
- Numbers use the invariant format, and digit separators and unit suffixes are omitted. For example,
  64 MiB is shown as `67108864`.
- Enum values use underscored tokens, such as `read_committed`.
- Durations are a whole number of milliseconds, like any other `*_ms` key.

Rows are sorted by name using ordinal comparison. This means that you can diff the output of two nodes
line by line.

## `LIKE`

The optional pattern matches against the **variable name**. `%` matches any run of characters and `_`
matches exactly one. A backslash makes the next character a literal. For example, `ttl\_%` reads the
underscore as itself. This is the rule of `LIKE` in a query. For more information, see
[`LIKE` and `ILIKE` patterns](sql-expression-syntax.md#like-and-ilike-patterns). If no name matches the
pattern, the statement returns zero rows and no error. A pattern that ends with a backslash returns
`CADB0400` when the match reaches it.

Matching is **case-sensitive**. It uses the same matcher as `SHOW TABLES`, `SHOW DATABASES`, and
`SHOW ENGINE STATS`. Variable names are all lowercase. This means that `LIKE 'TTL_%'` matches no rows. Write
`LIKE 'ttl_%'`.

The pattern can use any of the three string literal forms: `'ttl_%'`, `"ttl_%"`, or `E'ttl_%'`.

## What is not shown

**Secrets are masked.** Whether a node *has* a secret configured is an operational question. This
means that the secret settings are listed, but their value renders as `********` when set and is empty
when not. Three settings are masked.

- `bootstrap_superuser_password`
- `access_token_server_key`
- `node_secret`

Certificate and key-file settings, such as `https_certificate`, `raft_certificate` and
`kahuna.backup_mac_key_file`, hold **paths** to key material. These paths are shown in full, because an
operator debugging a misconfigured deployment needs them.

**Deployment and topology keys are not yet listed.** These keys are stored on a different configuration
object than the engine's settings, and they do not currently appear.

- `mode` and `node_name`
- The Raft and HTTP ports
- `peers` and `http_peers`
- The certificate paths
- The `diagnostics:` section

To check which port a node listens on, read the configuration file or the startup banner.

**Computed properties are not listed.** A value derived from other settings, such as the effective
spill threshold, is a view of the configuration. Reporting it would show a name that is not a valid
config file key. The settings it derives from are listed.

## Node-local

`SHOW VARIABLES` describes **the node that served the statement**. The statement is not forwarded to
the leader.
Nodes in a cluster can differ — a different `data_dir`, a different port, a stale
`config.yml` on one box — and answering from the leader would hide the drift you are looking
for.

To compare configuration across a cluster, run the statement against each node's endpoint. This is
the same caveat that applies to [`SHOW ENGINE STATS`](engine-stats.md).

## Permissions

Requires a **superuser** when authentication is enabled. Even with the three secrets masked, the
output describes the node's security posture and limits. This includes whether authentication and TLS
are on, the password hashing cost, the data directory, and each rate-limit ceiling. A per-database grant
cannot scope this down. A non-superuser gets `CADB0517` (HTTP 403).

With authentication disabled, as on a single-node development instance, any caller may run it.

## There is no `SET`

`SHOW VARIABLES` is read-only, and there is no `SET GLOBAL <variable>` or session variable namespace.
Settings are changed cluster-wide with `SET CLUSTER SETTING <name> = <value>` (and reverted with
`RESET CLUSTER SETTING <name>`), which is superuser-gated — see
[runtime cluster settings](runtime-cluster-settings.md).

The `mutability` column shows whether such a change takes effect immediately. A `runtime` setting is
re-read by the component that uses it. This means that a new value applies while the node is running.
A `restart` setting is latched when its component is constructed. The overlay stores the new value and
`SHOW VARIABLES` will show it, but the running node will use the old value until it restarts.
Editing `config.yml` — the local layer — always requires a restart, and a cluster setting overrides
that file until it is reset.

`SET TRANSACTION …` is unrelated. It adjusts settings of the in-flight transaction, such as its
isolation level or priority, and does not touch configuration. The `default_*` variables shown here are
the fallback used when a transaction does not set a value.

## See also

- [Configuration](configuration.md) — the file format and the precedence chain. It also covers the CLI
  mapping.
- [Runtime cluster settings](runtime-cluster-settings.md) — changing a setting fleet-wide with
  `SET CLUSTER SETTING`, and what `mutability` and `scope` mean for whether it takes hold.
- [Engine statistics](engine-stats.md) — `SHOW ENGINE STATS`, the runtime-metrics counterpart.
- [Slow query log](slow-query-log.md) — `SHOW SLOW QUERIES`, the per-statement counterpart.
- [Statement statistics](statement-stats.md) and [index usage](index-usage.md) — `statement_stats_enabled`, `statement_stats_max_shapes` and `index_usage_enabled`.
