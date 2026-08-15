# SHOW VARIABLES

Inspect the configuration a running CamusDB node is actually using, from a SQL prompt.

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

The rows come from the configuration object the engine was **constructed with**, not from re-reading
`config.yml`. That distinction is the point of the statement: a value overridden by an environment
variable or a command-line flag after the file was read differs from what the file says, and it is
the resolved value that the engine obeys.

A key you commented out in `config.yml` shows its built-in default. A key an environment variable
overrode shows the override. What you see is what the node is running.

## Columns

| column     | meaning |
|------------|---------|
| `variable` | The `snake_case` key you would write in `config.yml`. Keys of the nested `kahuna:` section appear in dotted form, e.g. `kahuna.wal_sync_writes`. |
| `value`    | The effective value. SQL `NULL` when the setting is genuinely unset — distinct from an empty string. |
| `type`     | `bool`, `int`, `long`, `double`, `string`, `enum`, `duration_ms`, or `list`. |
| `default`  | What the setting would be if nothing overrode it. A value differing from `default` is one somebody configured. |
| `source`   | Which layer supplied the value: `default`, `config`, `env`, `cli`, or `cluster` (precedence runs cluster > cli > env > config > default). |
| `mutability` | `runtime` if a new value takes effect without restarting the node, `restart` if the component that reads it latches the value when it is constructed. This is a claim about the reader, not a preference: a `restart` key can be changed, but the running node keeps obeying the old value until it is restarted. |
| `scope`    | `cluster` when every node must agree — a per-node disagreement would change user-visible transaction behavior — and `node` when per-node divergence is the point, such as tracing or local cache sizes. |

Values are rendered the way `config.yml` spells them, so a value read here can be pasted back into a
file unchanged: lowercase booleans, invariant numerics with no digit separators or unit suffixes
(`67108864`, not `64 MiB`), underscored enum tokens (`read_committed`), and durations as whole
milliseconds like every other `*_ms` key.

Rows are sorted by name using ordinal comparison, so output from two nodes diffs line by line.

## `LIKE`

The optional pattern matches against the **variable name**. `%` matches any run of characters, `_`
matches exactly one. A pattern that matches nothing returns zero rows rather than an error.

Matching is **case-sensitive**, the same as `SHOW TABLES`, `SHOW DATABASES`, and `SHOW ENGINE STATS`
— they share one matcher. Variable names are all lowercase, so `LIKE 'TTL_%'` legitimately matches
nothing; write `LIKE 'ttl_%'`.

All three string literal forms work as the pattern: `'ttl_%'`, `"ttl_%"`, and `E'ttl_%'`.

## What is not shown

**Secrets are masked.** `bootstrap_superuser_password`, `access_token_server_key`, and `node_secret`
are listed — whether a node *has* a secret configured is an operational question — but their value
renders as `********` when set and empty when not. Certificate and key-file settings
(`https_certificate`, `raft_certificate`, `kahuna.backup_mac_key_file`) hold **paths**, not key
material, and are shown in full: an operator debugging a misconfigured deployment needs them.

**Deployment and topology keys are not yet listed.** `mode`, `node_name`, the Raft and HTTP ports,
`peers`, `http_peers`, the certificate paths, and the whole `diagnostics:` section live on a
different configuration object than the engine's own settings and do not currently appear. To check
which port a node listens on, read the configuration file or the startup banner.

**Computed properties are not listed.** A value derived from other settings — the effective spill
threshold, for instance — is a view of the configuration rather than part of it, and reporting it
would offer a name no config file accepts. The settings it derives from are listed.

## Node-local

`SHOW VARIABLES` describes **the node that served the statement**, and never forwards to the leader.
Nodes in a cluster can legitimately differ — a different `data_dir`, a different port, a stale
`config.yml` on one box — and answering from the leader would hide exactly the drift you are looking
for.

To compare configuration across a cluster, run the statement against each node's endpoint. This is
the same caveat that applies to [`SHOW ENGINE STATS`](engine-stats.md).

## Permissions

Requires a **superuser** when authentication is enabled. Even with the three secrets masked, the
output describes the node's whole security posture and limits — whether authentication and TLS are
on, the password hashing cost, the data directory, every rate-limit ceiling — which no per-database
grant scopes down. A non-superuser gets `CADB0517` (HTTP 403).

With authentication disabled, as on a single-node development instance, any caller may run it.

## There is no `SET`

`SHOW VARIABLES` is read-only, and there is no `SET GLOBAL <variable>` or session variable namespace.
Settings are changed cluster-wide with `SET CLUSTER SETTING <name> = <value>` (and reverted with
`RESET CLUSTER SETTING <name>`), which is superuser-gated — see
[runtime cluster settings](runtime-cluster-settings.md).

Whether such a change takes effect immediately is what the `mutability` column reports. A `runtime`
setting is re-read by the component that uses it, so a new value applies without a restart. A
`restart` setting is latched when its component is constructed: the overlay accepts the new value and
`SHOW VARIABLES` will show it, but the running node keeps obeying the old one until it restarts.
Editing `config.yml` — the local layer — always requires a restart, and a cluster setting overrides
that file until it is reset.

`SET TRANSACTION …` is unrelated — it adjusts the in-flight transaction's isolation, locking, and
priority, and does not touch configuration. The `default_*` variables shown here are only the
fallback used when a transaction does not state its own.

## See also

- [Configuration](configuration.md) — the file format, the precedence chain, and the CLI mapping.
- [Runtime cluster settings](runtime-cluster-settings.md) — changing a setting fleet-wide with
  `SET CLUSTER SETTING`, and what `mutability` and `scope` mean for whether it takes hold.
- [Engine statistics](engine-stats.md) — `SHOW ENGINE STATS`, the runtime-metrics counterpart.
