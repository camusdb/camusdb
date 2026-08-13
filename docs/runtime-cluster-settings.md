# Runtime cluster settings

CamusDB can change a defined subset of its configuration while the cluster is running, and such a
change lands on every node no matter which node received it. This page explains the operator
surface: which settings are changeable live, how a change propagates, how to confirm it landed, and
what wins when a cluster value and a local `config.yml` disagree.

## The two axes: mutability and scope

Every setting carries two classifications, visible in `SHOW VARIABLES`:

- **Mutability** — `runtime` means a new value takes effect at the reader's next boundary (the next
  statement, the next transaction begin, the next iteration of a background loop) without a
  restart. `restart` means the value is baked into something built once per process — the data
  directory, node identity, Raft ports and peers, the whole `kahuna.*` section — and changing it
  requires editing configuration and restarting the node.
- **Scope** — `cluster` means the fleet must agree on the value (isolation defaults, mutation caps,
  TTL and lease policy): nodes disagreeing about it makes a user's transaction behave differently
  depending on which node accepted it. `node` means the setting is deliberately per-node (tracing
  switches, local cache sizes, whether *this* node runs materialized-view refreshes).

Do not maintain a list of changeable settings by hand — ask the database:

```sql
SHOW VARIABLES LIKE '%';            -- every setting: value, type, default, source, mutability, scope
SHOW VARIABLES LIKE 'ttl_%';        -- narrowed by name
```

Only `runtime`-class settings can be changed live. A `restart`-class key is rejected with an error
that says so; an unknown key is rejected as unknown — two different errors, because they send you
to two different places (the config file versus a typo hunt).

## Changing a setting

```sql
SET CLUSTER SETTING max_mutations_per_transaction = 40000;
SET CLUSTER SETTING default_isolation_level = read_committed;
SET CLUSTER SETTING query_tracing_enabled = true;
```

Values use the same spelling `config.yml` uses — lowercase booleans, plain numbers, underscored
enum members, durations as whole milliseconds. A value printed by `SHOW VARIABLES` pastes back
unchanged. All three statements require a superuser when authentication is enabled: several of
these knobs bound memory, concurrency and background work, so changing them fleet-wide is a
denial-of-service lever.

The change is validated **before** it is applied anywhere, against the configuration that would
result — including the cross-field invariants (a lease-renew interval must sit under its lease, the
materialized-view refresh chunk must stay well under the mutation cap, and so on). A value that
would break one is rejected with the validator's message, which names both sides of the check.

The statement may be sent to any node. A node that does not lead the settings partition forwards
the change to the leader over the authenticated internal routes; the change is then committed
through Raft and applied by every node, so two concurrent conflicting `SET`s resolve to the same
winner everywhere — commit order decides, never wall clocks. A node that is down during the change
catches up by log replay when it returns.

In standalone (non-cluster) mode the same statement, validation and persistence apply locally with
no replication involved.

## Undoing a change

```sql
RESET CLUSTER SETTING max_mutations_per_transaction;
```

`RESET` removes the cluster's entry for the key — it does **not** write the built-in default. Each
node then resolves the key through its own local chain again (command line, environment, file,
built-in default), so a node whose `config.yml` names that key returns to its file value.

## Precedence: the cluster layer wins, and says so

For a runtime key the cluster value overrides the node's command line, environment, and
`config.yml`. This is deliberate: if the local file won, a fleet-wide change would silently no-op
on the one node whose YAML happens to name that key — invisible, and the hardest drift to
diagnose. `restart`-class keys are never cluster-overridable, so genuinely node-defining values
(data directory, peers, ports) are unaffected.

A node whose behavior contradicts its own `config.yml` explains itself in `SHOW VARIABLES`: the
`source` column reads `cluster` for a key the cluster supplies, instead of `config`, `env`, `cli`,
or `default`.

## Confirming a change landed on every node

`SHOW VARIABLES` is deliberately node-local — it describes the node that served the statement, and
answering from the leader would hide exactly the drift you are looking for. So confirming a
fleet-wide change means asking each node and checking the row:

```sql
-- run against every node:
SHOW VARIABLES LIKE 'max_mutations_per_transaction';
```

Every node should report the new value with `source = cluster`. A node still showing its file value
either has not applied the change yet (replication lag — retry shortly) or cannot reach the
cluster; if it persists, check that node's connectivity to the settings partition leader and its
logs for failed-apply warnings.

To see what the cluster currently carries — every key a `SET` changed and no `RESET` dropped:

```sql
SHOW CLUSTER SETTINGS;
SHOW CLUSTER SETTINGS LIKE 'ttl_%';
```

This lists the cluster overlay itself (name and value), which is distinct from `SHOW VARIABLES`:
the overlay is what the fleet agreed on; the variables are what this node is actually running after
merging that overlay over its local configuration.

## Boot behavior

A node necessarily boots on its file configuration and swaps to the merged cluster view as soon as
its store is readable — before the query engine starts serving. Components built before that point
either react to the swap or hold only `restart`-class settings, so an ordinary restart cannot
produce a node that permanently disagrees with the cluster.
