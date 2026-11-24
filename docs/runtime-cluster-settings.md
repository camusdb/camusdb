# Runtime cluster settings

CamusDB can change a defined subset of its configuration while the cluster is running. A change
applies to all nodes, no matter which node received it. This page explains the operator
surface: which settings are changeable live, how a change propagates, how to confirm it landed, and
what wins when a cluster value and a local `config.yml` disagree.

## The two axes: mutability and scope

Each setting has two classifications, mutability and scope. `SHOW VARIABLES` shows both.

- **Mutability** — `runtime` means a new value takes effect at the reader's next boundary (the next
  statement, the next transaction begin, the next iteration of a background loop), and a
  restart is not needed. `restart` means the value is baked into something built once per process —
  the data directory, node identity, Raft ports and peers, the settings in the `kahuna.*` section — and changing it
  requires editing configuration and restarting the node.
- **Scope** — `cluster` means the fleet must agree on the value (isolation defaults, mutation caps,
  TTL and lease policy): nodes disagreeing about it makes a user's transaction behave differently
  depending on which node accepted it. `node` means the setting is deliberately per-node (tracing
  switches, local cache sizes, whether *this* node does materialized-view refreshes).

You don't need to maintain a list of changeable settings by hand. The database lists them.

```sql
SHOW VARIABLES LIKE '%';            -- every setting: value, type, default, source, mutability, scope
SHOW VARIABLES LIKE 'ttl_%';        -- narrowed by name
```

Only `runtime`-class settings can be changed live. A `restart`-class key returns an error that says
so. An unknown key returns a different error that names it as unknown. The two errors send you to
two different places: the config file for the first, and a typo hunt for the second.

## Changing a setting

```sql
SET CLUSTER SETTING max_mutations_per_transaction = 40000;
SET CLUSTER SETTING default_isolation_level = read_committed;
SET CLUSTER SETTING query_tracing_enabled = true;
```

Values use the same spelling that `config.yml` uses. For example, booleans are lowercase and enum
members use underscores, as in `read_committed`. Durations are an integer number of milliseconds. A value printed by `SHOW VARIABLES` pastes back
unchanged. All three statements require a superuser when authentication is enabled. Several of
these knobs bound memory, concurrency and background work. This means that changing them fleet-wide
is a denial-of-service lever.

The change is validated **before** it is applied anywhere, against the configuration that would
result — including the cross-field invariants (a lease-renew interval must sit under its lease, the
materialized-view refresh chunk must stay well under the mutation cap, and so on). A value that
would break one is rejected with the validator's message, which names both sides of the check.

The statement may be sent to any node. A node that does not lead the settings partition forwards
the change to the leader over the authenticated internal routes. The change is then committed
through Raft and applied by all nodes. This means that two concurrent conflicting `SET`s resolve to
the same winner everywhere. Commit order decides the winner; wall clocks are never used. A node that
is down during the change catches up by log replay when it returns.

In standalone (non-cluster) mode the same statement, validation and persistence apply locally, and
no replication is involved.

## Undoing a change

```sql
RESET CLUSTER SETTING max_mutations_per_transaction;
```

`RESET` removes the cluster's entry for the key. It does **not** write the built-in default. Each
node then resolves the key through its local chain again (command line, environment, file,
built-in default). A node whose `config.yml` names that key returns to its file value.

## Precedence: the cluster layer wins, and says so

For a runtime key the cluster value overrides the node's command line and environment. It also
overrides `config.yml`. This is deliberate: if the local file won, a fleet-wide change would silently no-op
on the one node whose YAML happens to name that key — invisible, and the hardest drift to
diagnose. `restart`-class keys are never cluster-overridable. Node-defining values such as the
data directory, peers and ports are not affected.

A node whose behavior contradicts its `config.yml` explains itself in `SHOW VARIABLES`. The
`source` column reads `cluster` for a key the cluster supplies. For a key from local configuration
it reads `config`, `env`, `cli` or `default`.

## Confirming a change landed on every node

`SHOW VARIABLES` is deliberately node-local — it describes the node that served the statement, and
answering from the leader would hide the drift you are looking for. To confirm a fleet-wide
change, send the statement to each node and check the row that it returns.

```sql
-- run against every node:
SHOW VARIABLES LIKE 'max_mutations_per_transaction';
```

Each node should show the new value with `source = cluster`. A node that shows its file value
either has not applied the change yet (replication lag — retry shortly) or cannot reach the
cluster. If the file value persists, check that node's connectivity to the settings partition
leader and its logs for failed-apply warnings.

`SHOW CLUSTER SETTINGS` lists the keys that a `SET` changed and no `RESET` dropped.

```sql
SHOW CLUSTER SETTINGS;
SHOW CLUSTER SETTINGS LIKE 'ttl_%';
```

This lists the cluster overlay itself (name and value), which is distinct from `SHOW VARIABLES`:
the overlay is what the fleet agreed on; the variables are what this node is running after
merging that overlay over its local configuration.

## Boot behavior

A node always boots on its file configuration and swaps to the merged cluster view as soon as
its store is readable — before the query engine starts serving. Components built before that point
either react to the swap or read only `restart`-class settings. This means that an ordinary restart
cannot produce a node that permanently disagrees with the cluster.

## Transactional DDL

`transactional_ddl_enabled` (runtime, cluster) decides what a schema statement does inside an explicit
transaction; see [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) §9.7. It is
read at each schema statement. A transaction that already changed the schema keeps its rule until it
ends, also when the setting is turned off meanwhile.

While the new value is on its way to the nodes, two nodes can answer differently for a moment. A node
with the setting off does not take the DDL lock for its schema statements. A transaction on another node
whose base schema moved that way is refused at its commit with the retryable `CADB0502`, so nothing is lost.
