# Growing a one-node cluster

A deployment that starts on one machine and expects to grow must start in **cluster mode**. Standalone
mode can't grow. This page gives the steps to start one node in cluster mode and to add nodes to it
later.

## Why standalone mode cannot grow

The embedded Raft group of a standalone node is a fixed single-node quorum. Join requests to a
standalone node are refused, and join seeds are not allowed at startup. To move the data of a
standalone node into a cluster, you must dump it and reimport it into the cluster
([logical-dump-and-reimport.md](logical-dump-and-reimport.md)).

A one-node cluster is a cluster with one voter. DDL and transactions work as they do on a larger
cluster, and other nodes can join it.

## Decide these settings before the first boot

Two settings are fixed when the first node boots for the first time. If you change them later, the
change is ignored.

| Setting | What is fixed |
|---------|---------------|
| `initial_partitions` | The number of data partitions in the hash pool. Kommander writes it into the committed partition map at the first boot. On a later boot, a different value is ignored, and the node logs a warning. After the first boot, only a key-range split can add partitions. |
| `key_range_sharding` | The routing mode of the cluster. A node set to the other mode can't start (see [key-range-sharding.md](key-range-sharding.md#the-mode-is-fixed-when-the-cluster-is-created)). |

The default of `initial_partitions` is 3 in cluster mode. The annotated reference file
`CamusDB/Config/config.yml` sets `initial_partitions: 1` explicitly. If you copy that file, change
the value before the first boot.

## Step 1: start the first node

```yml
mode: cluster
node_name: camus-node-1
raft_node_id: 1
raft_host: 10.0.0.1
raft_port: 7070
http_port: 5095
initial_partitions: 3
peers: []          # no other node yet
```

Do not set `join_existing` on this node. A founding node with `join_existing: true` finds no
cluster to join.

## Step 2: add the second node

Start the new node with `join_existing: true`. Put one or more members of the running cluster in
`peers`. In a join, `peers` is the seed list.

```yml
mode: cluster
node_name: camus-node-2
raft_node_id: 2
raft_host: 10.0.0.2
raft_port: 7070
http_port: 5095
peers: [10.0.0.1:7070]   # seed: a member of the running cluster
join_existing: true
```

Or use `--join-existing true` on the command line.

The node enters the committed roster as a learner. It receives a snapshot of each partition, and
the leader promotes it to voter when it has caught up. Kommander changes membership one server at a
time. This means that you should wait until the node is a voter before you add the next node.

Each node needs a unique `node_name` and `raft_node_id`. A `peers` entry must not be the node's own
`raft_host:raft_port`.

## Step 3: add the third node

Do step 2 again on the third machine. `peers` can name any node that is already in the cluster.

A cluster of 3 voters has quorum when one node stops. A cluster of 2 voters does not: if one of
the 2 nodes stops, the cluster cannot commit until the node comes back. Do not leave the cluster at
2 nodes.

## Node-to-node HTTP addresses

Schema DDL, cluster settings and query fragments go from node to node over HTTP. A node uses two
rules, in order, to find the HTTP address of a peer.

1. When `http_peers` has one entry for each `peers` entry, the node uses that entry.
2. In all other cases, the node uses the host of the peer's Raft endpoint and the local
   `http_port`.

The first node lists no `peers`. This means that it uses rule 2 to reach the joiners. A joiner
knows its seeds and no other node. This means that it uses rule 2 for the other joiners. If a joiner
sets `http_peers` for its seeds, it also logs a warning each time it looks up a node that is not in
that list. For these reasons, give all nodes the same `http_port`. With the same port on all nodes,
rule 2 is correct for each node, and you do not need `http_peers`.

## What the test suite proves

The cluster test `OneNodeCluster_TakesTwoJoiners_AndEveryNodeServesEveryRow`
(`CamusDB.Cluster.Tests/TestClusterGrowFromOneNode.cs`) exercises this path in process, in the
following steps.

- One node in cluster mode, with key-range routing and 3 partitions, creates two tables with
  secondary indexes and writes rows.
- It splits one row space before the joins. This means that the joiners must also take a partition
  that the split created.
- Two nodes join, one at a time.
- Each node serves all rows by scan, by point read and through each index. All nodes route the
  split row space to the same partitions.
- A DDL after the joins reaches all nodes, and each node can take writes.
- The first node stops. The two joiners still have quorum, serve all rows and take writes.

Some cases are not covered by the test.

- A restart of the first node after the joins.
- A join over TLS or with mutual TLS.
- A join into a cluster that uses hash routing.
- Joins under a sustained write load.

To add a node from a backup in place of a full snapshot, see
[Seeding a joining node from a backup](backups-and-point-in-time-recovery.md#seeding-a-joining-node-from-a-backup).
