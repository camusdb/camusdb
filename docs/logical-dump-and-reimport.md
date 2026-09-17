# Logical dump and reimport

This guide moves the data of a CamusDB server from one storage revision to the next with
[`camus-dump`](https://github.com/camusdb/camusdb-dump) and
[`camus-cli`](https://github.com/camusdb/camussqlsh). You need it when you upgrade to a
CamusDB version that opens a new storage revision. The current revision is `v2`.

The same procedure also copies a server to a new machine, or seeds a test cluster from
production data.

## 1. Why a reimport is necessary

CamusDB stores every row and every index entry in an embedded Kahuna key-value store. The
shape of those keys is an on-disk format. Storage revision `v2` changed that shape:

```text
v1   {dbId}:{tableId}:r/{rowId}            {dbId}:{tableId}:i:{indexId}/{key}
v2   {dbId}:{tableId}|r/{rowId}            {dbId}:{tableId}|i:{indexId}/{key}
```

The `|` is Kahuna's placement-group separator. Under hash routing Kahuna places a key space by
the prefix before its first `|`. In `v2` a table's rows and every one of its indexes share the
group `{dbId}:{tableId}`, so they land on the same partition. A transaction that reads an index
entry and then writes the row it points to now stays on one partition. That is the condition for
Kahuna's one-phase commit path. In `v1` each key space hashed on its own, so on a cluster with
more than one partition almost every indexed point read landed on a different partition from the
row, and the one-phase path was unreachable.

CamusDB does not read `v1` keys through the `v2` layout, and it does not rewrite a `v1` store in
place. Kahuna opens the store and the Raft log under a directory named after the revision:

```text
{data_dir}/kv/v1     {data_dir}/wal/v1     ← written by the previous version, left untouched
{data_dir}/kv/v2     {data_dir}/wal/v2     ← created empty by the current version
```

A `v2` server that starts over a data directory with `v1` subdirectories starts **empty** and
logs this warning:

```text
Found storage revision v1 at /opt/camusdb/kv/v1; this version opens revision v2 and starts empty.
Data crosses revisions by logical dump and reimport only (docs/logical-dump-and-reimport.md).
```

The `v1` directories are not deleted. The previous CamusDB version can still open them, which is
what makes the dump in section 4 possible after an upgrade, and what makes a rollback possible.

## 2. What a dump carries, and what it does not

`camus-dump` writes SQL. For each database it emits `CREATE DATABASE IF NOT EXISTS`, a `USE`
line, then for each table a `CREATE TABLE`, the `CREATE INDEX` statements, and `INSERT`
statements for the rows. The `CREATE TABLE` text comes from `SHOW CREATE TABLE`, so it carries
column defaults, `CHECK` constraints, `COMMENT` clauses, covering-index `INCLUDE` columns, and the
row-level TTL configuration of the table.

Index definitions carry their **per-column sort direction** and their **comment**, both in the
inline `KEY` clause of the `CREATE TABLE` and in the separate `CREATE INDEX` statements, so a
descending index restores descending. Dumping from a server older than the release that added them
loses both silently: the older server does not report the direction at all, and no amount of
rewriting in the tool can recover it. Check a descending index after reimporting such a dump.

**A revision upgrade empties the user catalog.** Accounts, grants and sessions live in the same
key-value store the rows do, under the same revision directory, so a `v2` server starting over a `v1`
data directory starts with no accounts at all — it then seeds its bootstrap superuser and nothing else.
Record the accounts and their grants **before** you upgrade, with `SHOW USERS` and
`SHOW GRANTS FOR *`, and recreate them after the reimport. Passwords cannot be exported at all, so
plan to set a new one for every account.

The dump does **not** carry the objects below. Record them before the upgrade with the
statements shown, and recreate them after the reimport.

| Object | How to record it | How to recreate it |
| --- | --- | --- |
| Views | `SHOW VIEWS;` then `SHOW CREATE VIEW name;` per view | Run the printed `CREATE VIEW` |
| Materialized views | `SHOW MATERIALIZED VIEWS;` then `SHOW CREATE MATERIALIZED VIEW name;` | Run the printed statement, then `REFRESH MATERIALIZED VIEW name;` |
| Users | `SHOW USERS;` as a superuser; passwords cannot be exported | `CREATE USER name IDENTIFIED BY '…';` |
| Grants | `SHOW GRANTS FOR *;` as a superuser | `GRANT … ON … TO name;` |
| Cluster settings | `SHOW VARIABLES LIKE '%';` and keep the rows whose source is not the default | `SET CLUSTER SETTING name = value;` |
| Table statistics | Nothing to record | `ANALYZE table;` per table after the rows are loaded |
| Database branches | `SHOW BRANCHES FROM parent;` | See the note below |
| Physical backups | Nothing to record | Take a fresh full backup after the reimport |

**Branches.** A branch is listed by `SHOW DATABASES` as a database of its own, so `--all-databases`
dumps it. The dump reads the branch through its lineage and writes every row it sees. The
reimported copy is therefore a complete, independent database, and the copy-on-write relation
with the parent is gone. Recreate a branch with `CREATE DATABASE name BRANCH FROM parent` after
the parent is reimported if you need the relation and not the diverged contents.

**Physical backups.** A backup chain records the storage revision it was taken under. A `v1`
chain cannot be restored into a `v2` server. To recover from a `v1` backup after the upgrade,
restore it with the previous CamusDB version into a fresh directory, start that version on it,
and dump it with this procedure. See
[backups-and-point-in-time-recovery.md](backups-and-point-in-time-recovery.md).

## 3. Install the tools

Both tools are .NET global tools. Install them on a machine that can reach the server:

```shell
dotnet tool install --global CamusDB.Dump      # provides camus-dump
dotnet tool install --global CamusDB.SqlSh     # provides camus-cli
```

`camus-dump` connects over the client gRPC port, `5096` by default. `camus-cli` connects
through the CamusDB .NET driver; its default endpoint is `https://localhost:5095`, the HTTP port,
and an endpoint on the client gRPC port (`5096`) also works, in which case the driver speaks gRPC.
Both tools accept `https://` endpoints. With authentication on, the server refuses a password
over plaintext to any address other than loopback, so use `https://` against a remote server.

## 4. Dump every database with the previous version

Do this step **before** you install the new version, or after it with the previous version
started on the `v1` data directory.

1. Stop the applications that write to the server. The dump reads every table as of one fixed
   instant, so a dump under live writes is consistent, but the writes after that instant are not
   in the dump.
2. Run the dump for every database:

```shell
camus-dump -e http://db1.internal:5096 --all-databases -b 100 \
  --output-directory /backup/camusdb-v1/
```

   Option by option:

   - `--all-databases` (`-A`) asks the server for its databases with `SHOW DATABASES` and dumps
     each one. Use `--exclude-database` to skip a database you do not want to carry over.
   - `-b 100` writes 100 rows per `INSERT` statement. **This is the option that matters most.**
     See "Choosing the batch size" below.
   - `--output-directory` writes one `<database>.sql` file per database. Use `-o server.sql`
     instead to get one file with every database in sequence.
   - `--defer-indexes` is **not** in the command above on purpose. See "Should you defer the
     indexes?" below; it is not the free win it sounds like.

### Choosing the batch size

One row per `INSERT` is the tool default and it is by far the slowest thing you can do. Measured on
a single node, restoring 20,000 narrow rows, 5,000 rows of about 1 KB, and 1,500 rows of about 8 KB:

| Rows per `INSERT` | narrow (~100 B) | ~1 KB rows | ~8 KB rows |
| --- | --- | --- | --- |
| 1 | 177 rows/s | 165 rows/s | 185 rows/s |
| 10 | 1,888 | 1,647 | 814 |
| 50 | 4,510 | 3,549 | 1,786 |
| **100** | **5,033** | **3,582** | **2,086** |
| 250 | 6,031 | 3,043 | 2,206 |
| 500 | 5,759 | 4,425 | 2,161 |
| 1,000 | 6,163 | 4,480 | — |

`-b 1` is far slower than a batched load: 41 times on narrow rows, 27 times on 1 KB rows and 12 times
on 8 KB rows, against the best batch size for each shape. Its own throughput barely depends on row
width, because the cost is one round trip per statement rather than the data; the gap narrows on wide
rows only because a batched load of wide rows is itself slower. **Anything above about 100 rows
recovers most of the difference**, and past that the curve is flat inside the run-to-run noise
of the measurement, which was about ±20 %.

`-b 100` is the recommended starting point because it is at the knee *and* keeps the statement small:
100 rows of an 8 KB-row table is about 805 KB of SQL, comfortably inside the 4 MiB gRPC message limit.
A larger batch on a wide table is what breaks — 500 rows of 8 KB rows is a 4.02 MB statement, right at
the edge. Raise `-b` for narrow tables if you want; do not raise it for wide ones without checking the
statement size.

Two hard limits bound the batch whatever the throughput says:

- **The server's mutation limit per transaction**, 20,000 by default. A row costs **two** mutations
  before any secondary index — its row key and its primary-key index entry — so the ceiling is
  `floor(20000 / (2 + secondary index entries per row))`: 10,000 rows on a table with no secondary
  index, 6,666 with one. See [transaction-limits.md](transaction-limits.md).
- **The transport message size.** gRPC accepts 4 MiB per message; the HTTP API accepts 30 MB per
  body. A wide-row table reaches the gRPC limit long before the mutation limit.

### Should you defer the indexes?

`--defer-indexes` writes each table's `CREATE INDEX` statements after its rows, so the rows load into
a table with no secondary index and each index is built once at the end instead of being maintained
row by row.

**It is not automatically faster.** Measured on the same single node, 20,000 rows, comparing total
restore time including the index build:

| Table | Without the flag | With `--defer-indexes` |
| --- | --- | --- |
| One secondary index | 4.56 s | 4.05 s (**about 10 % faster**) |
| Four secondary indexes | 9.01 s | 10.00 s (**about 18 % slower**) |

The index build has to read the whole table once per index, and past one index that costs more than
the per-row maintenance it saves. Use the flag for a table with a single secondary index; skip it for
a table with several. These figures are from one machine at 20,000 rows — if your tables are far
larger, measure rather than assume, because a bulk build and per-row maintenance do not scale the
same way.

Whether or not you use it, **a restore is complete only once every index is built and validated.**
Keep the target out of service until then.

If your dump comes from a server older than the release that added
`SHOW CREATE TABLE … WITHOUT INDEXES`, `--defer-indexes` cannot work: that server renders each index
inline in the table definition, so the index exists before the first row regardless. `camus-dump`
detects this and prints a warning rather than producing a file that only looks deferred.

3. With authentication on, add the user and the password:

```shell
CAMUSDB_PASSWORD=app-secret camus-dump -e https://db1.internal:5096 -A -u admin \
  -b 100 --output-directory /backup/camusdb-v1/
```

   The dump needs `SELECT` and `SHOW` privileges on every table. Give the password through the
   environment or `--ask-password`, not on the command line.

4. Read the end of the dump output on standard error. `camus-dump` reports every value it could
   not render exactly. A non-finite float (`NaN`, `Infinity`) becomes `NULL`, and a `DATETIME`
   is truncated to milliseconds. Pass `--strict` to fail the dump instead when such a value
   exists.

5. Record the objects that a dump does not carry (section 2). The point in time of the dump is
   written in the header of each file:

```text
-- Point in time: 2026-09-07 14:10:22.117+00:00
```

6. Keep the row count of every table for the verification in section 7:

```shell
camus-cli -c "Endpoint=http://db1.internal:5095;Database=shop" \
  -e "select count(*) from orders" > /backup/camusdb-v1/shop.orders.count
```

## 5. Upgrade the server

1. Stop every CamusDB node. In cluster mode stop all of them: the new version starts a fresh Raft
   log under `wal/v2`, and a cluster with nodes on two revisions does not form.
2. Install the new version on every node. Keep `data_dir` as it is.
3. Start the nodes. Each node creates `kv/v2` and `wal/v2`, logs the warning from section 1, and
   serves an empty server. In cluster mode the cluster forms as on a first deployment.
4. If authentication is on, the users are gone with the `v1` store, so the user catalog is
   empty again. The server then creates one superuser from `CAMUSDB_BOOTSTRAP_USER` and
   `CAMUSDB_BOOTSTRAP_PASSWORD`, and it refuses to start when they are not set; see
   [sql-authentication.md](sql-authentication.md). Log in as that superuser for the reimport.

Do not set `kahuna.storage_revision` to `v1` to make the new version open the old store. The new
version cannot read the `v1` key layout, and every table would appear empty.

## 6. Reimport with camus-cli

Restore in this order, so that each step finds what it depends on:

1. Users, so that the objects created later can be granted to them.
2. Databases, tables, indexes and rows, which is the dump.
3. Views and materialized views, which reference the tables.
4. Grants, which reference users and objects.
5. Cluster settings.
6. `ANALYZE` on every table.

Run each dump file with `-f`. A file from `--output-directory` opens with its own
`CREATE DATABASE IF NOT EXISTS` and `USE`, so the database named on the connection does not
matter:

```shell
camus-cli -c "Endpoint=http://db1.internal:5095;Database=test" -f /backup/camusdb-v1/shop.sql
camus-cli -c "Endpoint=http://db1.internal:5095;Database=test" -f /backup/camusdb-v1/billing.sql
```

A single file with every database loads the same way:

```shell
camus-cli -c "Endpoint=http://db1.internal:5095;Database=test" -f /backup/camusdb-v1/server.sql
```

`camus-cli` streams the file, so its size does not matter. It stops at the first statement that
fails and prints the statement and the line it started on. Fix the cause and run the file again:
the `CREATE DATABASE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` statements are no-ops on a
second run, but the `INSERT` statements are not. Drop the partly loaded database before the
second run:

```shell
camus-cli -c "Endpoint=http://db1.internal:5095;Database=test" -e "drop database shop"
```

With authentication on, log in as the superuser:

```shell
CAMUS_PASSWORD=admin-secret camus-cli -u admin \
  -c "Endpoint=https://db1.internal:5095;Database=test" -f /backup/camusdb-v1/shop.sql
```

Then run the statements you recorded in section 2, in the order listed above. Finish with an
`ANALYZE` per table so that the planner has fresh statistics:

```sql
ANALYZE orders;
```

## 7. Verify

1. List the databases and the tables:

```sql
SHOW DATABASES;
SHOW TABLES;
SHOW INDEXES FROM orders;
```

2. Compare the row count of every table with the count you kept in section 4:

```sql
SELECT COUNT(*) FROM orders;
```

3. Confirm the placement that the `v2` layout exists for. In cluster mode, the rows and the
   indexes of one table report the same partition:

```sql
SHOW RANGES FROM TABLE orders;
SHOW RANGES FROM INDEX orders@orders_by_customer;
```

   Under hash routing both statements return one span each, and the `partition_id` column of
   the two spans is equal. See [show-ranges.md](show-ranges.md) for the columns.

4. Confirm the views, the grants and the settings:

```sql
SHOW VIEWS;
SHOW GRANTS FOR app;
SHOW VARIABLES LIKE '%';
```

## 8. Reclaim the old store, or roll back

**Reclaim.** When the verification passed and a fresh full backup exists, delete the `v1`
directories on every node to free the disk:

```shell
rm -r /opt/camusdb/kv/v1 /opt/camusdb/wal/v1
```

The warning in section 1 stops at the next start.

**Roll back.** To return to the previous version, stop the nodes, install the previous version,
and start it. It opens `kv/v1` and `wal/v1`, which the new version did not touch. Data written
to the `v2` store after the upgrade is not in `v1`. Dump it from the new version first if you
need it; the procedure is the same in the other direction.
