# INSERT … RETURNING

An `INSERT` statement can send back values from the rows that it inserted. Add a `RETURNING` list
at the end of the statement:

```sql
INSERT INTO orders (customer, total) VALUES ('a', 10), ('b', 20)
RETURNING id, total * 2 AS doubled;

INSERT INTO archive SELECT * FROM orders WHERE total > 100
RETURNING *;
```

The result has the same shape as a `SELECT` result: an ordered column schema, then one row for each
inserted row.

## What the list can hold

The `RETURNING` list is a select list over the target table. It accepts the same items as the
select list of a `SELECT … FROM <target>`:

- `*`, which expands to every column of the table, in schema order.
- A column name. A qualified name (`orders.total`) also works.
- A scalar expression, such as arithmetic, a `CASE`, a cast or a scalar function.
- An alias (`AS name`).
- A bound parameter (`@p`).

The list cannot hold these items. The server refuses the statement before it writes a row:

| Item | Error |
| --- | --- |
| An aggregate (`COUNT(*)`, `SUM(x)`) | `CADB0400` (`InvalidInput`) |
| A subquery | `CADB0400` (`InvalidInput`) |
| A sequence function (`nextval`, `setval`, `currval`, `lastval`) | `CADB0547` (`SequenceCallNotAllowedHere`) |
| A name that is not a column of the target | `CADB0404` (`UnknownColumn`) |

## What the values are

RETURNING reports the stored value of each row, not the value in the statement text:

- A column that the statement did not name has its default: a constant default, a function default
  such as `gen_id()` or `now()`, or a value drawn from a sequence or an identity column.
- A value has the type of its column after coercion. For example, `10` inserted into a `float64`
  column returns `10.0`.
- A column with no value and no default returns `NULL`.
- A large value that the server stores out of line returns in full.

The rows come back in insert order. For `INSERT … SELECT`, the order is the order of the source
query.

## Where to send the statement

Both kinds of endpoint accept `INSERT … RETURNING`, and both return the rows.

| Endpoint | What it returns |
| --- | --- |
| HTTP `/execute-sql-query`, `/execute-sql-query-stream` | The usual query result: `columns`, then `rows`. |
| HTTP `/execute-sql-non-query` | `rows` (the count), plus `columns` and `returningRows`. |
| gRPC `ExecuteQuery` | The usual query stream: `ResultSchema`, then `ResultRow` messages. |
| gRPC `ExecuteNonQuery` | `NonQueryReply` with `affected_rows`, `returning_schema` and `returning_rows`. |
| gRPC `BatchExecute` | A `QUERY` op returns schema and rows. A `NON_QUERY` op returns a `NonQueryReply` as above. |

On the no-rows endpoints, the new fields are present only for a statement with a RETURNING list. A
statement without one gets the same response as before. When the statement inserts no rows, the
schema is present and the row list is empty.

`returningRows` uses the same positional encoding as the `rows` of a query response, so one decoder
reads both.

## Transactions and retries

- An autocommit `INSERT … RETURNING` on a query endpoint runs in a writable transaction, as it does on
  the no-rows endpoints. It honors `isolationLevel`, `transactionMode`, `locking` and `priority`.
- The server sends no row before the commit. The streaming endpoints also wait for the commit before
  they send the schema. Thus, a client never receives rows from an attempt that did not commit, and a
  Serializable conflict can retry from a new transaction.
- In an explicit transaction, the rows come back before the commit, as for any statement. A rollback
  removes them.

## Count only

A caller that needs only the count can ask the server not to send the rows:

- HTTP: set `"discardReturningRows": true` in the request body of `/execute-sql-non-query`.
- gRPC: set `discard_returning_rows = true` on the `SqlRequest` of `ExecuteNonQuery` or of a
  `NON_QUERY` batch op.
- .NET client: use the `ExecuteNonQueryAsync` overload with `discardReturningRows: true`.

With the flag set, the server still checks the RETURNING list and the SELECT privilege, so the same
statement gives the same errors with and without the flag. The response then has the count only. The
flag has no effect on a statement without RETURNING.

The query endpoints refuse a request with the flag set (`CADB0400`), because a query that asks for no
rows is a client error.

## Privileges

`INSERT … RETURNING` reads the rows that it inserts, so it needs `SELECT` on the target table in
addition to `INSERT`, as in PostgreSQL. A user with `INSERT` only can run a plain `INSERT`, but an
`INSERT … RETURNING` fails with `CADB0517` (`InsufficientPrivilege`, HTTP 403). The count-only
flag does not change this. For `INSERT … SELECT`, the source tables need `SELECT`, as before.

## Size limits

The rows stay in server memory until the statement completes. The per-transaction mutation limit
(`max_mutations_per_transaction`) limits the row count.

A gRPC `ExecuteNonQuery` reply, and a `NON_QUERY` batch reply, carry all rows in one message. A gRPC
client receives at most 4 MiB in one message by default. If the RETURNING rows do not fit, the server
refuses the statement with `CADB0550` (`ReturningResultTooLarge`, gRPC status `RESOURCE_EXHAUSTED`)
**before** the commit. The statement is rolled back and stores nothing. To insert the rows, do one of
these:

- Send the statement to `ExecuteQuery`, which streams the rows.
- Set the count-only flag.

In an explicit transaction, a `NON_QUERY` batch op that gets `CADB0550` also rolls back the
transaction. The rows are staged in the transaction, but the client is told the statement failed, so
a later `COMMIT` must not store them.

The HTTP endpoints have no such limit.

## Reserved word

`RETURNING` is a reserved word, as in PostgreSQL. A table or a column named `returning` needs
backticks: `` SELECT `returning` FROM t ``.

## Not supported yet

- `UPDATE … RETURNING` and `DELETE … RETURNING`.
- `ON CONFLICT`.

## Related

- [insert-select-and-ctas.md](insert-select-and-ctas.md) — `INSERT … SELECT`.
- [sequences.md](sequences.md) — sequence and identity defaults.
- [sql-authentication.md](sql-authentication.md) — privileges.
- [grpc-client-protocol.md](grpc-client-protocol.md) — the wire contract.
- [grpc-dotnet-client.md](grpc-dotnet-client.md) — the .NET client.
