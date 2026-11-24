# Vector search

CamusDB stores embeddings as `bytes` columns and ranks them with exact distance functions. A
nearest-neighbour query is an ordinary `SELECT` with an `ORDER BY` and a `LIMIT`, as in the following
example.

```sql
SELECT id
FROM docs
WHERE tenant_id = @tenant
ORDER BY l2_distance(embedding, @q)
LIMIT 10;
```

Search is **exact**. It examines all rows that the `WHERE` admits and returns the true nearest rows.
The results are not approximate, and there is no recall setting to tune. Search is also linear in the number of rows — see
[Cost](#cost) before pointing it at a very large table.

## Storing a vector

A vector is a `bytes` value that holds **tightly packed little-endian IEEE-754 float32 elements**. The
value does not start with a header. Its dimension is the byte count divided by four. For example, a
768-dimension embedding occupies 3 072 bytes.

That layout is the complete contract. The schema does not record it. A value written by one client
can be interpreted by another client only because both follow it. `1.0f` is `0x3F800000`, which on the
wire is the four bytes `00 00 80 3F` — low byte first.

```sql
CREATE TABLE docs (
    id         oid PRIMARY KEY,
    tenant_id  int64,
    embedding  bytes(3072) NOT NULL STORAGE PLAIN,
    CONSTRAINT embedding_is_768d CHECK (vector_dims(embedding) = 768)
);
```

With `STORAGE PLAIN`, each embedding is stored in its row. If you leave it out, each 768-dimension
embedding moves out of its row. For more information, see
[Storage strategy for embeddings](#storage-strategy-for-embeddings).

### `bytes(N)` is a maximum, not a width

`bytes(3072)` says a value may not exceed 3 072 bytes. A shorter value is allowed. This means that a
767-float embedding fits the column. The `CHECK` pins the dimension. If the column does not have one,
a short or truncated vector is stored as if it were valid. It is then compared against full-length
vectors until some query raises a dimension mismatch.

The two functions in the following table support that check.

| Function | Returns |
| --- | --- |
| `octet_length(bytes \| string)` | Byte count. For a string, the **UTF-8** byte count — `octet_length('áé')` is 4 while `length('áé')` is 2. |
| `vector_dims(bytes)` | Element count, `octet_length / 4`. |

`vector_dims` **returns an error** for a byte count that is not a multiple of four. If it rounded down, a
3 070-byte value would report 767 dimensions and satisfy a check written for 767, which hides
the corruption the check exists to catch.

A `NULL` embedding **passes** `CHECK (vector_dims(embedding) = 768)`. SQL violates a check
only on `false`, and `vector_dims(NULL)` is `NULL`. Use `NOT NULL` to forbid a missing vector; the
check cannot do it.

### Storage strategy for embeddings

An embedding is a `bytes` value. The large-value rules of [Large values](storage-layout.md) apply to
it. A 768-dimension embedding is 3 072 bytes, which is above the default out-of-line threshold of
2 048 bytes. Under the default strategy, `EXTENDED`, each embedding therefore moves out of its row. A
KNN query that ranks all rows then pays one extra batched fetch per scanned batch. LZ4 does not shrink
float32 data. An attempt to compress it costs CPU on each write, and the minimum-saving rule then
stores it raw.

Measured on 10 000 rows of 768 dimensions (the workload in [Cost](#cost)), a `LIMIT 10` KNN query
over out-of-line embeddings took about 90 ms. With `PLAIN`, or with all values inline, it took 15–35 ms.
Out-of-line storage is roughly 2.5 times slower. `PLAIN` measured the same as an engine that stores
all values inline, within the noise of the run.

You should choose the strategy for the query that reads the column.

- **`PLAIN`** — for an embedding that a KNN query reads on each row. The value is stored in the row
  and is not compressed. It costs exactly what it cost before large-value storage existed.
- **`EXTERNAL`** — for an embedding that most queries do not read. It moves out of the row, and no
  compression is attempted. Queries that read only the other columns are fast.

Do not leave an embedding column on `EXTENDED`. For an existing table, change the strategy and then
convert the stored rows with `REWRITE STORAGE`.

```sql
ALTER TABLE docs ALTER COLUMN embedding SET STORAGE PLAIN;
ALTER TABLE docs REWRITE STORAGE;
```

## Distance functions

All three take two vectors of equal dimension and return `float64`.

| Function | Meaning | Nearest is | Order with |
| --- | --- | --- | --- |
| `l2_distance(a, b)` | Euclidean distance | smaller | `ASC` (the default) |
| `cosine_distance(a, b)` | `1 - cosine_similarity` | smaller | `ASC` |
| `inner_product(a, b)` | Dot product | **larger** | **`DESC`** |

`inner_product` is ordered the other way: a larger value is nearer. If you order it ascending, the
query returns the *least* similar rows. No error is raised, and the mistake looks like a working
query.

```sql
-- Cosine: nearest first
SELECT id FROM docs ORDER BY cosine_distance(embedding, @q) LIMIT 10;

-- Inner product: most similar first
SELECT id FROM docs ORDER BY inner_product(embedding, @q) DESC LIMIT 10;
```

`cosine_distance` returns 0 for identical directions and 2 for opposite ones. It never returns a
negative value. The similarity is clamped before the subtraction. This means that rounding cannot
push an exact match below zero and sort it ahead of itself.

All intermediate values are widened to `double` before they are multiplied. Extreme finite values
therefore do not overflow the way `float` arithmetic would.

The three functions use SIMD on hardware that accelerates it. The vectorized loop behaves the same
as the scalar loop. It widens each element to `double` before any arithmetic, and it returns the
same error for `NaN` and infinity, naming the same offending element. Only the summation
order differs, which can change the last bits of a result. A big-endian host and very short vectors
use the scalar loop.

### Projecting the distance

To also get the value back, order by an alias.

```sql
SELECT id, l2_distance(embedding, @q) AS distance
FROM docs
ORDER BY distance
LIMIT 10;
```

## Sending the query vector

Send it as a **bind parameter**. Inlining a 768-dimension vector as a hex literal puts about 6 KB of
text in each statement. It also gives each query a different statement text, which defeats plan
reuse.

| Transport | Encoding |
| --- | --- |
| SQL text | `0x`-prefixed hex |
| REST (JSON) | base64, under `bytesValue` |
| gRPC | raw bytes, in `Value.bytes_value` |

```jsonc
// POST /execute-sql-query
{
  "databaseName": "app",
  "sql": "SELECT id FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 10",
  "parameters": {
    "@q": { "type": 7, "bytesValue": "AAAAAAAAgD8..." }   // type 7 = Bytes
  }
}
```

The statement text is the same for each query. This means that a thousand different query vectors
share one cached plan.
Prepared statements work the same way: prepare once, execute with a different vector each time.

The server logs the statement. It does not log the parameter values. This means that an embedding
does not end up in a log file.

A 3 072-byte vector is far below both transport limits — the gRPC 4 MB message default and the 30 MB
request-body default. Neither is narrowed for vectors.

## Errors

| Condition | Error |
| --- | --- |
| Byte count not divisible by four | `CADB0410` malformed vector |
| Operands of different dimensions | `CADB0411` dimension mismatch, naming both |
| `NaN` or infinity in a vector | `CADB0412` invalid vector value |
| `cosine_distance` on a zero-magnitude vector | `CADB0412` invalid vector value |

All map to HTTP 400. A dimension mismatch is an error. Truncating to the shorter operand would
return a plausible ranking computed from mismatched data.

Non-finite elements cause an error when a vector function reads them. Writing the row does not check
them. The schema cannot tell an embedding from a file. For this reason, no write path may reject a
`bytes` value for failing a vector rule.

## Cost

`EXPLAIN` shows how the query will be ranked. With a `LIMIT`, the sort is bounded and shows as a
`topk` step.

```
topk(k: 10, l2_distance(embedding, @q) ASC)
```

If the query has no `LIMIT`, the sort is a full sort. It ranks and materializes all matching rows.

The following times were measured on an Apple M3 with 10 000 rows of 768 dimensions and the SIMD
distance kernels.

| Query | Time |
| --- | ---: |
| `ORDER BY l2_distance(...) LIMIT 10` | 12 ms |
| `ORDER BY l2_distance(...)` (no limit) | 50 ms |
| `SELECT id FROM docs` (no ordering) | 6 ms |

The `LIMIT` cuts the query to roughly a quarter, and `k` itself barely matters: 10 and 100 measure
the same. Reading and ranking all rows dominates the cost, and the rows retained add little to it.

Cost is **linear in rows × dimensions**. The figures above extrapolate to roughly 1.2 s for the same
query over a million rows on the same hardware. You should always pair a vector `ORDER BY` with a
`LIMIT`. Where you can, narrow the candidate set with a `WHERE`. The filter is applied before the
ranking. This means that a selective predicate reduces the rows the distance is evaluated on.

In cluster mode, ranking happens at the coordinator. A query may ship all qualifying rows to the
node that executes it. There is no distributed top-k that bounds what crosses the network.

## Not supported yet

- **No approximate index.** No HNSW, no IVF, no quantization. Each query is an exact scan.
- **No vector-specific index type.** An ordinary index cannot help a distance ordering.
  Distance is not an order-preserving function of the stored bytes.
- **No native vector type.** The dimension lives in a `CHECK`, and the catalog does not hold it. The
  element type is a convention that the schema does not record.
- **No distributed top-k.** See the cluster note above.
