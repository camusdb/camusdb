# Vector search

CamusDB stores embeddings as `bytes` columns and ranks them with exact distance functions. A
nearest-neighbour query is an ordinary `SELECT` with an `ORDER BY` and a `LIMIT`:

```sql
SELECT id
FROM docs
WHERE tenant_id = @tenant
ORDER BY l2_distance(embedding, @q)
LIMIT 10;
```

Search is **exact**: it examines every row the `WHERE` admits and returns the true nearest rows, with
no approximation and no recall to tune. It is also linear in the number of rows — see
[Cost](#cost) before pointing it at a very large table.

## Storing a vector

A vector is a `bytes` value holding **tightly packed little-endian IEEE-754 float32 elements, with no
header**. Its dimension is the byte count divided by four, so a 768-dimension embedding occupies
3 072 bytes.

That layout is the whole contract. Nothing in the schema records it, so a value written by one client
and read by another is only interpretable because both follow it. `1.0f` is `0x3F800000`, which on the
wire is the four bytes `00 00 80 3F` — low byte first.

```sql
CREATE TABLE docs (
    id         oid PRIMARY KEY,
    tenant_id  int64,
    embedding  bytes(3072) NOT NULL,
    CONSTRAINT embedding_is_768d CHECK (vector_dims(embedding) = 768)
);
```

### `bytes(N)` is a maximum, not a width

`bytes(3072)` says a value may not exceed 3 072 bytes. It does **not** say a value must be exactly
that long, so a 767-float embedding fits the column happily. The `CHECK` is what pins the dimension,
and without one a short or truncated vector is stored without complaint and compared against
full-length vectors until some query raises a dimension mismatch.

Two functions support that check:

| Function | Returns |
| --- | --- |
| `octet_length(bytes \| string)` | Byte count. For a string, the **UTF-8** byte count — `octet_length('áé')` is 4 while `length('áé')` is 2. |
| `vector_dims(bytes)` | Element count, `octet_length / 4`. |

`vector_dims` **rejects** a byte count that is not a multiple of four rather than rounding down. A
3 070-byte value would otherwise report 767 dimensions and satisfy a check written for 767 — hiding
exactly the corruption the check exists to catch.

A `NULL` embedding **passes** `CHECK (vector_dims(embedding) = 768)`, because SQL violates a check
only on `false` and `vector_dims(NULL)` is `NULL`. Use `NOT NULL` to forbid a missing vector; the
check cannot do it.

## Distance functions

All three take two vectors of equal dimension and return `float64`.

| Function | Meaning | Nearest is | Order with |
| --- | --- | --- | --- |
| `l2_distance(a, b)` | Euclidean distance | smaller | `ASC` (the default) |
| `cosine_distance(a, b)` | `1 - cosine_similarity` | smaller | `ASC` |
| `inner_product(a, b)` | Dot product | **larger** | **`DESC`** |

`inner_product` is the one that runs the other way. Ordering it ascending returns the *least* similar
rows — and returns them without any error, so the mistake looks like a working query.

```sql
-- Cosine: nearest first
SELECT id FROM docs ORDER BY cosine_distance(embedding, @q) LIMIT 10;

-- Inner product: most similar first
SELECT id FROM docs ORDER BY inner_product(embedding, @q) DESC LIMIT 10;
```

`cosine_distance` returns 0 for identical directions and 2 for opposite ones. It never returns a
negative value: the similarity is clamped before the subtraction, so rounding cannot push an exact
match below zero and sort it ahead of itself.

Every intermediate is widened to `double` before it is multiplied, so extreme finite values do not
overflow the way `float` arithmetic would.

The three functions use SIMD on hardware that accelerates it. The vectorized loop keeps the same
behavior as the scalar loop: it widens each element to `double` before any arithmetic, it rejects
`NaN` and infinity with the same error, and it names the same offending element. Only the summation
order differs, which can change the last bits of a result. A big-endian host and very short vectors
use the scalar loop.

### Projecting the distance

Order by an alias when you also want the value back:

```sql
SELECT id, l2_distance(embedding, @q) AS distance
FROM docs
ORDER BY distance
LIMIT 10;
```

## Sending the query vector

Send it as a **bind parameter**. Inlining a 768-dimension vector as a hex literal puts about 6 KB of
text in every statement and gives each query a different statement text, which defeats plan reuse.

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

Because the statement text never changes, a thousand different query vectors share one cached plan.
Prepared statements work the same way: prepare once, execute with a different vector each time.

The server logs the statement, never the parameter values, so an embedding does not end up in a log
file.

A 3 072-byte vector is far below both transport limits — the gRPC 4 MB message default and the 30 MB
request-body default. Neither is narrowed for vectors.

## Errors

| Condition | Error |
| --- | --- |
| Byte count not divisible by four | `CADB0410` malformed vector |
| Operands of different dimensions | `CADB0411` dimension mismatch, naming both |
| `NaN` or infinity in a vector | `CADB0412` invalid vector value |
| `cosine_distance` on a zero-magnitude vector | `CADB0412` invalid vector value |

All map to HTTP 400. A dimension mismatch is refused rather than truncated to the shorter operand,
because truncating would return a plausible ranking computed from mismatched data.

Non-finite elements are rejected when a vector function reads them, not when the row is written. The
schema cannot tell an embedding from a file, so no write path may reject a `bytes` value for failing
a vector rule.

## Cost

`EXPLAIN` shows how the query will be ranked. With a `LIMIT`, the sort is bounded:

```
topk(k: 10, l2_distance(…) ASC)
```

Without one it is a full sort, which ranks and materializes every matching row.

Measured on an Apple M3, 10 000 rows, 768 dimensions, with the SIMD distance kernels:

| Query | Time |
| --- | ---: |
| `ORDER BY l2_distance(...) LIMIT 10` | 12 ms |
| `ORDER BY l2_distance(...)` (no limit) | 50 ms |
| `SELECT id FROM docs` (no ordering) | 6 ms |

The `LIMIT` cuts the query to roughly a quarter, and `k` itself barely matters — 10 and 100 measure
the same, because the cost is dominated by reading and ranking every row rather than by what is
retained.

Cost is **linear in rows × dimensions**. The figures above extrapolate to roughly 1.2 s for the same
query over a million rows on the same hardware. Always pair a vector `ORDER BY` with a `LIMIT`, and
narrow the candidate set with a `WHERE` where you can — the filter runs before the ranking, so a
selective predicate reduces the rows the distance is evaluated on.

In cluster mode, ranking happens at the coordinator, so a query may ship every qualifying row to the
node running it. There is no distributed top-k that bounds what crosses the network.

## Not supported yet

- **No approximate index.** No HNSW, no IVF, no quantization. Every query is an exact scan.
- **No vector-specific index type.** An ordinary index cannot help a distance ordering, because
  distance is not an order-preserving function of the stored bytes.
- **No native vector type.** The dimension lives in a `CHECK`, not in the catalog, and the element
  type is a convention rather than something the schema records.
- **No distributed top-k.** See the cluster note above.
