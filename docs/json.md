# JSON documents: the `jsonb` type

CamusDB stores JSON documents in a `jsonb` column. The SQL surface follows PostgreSQL: the same type
name, the same operators, the same functions, and GIN indexes. An application written for PostgreSQL
`jsonb` runs without changes to its SQL, except for the differences listed in
[Differences from PostgreSQL](#differences-from-postgresql).

```sql
CREATE TABLE events (
    id      int64 PRIMARY KEY,
    payload jsonb NOT NULL
);

INSERT INTO events (id, payload)
VALUES (1, '{"user": {"id": 7, "tags": ["vip"]}, "kind": "login"}');

SELECT payload -> 'user' ->> 'id' FROM events WHERE payload @> '{"kind": "login"}';
SELECT jsonb_set(payload, '{user,tags}', '["vip", "beta"]') FROM events;

CREATE INDEX events_payload_gin ON events USING GIN (payload);
```

A **document** is one JSON value: an object, an array, a string, a number, a boolean or JSON `null`.
A `jsonb` cell holds one document, or SQL `NULL`. JSON `null` and SQL `NULL` are different values:
`'null'::jsonb IS NULL` is false.

---

## The type

`jsonb` is the type. `json` is an alias of it, as in CockroachDB. Both words work in a column
definition, in `ALTER TABLE ... ADD COLUMN`, in a cast and in a typed literal. `SHOW CREATE TABLE`
shows `JSONB`.

A table or a column can still be named `json` or `jsonb`: the two words are not reserved.

### Writing a document

| Form | Example |
|------|---------|
| Text literal or text parameter written to a `jsonb` column | `INSERT INTO t (doc) VALUES ('{"a": 1}')` |
| Typed literal | `JSONB '{"a": 1}'`, `JSON '[1, 2]'` |
| Cast | `'{"a": 1}'::jsonb`, `CAST(@p AS jsonb)` |
| Builder function | `jsonb_build_object('a', 1)`, `to_jsonb(42)` |

Text that is not valid JSON fails with `InvalidJsonText` (`CADB0418`), and nothing is written. The
parser refuses what PostgreSQL refuses: a trailing comma, a leading zero (`01`), `NaN`, a lone
surrogate escape, and `\u0000` in a string.

Only text becomes a document through a cast. To make a document from another type, use `to_jsonb`:
`to_jsonb(42)`, `to_jsonb(now())`.

### Reading a document

- `CAST(doc AS string)` and `doc::string` give the document text.
- `->>` and `#>>` give the text of one part of the document.
- Casts from a scalar document, as in PostgreSQL: `('42'::jsonb)::int64`, `::float64`, `::numeric`,
  `::bool`. A document of the wrong kind fails with the PostgreSQL message, for example
  `cannot cast jsonb object to type bigint`. A cast to `int64` rounds half away from zero.

The text form is the PostgreSQL `jsonb_out` form: a space after each `:` and each `,`, object keys in
the stored order, and no other whitespace.

```sql
SELECT '{"b":1,   "a":[1,2]}'::jsonb;   -- {"a": [1, 2], "b": 1}
```

### What the stored document keeps

The stored form is canonical, as PostgreSQL `jsonb` is:

- **Key order.** Keys are sorted: the shorter key first (by UTF-8 byte length), then by bytes. This is
  the PostgreSQL order. The order of the input is not kept.
- **Duplicate keys.** The last value of a repeated key wins: `'{"a": 1, "a": 2}'` stores `{"a": 2}`.
- **Whitespace** is not kept.
- **Numbers are exact.** A number keeps all its digits and its scale: `1.50` reads back as `1.50`,
  and `123456789012345678901234567890` reads back unchanged. Exponent notation becomes plain digits:
  `1e3` reads back as `1000`. The limits are the PostgreSQL `numeric` limits: at most 131,072
  significant digits before the point and a scale of at most 16,383.
- **Strings** keep their characters. Escapes in the input are decoded, and the output escapes only
  what JSON requires.

### Comparison, sorting and grouping

`=`, `<>`, `<`, `>`, `ORDER BY`, `GROUP BY`, `DISTINCT` and joins on a `jsonb` value use the
PostgreSQL rules:

- Kinds sort `Object > Array > Boolean > Number > String > Null`.
- An object with more pairs is greater. An array with more elements is greater. Then the members
  compare in stored order.
- Numbers compare by value, so `'1.0'::jsonb = '1'::jsonb` is true, and the two values are one
  group.
- Strings compare by bytes.
- An empty top-level array sorts below every top-level scalar, `null` included. This is a PostgreSQL
  rule, copied on purpose.

A text operand compared with a document is read as a document: `doc = '{"a": 1}'`. Text that is not
valid JSON is equal to no document.

### Where `jsonb` cannot go

A `jsonb` column cannot be part of a primary key, a `UNIQUE` constraint, an ordered index or a
foreign key, because a document has no ordered key encoding. Use a GIN index (see [Indexes](#indexes)).
`array(jsonb)` is not a type: an array of documents is a document (`'[...]'::jsonb`).

---

## Operators

Each operator has the PostgreSQL meaning.

| Operator | Right operand | Result | Example |
|---|---|---|---|
| `->` | text key, or integer index | `jsonb` | `doc -> 'user'`, `doc -> 0`, `doc -> -1` |
| `->>` | text key, or integer index | `string` | `doc ->> 'kind'` |
| `#>` | path | `jsonb` | `doc #> '{user,tags,0}'` |
| `#>>` | path | `string` | `doc #>> ARRAY['user', 'id']` |
| `@>` | `jsonb` | `bool` | `doc @> '{"kind": "login"}'` |
| `<@` | `jsonb` | `bool` | `'{"kind": "login"}' <@ doc` |
| `?` | text | `bool` | `doc ? 'kind'` |
| `?\|` | text array | `bool` | `doc ?\| ARRAY['a', 'b']` |
| `?&` | text array | `bool` | `doc ?& '{a,b}'` |
| `\|\|` | `jsonb` | `jsonb` | `doc \|\| '{"seen": true}'` |
| `-` | text, text array, or integer | `jsonb` | `doc - 'tmp'`, `doc - 0`, `doc - ARRAY['a', 'b']` |
| `#-` | path | `jsonb` | `doc #- '{user,tags}'` |

Rules:

- A **path** or key list is an array of strings, `ARRAY['a', 'b']`, or the PostgreSQL text form of
  one, `'{a,b}'`. An integer step of a path selects an array element.
- A **negative index** counts from the end of an array.
- A missing key, a missing index or a path that does not exist gives SQL `NULL`, not an error.
- A **top-level scalar** acts as a one-element array for `-> 0`, `?` and `@>`, as in PostgreSQL:
  `'"a"'::jsonb ? 'a'` is true, and `'["foo"]'::jsonb @> '"foo"'` is true.
- `@>` is containment: every part of the right document is in the left document. An array contains
  another array when it contains each of its elements, in any order and with repeats.
  `'[1, 2, 3]'::jsonb @> '[3, 1]'` is true. Below the top level, a scalar does not contain an array:
  `'{"a": [1]}'::jsonb @> '{"a": 1}'` is false.
- `?` is true when the text is a key at the top level of an object, or a string element at the top
  level of an array, or the top-level string itself.
- `||` merges two objects (the right side wins on a shared key) and concatenates two arrays. A scalar
  operand becomes a one-element array.
- A text operand where a document belongs is parsed as a document: `doc @> '{"a": 1}'`,
  `doc || '{"b": 2}'`.

### Precedence

The jsonb operators bind tighter than comparison, `IS` and `IN`, and less tightly than unary minus
and `::`. So these forms parse as applications write them:

```sql
WHERE doc ->> 'deleted_at' IS NULL
WHERE doc ->> 'kind' IN ('login', 'logout')
WHERE doc -> 'user' ->> 'id' = '7'
SELECT doc -> -1, doc -> 'n'::string
```

PostgreSQL puts these operators at the level of `||`. CamusDB uses its own level, because CamusDB
binds `IS` tighter than every binary operator, and `doc ->> 'k' IS NULL` would then read as
`doc ->> ('k' IS NULL)`.

### Lexer notes

- `<@` is the contained-by operator only when no placeholder name follows it directly. `x<@p` and
  `x <@p` still mean `x < @p`. Write `x <@ @p` or `x <@'{...}'` for the operator.
- `#-` is the delete-path operator, as in PostgreSQL. **This changes old SQL:** `x #-1` was
  `x XOR -1`, and is now a path delete. Write `x # -1` (with a space) for XOR.
- `?`, `?|` and `?&` are operators. CamusDB placeholders are `@name`, so there is no conflict in
  CamusDB SQL. A driver that uses `?` for its own parameters (JDBC, ODBC) may rewrite the operator;
  use the functions `jsonb_exists(doc, key)`, `jsonb_exists_any(doc, keys)` and
  `jsonb_exists_all(doc, keys)` there.

---

## Functions

Each `jsonb_*` function also has the `json_*` spelling that PostgreSQL has, unless the table says
otherwise. Every function is strict: a SQL `NULL` argument gives SQL `NULL`.

### Build

| Function | Result |
|---|---|
| `to_jsonb(value)` (`to_json`) | The value as a document. `int64` and `numeric` stay exact; a float uses its shortest round-trip text, and `NaN` and the infinities become strings; `date` is `"yyyy-MM-dd"`; `datetime` is `"yyyy-MM-ddTHH:mm:ss[.fff]+00:00"`; `bytes` is `"\x…"`; an array becomes a JSON array; a document stays itself. |
| `jsonb_build_object(k1, v1, …)` | An object from key and value pairs. |
| `jsonb_build_array(v1, …)` | An array. |
| `jsonb_object(text_array)`, `jsonb_object(keys, values)` | An object of string values from a flat key/value array or from two arrays. |

### Inspect

| Function | Result |
|---|---|
| `jsonb_typeof(doc)` (`json_typeof`) | `object`, `array`, `string`, `number`, `boolean` or `null`. |
| `jsonb_array_length(doc)` | The element count. An object or a scalar is an error. The `json_array_length` name is the MySQL-style function (below). |
| `jsonb_extract_path(doc, step, …)` | The same as `#>`, with the steps as arguments. |
| `jsonb_extract_path_text(doc, step, …)` | The same as `#>>`. |
| `jsonb_exists(doc, key)` | The same as `?`. |
| `jsonb_exists_any(doc, keys)` | The same as `?|`. |
| `jsonb_exists_all(doc, keys)` | The same as `?&`. |

### Change

| Function | Result |
|---|---|
| `jsonb_set(doc, path, value [, create_missing])` | The value at the path replaced. With `create_missing` (default true) a missing last key is added, and an array index out of range adds at the start or the end. |
| `jsonb_set_lax(doc, path, value [, create_missing [, null_value_treatment]])` | As `jsonb_set`, with a rule for a SQL `NULL` value: `'raise_exception'`, `'use_json_null'` (default), `'delete_key'` or `'return_target'`. |
| `jsonb_insert(doc, path, value [, insert_after])` | The value inserted into an array before (or after) the index, or added at a new key. An existing key is an error. |
| `jsonb_strip_nulls(doc)` (`json_strip_nulls`) | Every object field with a JSON `null` value removed, at every level. |
| `jsonb_pretty(doc)` | The text with 4-space indents, as PostgreSQL prints it. |

A SQL `NULL` element in the path of a change function is an error, as in PostgreSQL.

### Set-returning functions (in `FROM`)

| Function | Columns |
|---|---|
| `jsonb_each(doc)` | `key string`, `value jsonb` |
| `jsonb_each_text(doc)` | `key string`, `value string` |
| `jsonb_object_keys(doc)` | one `string` column, named after the relation |
| `jsonb_array_elements(doc)` | `value jsonb` |
| `jsonb_array_elements_text(doc)` | `value string` |

```sql
SELECT key, value FROM jsonb_each('{"a": 1, "b": [2]}');
SELECT e.value FROM jsonb_array_elements(@doc) AS e WHERE e.value ->> 'ok' = 'true';
```

A column alias list renames the columns from the left: `jsonb_each(@doc) AS e(k, v)`. Object keys
come out in the stored order. A document of the wrong kind fails before the first row, with the
PostgreSQL message (for example `cannot call jsonb_each on a non-object`).

The argument is evaluated once, before the first row. It can be a literal, a parameter or an
expression without columns. A column of another relation in the same `FROM`
(`FROM t, jsonb_each(t.doc)`) is a `LATERAL` reference, and CamusDB does not support `LATERAL`.

`key` is a reserved word in CamusDB (`PRIMARY KEY`). It is accepted as a column reference in an
expression and after a dot (`SELECT key`, `e.key`), so the `jsonb_each` columns can be read by name.

### Aggregates

| Aggregate | Result |
|---|---|
| `jsonb_agg(value)` (`json_agg`) | An array of the values of the group. A SQL `NULL` value becomes JSON `null`. |
| `jsonb_object_agg(key, value)` (`json_object_agg`) | An object. A `NULL` key is an error. The last value of a repeated key wins. |

A group of zero rows gives SQL `NULL`. Both aggregates work in grouped, global and spilled plans.
`ORDER BY` inside the call (`jsonb_agg(x ORDER BY y)`) is not supported; the elements come in the
order the rows reach the aggregate.

### The MySQL-style functions

The older functions `json_valid`, `json_type`, `json_extract`, `json_value`, `json_contains` and
`json_array_length` stay as they were. They take text and use `$.a.b[0]` paths. A `jsonb` argument
is read as its text, so each name keeps one behavior.

---

## Indexes

A GIN index (a generalized inverted index) maps each part of a document to the rows that hold it:

```sql
CREATE INDEX events_payload_gin ON events USING GIN (payload);
CREATE INDEX events_payload_gin ON events USING GIN (payload jsonb_path_ops);
CREATE INVERTED INDEX events_payload_gin ON events (payload);          -- CockroachDB form

CREATE TABLE events (
    id      int64 PRIMARY KEY,
    payload jsonb,
    INVERTED INDEX events_payload_gin (payload)
);
```

A GIN index has exactly one column, which must be `jsonb`. It has no `INCLUDE` columns and no `DESC`.
`SHOW INDEXES` shows `Index_type` `GIN`. `SHOW CREATE TABLE` shows the inline `INVERTED INDEX` form.
`DROP INDEX` removes it. The operator class names `jsonb_ops` and `jsonb_path_ops` are accepted and
build the same index.

### What the index serves

The planner uses a GIN index for these top-level `WHERE` conjuncts, when the other operand is a
constant (a literal, a parameter, or an expression without columns):

| Conjunct | Index lookup |
|---|---|
| `col @> doc`, `doc <@ col` | every part of `doc` |
| `col ? key` | the key, or the string element or scalar |
| `col ?\| keys` | any of the keys |
| `col ?& keys` | all of the keys |
| `col ->> 'k' = text`, `text = col ->> 'k'` | the value at `k` as a string, or as the number or boolean with that text |
| `col -> 'k' = doc` | the same as `col @> {"k": doc}` |

The function forms (`jsonb_contains(col, doc)`, `jsonb_exists(col, key)`, …) are served the same way.
Several such conjuncts on one index are combined. The index is not used for:

- a conjunct under `OR` or `NOT`;
- `col <@ doc` (PostgreSQL GIN does not serve it either);
- `col @> '{}'`, `col @> '[]'` and `col @> <scalar>`, which match too many rows to narrow;
- `col ->> 'k' = text` when the text starts with `{` or `[`;
- a join: the GIN scan is a single-table access path.

The full predicate always stays in the filter. The index gives a set of candidate rows that holds
every matching row, and each candidate is checked against the predicate. See
[query-planner.md](query-planner.md#gin-index-scans) for the planner rules and
[storage-layout.md](storage-layout.md#gin-index-entries) for the entry keys.

### Writes

An INSERT writes one index entry for each distinct leaf of the document. A DELETE removes them. An
UPDATE writes only the entries that changed. A `NULL` document has no entries. All entries are written
in the row's own transaction, so a rollback leaves none. A document with many leaves costs one write
for each distinct entry, and counts against the transaction mutation limit
([transaction-limits.md](transaction-limits.md)): a statement over the limit fails, and the index never
leaves entries out.

`CREATE INDEX` over an existing table backfills the entries in batches, while writes continue.

### Locks

Under Serializable, a GIN scan takes a range lock over each entry range it reads. A concurrent write
of a row that would match the predicate writes an entry into one of those ranges, and so it waits or
retries ([transactions-locking-and-isolation.md](transactions-locking-and-isolation.md)). A frequent
entry, for example a key that every document has, gives a large range, and the lock then blocks more
writers.

---

## Storage

A cell is one format byte (`1`) followed by one CBOR data item (RFC 8949) in canonical form: definite
lengths, shortest encodings, and sorted unique map keys. CBOR's canonical key order is the PostgreSQL
key order. Numbers are CBOR integers, bignums or decimal fractions, never floats. The cell goes
through the normal large-value pipeline: LZ4 compression when it saves space, and out-of-line storage
for a large cell. The `STORAGE` clause applies as for `string` (default `EXTENDED`). See
[storage-layout.md](storage-layout.md#jsonb-cells).

Space measured on public corpora (the simdjson example files), with the engine's LZ4 rule:

| Corpus | Minified text | Cell | Text + LZ4 | Cell + LZ4 |
|---|---|---|---|---|
| amazon_cellphones (one row per line) | 283,837 B | 94% | 80% | 79% |
| canada.json (numbers) | 2,251,027 B | 61% | 45% | 42% |
| citm_catalog.json | 501,093 B | 68% | 5% | 4% |
| github_events (one row per event) | 53,924 B | 91% | 46% | 44% |
| twitter.json (one row per tweet) | 570,598 B | 71% | 41% | 34% |

A read of one key does not parse text: `->` returns a slice of the stored bytes.

---

## Wire forms

| Transport | Form |
|---|---|
| HTTP / REST | By default, the document itself, as native JSON in the row (an object, array, string, number, boolean or `null` token). Numbers keep all their digits. With `"jsonbAsText": true` on the request, a JSON string that holds the text form (see [JSON null over REST](#json-null-over-rest)). The column type in the schema is `13`. A parameter is `{"type": 13, "strValue": "<json text>"}`, or a plain string written to a `jsonb` column. |
| gRPC | `Value.json_value` (field 14), the text form. Column type `COLUMN_TYPE_JSONB`. See [grpc-client-protocol.md](grpc-client-protocol.md). |

A gRPC client built before `json_value` existed reads every `jsonb` cell as `NULL`, with no error.
Update the client before you read `jsonb` columns over gRPC.

### JSON null over REST

In the default REST form, a document that is JSON `null` and a SQL `NULL` cell are both the token
`null`. A client cannot tell them apart. If it writes the value back, it can store SQL `NULL` in place
of the document.

To keep the two values apart, set `"jsonbAsText": true` on the request. The field applies to
`/execute-sql-query`, `/execute-sql-query-stream` and the `RETURNING` rows of
`/execute-sql-non-query`. Each `jsonb` cell is then a JSON string that holds the text form of the
document, the same text that gRPC sends. SQL `NULL` stays the token `null`.

| Value | Default form | Text form |
|---|---|---|
| SQL `NULL` | `null` | `null` |
| JSON `null` | `null` | `"null"` |
| `"abc"` | `"abc"` | `"\"abc\""` |
| `{"a": 1}` | `{"a": 1}` | `"{\"a\": 1}"` |

The response marks each `jsonb` column that uses the text form:

```json
{"name": "payload", "type": 13, "jsonbAsText": true}
```

A client must decode a cell as text only when its column has this mark. A server older than the
field ignores it and sends the default form. A string document such as `"abc"` is valid in both
forms, so the mark is the only safe test.

---

## Limits

| Limit | Value |
|---|---|
| Nesting depth | 512 levels. A deeper document fails with `JsonNestingTooDeep` (`CADB0419`). The limit is fixed, not a setting. |
| Number size | 131,072 digits before the point, a scale of 16,383 (the PostgreSQL limits). |
| Document size | The large-value and transaction limits that apply to any cell. |

A stored cell that does not decode fails with `JsonbValueCorrupt` (`CADB0559`). It never returns a
part of a document.

---

## Differences from PostgreSQL

| Topic | PostgreSQL | CamusDB |
|---|---|---|
| `json` type | Keeps the input text: whitespace, key order, duplicate keys. | An alias of `jsonb`. The text is canonical. |
| `jsonb_path_ops` | A smaller index that serves `@>` only. | The same index as `jsonb_ops`; it serves every GIN operator. |
| Precedence | The jsonb operators are at the `||` level. | Their own level, above `IS` and `IN` (see [Precedence](#precedence)). |
| `x #-1` | Path delete. | Path delete; before `jsonb` it was XOR with `-1`. |
| A text column as a document operand | Refused: the operator needs `jsonb`. | Parsed as a document, as a literal is. |
| Text that is not JSON compared with a document | The literal is refused. | Equal to no document. |
| `jsonb_agg(x ORDER BY y)` | Supported. | Not supported. |
| `LATERAL` (`FROM t, jsonb_each(t.doc)`) | Supported. | Not supported. |
| SQL/JSON path (`jsonpath`, `@?`, `@@`, `jsonb_path_query`) | Supported. | Not supported. |
| Subscripts (`doc['key']`) | Supported (PostgreSQL 14). | Not supported. |
| `row_to_json`, `jsonb_to_record`, `jsonb_populate_record` | Supported. | Not supported. |
| Expression index on `(doc ->> 'k')` | Supported. | Not yet supported. |
