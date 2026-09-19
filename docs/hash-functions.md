# Hash functions

CamusDB has four digest functions. Each one takes one `string` or `bytes` value and returns the
digest as lowercase hexadecimal text.

| Function | Digest | Result length |
|----------|--------|---------------|
| `md5(x)` | MD5 | 32 characters |
| `sha1(x)` | SHA-1 | 40 characters |
| `sha256(x)` | SHA-256 | 64 characters |
| `sha512(x)` | SHA-512 | 128 characters |

```sql
SELECT md5('abc');                      -- 900150983cd24fb0d6963f7d28e17f72
SELECT sha256(X'616263');               -- ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
SELECT substring(md5('x'), 1, 12);      -- 9dd4e461268c
```

## Rules

- A `string` is hashed as its UTF-8 bytes. The digest of a text is the same as in PostgreSQL and in
  other tools that use UTF-8.
- A `bytes` value is hashed as its raw bytes.
- `NULL` gives `NULL`.
- Any other type is an error (`InvalidInput`). To hash a number, cast it first: `md5(x::text)`.
- The functions are deterministic. You can use them in `WHERE`, in a `CHECK` constraint, and in a
  column `DEFAULT`, and the query-result cache can keep their results.

## Difference from PostgreSQL

`md5` is the same as in PostgreSQL. In PostgreSQL, `sha1`, `sha256` and `sha512` return `bytea`. In
CamusDB they return hex text, which is the same as PostgreSQL's `encode(sha256(x), 'hex')`.

The digest of `md5(random()::text)` is not the same as in PostgreSQL, because `random()` gives other
values and the float-to-text format can be different. Only the digest of the same input is the same.

## Not for security

MD5 and SHA-1 have known collision attacks. None of these functions is a password hash: they have no
salt and no work factor. Use them for checksums, change detection, bucketing, and test data.
