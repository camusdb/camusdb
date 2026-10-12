# Bytes functions

These functions read, change and convert `bytes` values. The names and the rules follow PostgreSQL.
Some MySQL names are also available. The digest functions (`md5`, `sha256`, `digest`, `hmac`,
`crc32`) are in [hash-functions.md](hash-functions.md).

All functions give `NULL` when an argument is `NULL`. A wrong argument type, a bad format name, bad
text and an offset out of range are errors with the code `InvalidInput`. No function gives a partial
result.

A bytes literal is written `X'DEADBEEF'`. See [data-types.md](data-types.md) for the literal and text
forms of `bytes`.

## Convert between bytes and text

| Function | Result | Notes |
| --- | --- | --- |
| `encode(bytes, format)` | `string` | `format` is `'hex'`, `'base64'` or `'escape'`, in any case. |
| `decode(string, format)` | `bytes` | The reverse of `encode`. `decode(encode(b, f), f) = b` for each format. |
| `hex(x)` | `string` | MySQL. Uppercase hex of `bytes`, of the UTF-8 bytes of a `string`, or of an `int64`. |
| `unhex(string)` | `bytes` | MySQL. Hex digits in either case. |
| `to_base64(x)` | `string` | MySQL. Base64 of `bytes`, or of the UTF-8 bytes of a `string`. |
| `from_base64(string)` | `bytes` | MySQL. |
| `convert_to(string, 'UTF8')` | `bytes` | The UTF-8 bytes of the text. |
| `convert_from(bytes, 'UTF8')` | `string` | The text of UTF-8 bytes. |

```sql
SELECT encode(X'DEADBEEF', 'hex');           -- deadbeef
SELECT encode(X'616263', 'base64');          -- YWJj
SELECT encode(X'00415C80', 'escape');        -- \000A\\\200
SELECT decode('YWJj', 'base64');             -- X'616263'
SELECT hex('abc'), hex(255), hex(-1);        -- 616263, FF, FFFFFFFFFFFFFFFF
SELECT convert_from(X'C3A9', 'UTF8');        -- é
```

### The formats

- **`hex`**. `encode` writes lowercase digits. `decode` reads digits in either case. White space
  between two digit pairs is ignored, as in PostgreSQL. An odd number of digits is an error.
- **`base64`**. The RFC 4648 alphabet with `=` padding. `decode` ignores white space, so text with
  line breaks decodes.
- **`escape`**. A zero byte and each byte from `0x80` to `0xFF` become a three-digit octal escape
  (`\000`, `\377`). A backslash becomes two backslashes. Each other byte is its ASCII character.
  `decode` reads `\\` as one backslash and `\nnn` as one byte. Any other backslash is an error. A
  character that is not ASCII is read as its UTF-8 bytes.

### UTF-8 only

`convert_to` and `convert_from` accept the encoding names `UTF8` and `UTF-8`, in any case. Other
encodings are an error, because CamusDB keeps all text as UTF-8. `convert_from` rejects bytes that are
not valid UTF-8. It does not put the replacement character U+FFFD in their place.

### Differences from PostgreSQL and MySQL

- `encode(x, 'base64')` adds no line break. PostgreSQL adds a line break after each 76 characters.
  Text without line breaks is easier to compare and to store, and `decode` accepts both forms.
- `unhex` and `from_base64` give an error for bad text. MySQL gives `NULL`.
- `unhex` does not accept an odd number of digits. MySQL adds a leading zero.
- `to_base64` adds no line break. MySQL adds a line break after each 76 characters.

## Text functions that also take bytes

These functions take `string` or `bytes`. For `bytes` they count bytes. When a function takes two
values, both must have the same type: `contains(bytes, string)` is an error. To convert, use
`convert_to` or `convert_from`.

| Function | Result for bytes | Notes |
| --- | --- | --- |
| `length(x)` | `int64` | The number of bytes. The same as `octet_length`. |
| `substring(x, start [, count])` | `bytes` | `start` is 1-based. |
| `strpos(x, part)` | `int64` | The 1-based position of the first `part` in `x`, or `0`. An empty `part` gives `1`. |
| `position(part IN x)` | `int64` | The SQL-standard form of `strpos(x, part)`. |
| `trim(x [, set])`, `ltrim`, `rtrim` | `bytes` | Without `set`, remove `0x00` bytes. With `set`, remove each byte in `set`. |
| `starts_with(x, part)`, `ends_with`, `contains` | `bool` | |

```sql
SELECT substring(X'0102030405', 2, 3);       -- X'020304'
SELECT position(X'03' IN X'010203');          -- 3
SELECT trim(X'0000410000');                   -- X'41'
SELECT trim(X'0102030201', X'0102');          -- X'03'
SELECT trim('xyaxy', 'yx');                   -- a
```

`substring` and the trims return the type of their first argument. The query planner knows the result
type before it runs the query.

The trims also take a set for a `string` (PostgreSQL `btrim(text, text)`). An empty set removes
nothing. Without a set, a `string` loses its white space, as before.

### Limits of `position(part IN x)`

`IN` binds more tightly than the arithmetic operators. Put parentheses around a `part` that uses an
operator: `position((a + b) IN x)`. The value `x` can be a column, a literal, a parameter, a function
call, a `CAST` or a `CASE`. It cannot start with a parenthesis, because `IN (` starts an `IN` list.
For any other expression, use `strpos(x, part)`.

## Read and change bytes and bits

| Function | Result | Notes |
| --- | --- | --- |
| `get_byte(bytes, n)` | `int64` | The byte at offset `n`, from 0 to 255. |
| `set_byte(bytes, n, value)` | `bytes` | A copy with the byte at offset `n` changed. `value` is from 0 to 255. |
| `get_bit(bytes, n)` | `int64` | The bit at offset `n`, `0` or `1`. |
| `set_bit(bytes, n, value)` | `bytes` | A copy with the bit at offset `n` changed. `value` is `0` or `1`. |
| `bit_length(x)` | `int64` | Eight times the number of bytes of `bytes`, or of the UTF-8 form of a `string`. |
| `bit_count(bytes)` | `int64` | The number of bits that are `1`. |

Offsets are 0-based, as in PostgreSQL. Bit `n` is bit `n % 8` of byte `n / 8`. Bit 0 of a byte is
its least significant bit. So bit 15 is the most significant bit of the second byte.

```sql
SELECT get_byte(X'0AFF', 1);                  -- 255
SELECT set_byte(X'0000', 1, 255);             -- X'00FF'
SELECT get_bit(X'0080', 15);                  -- 1
SELECT set_bit(X'FF', 7, 0);                  -- X'7F'
SELECT bit_count(X'FF0F');                    -- 12
```

An offset below 0 or past the last byte (or bit) is an error. An empty value has no offsets, so each
offset is an error.

`set_byte` and `set_bit` do not change their input. To change a stored value, write the result back:
`UPDATE t SET b = set_byte(b, 0, 1) WHERE id = 1`.

## Random bytes

`gen_random_bytes(n)` returns `n` bytes from the cryptographic random number generator. `n` is from 1
to 1024, the same limit as PostgreSQL's pgcrypto.

The function is volatile. Each call gives new bytes, and a query that uses it does not use the
query-result cache. You cannot use it as a column `DEFAULT`, because a per-row default must be a call
with no arguments, such as `gen_uuid_v4()`.

## Sizes and large values

A result can be as large as its input. When you write a result to a `bytes(N)` column, the length
limit `N` applies, the same as for any other value. A value that is too long is an error
(`ValueTooLong`).

The functions read a column that is stored out of line (`STORAGE EXTERNAL`, see
[storage-layout.md](storage-layout.md)) the same as any other column.
