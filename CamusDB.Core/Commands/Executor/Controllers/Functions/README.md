# Functions

Scalar function registry and evaluation engine used during query projection and filtering.

`ScalarFunctionRegistry` holds a case-insensitive map from function name to `ScalarFunctionDescriptor`. `ScalarFunctionEvaluator` resolves a function call AST node against the registry and invokes the matching implementation.

Built-in function groups. Each lowercase name in the second column is a registered name or alias; `TestFunctionsReadme` fails if one of them does not resolve in `ScalarFunctionRegistry.CreateDefault()`.

| File | Functions |
|------|-----------|
| `MathScalarFunctions` | `abs`, `ceil` / `ceiling`, `floor`, `round`, `trunc`, `sqrt`, `pow` / `power`, `mod`, `sign`, `random` |
| `StringScalarFunctions` | `length`, `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `substring`, `replace`, `contains`, `starts_with`, `ends_with`, `concat` |
| `HashScalarFunctions` | `md5`, `sha1`, `sha256`, `sha512` — lowercase hex digest of a string (UTF-8) or bytes value; not for security |
| `DateTimeScalarFunctions` | `current_timestamp` / `now`, `current_date`, `date_part`, `date_trunc`, `date_add`, `date_diff`, `unix_timestamp`, `from_unixtime` |
| `DateTimeScalarFunctions` (MySQL shortcuts) | `year`, `month`, `day` / `dayofmonth`, `hour`, `minute`, `second`, `quarter`, `week`, `dayofweek`, `weekday`, `dayofyear`, `dayname`, `monthname` |
| `DateTimeScalarFunctions` (SQL standard) | `EXTRACT(field FROM x)` — the parser turns it into a call to `extract`, an alias of `date_part` |
| `CastScalarFunctions` | `CAST(x AS type)` and `x::type` (grammar forms); `to_string`, `to_int64`, `to_float64`, `to_float32`, `to_bool`, `to_date`, `to_datetime`, `to_bytes`, `to_id` / `str_id` |
| `IdScalarFunctions` | `gen_id` — generates a new ObjectId |
| `UuidScalarFunctions` | `gen_uuid_v4`, `gen_uuid_v7` |
| `NullScalarFunctions` | `coalesce`, `ifnull` / `nvl` |
| `JsonScalarFunctions` | `json_valid`, `json_type`, `json_extract`, `json_value`, `json_contains`, `json_array_length` |
| `RegexScalarFunctions` | `regexp_like`, `regexp_match`, `regexp_matches`, `regexp_replace`, `regexp_count`, `regexp_instr`, `regexp_substr`, `regexp_split_to_array`, `regexp_split_to_table` |
| `VectorScalarFunctions` | `vector_dims`, `octet_length`, `l2_distance`, `inner_product`, `cosine_distance` |
| `SequenceScalarFunctions` | `nextval`, `currval`, `lastval`, `setval` |
| `SessionScalarFunctions` | `current_database`, `current_user`, `current_role`, `is_superuser` — the session the statement runs in |
| `ArrayScalarFunctions` | `cardinality`, `array_length`, `array_contains` — an array read as a whole, for length and membership checks |
| `QuantifierScalarFunctions` | `any`, `some`, `all` — stubs that reject a call; the parser rewrites `x = ANY (a)` and `x <> ALL (a)` before evaluation |

The date units (`'week'`, `'quarter'`, `'dow'`, `'epoch'`, ...) and the functions that accept each one are listed in `docs/data-types.md`, section "Temporal functions".

`ScalarFunctionDescriptor` carries the function name(s), arity constraints, and the delegate that implements the function.
`ScalarFunctionArguments` is the helper that evaluates argument expressions before passing them to the function delegate.

A function whose result comes from the session rather than from its arguments sets `SessionEvaluator` instead of relying on `Evaluator`, and reads the session snapshot the SQL entry points place in the statement's parameters. Such a function is also marked volatile so a query naming it bypasses the shared result cache, and `IsSessionScoped` keeps it from being accepted where a value must be replayed without a session (a column `DEFAULT`, a stored `CHECK`).
