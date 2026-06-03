# Functions

Scalar function registry and evaluation engine used during query projection and filtering.

`ScalarFunctionRegistry` holds a case-insensitive map from function name to `ScalarFunctionDescriptor`. `ScalarFunctionEvaluator` resolves a function call AST node against the registry and invokes the matching implementation.

Built-in function groups:

| File | Functions |
|------|-----------|
| `MathScalarFunctions` | `ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POW`, `MOD`, … |
| `StringScalarFunctions` | `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `SUBSTRING`, `CONCAT`, `REPLACE`, … |
| `DateTimeScalarFunctions` | `NOW`, `DATE`, `YEAR`, `MONTH`, `DAY`, `DATEDIFF`, `DATEADD`, … |
| `CastScalarFunctions` | `CAST(x AS type)` — converts between column value types |
| `IdScalarFunctions` | `GEN_ID()` — generates a new ObjectId |
| `JsonScalarFunctions` | `JSON_EXTRACT`, `JSON_SET`, `JSON_REMOVE` — JSON field access and mutation |

`ScalarFunctionDescriptor` carries the function name(s), arity constraints, and the delegate that implements the function.
`ScalarFunctionArguments` is the helper that evaluates argument expressions before passing them to the function delegate.
