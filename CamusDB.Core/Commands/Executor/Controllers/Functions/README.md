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
| `SessionScalarFunctions` | `CURRENT_DATABASE()`, `CURRENT_USER()`, `CURRENT_ROLE()`, `IS_SUPERUSER()` — the session the statement runs in |

`ScalarFunctionDescriptor` carries the function name(s), arity constraints, and the delegate that implements the function.
`ScalarFunctionArguments` is the helper that evaluates argument expressions before passing them to the function delegate.

A function whose result comes from the session rather than from its arguments sets `SessionEvaluator` instead of relying on `Evaluator`, and reads the session snapshot the SQL entry points place in the statement's parameters. Such a function is also marked volatile so a query naming it bypasses the shared result cache, and `IsSessionScoped` keeps it from being accepted where a value must be replayed without a session (a column `DEFAULT`, a stored `CHECK`).
