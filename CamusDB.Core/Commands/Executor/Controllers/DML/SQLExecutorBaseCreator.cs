
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal abstract class SQLExecutorBaseCreator
{
    /// <summary>
    /// Per-string-literal cache of its decoded UTF-8 bytes, so the byte-native string-equality fast path
    /// encodes each literal at most once per query rather than once per scanned row. Keyed by the literal
    /// AST node's identity via a weak table, so a parsed query being GC'd drops its cached bytes too.
    /// </summary>
    private static readonly ConditionalWeakTable<NodeAst, byte[]> Utf8LiteralCache = new();

    /// <summary>
    /// Per-literal-node cache of the evaluated <see cref="ColumnValue"/>, so a literal in a WHERE or
    /// projection expression is parsed and allocated once per query instead of once per scanned row —
    /// a filter like <c>status = 'active' AND age &gt; 30</c> would otherwise decode a string and
    /// allocate two <see cref="ColumnValue"/>s for every row. Safe because literal AST nodes are
    /// immutable and their value is a pure function of <c>yytext</c>, and <see cref="ColumnValue"/> is
    /// immutable so one instance can back every row. Keyed weakly like <see cref="Utf8LiteralCache"/>
    /// so a parsed query being GC'd drops its cached values. Only successful evaluations are cached —
    /// a malformed literal keeps throwing on every evaluation, unchanged. An array literal is cached
    /// only when every element is itself a constant literal (<see cref="IsConstantArrayElementList"/>):
    /// an element may otherwise reference a row column, or a bind placeholder whose value changes
    /// between executions of the same cached tree.
    /// </summary>
    private static readonly ConditionalWeakTable<NodeAst, ColumnValue> LiteralValueCache = new();

    private static ColumnValue CacheLiteralValue(NodeAst expr, ColumnValue value)
    {
        // Thread-safe under concurrent executions of a shared cached AST; a benign race stores
        // whichever identical instance wins last.
        LiteralValueCache.AddOrUpdate(expr, value);
        return value;
    }

    protected static void GetIdentifierList(NodeAst orderByAst, List<string> identifierList)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
        {
            identifierList.Add(orderByAst.yytext ?? "");
            return;
        }

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {            
            if (orderByAst.leftAst is not null)
                GetIdentifierList(orderByAst.leftAst, identifierList);

            if (orderByAst.rightAst is not null)
                GetIdentifierList(orderByAst.rightAst, identifierList);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }

    /// <summary>
    /// Evaluates an expression AST against a row, resolving column identifiers via the
    /// <paramref name="row"/> dictionary. When <paramref name="queryRow"/> is non-null the
    /// <see cref="NodeType.Identifier"/> branch bypasses the <see cref="IReadOnlyDictionary{TKey,TValue}"/>
    /// adapter and reads <see cref="QueryRow.Values"/> by ordinal through
    /// <see cref="RowLayout.IndexOf"/>, avoiding virtual dispatch on the interface call.
    /// All other expression types are unaffected — only the column-reference lookup differs
    /// between the two paths, keeping a single shared implementation body.
    /// </summary>
    /// <summary>
    /// Evaluates one comparison operator with SQL three-valued logic: a NULL operand makes the
    /// result UNKNOWN (a Null-typed value), never true or false. Without this guard the operator
    /// would fall through to <see cref="CompareValues"/>, whose NULL rule is an <em>ordering</em>
    /// rule (NULL sorts first), so <c>NULL &lt; 10</c> and <c>NULL != 10</c> evaluated to true and a
    /// <c>WHERE b &lt; 10</c> returned the NULL rows. The WHERE filter treats UNKNOWN as "row
    /// excluded"; <c>NOT</c>, <c>AND</c>, <c>OR</c> and <c>CASE</c> propagate it. CHECK constraints
    /// (<see cref="CheckEvaluator"/>) apply the same rule on their own path.
    /// </summary>
    internal static ColumnValue EvalComparison(NodeType op, ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        int cmp = CompareValues(left, right);

        return op switch
        {
            NodeType.ExprEquals => ColumnValue.FromBool(cmp == 0),
            NodeType.ExprNotEquals => ColumnValue.FromBool(cmp != 0),
            NodeType.ExprLessThan => ColumnValue.FromBool(cmp < 0),
            NodeType.ExprGreaterThan => ColumnValue.FromBool(cmp > 0),
            NodeType.ExprLessEqualsThan => ColumnValue.FromBool(cmp <= 0),
            NodeType.ExprGreaterEqualsThan => ColumnValue.FromBool(cmp >= 0),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Not a comparison operator: {op}"),
        };
    }

    /// <summary>
    /// Three-valued AND: false if either side is false, UNKNOWN if either side is UNKNOWN, else true.
    /// Shared with <see cref="Queries.QueryFilterer"/> so the async predicate walker and this
    /// evaluator never disagree on a NULL operand. A non-boolean, non-NULL operand is a user error.
    /// </summary>
    internal static ColumnValue EvalAnd(ColumnValue left, ColumnValue right)
    {
        RequireBooleanOrNull(left, right, "AND");

        if (left.Type == ColumnType.Bool && !left.BoolValue)
            return ColumnValue.False;

        if (right.Type == ColumnType.Bool && !right.BoolValue)
            return ColumnValue.False;

        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        return ColumnValue.True;
    }

    /// <summary>
    /// Three-valued OR: true if either side is true, UNKNOWN if either side is UNKNOWN, else false.
    /// See <see cref="EvalAnd"/>.
    /// </summary>
    internal static ColumnValue EvalOr(ColumnValue left, ColumnValue right)
    {
        RequireBooleanOrNull(left, right, "OR");

        if (left.Type == ColumnType.Bool && left.BoolValue)
            return ColumnValue.True;

        if (right.Type == ColumnType.Bool && right.BoolValue)
            return ColumnValue.True;

        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        return ColumnValue.False;
    }

    private static void RequireBooleanOrNull(ColumnValue left, ColumnValue right, string op)
    {
        if (left.Type is not (ColumnType.Bool or ColumnType.Null) || right.Type is not (ColumnType.Bool or ColumnType.Null))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator {op} for argument types: {left.Type}, {right.Type}");
    }

    /// <summary>
    /// Orders two values for a filter/predicate comparison, reconciling a bare string literal against
    /// a <see cref="ColumnType.Uuid"/> or <see cref="ColumnType.Id"/> operand by parsing/normalizing
    /// the string to that type. This lets <c>WHERE uuid_col = '…'</c> / <c>WHERE id = '…'</c> (and
    /// range comparisons) work without an explicit CAST, and — for Id — normalizes the literal to the
    /// canonical lowercase 24-hex form so it matches the stored value regardless of input casing.
    /// </summary>
    private static int CompareValues(ColumnValue left, ColumnValue right)
    {
        // Mixed integer/float operands (e.g. `price > 0` where price is Float64 and 0 is an integer
        // literal) compare numerically by widening both to double; ColumnValue.CompareTo rejects the
        // cross-type comparison. One shared rule, so IN membership, CHECK constraints and the
        // planner's index bounds all agree with this comparison.
        if (MixedNumericComparison.TryCompare(left, right, out int numericCmp))
            return numericCmp;

        // Coercion is best-effort. If a String operand is not a valid Uuid/Id it can equal no such
        // value, so return a deterministic non-zero ordering rather than throwing — and never call
        // ColumnValue.CompareTo with mismatched types (it throws on Uuid/Id vs String).
        try
        {
            if (left.Type == ColumnType.Uuid && right.Type == ColumnType.String)
                right = ColumnValue.FromUuidString(right.StrValue!);
            else if (right.Type == ColumnType.Uuid && left.Type == ColumnType.String)
                left = ColumnValue.FromUuidString(left.StrValue!);
            else if (left.Type == ColumnType.Id && right.Type == ColumnType.String)
                right = CastScalarFunctions.CoerceToColumnType(right, ColumnType.Id);
            else if (right.Type == ColumnType.Id && left.Type == ColumnType.String)
                left = CastScalarFunctions.CoerceToColumnType(left, ColumnType.Id);
        }
        catch (CamusDBException)
        {
            // Malformed Uuid/Id literal: unequal to any real value. Order the String operand first.
            return left.Type == ColumnType.String ? -1 : 1;
        }

        return left.CompareTo(right);
    }

    private static bool IsNumeric(ColumnType type) => MixedNumericComparison.IsNumeric(type);

    /// <summary>
    /// Attempts the byte-native equality fast path for an <c>=</c>/<c>&lt;&gt;</c> node of the shape
    /// <c>stringColumn = 'literal'</c> (either operand order). Succeeds only when one operand is a plain
    /// column identifier resolving to a non-NULL <see cref="ColumnType.String"/> cell that
    /// <paramref name="queryRow"/> can hand back as a borrowed UTF-8 slice, and the other is a string
    /// literal. On success <paramref name="equal"/> is whether the two UTF-8 byte sequences match — which
    /// is identical to <see cref="CompareValues"/> reporting 0 for two strings — and no managed
    /// <see cref="string"/> is materialized for the row's cell. Returns <see langword="false"/> for every
    /// other shape (non-string column, NULL, both-identifier, non-string literal, eager/slot row), and the
    /// caller falls back to the ordinary <see cref="ColumnValue"/> comparison.
    /// </summary>
    private static bool TryUtf8StringEquality(NodeAst expr, QueryRowNameResolver? rowNameResolver, QueryRow queryRow, out bool equal)
    {
        equal = false;

        NodeAst left = expr.leftAst!;
        NodeAst right = expr.rightAst!;

        NodeAst? identifier = null, literal = null;
        if (left.nodeType == NodeType.Identifier && right.nodeType == NodeType.String) { identifier = left; literal = right; }
        else if (right.nodeType == NodeType.Identifier && left.nodeType == NodeType.String) { identifier = right; literal = left; }

        if (identifier is null || literal is null)
            return false;

        string lookupKey = rowNameResolver?.ResolveRowLookupKey(identifier.yytext!) ?? identifier.yytext!;
        int ordinal = queryRow.Layout.IndexOf(lookupKey);
        if (ordinal < 0)
            return false;

        if (!queryRow.TryGetUtf8(ordinal, out ReadOnlySpan<byte> rowUtf8))
            return false;

        byte[] literalUtf8 = Utf8LiteralCache.GetValue(literal, static node => Encoding.UTF8.GetBytes(UnquoteStringLiteral(node.yytext!)));
        equal = rowUtf8.SequenceEqual(literalUtf8);
        return true;
    }

    /// <summary>
    /// Applies <c>+ - * /</c> to two numeric operands using SQL-style numeric promotion: the wider
    /// operand type wins, so <c>Integer64 op Integer64</c> is the only combination that stays integral
    /// (and division there truncates), any <see cref="ColumnType.Float64"/> operand promotes both sides
    /// to double, and a <see cref="ColumnType.Float32"/> operand mixed with Integer64 yields Float32.
    /// Float32 results are rounded through <c>float</c> before being carried in the double-backed
    /// <see cref="ColumnValue.FloatValue"/>, so a Float32 expression never reports precision the type
    /// cannot hold. Non-numeric operands are rejected rather than silently coerced — mixed-type
    /// comparison lives in <see cref="CompareValues"/>, which has different (ordering) semantics.
    /// Division by a zero of any type raises an error instead of yielding IEEE infinity/NaN.
    /// </summary>
    private static ColumnValue EvalArithmetic(NodeType op, ColumnValue left, ColumnValue right)
    {
        if (!IsNumeric(left.Type) || !IsNumeric(right.Type))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {ArithmeticSymbol(op)} for argument types: {left.Type}, {right.Type}"
            );

        if (left.Type == ColumnType.Integer64 && right.Type == ColumnType.Integer64)
        {
            long l = left.LongValue, r = right.LongValue;

            if (op == NodeType.ExprDiv && r == 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");

            long integerResult = op switch
            {
                NodeType.ExprAdd => l + r,
                NodeType.ExprSub => l - r,
                NodeType.ExprMult => l * r,
                _ => l / r
            };

            return new ColumnValue(ColumnType.Integer64, integerResult);
        }

        // A Float64 operand forces double precision; otherwise a Float32 operand (possibly against an
        // Integer64) keeps the narrower single-precision result type.
        ColumnType resultType = left.Type == ColumnType.Float64 || right.Type == ColumnType.Float64
            ? ColumnType.Float64
            : ColumnType.Float32;

        double dl = left.Type == ColumnType.Integer64 ? left.LongValue : left.FloatValue;
        double dr = right.Type == ColumnType.Integer64 ? right.LongValue : right.FloatValue;

        if (op == NodeType.ExprDiv && dr == 0.0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");

        double result = op switch
        {
            NodeType.ExprAdd => dl + dr,
            NodeType.ExprSub => dl - dr,
            NodeType.ExprMult => dl * dr,
            _ => dl / dr
        };

        return new ColumnValue(resultType, resultType == ColumnType.Float32 ? (float)result : result);
    }

    private static string ArithmeticSymbol(NodeType op) => op switch
    {
        NodeType.ExprAdd => "+",
        NodeType.ExprSub => "-",
        NodeType.ExprMult => "*",
        _ => "/"
    };

    /// <summary>
    /// Decodes a string-literal token (<see cref="NodeType.String"/> <c>yytext</c>) into its value.
    /// Delegates to <see cref="SqlStringLiteral.Decode"/>, which owns the dialect's escape rules and
    /// is the exact inverse of <see cref="SqlStringLiteral.Quote"/>.
    /// </summary>
    internal static string UnquoteStringLiteral(string raw) => SqlStringLiteral.Decode(raw);

    /// <summary>
    /// Evaluates an <c>ARRAY[…]</c> literal.
    ///
    /// <para>The element type is inferred from the first non-NULL element, and
    /// <see cref="ColumnValue.FromArray"/> then rejects any element that disagrees — a mixed-type
    /// list has no single element type and would otherwise be stored under whichever type happened to
    /// come first. An empty <c>ARRAY[]</c> carries <see cref="ColumnType.Null"/> as its element type,
    /// meaning "not yet known"; the coercion applied against the target column adopts the declared
    /// element type. Nested arrays are rejected because <see cref="ColumnValue"/> models exactly one
    /// element type and cannot represent them.</para>
    /// </summary>
    private static ColumnValue EvalArrayLiteral(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        if (LiteralValueCache.TryGetValue(expr, out ColumnValue? cached))
            return cached;

        List<ColumnValue> elements = new();
        CollectArrayElements(expr.leftAst, row, parameters, rowNameResolver, queryRow, elements);

        ColumnType elementType = ColumnType.Null;

        foreach (ColumnValue element in elements)
        {
            if (element.Type == ColumnType.Array)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Nested arrays are not supported; an array element must be a scalar or NULL");

            if (elementType == ColumnType.Null && element.Type != ColumnType.Null)
                elementType = element.Type;
        }

        ColumnValue array = ColumnValue.FromArray(elementType, elements);

        // A constant list such as ARRAY['paid', 'shipped'] is otherwise rebuilt for every row it
        // is evaluated against.
        return IsConstantArrayElementList(expr.leftAst) ? CacheLiteralValue(expr, array) : array;
    }

    /// <summary>
    /// True when an array literal's element list holds only constant literals, so its value is the
    /// same for every row and every execution. Placeholders are not constant: a cached statement tree
    /// is shared by executions that bind different values.
    /// </summary>
    private static bool IsConstantArrayElementList(NodeAst? node)
    {
        while (node is not null && node.nodeType == NodeType.ExprList)
        {
            if (!IsConstantArrayElementList(node.rightAst))
                return false;

            node = node.leftAst;
        }

        return node is null || node.nodeType is
            NodeType.Integer or NodeType.Float or NodeType.String or NodeType.Bool or
            NodeType.Null or NodeType.BytesLiteral or NodeType.ObjectIdLiteral;
    }

    /// <summary>
    /// Flattens the left-leaning <see cref="NodeType.ExprList"/> tree the grammar builds for an array
    /// literal into evaluated elements, in source order. An element is any expression, so each one
    /// is evaluated with the caller's row-name resolver and row: without them a qualified column
    /// (<c>t.n</c>) or a column of a joined row would not resolve inside the brackets, although it
    /// resolves one level up.
    /// </summary>
    private static void CollectArrayElements(
        NodeAst? node,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow,
        List<ColumnValue> elements)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.ExprList)
        {
            CollectArrayElements(node.leftAst, row, parameters, rowNameResolver, queryRow, elements);
            CollectArrayElements(node.rightAst, row, parameters, rowNameResolver, queryRow, elements);
            return;
        }

        elements.Add(EvalExpr(node, row, parameters, rowNameResolver, queryRow));
    }

    /// <summary>
    /// Evaluates one expression node against a row. This is the per-row interpreter: every WHERE
    /// term, projection, UPDATE assignment and CHECK expression reaches it once per node per row.
    ///
    /// <para><b>Keep this method a thin dispatcher.</b> It recurses once per level of expression
    /// nesting, and the JIT gives a method one frame big enough for the locals of <em>every</em> case
    /// in it. When the case bodies lived here the frame was about 1.7 KB, so <c>a + 0 + 0 …</c> with
    /// fewer than 900 terms — a 3 KiB statement — overflowed the stack, and a stack overflow ends the
    /// process. Each case therefore calls a helper that owns its locals, its string formatting and its
    /// throw, and a new case must follow the same pattern. The expression depth the whole engine
    /// accepts is bounded by <see cref="StatementDepthGuard"/>, whose limit assumes this frame stays
    /// small.</para>
    /// </summary>
    public static ColumnValue EvalExpr(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver = null,
        QueryRow? queryRow = null)
    {
        switch (expr.nodeType)
        {
            case NodeType.Integer:
                return EvalIntegerLiteral(expr);

            case NodeType.Float:
                return EvalFloatLiteral(expr);

            case NodeType.String:
                return EvalStringLiteral(expr);

            case NodeType.BytesLiteral:
                return EvalBytesLiteral(expr);

            case NodeType.ArrayLiteral:
                return EvalArrayLiteral(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.Bool:
                return EvalBoolLiteral(expr);

            case NodeType.Null:
                return ColumnValue.Null;

            case NodeType.ObjectIdLiteral:
                return EvalObjectIdLiteral(expr);

            case NodeType.Identifier:
                return EvalIdentifier(expr, row, rowNameResolver, queryRow);

            case NodeType.Placeholder:
                return EvalPlaceholder(expr, parameters);

            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprLessThan:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprGreaterEqualsThan:
                return EvalComparisonNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprBetween:
                return EvalBetweenNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprOr:
            case NodeType.ExprAnd:
                return EvalLogicalNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprNot:
                return EvalNotNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprAdd:
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprDiv:
                return EvalArithmeticNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprFuncCall:
                // Called directly rather than through a helper: a nested call re-enters this method
                // once per level, so every frame on that cycle is paid once per level. The static
                // lambda pins the 4-param (IReadOnlyDictionary) overload because the optional
                // queryRow param makes the method group ambiguous for delegate conversion. Column refs
                // inside function arguments therefore go through the IReadOnlyDictionary adapter
                // rather than the ordinal fast path. This is correctness-neutral (QueryRow implements
                // the interface) but leaves a small per-argument hashing cost on the table. Reaching
                // the ordinal path here would mean adding queryRow to EvaluateExpressionDelegate,
                // which changes the signature every scalar function implements — a wider refactor
                // than the saving justifies on its own.
                //
                // A nested call is the costliest level of this recursion (the evaluator, the argument
                // list and the delegate each add a frame), so it checks its stack headroom: a call is
                // already expensive, and the check turns an overflow into a statement error.
                StatementDepthGuard.EnsureStackHeadroom();
                return ScalarFunctionEvaluator.Evaluate(expr, row, parameters, rowNameResolver,
                    static (e, r, p, rnr) => EvalExpr(e, r, p, rnr));

            case NodeType.ExprCast:
                return EvalCastNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprSubscript:
                return EvalSubscriptNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprCase:
                return EvalCaseNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprIsNull:
            case NodeType.ExprIsNotNull:
            case NodeType.ExprIsTrue:
            case NodeType.ExprIsNotTrue:
            case NodeType.ExprIsFalse:
            case NodeType.ExprIsNotFalse:
                return EvalTruthTestNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprLike:
            case NodeType.ExprILike:
            case NodeType.ExprRegexMatch:
            case NodeType.ExprRegexMatchCi:
            case NodeType.ExprRegexNotMatch:
            case NodeType.ExprRegexNotMatchCi:
                return EvalPatternMatchNode(expr, row, parameters, rowNameResolver, queryRow);

            case NodeType.ExprInMembership:
            case NodeType.ExprNotInMembership:
                return EvalMembershipNode(expr, row, parameters, rowNameResolver, queryRow);

            default:
                throw UnevaluableExpression(expr.nodeType);
        }
    }

    private static ColumnValue EvalIntegerLiteral(NodeAst expr)
    {
        if (LiteralValueCache.TryGetValue(expr, out ColumnValue? cached))
            return cached;

        if (!long.TryParse(expr.yytext!, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Int64: " + expr.yytext!);

        return CacheLiteralValue(expr, new ColumnValue(ColumnType.Integer64, longValue));
    }

    private static ColumnValue EvalFloatLiteral(NodeAst expr)
    {
        if (LiteralValueCache.TryGetValue(expr, out ColumnValue? cached))
            return cached;

        if (!double.TryParse(expr.yytext!, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleValue))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Float64: " + expr.yytext!);

        return CacheLiteralValue(expr, new ColumnValue(ColumnType.Float64, doubleValue));
    }

    private static ColumnValue EvalStringLiteral(NodeAst expr)
    {
        if (LiteralValueCache.TryGetValue(expr, out ColumnValue? cached))
            return cached;

        return CacheLiteralValue(expr, new ColumnValue(ColumnType.String, UnquoteStringLiteral(expr.yytext!)));
    }

    private static ColumnValue EvalBytesLiteral(NodeAst expr)
    {
        // The cached instance's byte[] is shared across rows; consumers treat ColumnValue
        // payloads as read-only (encode paths copy bytes out), same as the string case.
        if (LiteralValueCache.TryGetValue(expr, out ColumnValue? cached))
            return cached;

        return CacheLiteralValue(expr, new ColumnValue(SqlStringLiteral.DecodeBytes(expr.yytext!)));
    }

    private static ColumnValue EvalBoolLiteral(NodeAst expr)
    {
        if (!bool.TryParse(expr.yytext!, out bool boolValue))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid Bool: " + expr.yytext!);

        return ColumnValue.FromBool(boolValue);
    }

    private static ColumnValue EvalObjectIdLiteral(NodeAst expr)
    {
        if (LiteralValueCache.TryGetValue(expr, out ColumnValue? cached))
            return cached;

        if (string.IsNullOrEmpty(expr.yytext))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid ObjectId literal");

        return CacheLiteralValue(expr, new ColumnValue(ColumnType.Id, expr.yytext));
    }

    private static ColumnValue EvalIdentifier(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        string lookupKey = rowNameResolver?.ResolveRowLookupKey(expr.yytext!) ?? expr.yytext!;

        // Ordinal fast path: when the caller supplies a QueryRow directly, bypass the
        // IReadOnlyDictionary adapter and read the cell by ordinal without virtual dispatch.
        // Uses GetColumnValue (per-cell), not Values, so evaluating a predicate against a
        // slot-backed row materializes only the columns the expression references — a row
        // rejected by a WHERE clause never materializes its projection cells.
        // Falls through to the dictionary path for any key not found in the layout
        // (e.g. a parameter alias or a column absent from this row's schema version).
        if (queryRow is not null)
        {
            int ordinal = queryRow.Layout.IndexOf(lookupKey);
            if (ordinal >= 0)
                return queryRow.GetColumnValue(ordinal);
        }

        if (row.TryGetValue(lookupKey, out ColumnValue? columnValue))
            return columnValue;

        throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, "Unknown column: " + expr.yytext!);
    }

    private static ColumnValue EvalPlaceholder(NodeAst expr, Dictionary<string, ColumnValue>? parameters)
    {
        if (parameters is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing placeholders to replace: " + expr.yytext!);

        if (parameters.TryGetValue(expr.yytext!, out ColumnValue? columnValue))
            return columnValue;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown placeholder: " + expr.yytext!);
    }

    private static ColumnValue EvalComparisonNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        // Byte-native fast path for `stringColumn = 'literal'` against a borrowed row: compares
        // the row's stored UTF-8 slice to the literal's UTF-8 bytes without materializing the
        // row's string. Byte equality is exactly string equality, so this matches CompareValues.
        if (expr.nodeType is NodeType.ExprEquals or NodeType.ExprNotEquals
            && queryRow is { IsBorrowedBacked: true }
            && TryUtf8StringEquality(expr, rowNameResolver, queryRow, out bool eq))
        {
            return ColumnValue.FromBool(expr.nodeType == NodeType.ExprEquals ? eq : !eq);
        }

        ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

        return EvalComparison(expr.nodeType, leftValue, rightValue);
    }

    private static ColumnValue EvalBetweenNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue subject = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        ColumnValue low = EvalExpr(expr.extendedOne!, row, parameters, rowNameResolver, queryRow);
        ColumnValue high = EvalExpr(expr.extendedTwo!, row, parameters, rowNameResolver, queryRow);

        if (subject.Type == ColumnType.Null || low.Type == ColumnType.Null || high.Type == ColumnType.Null)
            return ColumnValue.False;

        return ColumnValue.FromBool(
            CompareValues(subject, low) >= 0 && CompareValues(subject, high) <= 0);
    }

    private static ColumnValue EvalLogicalNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

        return expr.nodeType == NodeType.ExprOr
            ? EvalOr(leftValue, rightValue)
            : EvalAnd(leftValue, rightValue);
    }

    private static ColumnValue EvalNotNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue value = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

        // Three-valued logic: NOT NULL is NULL (unknown), which the predicate filter
        // treats as non-matching.
        if (value.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (value.Type != ColumnType.Bool)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"No matching signature for operator NOT for argument type: {value.Type}");

        return ColumnValue.FromBool(!value.BoolValue);
    }

    private static ColumnValue EvalArithmeticNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

        return EvalArithmetic(expr.nodeType, leftValue, rightValue);
    }

    private static ColumnValue EvalCastNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue input = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        return CastScalarFunctions.CastExpression("cast", input, expr.rightAst!);
    }

    /// <summary>
    /// Evaluates <c>array[index]</c> with PostgreSQL's rules: the index counts from 1, and an index
    /// outside the array gives NULL rather than an error, as does a NULL array or a NULL index. Only
    /// the operand types are errors — subscripting a non-array, or indexing with a non-integer — since
    /// those are mistakes in the statement, not properties of one row's data.
    /// </summary>
    private static ColumnValue EvalSubscriptNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue array = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        ColumnValue index = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

        if (array.Type != ColumnType.Array && array.Type != ColumnType.Null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Cannot subscript a value of type {array.Type}; only an array can be subscripted");

        if (index.Type != ColumnType.Integer64 && index.Type != ColumnType.Null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"An array subscript must be an integer, got {index.Type}");

        if (array.Type == ColumnType.Null || index.Type == ColumnType.Null)
            return ColumnValue.Null;

        IReadOnlyList<ColumnValue> elements = array.ArrayValues!;
        long position = index.LongValue;

        return position >= 1 && position <= elements.Count
            ? elements[(int)(position - 1)]
            : ColumnValue.Null;
    }

    private static ColumnValue EvalCaseNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        // Simple CASE evaluates its operand once; each WHEN then tests operand = value.
        // Searched CASE has no operand and each WHEN is a boolean condition. Either way the
        // FIRST matching branch wins and later branches are never evaluated — a later branch
        // may reference a column only valid under a different WHEN, so evaluating it eagerly
        // could raise a spurious error.
        ColumnValue? operand = expr.leftAst is null
            ? null
            : EvalExpr(expr.leftAst, row, parameters, rowNameResolver, queryRow);

        foreach (NodeAst clause in EnumerateWhenClauses(expr.rightAst!))
        {
            bool matched;
            if (operand is null)
            {
                ColumnValue cond = EvalExpr(clause.leftAst!, row, parameters, rowNameResolver, queryRow);

                // Only TRUE matches; FALSE and NULL/UNKNOWN skip, consistent with how the
                // WHERE evaluator treats a NULL predicate as non-matching.
                if (cond.Type == ColumnType.Null)
                    matched = false;
                else if (cond.Type != ColumnType.Bool)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"No matching signature for CASE WHEN condition; expected Bool, got: {cond.Type}");
                else
                    matched = cond.BoolValue;
            }
            else
            {
                ColumnValue value = EvalExpr(clause.leftAst!, row, parameters, rowNameResolver, queryRow);

                // operand = value with normal equality; a NULL on either side is UNKNOWN → no match.
                matched = operand.Type != ColumnType.Null
                       && value.Type != ColumnType.Null
                       && CompareValues(operand, value) == 0;
            }

            if (matched)
                return EvalExpr(clause.rightAst!, row, parameters, rowNameResolver, queryRow);
        }

        // No WHEN matched: the ELSE result, or typed NULL when ELSE is omitted.
        return expr.extendedOne is null
            ? ColumnValue.Null
            : EvalExpr(expr.extendedOne, row, parameters, rowNameResolver, queryRow);
    }

    private static ColumnValue EvalTruthTestNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue columnValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

        return expr.nodeType switch
        {
            NodeType.ExprIsNull => ColumnValue.FromBool(columnValue.Type == ColumnType.Null),
            NodeType.ExprIsNotNull => ColumnValue.FromBool(columnValue.Type != ColumnType.Null),
            _ => ColumnValue.FromBool(BooleanTruthTest.Evaluate(expr.nodeType, columnValue)),
        };
    }

    private static ColumnValue EvalPatternMatchNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);
        ColumnValue rightValue = EvalExpr(expr.rightAst!, row, parameters, rowNameResolver, queryRow);

        switch (expr.nodeType)
        {
            case NodeType.ExprLike:
                if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator LIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                return ColumnValue.FromBool(Like(leftValue.StrValue!, rightValue.StrValue!));

            case NodeType.ExprILike:
                if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, $"No matching signature for operator ILIKE for argument types: {leftValue.Type}, {rightValue.Type}");

                return ColumnValue.FromBool(ILike(leftValue.StrValue!, rightValue.StrValue!));

            default:
                {
                    if (leftValue.Type != ColumnType.String || rightValue.Type != ColumnType.String)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt,
                            $"No matching signature for operator ~ for argument types: {leftValue.Type}, {rightValue.Type}");

                    bool ci = expr.nodeType is NodeType.ExprRegexMatchCi or NodeType.ExprRegexNotMatchCi;
                    bool negate = expr.nodeType is NodeType.ExprRegexNotMatch or NodeType.ExprRegexNotMatchCi;
                    bool matched = Functions.RegexMatcher.IsMatch(leftValue.StrValue!, rightValue.StrValue!, ci);
                    return ColumnValue.FromBool(negate ? !matched : matched);
                }
        }
    }

    private static ColumnValue EvalMembershipNode(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver,
        QueryRow? queryRow)
    {
        ColumnValue leftValue = EvalExpr(expr.leftAst!, row, parameters, rowNameResolver, queryRow);

        // UNKNOWN comes back as a NULL value, never as FALSE: NOT (x IN (…)) and a CHECK must see it.
        return SubqueryValueListAst.EvaluateMembership(leftValue, expr, parameters);
    }

    /// <summary>
    /// The error for a node <see cref="EvalExpr"/> cannot evaluate: a subquery form that a rewrite
    /// step must have replaced first, or an unknown node type. Returned rather than thrown so the
    /// dispatcher's frame carries no message formatting.
    /// </summary>
    private static CamusDBException UnevaluableExpression(NodeType nodeType) => nodeType switch
    {
        NodeType.ExprScalarSubquery => new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Scalar subquery must be resolved before expression evaluation"),

        NodeType.ExprInSubquery => new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "IN subquery must be resolved before expression evaluation"),

        NodeType.ExprNotInSubquery => new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "NOT IN subquery must be resolved before expression evaluation"),

        NodeType.ExprExistsSubquery => new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "EXISTS subquery must be resolved before expression evaluation"),

        NodeType.ExprExistsCorrelated => new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Correlated EXISTS subquery must be evaluated by the query filter"),

        _ => new CamusDBException(CamusDBErrorCodes.UnknownType, $"ERROR {nodeType}"),
    };

    /// <summary>
    /// Flattens the left-recursive <see cref="NodeType.ExprCaseWhenList"/> chain that a CASE's WHEN
    /// clauses parse into, yielding each <see cref="NodeType.ExprCaseWhen"/> in source order (top to
    /// bottom). The list is built left-associatively — <c>((c1 c2) c3)</c> — so the leftmost clause is
    /// the deepest node; walking the left spine first restores first-match ordering. A single-clause
    /// CASE has no wrapper node (the grammar collapses it to the bare <see cref="NodeType.ExprCaseWhen"/>).
    /// This is the one place WHEN ordering is defined; every site that visits CASE branches reuses it.
    /// </summary>
    internal static IEnumerable<NodeAst> EnumerateWhenClauses(NodeAst whenList)
    {
        if (whenList.nodeType != NodeType.ExprCaseWhenList)
        {
            yield return whenList;
            yield break;
        }

        // Walk down the left spine, then yield bottom-up. Iterative, so the number of WHEN clauses
        // cannot overflow the stack, and each clause costs constant time rather than one nested
        // iterator per level above it.
        Stack<NodeAst> spine = new();
        NodeAst node = whenList;

        while (node.nodeType == NodeType.ExprCaseWhenList)
        {
            spine.Push(node);
            node = node.leftAst!;
        }

        yield return node;

        while (spine.Count > 0)
            yield return spine.Pop().rightAst!;
    }

    protected static void GetColumnConstraintList(NodeAst constraintsList, List<(ColumnConstraintType, ColumnValue?)> constraintTypes)
    {
        if (constraintsList.nodeType == NodeType.ConstraintNotNull)
        {
            constraintTypes.Add((ColumnConstraintType.NotNull, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintNotNullNamed)
        {
            // Carry the user-supplied constraint name as the ColumnValue so callers can thread it
            // into ColumnInfo.NotNullConstraintName.
            constraintTypes.Add((ColumnConstraintType.NotNull, new ColumnValue(ColumnType.String, constraintsList.yytext!)));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintNull)
        {
            constraintTypes.Add((ColumnConstraintType.Null, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintPrimaryKey)
        {
            constraintTypes.Add((ColumnConstraintType.PrimaryKey, null));
            constraintTypes.Add((ColumnConstraintType.NotNull, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintUnique)
        {
            constraintTypes.Add((ColumnConstraintType.Unique, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintDefault)
        {
            NodeAst defaultExpr = constraintsList.leftAst!;

            // A volatile default (e.g. gen_uuid_v7()) must be evaluated per inserted row, not once
            // at DDL time — otherwise every defaulted row would share one value. Store the function
            // name so the insert path can call it per row. Only a bare zero-argument volatile call is
            // supported; any richer volatile expression is rejected rather than silently frozen to a
            // constant. Non-volatile defaults (literals, deterministic calls) stay pre-evaluated.
            if (ScalarFunctionEvaluator.ContainsVolatileFunction(defaultExpr))
            {
                // A session function (current_user() and friends) is volatile too, but it reports the
                // session that runs the statement — there is none when the insert path applies the
                // stored default, so say that rather than let it fall into the generic message below.
                if (defaultExpr.nodeType == NodeType.ExprFuncCall
                    && defaultExpr.leftAst?.yytext is string sessionFunctionName
                    && ScalarFunctionEvaluator.IsSessionScopedFunction(sessionFunctionName.ToLowerInvariant()))
                {
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Function '{sessionFunctionName.ToLowerInvariant()}' cannot be used as a column default");
                }

                if (defaultExpr.nodeType == NodeType.ExprFuncCall
                    && defaultExpr.rightAst is null
                    && defaultExpr.leftAst?.yytext is string functionName
                    && ScalarFunctionEvaluator.TryResolveVolatileNullary(functionName.ToLowerInvariant(), out _))
                {
                    constraintTypes.Add((ColumnConstraintType.DefaultFunction,
                        new ColumnValue(ColumnType.String, functionName.ToLowerInvariant())));
                }
                else
                {
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        "A volatile function default must be a zero-argument function call, e.g. DEFAULT(gen_uuid_v7())");
                }
            }
            else
            {
                constraintTypes.Add((ColumnConstraintType.Default,
                    EvalExpr(defaultExpr, new Dictionary<string, ColumnValue>(), null)));
            }
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintCheck)
        {
            // The column-level check is collected straight from the AST by
            // SQLExecutorCreateTableCreator.CollectCheckConstraints (which desugars it to a named
            // table-level check); this arm only needs to consume the node so it isn't treated as
            // an unknown constraint. Nothing here reads the value back.
            constraintTypes.Add((ColumnConstraintType.Check, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintStorage)
        {
            constraintTypes.Add((ColumnConstraintType.Storage, new ColumnValue(ColumnType.String, constraintsList.yytext ?? "")));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintComment)
        {
            constraintTypes.Add((ColumnConstraintType.Comment,
                new ColumnValue(ColumnType.String, UnquoteStringLiteral(constraintsList.leftAst?.yytext ?? ""))));
            return;
        }

        if (constraintsList.nodeType == NodeType.CreateTableFieldConstraintList)
        {
            if (constraintsList.leftAst != null)
                GetColumnConstraintList(constraintsList.leftAst, constraintTypes);

            if (constraintsList.rightAst != null)
                GetColumnConstraintList(constraintsList.rightAst, constraintTypes);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid constraint type found: " + constraintsList.nodeType);
    }

    protected static ColumnValue? GetDefaultFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.Default)
                return value;
        }

        return null;
    }

    /// <summary>
    /// Returns the name of the volatile nullary function to evaluate per inserted row for a
    /// <c>DEFAULT(fn())</c> column default, or null when the default is a constant or absent.
    /// The name is carried in a String <see cref="ColumnValue"/> by <see cref="GetColumnConstraintList"/>.
    /// </summary>
    protected static string? GetDefaultFunctionFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.DefaultFunction)
                return value?.StrValue;
        }

        return null;
    }

    /// <summary>
    /// Returns the user-supplied NOT NULL constraint name when the column was declared with
    /// <c>CONSTRAINT name NOT NULL</c> (carried as a String <see cref="ColumnValue"/> by
    /// <see cref="GetColumnConstraintList"/>), or null for bare <c>NOT NULL</c>.
    /// </summary>
    protected static string? GetNotNullConstraintNameFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.NotNull && value?.StrValue is { Length: > 0 } name)
                return name;
        }

        return null;
    }

    /// <summary>
    /// Returns the storage strategy declared inline with <c>STORAGE &lt;strategy&gt;</c>, or null when
    /// none was declared. Rejects a strategy on a column type with no variable-length value with
    /// <see cref="CamusDBErrorCodes.ColumnStorageNotApplicable"/>, so a strategy that could never apply
    /// is not silently kept.
    /// </summary>
    protected static ColumnStorageStrategy? GetStorageFromConstraints(
        List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes, ColumnType columnType, string columnName)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type != ColumnConstraintType.Storage)
                continue;

            ColumnStorageStrategy strategy = ColumnStorageStrategies.Parse(value?.StrValue ?? "");
            if (!TableColumnSchema.SupportsStorageStrategy(columnType))
                throw new CamusDBException(
                    CamusDBErrorCodes.ColumnStorageNotApplicable,
                    $"Column '{columnName}' of type {columnType} has no variable-length value, so it cannot take a storage strategy");

            return strategy;
        }

        return null;
    }

    /// <summary>
    /// Returns the inline column comment when the column was declared with <c>COMMENT '…'</c>, or
    /// null when no comment clause was present. An empty declared comment returns <c>""</c>, not
    /// null — the two are different states downstream, so this must not collapse them.
    /// </summary>
    protected static string? GetCommentFromConstraints(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.Comment)
                return value?.StrValue ?? "";
        }

        return null;
    }

    /// <summary>
    /// Validates that a <c>DEFAULT(fn())</c> function is a supported zero-argument volatile function
    /// whose return type matches the column's declared type (e.g. <c>gen_uuid_v7</c> → <c>uuid</c>).
    /// Throws <c>InvalidInput</c> otherwise. Called at DDL time so a bad default fails at CREATE/ALTER,
    /// not at insert.
    /// </summary>
    protected static void ValidateDefaultFunctionType(string functionName, ColumnType columnType, string columnName)
    {
        if (!ScalarFunctionEvaluator.TryResolveVolatileNullary(functionName, out ColumnType returnType))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"DEFAULT function '{functionName}()' on column '{columnName}' is not a supported zero-argument volatile function");

        if (returnType != columnType)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"DEFAULT function '{functionName}()' returns {returnType} but column '{columnName}' is {columnType}");
    }

    private static bool Like(string text, string pattern)
    {
        // Fast paths: only '%' is a wildcard ('_' is literal, matching current semantics).
        // Use ordinal comparison to avoid culture-dependent results for non-ASCII input.
        int percentCount = CountChar(pattern, '%');

        if (percentCount == 0)
            return text.Equals(pattern, StringComparison.Ordinal);

        if (percentCount == 1)
        {
            if (pattern[^1] == '%')
                return text.AsSpan().StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.Ordinal);
            if (pattern[0] == '%')
                return text.AsSpan().EndsWith(pattern.AsSpan(1), StringComparison.Ordinal);
        }

        if (percentCount == 2 && pattern[0] == '%' && pattern[^1] == '%')
        {
            ReadOnlySpan<char> inner = pattern.AsSpan(1, pattern.Length - 2);
            if (!inner.Contains('%'))
                return text.Contains(inner, StringComparison.Ordinal);
        }

        // Fallback: regex for multi-wildcard patterns (e.g. 'a%b%c'). Route through RegexMatcher
        // so this path inherits the bounded compiled-pattern cache and the ReDoS match-timeout.
        // The glob is escaped first and only '%' becomes '.*', so LIKE semantics are unchanged
        // (still ordinal, case-sensitive).
        string regexPattern = string.Concat("^", Regex.Escape(pattern).Replace("%", ".*"), "$");
        return Functions.RegexMatcher.IsMatch(text, regexPattern, ignoreCase: false);
    }

    private static bool ILike(string text, string pattern)
    {
        int percentCount = CountChar(pattern, '%');

        if (percentCount == 0)
            return text.Equals(pattern, StringComparison.OrdinalIgnoreCase);

        if (percentCount == 1)
        {
            if (pattern[^1] == '%')
                return text.AsSpan().StartsWith(pattern.AsSpan(0, pattern.Length - 1), StringComparison.OrdinalIgnoreCase);
            if (pattern[0] == '%')
                return text.AsSpan().EndsWith(pattern.AsSpan(1), StringComparison.OrdinalIgnoreCase);
        }

        if (percentCount == 2 && pattern[0] == '%' && pattern[^1] == '%')
        {
            ReadOnlySpan<char> inner = pattern.AsSpan(1, pattern.Length - 2);
            if (!inner.Contains('%'))
                return text.Contains(inner, StringComparison.OrdinalIgnoreCase);
        }

        // Multi-wildcard fallback: sequential ordinal-ignore-case segment scan.
        // Keeps ILIKE fully ordinal — culture-default regex would diverge for non-ASCII.
        return ILikeMultiWildcard(text, pattern);
    }

    private static bool ILikeMultiWildcard(string text, string pattern)
    {
        // Split pattern on '%' and match each segment in sequence with OrdinalIgnoreCase.
        // Leading segment anchors to the start; trailing segment anchors to the end;
        // interior segments advance a left-to-right scan cursor.
        string[] segments = pattern.Split('%');
        ReadOnlySpan<char> span = text.AsSpan();
        int pos = 0;

        string first = segments[0];
        if (first.Length > 0)
        {
            if (!span.StartsWith(first.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;
            pos += first.Length;
        }

        for (int i = 1; i < segments.Length - 1; i++)
        {
            string seg = segments[i];
            if (seg.Length == 0) continue;
            int found = span[pos..].IndexOf(seg.AsSpan(), StringComparison.OrdinalIgnoreCase);
            if (found < 0) return false;
            pos += found + seg.Length;
        }

        string last = segments[^1];
        if (last.Length > 0)
        {
            int lastStart = text.Length - last.Length;
            if (lastStart < pos) return false;
            if (!span[lastStart..].StartsWith(last.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private static int CountChar(string s, char c)
    {
        int count = 0;
        foreach (char ch in s)
            if (ch == c) count++;
        return count;
    }
}
