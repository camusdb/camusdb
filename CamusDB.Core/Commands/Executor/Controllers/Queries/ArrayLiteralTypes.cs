/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Gives an <c>ARRAY[…]</c> literal one element type: the common type of its elements by
/// <see cref="ResultTypeRule"/>, the rule CASE, COALESCE and UNION use, as PostgreSQL does.
/// <c>ARRAY[1, 2.5]</c> is an <c>ARRAY&lt;Float64&gt;</c>, and <c>ARRAY[d, '2026-01-01']</c> over a
/// DATE column is an <c>ARRAY&lt;Date&gt;</c> with the literal parsed. <c>ARRAY[1, true]</c> has no
/// common type and is <see cref="CamusDBErrorCodes.InvalidInput"/>. So is <c>ARRAY[1, '2']</c>:
/// PostgreSQL reads <c>'2'</c> as an integer, and CamusDB keeps the rule it uses for CASE, where a
/// quoted literal takes only a date or datetime type.
///
/// <para><b>The type comes from the statement.</b> A NULL element hides the type of its column, so a
/// type taken from the values of one row can differ between rows: <c>ARRAY[int_col, float_col]</c>
/// would be an <c>ARRAY&lt;Integer64&gt;</c> in a row where <c>float_col</c> is NULL, and two arrays
/// of different element types do not compare. The element type is therefore taken from the element
/// expressions, from the first of these that has it:</para>
/// <list type="number">
/// <item>The statement check, which types column references (<see cref="Check"/>). A SELECT (also a
///   view body, a derived table and <c>INSERT … SELECT</c>) records the type on its binding, the
///   <see cref="QueryRowNameResolver"/>. An UPDATE, a DELETE and an upsert DO UPDATE have no binding
///   and record it on a <see cref="StatementExpressionTypes"/>, which is current while they run.</item>
/// <item>A CHECK condition or an index expression, typed once from its table's columns. Each has its
///   own tree, so the type stays on the node (<see cref="OwnedExpressionTypes"/>).</item>
/// <item>The SQL text alone: literals, casts and built-in calls over those
///   (<see cref="StaticElementType"/>). It is cached per node, because the parse cache shares one tree
///   between databases and only a type the text decides is the same for all of them.</item>
/// </list>
///
/// <para>When the array is evaluated (<see cref="Build"/>), each element value is combined with that
/// type, so an element whose type only its value shows (a parameter, a call whose type comes from its
/// value) is checked too. Every element is then converted to the result with
/// <see cref="ResultTypeRule.Widen"/>. A string value can take a date or datetime type there, as a
/// string parameter does in a CASE (<see cref="Functions.ExtremumScalarFunctions"/> folds its
/// arguments the same way).</para>
/// </summary>
internal static class ArrayLiteralTypes
{
    // The element type the SQL text alone gives each ARRAY[...] node. Boxed so a "not known" result is
    // cached too.
    private static readonly ConditionalWeakTable<NodeAst, StrongBox<ExpressionType?>> ElementTypes = new();

    /// <summary>
    /// The error for an element that is itself an array. <see cref="ColumnValue"/> holds exactly one
    /// element type per array, so it cannot represent a nested array.
    /// </summary>
    internal static CamusDBException NestedArray() =>
        new(CamusDBErrorCodes.InvalidInput, "Nested arrays are not supported; an array element must be a scalar or NULL");

    // ── Bind time ────────────────────────────────────────────────────────────

    /// <summary>
    /// Types the <c>ARRAY[…]</c> node <paramref name="node"/> while a statement binds, from
    /// <see cref="ConditionalResultTypes.Check"/>. Throws when two elements have no common type or an
    /// element is an array, so the statement fails before any row is read, also over an empty table.
    /// Records the element type on <paramref name="recorder"/> when there is one, even when only some
    /// element types are known: evaluation combines the others in.
    /// </summary>
    /// <returns>The static type of the array. Its element type is <see cref="ColumnType.Null"/> (not
    /// known) unless the type of every element is known: an element of unknown type can still widen
    /// it.</returns>
    internal static ExpressionType Check(
        NodeAst node,
        ConditionalResultTypes.ColumnTypeLookup? lookup,
        IExpressionTypeRecorder? recorder,
        ConditionalResultTypes.CallTyping calls)
    {
        ExpressionType? element = Combine(node.leftAst, lookup, calls, throwOnConflict: true, out bool complete);

        if (element is { } known)
            recorder?.RecordArrayElementType(node, known);

        return ArrayOf(element, complete);
    }

    /// <summary>
    /// The static type of the <c>ARRAY[…]</c> node <paramref name="node"/> for an expression around it,
    /// without throwing: a pair of elements with no common type gives an unknown element type, and the
    /// error is raised by <see cref="Check"/> or by evaluation.
    /// </summary>
    internal static ExpressionType StaticType(NodeAst node, ConditionalResultTypes.ColumnTypeLookup? lookup, ConditionalResultTypes.CallTyping calls) =>
        ArrayOf(Combine(node.leftAst, lookup, calls, throwOnConflict: false, out bool complete), complete);

    private static ExpressionType ArrayOf(ExpressionType? element, bool complete) =>
        new(ColumnType.Array, complete && element is { } whole ? whole.Type : ColumnType.Null);

    /// <summary>
    /// The element type the SQL text alone gives <paramref name="node"/>, for an array whose type no
    /// check recorded. Throws when two elements the text types have no common type, so a statement that
    /// no binding checked still fails whenever the array is evaluated. Null when no element type is
    /// known.
    /// </summary>
    internal static ExpressionType? StaticElementType(NodeAst node)
    {
        if (ElementTypes.TryGetValue(node, out StrongBox<ExpressionType?>? cached))
            return cached.Value;

        // A throw adds nothing, so a statement with no common type fails each time it runs.
        return ElementTypes.GetValue(node, static n => new StrongBox<ExpressionType?>(
            Combine(n.leftAst, lookup: null, ConditionalResultTypes.CallTyping.BuiltIn, throwOnConflict: true, out _))).Value;
    }

    /// <summary>
    /// The element type of the <c>ARRAY[…]</c> literal <paramref name="node"/> for column metadata, or
    /// null unless the statement shows the type of every element.
    /// </summary>
    internal static ColumnType? DeclaredElementType(NodeAst node, ConditionalResultTypes.ColumnTypeLookup lookup) =>
        StaticType(node, lookup, ConditionalResultTypes.CallTyping.All) is { ElementType: not ColumnType.Null } array
            ? array.ElementType
            : null;

    /// <summary>
    /// Combines the static types of the elements in <paramref name="list"/>, in source order. Returns
    /// null when no element type is known. <paramref name="complete"/> is true when every element type
    /// is known. With <paramref name="throwOnConflict"/>, a pair with no common type, or an element that
    /// is an array, throws; without it, the result is null. When the result is a date or datetime, a
    /// string literal element is parsed now, so a malformed one fails before any row is read.
    /// </summary>
    private static ExpressionType? Combine(
        NodeAst? list,
        ConditionalResultTypes.ColumnTypeLookup? lookup,
        ConditionalResultTypes.CallTyping calls,
        bool throwOnConflict,
        out bool complete)
    {
        ExpressionType? result = null;
        bool allKnown = true;
        bool conflict = false;

        Fold(list, lookup, calls, throwOnConflict, ref result, ref allKnown, ref conflict);

        complete = allKnown && !conflict;

        if (conflict)
            return null;

        if (throwOnConflict && result is { } final && TemporalComparison.IsTemporal(final.Type))
            ParseStringLiterals(list, final.Type);

        return result;
    }

    // The grammar builds the element list as a tree of ExprList nodes; a long list is balanced
    // (ExpressionChains), so an in-order walk visits the elements in source order at log depth.
    private static void Fold(
        NodeAst? node,
        ConditionalResultTypes.ColumnTypeLookup? lookup,
        ConditionalResultTypes.CallTyping calls,
        bool throwOnConflict,
        ref ExpressionType? result,
        ref bool allKnown,
        ref bool conflict)
    {
        if (node is null || conflict)
            return;

        if (node.nodeType == NodeType.ExprList)
        {
            Fold(node.leftAst, lookup, calls, throwOnConflict, ref result, ref allKnown, ref conflict);
            Fold(node.rightAst, lookup, calls, throwOnConflict, ref result, ref allKnown, ref conflict);
            return;
        }

        if (ConditionalResultTypes.TypeOf(node, lookup, calls) is not { } type)
        {
            allKnown = false;
            return;
        }

        if (type.Type == ColumnType.Array)
        {
            if (throwOnConflict)
                throw NestedArray();

            conflict = true;
            return;
        }

        if (result is not { } current)
        {
            result = type;
            return;
        }

        if (ResultTypeRule.TryCombine(current, type, out ExpressionType combined))
        {
            result = combined;
            return;
        }

        if (throwOnConflict)
            throw ResultTypeRule.NoCommonElementType(current, type);

        conflict = true;
    }

    private static void ParseStringLiterals(NodeAst? node, ColumnType target)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.ExprList)
        {
            ParseStringLiterals(node.leftAst, target);
            ParseStringLiterals(node.rightAst, target);
            return;
        }

        if (QueryExpressionClassifier.UnwrapAlias(node) is { nodeType: NodeType.String, yytext: { } raw })
            _ = ResultTypeRule.ParseTemporal(SQLExecutorBaseCreator.UnquoteStringLiteral(raw), target);
    }

    // ── Evaluation time ──────────────────────────────────────────────────────

    /// <summary>
    /// The element type a check recorded for <paramref name="node"/>, by the order of
    /// <see cref="ConditionalResultTypes.TryGetRecordedResultType"/>: the binding of a SELECT
    /// (<paramref name="resolver"/>); for an evaluation with no resolver, the record of the UPDATE,
    /// DELETE or upsert that is running, and then the types of a CHECK condition or an index expression.
    /// </summary>
    private static bool TryGetRecordedElementType(NodeAst node, QueryRowNameResolver? resolver, out ExpressionType elementType)
    {
        if (resolver is not null)
            return resolver.TryGetArrayElementType(node, out elementType);

        if (StatementExpressionTypes.Current is { } statement && statement.TryGetArrayElementType(node, out elementType))
            return true;

        return OwnedExpressionTypes.TryGetArrayElementType(node, out elementType);
    }

    /// <summary>
    /// Builds the value of the <c>ARRAY[…]</c> node <paramref name="node"/> from its evaluated
    /// <paramref name="elements"/>, in source order. The element type is the one the binding recorded,
    /// or else <see cref="StaticElementType"/>, combined with the type of each element value. Every
    /// element is converted to that type in place, so <paramref name="elements"/> must be a list the
    /// caller owns. An empty or all-NULL array whose type nothing shows has element type
    /// <see cref="ColumnType.Null"/>, which a write adopts from the target column.
    /// </summary>
    internal static ColumnValue Build(NodeAst node, List<ColumnValue> elements, QueryRowNameResolver? resolver)
    {
        ExpressionType? type = TryGetRecordedElementType(node, resolver, out ExpressionType recorded)
            ? recorded
            : StaticElementType(node);

        for (int i = 0; i < elements.Count; i++)
        {
            ColumnValue element = elements[i];

            if (element.Type == ColumnType.Array)
                throw NestedArray();

            if (element.Type == ColumnType.Null)
                continue;

            if (type is { } current && current.Type == element.Type)
                continue;

            // A string value may be a literal or a parameter, so it can take a date or datetime type.
            ExpressionType valueType = ExpressionType.Of(element) with { IsStringLiteral = element.Type == ColumnType.String };

            if (type is not { } known)
            {
                type = valueType;
                continue;
            }

            if (!ResultTypeRule.TryCombine(known, valueType, out ExpressionType combined))
                throw ResultTypeRule.NoCommonElementType(known, valueType);

            type = combined;
        }

        ColumnType elementType = type?.Type ?? ColumnType.Null;

        for (int i = 0; i < elements.Count; i++)
        {
            ColumnValue element = elements[i];

            if (element.Type != elementType && element.Type != ColumnType.Null)
                elements[i] = ResultTypeRule.Widen(element, elementType);
        }

        return ColumnValue.FromArray(elementType, elements);
    }
}
