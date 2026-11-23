/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The result types of the CASE, COALESCE, IFNULL, IF, GREATEST and LEAST nodes, and the element types
/// of the <c>ARRAY[…]</c> nodes, of an expression tree that one schema object or one statement owns: a
/// CHECK condition (<see cref="CheckConstraintSchema.ParsedCondition"/>), an expression-index key part
/// (<see cref="BoundIndexExpression.Expression"/>), and the copy of a view's <c>WITH CHECK OPTION</c>
/// condition that one write through the view checks (<see cref="TypedCopy"/>). Each tree is typed once
/// from its table's columns (<see cref="Type"/>), and the evaluator reads the types per node.
///
/// <para><b>Effect.</b> These trees are evaluated with no binding and no statement record, so without
/// these types a NULL hides the type of its column and the type comes from the values of each row. In
/// <c>CHECK (COALESCE(i, f) / 4 &gt; 0.1)</c> the division ran on integers in a row where <c>f</c> is
/// NULL and refused the row. An index on <c>(COALESCE(i, p))</c> over a NUMERIC <c>p</c> refused such
/// a row, and an index on <c>(COALESCE(i, f) / 4)</c> stored 0 for it where a SELECT computes 0.5, so
/// a seek on the index missed the row.</para>
///
/// <para><b>Keeping the type on the node.</b> A statement tree comes from the parse cache, which
/// shares one tree between databases, so its column types are kept on its binding or on a
/// <see cref="StatementExpressionTypes"/>. These trees are not shared:
/// <see cref="SQLParserProcessor.ParseCondition"/> parses without the cache, each constraint and each
/// bound index expression has its own tree, and a column never changes type, so the types stay right
/// for as long as the tree exists. A call is typed only when it is a built-in function, because an
/// extension function can be replaced after the tree is typed.</para>
///
/// <para><b>A tree whose check fails</b>, such as a condition created before a type rule existed, is
/// left untyped and keeps the types its values give, as before.</para>
///
/// <para><b>Concurrency.</b> Two threads can type one tree at once. Both write the same types, and the
/// tree is marked typed only after its types are written. Every path that evaluates a CHECK condition
/// calls <see cref="TypeCheckCondition"/> first, an index expression is typed when it is bound, and a
/// check-option copy is typed when the write is rewritten, each before any path can evaluate it.</para>
/// </summary>
internal static class OwnedExpressionTypes
{
    private static readonly ConditionalWeakTable<NodeAst, StrongBox<ExpressionType>> ConditionalTypes = new();

    private static readonly ConditionalWeakTable<NodeAst, StrongBox<ExpressionType>> ElementTypes = new();

    // The roots Type has typed, so a CHECK condition typed on its first row costs one lookup per row after.
    private static readonly ConditionalWeakTable<NodeAst, object> TypedRoots = new();

    private static readonly object Typed = new();

    /// <summary>
    /// Types the CHECK condition <paramref name="condition"/> from the columns of
    /// <paramref name="schema"/>, once. Call it before the condition is evaluated against a row.
    /// </summary>
    internal static void TypeCheckCondition(NodeAst condition, TableSchema schema)
    {
        if (TypedRoots.TryGetValue(condition, out _))
            return;

        Type(condition, ConditionalResultTypes.LookupFor(schema));
    }

    /// <summary>
    /// Types the tree <paramref name="root"/>, which one schema object owns, from the column types
    /// <paramref name="lookup"/> gives, once. See the class summary for which trees qualify.
    /// </summary>
    internal static void Type(NodeAst root, ConditionalResultTypes.ColumnTypeLookup lookup)
    {
        if (TypedRoots.TryGetValue(root, out _))
            return;

        Collector collected = new();

        try
        {
            ConditionalResultTypes.Check(root, lookup, collected, allowAggregates: false, builtInCallsOnly: true);
        }
        catch (CamusDBException)
        {
            // The values of each row type the tree, as they did before it was typed.
            collected = new();
        }

        foreach ((NodeAst node, ExpressionType type) in collected.Conditionals)
            ConditionalTypes.AddOrUpdate(node, new StrongBox<ExpressionType>(type));

        foreach ((NodeAst node, ExpressionType type) in collected.ArrayElements)
            ElementTypes.AddOrUpdate(node, new StrongBox<ExpressionType>(type));

        TypedRoots.AddOrUpdate(root, Typed);
    }

    /// <summary>
    /// A tree to evaluate in place of <paramref name="condition"/>, typed from the column types
    /// <paramref name="lookup"/> gives, for a condition that may share nodes with a tree the parse cache
    /// shares: the <c>WITH CHECK OPTION</c> condition of a view, taken from the view's parsed body. The
    /// cache shares one tree for one text. A body names its tables by id, so its text differs between
    /// databases, but a body stored before relations were named by id names them directly and can have
    /// the same text in two databases whose columns have other types. Typing shared nodes would then hand
    /// one database's types to the other, so the condition is copied first
    /// (<see cref="NodeAstWalk.Copy"/>) and the copy is typed. The caller must hold the copy for as long
    /// as it evaluates it, and call this once per statement. A condition with no CASE, COALESCE, IFNULL,
    /// IF, GREATEST, LEAST or <c>ARRAY[…]</c> node has nothing to type and is returned as it is, without a
    /// copy.
    /// </summary>
    internal static NodeAst TypedCopy(NodeAst condition, ConditionalResultTypes.ColumnTypeLookup lookup)
    {
        if (!NodeAstWalk.Any(condition, static node =>
                node.nodeType is NodeType.ExprCase or NodeType.ArrayLiteral || ConditionalResultTypes.IsCommonTypeCall(node)))
            return condition;

        NodeAst copy = NodeAstWalk.Copy(condition);
        Type(copy, lookup);
        return copy;
    }

    /// <summary>The result type stored for the conditional node <paramref name="node"/>, if it belongs to a typed tree.</summary>
    internal static bool TryGetConditionalResultType(NodeAst node, out ExpressionType resultType)
    {
        if (ConditionalTypes.TryGetValue(node, out StrongBox<ExpressionType>? stored))
        {
            resultType = stored.Value;
            return true;
        }

        resultType = default;
        return false;
    }

    /// <summary>The element type stored for the <c>ARRAY[…]</c> node <paramref name="node"/>, if it belongs to a typed tree.</summary>
    internal static bool TryGetArrayElementType(NodeAst node, out ExpressionType elementType)
    {
        if (ElementTypes.TryGetValue(node, out StrongBox<ExpressionType>? stored))
        {
            elementType = stored.Value;
            return true;
        }

        elementType = default;
        return false;
    }

    /// <summary>
    /// Holds what the check records until it succeeds, so a check that fails halfway publishes nothing.
    /// </summary>
    private sealed class Collector : IExpressionTypeRecorder
    {
        public List<(NodeAst Node, ExpressionType Type)> Conditionals { get; } = [];

        public List<(NodeAst Node, ExpressionType Type)> ArrayElements { get; } = [];

        public void RecordConditionalResultType(NodeAst node, ExpressionType resultType) => Conditionals.Add((node, resultType));

        public void RecordArrayElementType(NodeAst node, ExpressionType elementType) => ArrayElements.Add((node, elementType));
    }
}
