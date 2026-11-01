/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Finds the top-level WHERE conjuncts a GIN index can narrow, and builds the
/// <see cref="GinIndexScanNode"/> for them.
///
/// <para><b>What it reads.</b> The jsonb operators reach the planner as calls to their functions, in
/// <see cref="PredicateAnalysis.ResidualConjuncts"/>. These conjuncts can use a GIN index on
/// <c>col</c> when the other operand is a constant (a literal, a parameter, or an expression without
/// columns):</para>
/// <list type="bullet">
/// <item><c>col @&gt; c</c> and <c>c &lt;@ col</c>;</item>
/// <item><c>col ? c</c>, <c>col ?| c</c> and <c>col ?&amp; c</c>;</item>
/// <item><c>col -&gt;&gt; 'k' = c</c> and <c>col -&gt; 'k' = c</c>, in either operand order.</item>
/// </list>
/// <para>All the conjuncts on one index are combined (their groups are ANDed). When several GIN
/// indexes qualify, the one with the most groups wins. A conjunct under OR or NOT is not a top-level
/// conjunct and is never used. <c>col &lt;@ c</c> has no GIN form, as in PostgreSQL.</para>
///
/// <para>The selector never removes a conjunct from the execution filter: the scan returns a superset
/// (see <see cref="GinIndexScanNode"/>), so the filter checks every candidate row.</para>
/// </summary>
internal static class GinScanSelector
{
    /// <summary>
    /// The GIN scan for <paramref name="analysis"/>, or null when no readable GIN index of
    /// <paramref name="table"/> can narrow it. <paramref name="onlyIndex"/> limits the choice to one
    /// index (a <c>FORCE_INDEX</c> hint).
    /// </summary>
    public static GinIndexScanNode? TryBuild(
        TableDescriptor table,
        PredicateAnalysis analysis,
        Dictionary<string, ColumnValue>? parameters,
        string? onlyIndex = null)
    {
        IReadOnlyList<TableIndexSchema> invertedIndexes = table.InvertedIndexes;
        if (invertedIndexes.Count == 0 || analysis.ResidualConjuncts.Count == 0)
            return null;

        GinIndexScanNode? best = null;

        foreach (TableIndexSchema index in invertedIndexes)
        {
            if (onlyIndex is not null && !string.Equals(index.Name, onlyIndex, StringComparison.Ordinal))
                continue;

            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index) || index.Columns.Length != 1)
                continue;

            string column = index.Columns[0];
            List<IReadOnlyList<JsonbGinEntries.Requirement>>? groups = null;

            foreach (NodeAst conjunct in analysis.ResidualConjuncts)
            {
                if (TryQuery(conjunct, column, parameters) is { } query)
                    (groups ??= []).AddRange(query.Groups);
            }

            if (groups is null)
                continue;

            if (best is null || groups.Count > best.Query.Groups.Count)
                best = new GinIndexScanNode(index, column, new JsonbGinEntries.Query(groups));
        }

        return best;
    }

    /// <summary>True when <paramref name="table"/> has a GIN index named <paramref name="name"/>.</summary>
    public static bool IsInvertedIndexName(TableDescriptor table, string name)
    {
        foreach (TableIndexSchema index in table.InvertedIndexes)
        {
            if (string.Equals(index.Name, name, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>The entries <paramref name="conjunct"/> needs on <paramref name="column"/>, or null.</summary>
    private static JsonbGinEntries.Query? TryQuery(NodeAst conjunct, string column, Dictionary<string, ColumnValue>? parameters)
    {
        if (conjunct.nodeType == NodeType.ExprEquals)
            return TryFieldEquality(conjunct.leftAst, conjunct.rightAst, column, parameters)
                ?? TryFieldEquality(conjunct.rightAst, conjunct.leftAst, column, parameters);

        if (!TryCall(conjunct, out string name, out NodeAst first, out NodeAst second))
            return null;

        switch (name)
        {
            case "jsonb_contains" when IsColumn(first, column):
                return Document(second, parameters) is { } contained ? JsonbGinEntries.Contains(contained) : null;

            case "jsonb_contained" when IsColumn(second, column):
                return Document(first, parameters) is { } containedLeft ? JsonbGinEntries.Contains(containedLeft) : null;

            case "jsonb_exists" when IsColumn(first, column):
                return Constant(second, parameters) is { Type: ColumnType.String } key
                    ? new JsonbGinEntries.Query([JsonbGinEntries.Exists(key.StrValue!)])
                    : null;

            case "jsonb_exists_any" when IsColumn(first, column):
                return Keys(name, second, parameters) is { } anyKeys ? JsonbGinEntries.ExistsMany(anyKeys, all: false) : null;

            case "jsonb_exists_all" when IsColumn(first, column):
                return Keys(name, second, parameters) is { } allKeys ? JsonbGinEntries.ExistsMany(allKeys, all: true) : null;

            default:
                return null;
        }
    }

    /// <summary><c>col -&gt;&gt; 'k' = text</c> or <c>col -&gt; 'k' = document</c>, with the call on the left.</summary>
    private static JsonbGinEntries.Query? TryFieldEquality(NodeAst? call, NodeAst? other, string column, Dictionary<string, ColumnValue>? parameters)
    {
        if (call is null || other is null || !TryCall(call, out string name, out NodeAst first, out NodeAst second))
            return null;

        if (name is not ("jsonb_object_field" or "jsonb_object_field_text") || !IsColumn(first, column))
            return null;

        if (Constant(second, parameters) is not { Type: ColumnType.String } key)
            return null;

        if (name == "jsonb_object_field_text")
            return Constant(other, parameters) is { Type: ColumnType.String } text
                ? JsonbGinEntries.FieldTextEquals(key.StrValue!, text.StrValue!)
                : null;

        return Document(other, parameters) is { } item ? JsonbGinEntries.FieldEquals(key.StrValue!, item) : null;
    }

    /// <summary>A two-argument function call: its lower-case name and both arguments.</summary>
    private static bool TryCall(NodeAst node, out string name, out NodeAst first, out NodeAst second)
    {
        name = "";
        first = second = node;

        if (node.nodeType != NodeType.ExprFuncCall
            || node.leftAst?.yytext is not { } functionName
            || node.rightAst is not { nodeType: NodeType.ExprArgumentList, leftAst: { } left, rightAst: { } right }
            || left.nodeType is NodeType.ExprArgumentList or NodeType.ExprList
            || right.nodeType is NodeType.ExprArgumentList or NodeType.ExprList)
            return false;

        name = functionName.ToLowerInvariant();
        first = left;
        second = right;
        return true;
    }

    /// <summary>
    /// True when <paramref name="node"/> names <paramref name="column"/>, bare or qualified. In a
    /// single-table plan a qualifier can only name the planned table, or the query does not bind.
    /// </summary>
    private static bool IsColumn(NodeAst node, string column)
    {
        if (node.nodeType != NodeType.Identifier || node.yytext is not { } text)
            return false;

        int dot = text.LastIndexOf('.');
        ReadOnlySpan<char> name = dot >= 0 ? text.AsSpan(dot + 1) : text.AsSpan();
        return name.Equals(column, StringComparison.OrdinalIgnoreCase);
    }

    private static ColumnValue? Constant(NodeAst node, Dictionary<string, ColumnValue>? parameters) =>
        PredicateAnalyzer.TryGetConstant(node, parameters, out ColumnValue? value) ? value : null;

    /// <summary>
    /// The CBOR item of a constant document operand, or null. A string is parsed the way the operator
    /// parses it; one that does not parse narrows nothing (the filter raises the error).
    /// </summary>
    private static byte[]? Document(NodeAst node, Dictionary<string, ColumnValue>? parameters)
    {
        try
        {
            return Constant(node, parameters) switch
            {
                { Type: ColumnType.Jsonb } jsonb => JsonbDocument.Item(jsonb.BytesValue).ToArray(),
                { Type: ColumnType.String } text => JsonbDocument.Item(JsonbEncoder.FromText(text.StrValue!)).ToArray(),
                _ => null,
            };
        }
        catch (CamusDBException)
        {
            return null;
        }
    }

    /// <summary>The non-NULL keys of a constant key list, as the operator skips a NULL key.</summary>
    private static List<string>? Keys(string function, NodeAst node, Dictionary<string, ColumnValue>? parameters)
    {
        if (Constant(node, parameters) is not { } value)
            return null;

        try
        {
            List<string> keys = [];
            foreach (string? key in JsonbScalarFunctions.TextArray(function, value, 1))
            {
                if (key is not null)
                    keys.Add(key);
            }

            return keys;
        }
        catch (CamusDBException)
        {
            return null;
        }
    }
}
