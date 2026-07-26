
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Chooses the inner index a correlated EXISTS can seek, by finding equalities in the inner WHERE
/// that pin a leading prefix of an index's key columns.
///
/// This deliberately does not reuse <see cref="IndexScanSelector"/>/<c>PredicateAnalyzer</c>: those
/// classify a predicate over a single table into column-vs-*constant* comparisons, and the whole
/// point here is the comparison whose other side is an outer-row column — which they would reject
/// as non-constant. The shapes accepted below are intentionally narrow (literal, placeholder, or
/// outer-qualified identifier) so a bound expression can never depend on an inner column, which is
/// what makes it evaluable against the outer row alone.
///
/// Returning null is always safe: the executor keeps the full-scan path, which is the correctness
/// baseline. Nothing here may change which rows match — only how they are found.
/// </summary>
internal static class CorrelatedExistsSeekPlanner
{
    /// <summary>
    /// Builds a seek plan for <paramref name="innerWhere"/> against <paramref name="innerTable"/>,
    /// or null when no readable index has its leading key column pinned by an eligible equality.
    ///
    /// <paramref name="innerAliases"/> are the aliases the inner query's own sources answer to; an
    /// identifier qualified with anything else is an outer reference, which is precisely the
    /// correlation this plan exists to push into the seek.
    /// </summary>
    public static CorrelatedExistsSeekPlan? TryPlan(
        TableDescriptor innerTable,
        NodeAst? innerWhere,
        string innerAlias,
        IReadOnlySet<string> innerAliases)
    {
        if (innerWhere is null)
            return null;

        Dictionary<string, NodeAst> equalityByColumn = new(StringComparer.OrdinalIgnoreCase);
        CollectEqualityBindings(innerWhere, innerAlias, innerAliases, equalityByColumn);

        if (equalityByColumn.Count == 0)
            return null;

        CorrelatedExistsSeekPlan? best = null;
        int bestPrefixLength = 0;

        foreach (TableIndexSchema index in innerTable.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadableIndex(innerTable.Schema, index))
                continue;

            List<CorrelatedExistsSeekBinding> bindings = new(index.Columns.Length);

            foreach (string keyColumn in index.Columns)
            {
                if (!equalityByColumn.TryGetValue(keyColumn, out NodeAst? bound))
                    break;

                bindings.Add(new CorrelatedExistsSeekBinding(bound, GetColumnType(innerTable, keyColumn)));
            }

            if (bindings.Count == 0)
                continue;

            bool unique = index.Type == IndexType.Unique;

            // Longer pinned prefix first (a tighter seek); on a tie prefer a unique index, which
            // cannot hold more than one row per full key.
            if (bindings.Count < bestPrefixLength)
                continue;

            if (bindings.Count == bestPrefixLength && !(unique && best is { Unique: false }))
                continue;

            best = new CorrelatedExistsSeekPlan(
                index,
                GetIndexColumnTypes(innerTable, index),
                unique,
                bindings);

            bestPrefixLength = bindings.Count;
        }

        return best;
    }

    /// <summary>
    /// Walks the AND spine collecting <c>innerColumn = &lt;outer-or-constant&gt;</c> equalities.
    /// Only conjunctions are traversed: a disjunct does not have to hold for a matching row, so an
    /// equality under an OR cannot bound the seek. The first binding found for a column wins;
    /// contradictory duplicates (<c>a = 1 AND a = 2</c>) are harmless because the full inner
    /// predicate is still evaluated on every row the seek returns.
    /// </summary>
    private static void CollectEqualityBindings(
        NodeAst node,
        string innerAlias,
        IReadOnlySet<string> innerAliases,
        Dictionary<string, NodeAst> equalityByColumn)
    {
        switch (node.nodeType)
        {
            case NodeType.ExprAnd:
                if (node.leftAst is not null)
                    CollectEqualityBindings(node.leftAst, innerAlias, innerAliases, equalityByColumn);
                if (node.rightAst is not null)
                    CollectEqualityBindings(node.rightAst, innerAlias, innerAliases, equalityByColumn);
                return;

            case NodeType.ExprEquals:
            {
                if (node.leftAst is null || node.rightAst is null)
                    return;

                if (TryBind(node.leftAst, node.rightAst, innerAlias, innerAliases, equalityByColumn))
                    return;

                TryBind(node.rightAst, node.leftAst, innerAlias, innerAliases, equalityByColumn);
                return;
            }

            default:
                return;
        }
    }

    private static bool TryBind(
        NodeAst columnSide,
        NodeAst valueSide,
        string innerAlias,
        IReadOnlySet<string> innerAliases,
        Dictionary<string, NodeAst> equalityByColumn)
    {
        if (!TryGetInnerColumnName(columnSide, innerAlias, innerAliases, out string columnName))
            return false;

        if (!IsOuterOrConstant(valueSide, innerAliases))
            return false;

        if (!equalityByColumn.ContainsKey(columnName))
            equalityByColumn[columnName] = valueSide;

        return true;
    }

    /// <summary>
    /// True when <paramref name="node"/> names a column of the inner table — either unqualified
    /// (an unqualified name in a subquery binds to the subquery's own source) or qualified with the
    /// inner source's alias.
    /// </summary>
    private static bool TryGetInnerColumnName(
        NodeAst node,
        string innerAlias,
        IReadOnlySet<string> innerAliases,
        out string columnName)
    {
        columnName = "";

        if (node.nodeType != NodeType.Identifier || node.yytext is not string identifier || identifier.Length == 0)
            return false;

        int dot = identifier.IndexOf('.');

        if (dot <= 0)
        {
            columnName = identifier;
            return true;
        }

        if (dot >= identifier.Length - 1)
            return false;

        string alias = identifier[..dot];

        // Qualified with some *other* in-scope inner alias (a joined inner source) is not this
        // table's column, so it cannot key this table's index.
        if (!string.Equals(alias, innerAlias, StringComparison.Ordinal))
            return false;

        _ = innerAliases;
        columnName = identifier[(dot + 1)..];
        return true;
    }

    /// <summary>
    /// True when <paramref name="node"/> can be evaluated with only the outer row and the query
    /// parameters in hand. Restricted to literals, placeholders, and identifiers qualified with an
    /// alias the inner query does not own — anything richer risks reaching an inner column, which
    /// has no value at the time the seek key is built.
    /// </summary>
    private static bool IsOuterOrConstant(NodeAst node, IReadOnlySet<string> innerAliases)
    {
        switch (node.nodeType)
        {
            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
                return true;

            case NodeType.Identifier:
            {
                if (node.yytext is not string identifier)
                    return false;

                int dot = identifier.IndexOf('.');
                if (dot <= 0 || dot >= identifier.Length - 1)
                    return false;

                return !innerAliases.Contains(identifier[..dot]);
            }

            default:
                return false;
        }
    }

    private static ColumnType GetColumnType(TableDescriptor table, string columnName)
    {
        TableColumnSchema? column = table.Schema.Columns?.Find(
            c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

        return column?.Type ?? ColumnType.String;
    }

    private static ColumnType[] GetIndexColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
            types[i] = GetColumnType(table, index.Columns[i]);

        return types;
    }
}
