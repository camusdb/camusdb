/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Turns a WHERE term over an indexed expression into a comparison the index selector can use.
///
/// <para><b>How a match works.</b> The predicate analysis recognizes only <c>column op constant</c>,
/// so <c>lower(email) = 'a@x'</c> reaches it as a residual conjunct. For each residual conjunct of
/// the shapes <c>expr op constant</c>, <c>constant op expr</c>, <c>expr BETWEEN a AND b</c>,
/// <c>expr IN (...)</c> and <c>expr = a OR expr = b ...</c>, this pass renders <c>expr</c> in its
/// canonical text (<see cref="IndexExpressionBinder.Canonical"/>: names in lower case, no table
/// qualifier, no redundant parentheses) and looks for an index expression with the same text. A match
/// becomes an <see cref="AnalyzedComparison"/> or an <see cref="AnalyzedInList"/> on the hidden key
/// name (<see cref="BoundIndexExpression.KeyName"/>), which is the name at that position of the
/// index's columns. Everything after this pass treats the name like a column: index selection, bound
/// absorption, constant coercion (through <see cref="BoundIndexExpression.KeyColumn"/>) and the
/// residual filter.</para>
///
/// <para><b>Why results stay right.</b> Each comparison keeps its original conjunct, except the two
/// bounds of a BETWEEN, which get conjuncts of the same shape <see cref="PredicateAnalyzer"/> builds
/// for a column BETWEEN. A scan that absorbs a comparison drops it from the filter only because every
/// entry key equals the expression's value for its row: the write paths evaluate the same expression
/// with the same evaluator. A comparison no scan absorbs is evaluated on the row exactly as written.</para>
///
/// <para>Only a single-table plan is matched. A join leaf or a query whose rows are keyed by
/// qualified names keeps the conjunct residual, so it reads through a plain path.</para>
/// </summary>
internal static class ExpressionIndexPredicates
{
    /// <summary>
    /// <paramref name="analysis"/> with every residual conjunct that compares an indexed expression of
    /// <paramref name="table"/> to a constant moved into the indexable comparisons or the IN lists.
    /// Returns <paramref name="analysis"/> itself when the table has no expression index or nothing
    /// matched.
    /// </summary>
    public static PredicateAnalysis Match(
        PredicateAnalysis analysis,
        TableDescriptor table,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? resolver)
    {
        IReadOnlyDictionary<string, BoundIndexExpression> keys = table.ExpressionKeys;
        if (keys.Count == 0 || analysis.ResidualConjuncts.Count == 0)
            return analysis;

        if (resolver is not null && resolver.UsesQualifiedRowKeys())
            return analysis;

        Dictionary<string, string> keyNameByCanonical = new(keys.Count, StringComparer.Ordinal);
        foreach (BoundIndexExpression expression in keys.Values)
            keyNameByCanonical.TryAdd(expression.Canonical, expression.KeyName);

        Matcher matcher = new(table, parameters, resolver, keyNameByCanonical);

        List<AnalyzedComparison>? comparisons = null;
        List<AnalyzedInList>? inLists = null;
        List<NodeAst> residual = new(analysis.ResidualConjuncts.Count);

        foreach (NodeAst conjunct in analysis.ResidualConjuncts)
        {
            if (matcher.TryComparisons(conjunct, out List<AnalyzedComparison>? matched))
                (comparisons ??= [.. analysis.IndexableComparisons]).AddRange(matched!);
            else if (matcher.TryInList(conjunct, out AnalyzedInList? inList))
                (inLists ??= [.. analysis.InListComparisons]).Add(inList!);
            else
                residual.Add(conjunct);
        }

        if (comparisons is null && inLists is null)
            return analysis;

        return new PredicateAnalysis(
            comparisons ?? analysis.IndexableComparisons,
            analysis.ColumnComparisons,
            residual,
            inLists ?? analysis.InListComparisons);
    }

    private sealed class Matcher
    {
        private readonly TableDescriptor table;
        private readonly Dictionary<string, ColumnValue>? parameters;
        private readonly QueryRowNameResolver? resolver;
        private readonly Dictionary<string, string> keyNameByCanonical;

        public Matcher(TableDescriptor table, Dictionary<string, ColumnValue>? parameters, QueryRowNameResolver? resolver, Dictionary<string, string> keyNameByCanonical)
        {
            this.table = table;
            this.parameters = parameters;
            this.resolver = resolver;
            this.keyNameByCanonical = keyNameByCanonical;
        }

        /// <summary><c>expr op constant</c>, <c>constant op expr</c> and <c>expr BETWEEN a AND b</c>.</summary>
        public bool TryComparisons(NodeAst conjunct, out List<AnalyzedComparison>? comparisons)
        {
            comparisons = null;

            if (conjunct.nodeType == NodeType.ExprBetween)
            {
                if (conjunct.leftAst is not { } operand
                    || conjunct.extendedOne is not { } lowNode
                    || conjunct.extendedTwo is not { } highNode
                    || KeyNameOf(operand) is not { } keyName
                    || !TryConstant(lowNode, out ColumnValue? low)
                    || !TryConstant(highNode, out ColumnValue? high))
                    return false;

                comparisons =
                [
                    new AnalyzedComparison(keyName, ">=", low!, Comparison(NodeType.ExprGreaterEqualsThan, operand, lowNode)),
                    new AnalyzedComparison(keyName, "<=", high!, Comparison(NodeType.ExprLessEqualsThan, operand, highNode)),
                ];
                return true;
            }

            if (OperatorOf(conjunct.nodeType) is not { } op || conjunct.leftAst is null || conjunct.rightAst is null)
                return false;

            if (KeyNameOf(conjunct.leftAst) is { } leftKey && TryConstant(conjunct.rightAst, out ColumnValue? right))
            {
                comparisons = [new AnalyzedComparison(leftKey, op, right!, conjunct)];
                return true;
            }

            if (KeyNameOf(conjunct.rightAst) is { } rightKey && TryConstant(conjunct.leftAst, out ColumnValue? left))
            {
                comparisons = [new AnalyzedComparison(rightKey, Mirror(op), left!, conjunct)];
                return true;
            }

            return false;
        }

        /// <summary><c>expr IN (constants)</c> and <c>expr = a OR expr = b ...</c>.</summary>
        public bool TryInList(NodeAst conjunct, out AnalyzedInList? inList)
        {
            inList = null;

            if (conjunct.nodeType == NodeType.ExprInMembership)
            {
                if (conjunct.leftAst is null || conjunct.rightAst is null || KeyNameOf(conjunct.leftAst) is not { } keyName)
                    return false;

                List<ColumnValue> values = [];
                bool containsNull = SubqueryValueListAst.ListContainsRemovedNull(conjunct);

                List<NodeAst> items = [];
                ExpressionChains.Flatten(conjunct.rightAst, NodeType.ExprList, items);

                foreach (NodeAst item in items)
                {
                    if (!TryEvaluate(item, out ColumnValue? value))
                        return false;

                    if (value!.Type == ColumnType.Null)
                        containsNull = true;
                    else
                        values.Add(value);
                }

                inList = new AnalyzedInList(keyName, values, conjunct, containsNull);
                return true;
            }

            if (conjunct.nodeType != NodeType.ExprOr)
                return false;

            List<NodeAst> leaves = [];
            ExpressionChains.Flatten(conjunct, NodeType.ExprOr, leaves);

            string? key = null;
            List<ColumnValue> equalities = new(leaves.Count);

            foreach (NodeAst leaf in leaves)
            {
                if (leaf.nodeType != NodeType.ExprEquals
                    || !TryComparisons(leaf, out List<AnalyzedComparison>? matched)
                    || matched is not [{ } equality])
                    return false;

                if (key is null)
                    key = equality.ColumnName;
                else if (!string.Equals(key, equality.ColumnName, StringComparison.OrdinalIgnoreCase))
                    return false;

                equalities.Add(equality.Constant);
            }

            if (key is null)
                return false;

            inList = new AnalyzedInList(key, equalities, conjunct);
            return true;
        }

        /// <summary>
        /// The hidden key name of the index expression <paramref name="operand"/> spells, or null. A
        /// bare column is never a match here: the predicate analysis already handled it.
        /// </summary>
        private string? KeyNameOf(NodeAst operand)
        {
            if (operand.nodeType is NodeType.Identifier or NodeType.Placeholder
                || operand.nodeType is NodeType.Integer or NodeType.Float or NodeType.String or NodeType.Bool or NodeType.Null)
                return null;

            if (IndexExpressionBinder.ColumnReferences(operand).Count == 0)
                return null;

            string? canonical = IndexExpressionBinder.Canonical(operand, ResolveColumn);
            if (canonical is null)
                return null;

            if (keyNameByCanonical.TryGetValue(canonical, out string? keyName))
                return keyName;

            // The index text went through one more render and parse than this one did. Where the
            // renderer adds a shape of its own (a negative literal in parentheses), one round trip
            // gives both the same text.
            try
            {
                string? normalized = IndexExpressionBinder.Canonical(SQLParserProcessor.ParseCondition(canonical), static name => name);
                return normalized is not null && keyNameByCanonical.TryGetValue(normalized, out keyName) ? keyName : null;
            }
            catch (CamusDBException)
            {
                return null;
            }
        }

        /// <summary>
        /// The column of the scanned table that <paramref name="identifier"/> names, or null when it
        /// names anything else (an outer reference, another relation, an alias).
        /// </summary>
        private string? ResolveColumn(string identifier)
        {
            string name = identifier;

            if (name.Contains('.'))
            {
                if (resolver is null)
                    return null;

                try
                {
                    name = resolver.ResolveRowLookupKey(identifier);
                }
                catch (CamusDBException)
                {
                    return null;
                }

                if (name.Contains('.'))
                    return null;
            }

            List<TableColumnSchema>? columns = table.Schema.Columns;
            if (columns is null)
                return null;

            foreach (TableColumnSchema column in columns)
            {
                if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
                    return column.Name;
            }

            return null;
        }

        private bool TryConstant(NodeAst node, out ColumnValue? value) =>
            PredicateAnalyzer.TryGetConstant(node, parameters, out value);

        private bool TryEvaluate(NodeAst node, out ColumnValue? value)
        {
            value = null;

            if (node.nodeType == NodeType.Identifier)
                return false;

            try
            {
                value = SqlExecutor.EvalExpr(node, new Dictionary<string, ColumnValue>(), parameters);
                return true;
            }
            catch (CamusDBException)
            {
                return false;
            }
        }

        private static NodeAst Comparison(NodeType type, NodeAst operand, NodeAst constant) =>
            new(type, operand, constant, null, null, null, null, null, null);

        private static string? OperatorOf(NodeType type) => type switch
        {
            NodeType.ExprEquals => "=",
            NodeType.ExprLessThan => "<",
            NodeType.ExprGreaterThan => ">",
            NodeType.ExprLessEqualsThan => "<=",
            NodeType.ExprGreaterEqualsThan => ">=",
            _ => null,
        };

        private static string Mirror(string op) => op switch
        {
            "<" => ">",
            ">" => "<",
            "<=" => ">=",
            ">=" => "<=",
            _ => op,
        };
    }
}
