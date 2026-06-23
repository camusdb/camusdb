
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Parses WHERE clauses into index-friendly comparisons and residual filters (QP2.2).
/// </summary>
public static class PredicateAnalyzer
{
    public static PredicateAnalysis Analyze(NodeAst? where, Dictionary<string, ColumnValue>? parameters)
    {
        if (where is null)
            return PredicateAnalysis.Empty;

        List<NodeAst> conjuncts = new();
        CollectAndConjuncts(where, conjuncts);

        List<AnalyzedComparison> indexable = new();
        List<AnalyzedColumnComparison> columnComparisons = new();
        List<AnalyzedInList> inListComparisons = new();
        List<NodeAst> residual = new();

        foreach (NodeAst conjunct in conjuncts)
        {
            if (TryAnalyzeBetween(conjunct, parameters, out List<AnalyzedComparison>? betweenComparisons))
            {
                indexable.AddRange(betweenComparisons!);
                continue;
            }

            if (TryAnalyzeInMembership(conjunct, parameters, out AnalyzedInList? inList))
            {
                inListComparisons.Add(inList!);
                continue;
            }

            if (TryAnalyzeColumnConstantComparison(conjunct, parameters, out AnalyzedComparison? comparison))
                indexable.Add(comparison!);
            else if (TryAnalyzeColumnColumnComparison(conjunct, out AnalyzedColumnComparison? columnComparison))
                columnComparisons.Add(columnComparison!);
            else
                residual.Add(conjunct);
        }

        return new PredicateAnalysis(indexable, columnComparisons, residual, inListComparisons);
    }

    public static PredicateAnalysis AnalyzeFilters(List<QueryFilter>? filters)
    {
        if (filters is null || filters.Count == 0)
            return PredicateAnalysis.Empty;

        List<AnalyzedComparison> indexable = new(filters.Count);

        foreach (QueryFilter filter in filters)
        {
            ValidateFilter(filter);
            NodeAst conjunct = BuildFilterConjunct(filter);
            indexable.Add(new AnalyzedComparison(filter.ColumnName, filter.Op, filter.Value, conjunct));
        }

        return new PredicateAnalysis(indexable, [], []);
    }

    public static PredicateAnalysis AnalyzeTicket(QueryTicket ticket)
    {
        PredicateAnalysis whereAnalysis = ticket.AnalyzedWhere ?? Analyze(ticket.Where, ticket.Parameters);
        PredicateAnalysis filterAnalysis = AnalyzeFilters(ticket.Filters);
        return Merge(whereAnalysis, filterAnalysis);
    }

    public static PredicateAnalysis Merge(PredicateAnalysis left, PredicateAnalysis right)
    {
        if (right.IndexableComparisons.Count == 0
            && right.ColumnComparisons.Count == 0
            && right.ResidualConjuncts.Count == 0
            && right.InListComparisons.Count == 0)
            return left;

        if (left.IndexableComparisons.Count == 0
            && left.ColumnComparisons.Count == 0
            && left.ResidualConjuncts.Count == 0
            && left.InListComparisons.Count == 0)
            return right;

        List<AnalyzedComparison> indexable = new(left.IndexableComparisons.Count + right.IndexableComparisons.Count);
        indexable.AddRange(left.IndexableComparisons);
        indexable.AddRange(right.IndexableComparisons);

        List<AnalyzedColumnComparison> columnComparisons = new(left.ColumnComparisons.Count + right.ColumnComparisons.Count);
        columnComparisons.AddRange(left.ColumnComparisons);
        columnComparisons.AddRange(right.ColumnComparisons);

        List<NodeAst> residual = new(left.ResidualConjuncts.Count + right.ResidualConjuncts.Count);
        residual.AddRange(left.ResidualConjuncts);
        residual.AddRange(right.ResidualConjuncts);

        List<AnalyzedInList> inList = new(left.InListComparisons.Count + right.InListComparisons.Count);
        inList.AddRange(left.InListComparisons);
        inList.AddRange(right.InListComparisons);

        return new PredicateAnalysis(indexable, columnComparisons, residual, inList);
    }

    public static NodeAst? BuildExecutionFilter(
        PredicateAnalysis analysis,
        QueryPlanStep? scanStep,
        TableDescriptor table,
        NodeAst? absorbedInListConjunct = null)
    {
        HashSet<NodeAst> conjuncts = new(ReferenceEqualityComparer.Instance);

        foreach (NodeAst conjunct in analysis.ResidualConjuncts)
            conjuncts.Add(conjunct);

        foreach (AnalyzedColumnComparison comparison in analysis.ColumnComparisons)
            conjuncts.Add(comparison.Conjunct);

        foreach (AnalyzedComparison comparison in analysis.IndexableComparisons)
        {
            if (scanStep is null || !IndexScanBoundAnalysis.IsComparisonAbsorbedByScan(comparison, scanStep, table))
                conjuncts.Add(comparison.Conjunct);
        }

        // IN-list comparisons: include as residual filter unless absorbed by an IndexInListScanNode.
        foreach (AnalyzedInList inList in analysis.InListComparisons)
        {
            if (!ReferenceEquals(inList.Conjunct, absorbedInListConjunct))
                conjuncts.Add(inList.Conjunct);
        }

        return CombineConjuncts(conjuncts);
    }

    internal static NodeAst? CombineConjuncts(IReadOnlyCollection<NodeAst> conjuncts)
    {
        if (conjuncts.Count == 0)
            return null;

        if (conjuncts.Count == 1)
            return conjuncts.First();

        NodeAst? combined = null;

        foreach (NodeAst conjunct in conjuncts)
        {
            combined = combined is null
                ? conjunct
                : new NodeAst(
                    NodeType.ExprAnd,
                    combined,
                    conjunct,
                    extendedOne: null,
                    extendedTwo: null,
                    extendedThree: null,
                    extendedFour: null,
                    extendedFive: null,
                    yytext: null);
        }

        return combined;
    }

    internal static void CollectAndConjuncts(NodeAst node, ICollection<NodeAst> conjuncts)
    {
        if (node.nodeType == NodeType.ExprAnd)
        {
            if (node.leftAst is not null)
                CollectAndConjuncts(node.leftAst, conjuncts);

            if (node.rightAst is not null)
                CollectAndConjuncts(node.rightAst, conjuncts);

            return;
        }

        conjuncts.Add(node);
    }

    /// <summary>
    /// Recognizes <c>column IN (v1, v2, …)</c> where the LHS is a bare column identifier and
    /// every RHS item is a constant or resolved parameter. NULL items are silently dropped
    /// (a NULL list value matches nothing in SQL). Returns false for expressions, column
    /// references in the list, or subqueries — those stay residual.
    /// </summary>
    private static bool TryAnalyzeInMembership(
        NodeAst conjunct,
        Dictionary<string, ColumnValue>? parameters,
        out AnalyzedInList? result)
    {
        result = null;

        if (conjunct.nodeType != NodeType.ExprInMembership)
            return false;

        if (conjunct.leftAst?.nodeType != NodeType.Identifier || conjunct.leftAst.yytext is null)
            return false;

        if (conjunct.rightAst is null)
            return false;

        List<ColumnValue> values = new();
        if (!TryExtractInListValues(conjunct.rightAst, parameters, values))
            return false;

        // An all-NULL list still has an empty non-null values list — valid but zero seeks.
        string columnName = conjunct.leftAst.yytext;
        result = new AnalyzedInList(columnName, values, conjunct);
        return true;
    }

    private static bool TryExtractInListValues(
        NodeAst node,
        Dictionary<string, ColumnValue>? parameters,
        List<ColumnValue> values)
    {
        if (node.nodeType == NodeType.ExprList)
        {
            if (node.leftAst is not null && !TryExtractInListValues(node.leftAst, parameters, values))
                return false;
            if (node.rightAst is not null && !TryExtractInListValues(node.rightAst, parameters, values))
                return false;
            return true;
        }

        // Reject bare column references in the list.
        if (node.nodeType == NodeType.Identifier)
            return false;

        ColumnValue? value;
        try
        {
            value = SqlExecutor.EvalExpr(node, new(), parameters);
        }
        catch (CamusDBException)
        {
            return false;
        }

        // NULL list values match nothing — skip them.
        if (value.Type != ColumnType.Null)
            values.Add(value);

        return true;
    }

    private static bool TryAnalyzeBetween(
        NodeAst conjunct,
        Dictionary<string, ColumnValue>? parameters,
        out List<AnalyzedComparison>? comparisons)
    {
        comparisons = null;

        if (conjunct.nodeType != NodeType.ExprBetween)
            return false;

        if (conjunct.leftAst?.nodeType != NodeType.Identifier || conjunct.leftAst.yytext is null)
            return false;

        if (conjunct.extendedOne is null || conjunct.extendedTwo is null)
            return false;

        if (!TryGetConstant(conjunct.extendedOne, parameters, out ColumnValue? low) || low is null)
            return false;

        if (!TryGetConstant(conjunct.extendedTwo, parameters, out ColumnValue? high) || high is null)
            return false;

        string columnName = conjunct.leftAst.yytext;
        comparisons =
        [
            new AnalyzedComparison(
                columnName,
                ">=",
                low,
                BuildComparisonConjunct(conjunct.leftAst, conjunct.extendedOne, NodeType.ExprGreaterEqualsThan)),
            new AnalyzedComparison(
                columnName,
                "<=",
                high,
                BuildComparisonConjunct(conjunct.leftAst, conjunct.extendedTwo, NodeType.ExprLessEqualsThan)),
        ];

        return true;
    }

    private static NodeAst BuildComparisonConjunct(NodeAst column, NodeAst constant, NodeType nodeType) =>
        new(
            nodeType,
            column,
            constant,
            extendedOne: null,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: null);

    private static bool TryAnalyzeColumnConstantComparison(
        NodeAst conjunct,
        Dictionary<string, ColumnValue>? parameters,
        out AnalyzedComparison? comparison)
    {
        comparison = null;

        string? op = TryGetComparisonOperator(conjunct.nodeType);
        if (op is null)
            return false;

        // Canonical form: column OP constant. Try this first so that column-vs-column
        // comparisons (where the right side is an identifier, not a constant) fall through
        // to the column-column analyzer instead of being mistaken for the mirrored form.
        if (conjunct.leftAst?.nodeType == NodeType.Identifier
            && conjunct.leftAst.yytext is not null
            && conjunct.rightAst is not null
            && TryGetConstant(conjunct.rightAst, parameters, out ColumnValue? rightConstant)
            && rightConstant is not null)
        {
            comparison = new AnalyzedComparison(conjunct.leftAst.yytext, op, rightConstant, conjunct);
            return true;
        }

        // Mirrored form: constant OP column (e.g. str_id('…') = id, 5 > age). Swap the sides
        // for index matching and flip the operator. The original conjunct is preserved so
        // residual evaluation is unaffected when the scan does not absorb the comparison.
        if (conjunct.rightAst?.nodeType == NodeType.Identifier
            && conjunct.rightAst.yytext is not null
            && conjunct.leftAst is not null
            && TryGetConstant(conjunct.leftAst, parameters, out ColumnValue? leftConstant)
            && leftConstant is not null)
        {
            comparison = new AnalyzedComparison(conjunct.rightAst.yytext, MirrorOperator(op), leftConstant, conjunct);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Flips a comparison operator when the column and constant operands are swapped, so that
    /// <c>constant OP column</c> matches an index the same way <c>column OP' constant</c> would.
    /// </summary>
    private static string MirrorOperator(string op)
    {
        return op switch
        {
            ">" => "<",
            ">=" => "<=",
            "<" => ">",
            "<=" => ">=",
            _ => op, // "=" and "!=" are symmetric
        };
    }

    private static bool TryAnalyzeColumnColumnComparison(
        NodeAst conjunct,
        out AnalyzedColumnComparison? comparison)
    {
        comparison = null;

        if (conjunct.leftAst?.nodeType != NodeType.Identifier || conjunct.leftAst.yytext is null)
            return false;

        if (conjunct.rightAst?.nodeType != NodeType.Identifier || conjunct.rightAst.yytext is null)
            return false;

        string? op = TryGetComparisonOperator(conjunct.nodeType);
        if (op is null)
            return false;

        comparison = new AnalyzedColumnComparison(
            conjunct.leftAst.yytext,
            op,
            conjunct.rightAst.yytext,
            conjunct);
        return true;
    }

    private static string? TryGetComparisonOperator(NodeType nodeType)
    {
        return nodeType switch
        {
            NodeType.ExprEquals => "=",
            NodeType.ExprNotEquals => "!=",
            NodeType.ExprGreaterThan => ">",
            NodeType.ExprGreaterEqualsThan => ">=",
            NodeType.ExprLessThan => "<",
            NodeType.ExprLessEqualsThan => "<=",
            _ => null,
        };
    }

    private static bool TryGetConstant(NodeAst nodeAst, Dictionary<string, ColumnValue>? parameters, out ColumnValue? columnValue)
    {
        try
        {
            columnValue = SqlExecutor.EvalExpr(nodeAst, new(), parameters);
            if (columnValue.Type != ColumnType.Null)
                return true;
        }
        catch (CamusDBException)
        {
        }

        columnValue = null;
        return false;
    }

    private static void ValidateFilter(QueryFilter filter)
    {
        if (string.IsNullOrEmpty(filter.ColumnName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Found empty or null column name in filters");

        if (string.IsNullOrEmpty(filter.Op))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Found empty or null operator in filters");

        switch (filter.Op)
        {
            case "=":
            case "!=":
            case ">":
            case ">=":
            case "<":
            case "<=":
                return;
            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown operator :" + filter.Op);
        }
    }

    private static NodeAst BuildFilterConjunct(QueryFilter filter)
    {
        NodeAst column = new(
            NodeType.Identifier,
            leftAst: null,
            rightAst: null,
            extendedOne: null,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: filter.ColumnName);

        NodeAst constant = BuildConstantAst(filter.Value);

        NodeType nodeType = filter.Op switch
        {
            "=" => NodeType.ExprEquals,
            "!=" => NodeType.ExprNotEquals,
            ">" => NodeType.ExprGreaterThan,
            ">=" => NodeType.ExprGreaterEqualsThan,
            "<" => NodeType.ExprLessThan,
            "<=" => NodeType.ExprLessEqualsThan,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown operator :" + filter.Op),
        };

        return new NodeAst(
            nodeType,
            column,
            constant,
            extendedOne: null,
            extendedTwo: null,
            extendedThree: null,
            extendedFour: null,
            extendedFive: null,
            yytext: null);
    }

    private static NodeAst BuildConstantAst(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Integer64 => new NodeAst(
                NodeType.Integer,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.LongValue.ToString()),
            ColumnType.Float64 => new NodeAst(
                NodeType.Float,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.FloatValue.ToString()),
            ColumnType.Bool => value.BoolValue ? NodeAst.True : NodeAst.False,
            ColumnType.String => new NodeAst(
                NodeType.String,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: $"\"{value.StrValue}\""),
            ColumnType.Id => new NodeAst(
                NodeType.ObjectIdLiteral,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.StrValue),
            ColumnType.Null => NodeAst.Null,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unsupported filter constant type: " + value.Type),
        };
    }
}
