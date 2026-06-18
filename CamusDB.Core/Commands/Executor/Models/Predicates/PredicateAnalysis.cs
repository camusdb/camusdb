
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Predicates;

/// <summary>
/// Output of <see cref="Controllers.Queries.PredicateAnalyzer"/> for a WHERE clause.
/// </summary>
public sealed class PredicateAnalysis
{
    public static PredicateAnalysis Empty { get; } = new([], [], [], []);

    /// <summary>Column-vs-constant comparisons usable for index selection.</summary>
    public IReadOnlyList<AnalyzedComparison> IndexableComparisons { get; }

    /// <summary>Column-vs-column comparisons extracted for filtering and future join planning.</summary>
    public IReadOnlyList<AnalyzedColumnComparison> ColumnComparisons { get; }

    public IReadOnlyList<NodeAst> ResidualConjuncts { get; }

    /// <summary>
    /// IN-list predicates of the form <c>column IN (v1, v2, …)</c> where every list item is a
    /// constant or resolved parameter. These are candidates for index-driven multi-seek.
    /// When no usable index is found, <see cref="Controllers.Queries.PredicateAnalyzer.BuildExecutionFilter"/>
    /// folds them back into the per-row filter as residual conjuncts.
    /// </summary>
    public IReadOnlyList<AnalyzedInList> InListComparisons { get; }

    public PredicateAnalysis(
        IReadOnlyList<AnalyzedComparison> indexableComparisons,
        IReadOnlyList<AnalyzedColumnComparison> columnComparisons,
        IReadOnlyList<NodeAst> residualConjuncts,
        IReadOnlyList<AnalyzedInList>? inListComparisons = null)
    {
        IndexableComparisons = indexableComparisons;
        ColumnComparisons = columnComparisons;
        ResidualConjuncts = residualConjuncts;
        InListComparisons = inListComparisons ?? [];
    }
}
