
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
    public static PredicateAnalysis Empty { get; } = new([], [], []);

    /// <summary>Column-vs-constant comparisons usable for index selection.</summary>
    public IReadOnlyList<AnalyzedComparison> IndexableComparisons { get; }

    /// <summary>Column-vs-column comparisons extracted for filtering and future join planning.</summary>
    public IReadOnlyList<AnalyzedColumnComparison> ColumnComparisons { get; }

    public IReadOnlyList<NodeAst> ResidualConjuncts { get; }

    public PredicateAnalysis(
        IReadOnlyList<AnalyzedComparison> indexableComparisons,
        IReadOnlyList<AnalyzedColumnComparison> columnComparisons,
        IReadOnlyList<NodeAst> residualConjuncts)
    {
        IndexableComparisons = indexableComparisons;
        ColumnComparisons = columnComparisons;
        ResidualConjuncts = residualConjuncts;
    }
}
