/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Predicates;

/// <summary>
/// A pattern predicate on one column, for the cardinality estimate only: <c>column LIKE 'p'</c>,
/// <c>column NOT LIKE 'p'</c>, <c>column ILIKE 'p'</c>, or an <c>OR</c> chain of such patterns on
/// the same column, with constant patterns. The conjunct stays in
/// <see cref="PredicateAnalysis.ResidualConjuncts"/>, so the execution filter is unchanged; the
/// estimator prices it instead of leaving it at the fallback selectivity.
/// </summary>
/// <param name="ColumnName">The column, as written (an alias prefix is possible).</param>
/// <param name="Patterns">The patterns of the <c>OR</c> chain; one for a single predicate.</param>
/// <param name="Negated">True for <c>NOT</c> over the pattern or over the whole chain.</param>
/// <param name="IgnoreCase">True for <c>ILIKE</c>.</param>
/// <param name="Conjunct">The original conjunct.</param>
public sealed record AnalyzedPattern(
    string ColumnName,
    IReadOnlyList<string> Patterns,
    bool Negated,
    bool IgnoreCase,
    NodeAst Conjunct);
