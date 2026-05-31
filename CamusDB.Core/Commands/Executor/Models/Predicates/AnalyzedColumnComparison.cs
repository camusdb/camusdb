
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Predicates;

/// <summary>
/// A column-vs-column comparison extracted from an ANDed WHERE conjunct.
/// Not used for index selection on single-table scans; reserved for execution filtering
/// and future join/subquery planning.
/// </summary>
public sealed class AnalyzedColumnComparison
{
    public string LeftColumnName { get; }

    public string Operator { get; }

    public string RightColumnName { get; }

    /// <summary>Original AND conjunct this comparison came from.</summary>
    public NodeAst Conjunct { get; }

    public AnalyzedColumnComparison(string leftColumnName, string op, string rightColumnName, NodeAst conjunct)
    {
        LeftColumnName = leftColumnName;
        Operator = op;
        RightColumnName = rightColumnName;
        Conjunct = conjunct;
    }
}
