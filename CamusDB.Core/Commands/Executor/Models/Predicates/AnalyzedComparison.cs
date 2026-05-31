
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Predicates;

/// <summary>
/// A column-vs-constant comparison extracted from an ANDed WHERE conjunct.
/// </summary>
public sealed class AnalyzedComparison
{
    public string ColumnName { get; }

    public string Operator { get; }

    public ColumnValue Constant { get; }

    /// <summary>Original AND conjunct this comparison came from.</summary>
    public NodeAst Conjunct { get; }

    public AnalyzedComparison(string columnName, string op, ColumnValue constant, NodeAst conjunct)
    {
        ColumnName = columnName;
        Operator = op;
        Constant = constant;
        Conjunct = conjunct;
    }
}
