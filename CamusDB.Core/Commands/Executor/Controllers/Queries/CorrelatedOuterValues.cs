/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Reads, from one outer row, the value of each outer name that a correlated subquery reads. The values
/// come in the order of <see cref="Names"/>, one value for each distinct name.
///
/// <para>The body form (<see cref="CorrelatedSubqueryBody"/>) passes these values to its run as hidden
/// parameters, and <see cref="CorrelatedResultCache{T}"/> uses the same values as its key. So the key holds
/// exactly what the body can read of the outer row: two rows with the same values give the body the same
/// input.</para>
///
/// <para>The resolver holds only the outer sources, so a bare name resolves to the outer query, as it does
/// for the body. Its memo is filled when this object is built and only read for each row.</para>
/// </summary>
internal sealed class CorrelatedOuterValues
{
    private readonly NodeAst[] names;

    private readonly IReadOnlyList<BoundTableSource> outerSources;

    private readonly IReadOnlyList<BoundDerivedTableSource> outerDerivedSources;

    private readonly QueryRowNameResolver outerRowNames;

    /// <summary>One reference node for each distinct outer name, in the order of the values.</summary>
    public IReadOnlyList<NodeAst> Names => names;

    private CorrelatedOuterValues(
        NodeAst[] names,
        IReadOnlyList<BoundTableSource> outerSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        QueryRowNameResolver outerRowNames)
    {
        this.names = names;
        this.outerSources = outerSources;
        this.outerDerivedSources = outerDerivedSources;
        this.outerRowNames = outerRowNames;
    }

    /// <summary>
    /// Builds the reader for <paramref name="outerReferences"/>, one value for each distinct name (case does
    /// not matter, as for a column name). Each name is checked against the outer sources here.
    /// </summary>
    public static CorrelatedOuterValues Create(
        IReadOnlyList<NodeAst> outerReferences,
        IReadOnlyList<BoundTableSource> outerSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        QueryRowNameResolver outerRowNames)
    {
        HashSet<string> seen = new(StringComparer.OrdinalIgnoreCase);
        List<NodeAst> names = new(outerReferences.Count);

        foreach (NodeAst reference in outerReferences)
        {
            string name = reference.yytext!;

            if (!seen.Add(name))
                continue;

            // Also fills the memo of the resolver, which each row then reads.
            outerRowNames.ValidateColumnReference(name);
            names.Add(reference);
        }

        return new CorrelatedOuterValues([.. names], outerSources, outerDerivedSources, outerRowNames);
    }

    /// <summary>The value of each name in <paramref name="outerRow"/>, in the order of <see cref="Names"/>.</summary>
    public ColumnValue[] For(IReadOnlyDictionary<string, ColumnValue> outerRow, Dictionary<string, ColumnValue>? statementParameters)
    {
        Dictionary<string, ColumnValue> merged = CorrelatedRowMerger.MergeOuterForEvaluation(
            outerRow, outerSources, outerDerivedSources);

        ColumnValue[] values = new ColumnValue[names.Length];

        for (int i = 0; i < names.Length; i++)
            values[i] = SqlExecutor.EvalExpr(names[i], merged, statementParameters, outerRowNames);

        return values;
    }
}
