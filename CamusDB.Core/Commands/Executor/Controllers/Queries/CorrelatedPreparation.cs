/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The state of <see cref="ExistsSubqueryPreparer"/> for one query level: the outer sources that a correlated
/// subquery can read, and the registry that collects every prepared subquery of that level. All clauses of
/// the level (the WHERE and the select list of a SELECT, or the WHERE and the SET values of an UPDATE) go
/// through one instance, so their hidden names never collide, and one query ticket carries the registry.
/// </summary>
internal sealed class CorrelatedPreparation
{
    public CorrelatedPreparation(
        NestedSelectBinder nested,
        IReadOnlyList<BoundTableSource> outerTables,
        IReadOnlyList<BoundDerivedTableSource> outerDerived)
    {
        Nested = nested;
        OuterTables = outerTables;
        OuterDerived = outerDerived;
        OuterScope = OuterNameScope.Of(outerTables, outerDerived);
    }

    /// <summary>The prepared subqueries, or null while the level has none.</summary>
    public ExistsSubqueryRegistry? Registry { get; set; }

    /// <summary>Binds the derived tables of a correlated body's FROM through the pipeline.</summary>
    public NestedSelectBinder Nested { get; }

    /// <summary>The table sources of the outer query.</summary>
    public IReadOnlyList<BoundTableSource> OuterTables { get; }

    /// <summary>The derived sources of the outer query.</summary>
    public IReadOnlyList<BoundDerivedTableSource> OuterDerived { get; }

    /// <summary>The names of the outer query, which a body can read.</summary>
    public OuterNameScope OuterScope { get; }
}
