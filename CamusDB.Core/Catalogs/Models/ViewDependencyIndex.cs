/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Immutable;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The reverse edges of every stored view and materialized-view definition: for each relation, view
/// and column, the ids of the definitions that read it.
///
/// <para><b>Derived from the forward lists.</b> The forward lists on <see cref="ViewDefinition"/> —
/// <c>DependsOnTableIds</c>, <c>DependsOnViewIds</c>, <c>DependsOnColumnIds</c> — are what is stored
/// and replicated. This index is rebuilt from them whenever a schema is loaded, and is changed only
/// inside the apply that changes a definition, under the schema lock, so it can never describe a
/// definition that is not there. Rebuilding it from the forward lists gives the identical index
/// (<see cref="Build"/>), so keeping it only in memory is safe.</para>
///
/// <para><b>Purpose.</b> "Which views read this relation" used to be a scan of every definition
/// in the database, and a cascading drop, a rename and a cycle check repeated that scan per vertex —
/// quadratic in the catalog. With the index each question costs what its answer contains.</para>
///
/// <para><b>Keyed by id on both sides.</b> A rename changes no edge, so it changes nothing here; a
/// caller that needs names resolves the ids it got back.</para>
///
/// <para>Immutable: a change returns a new index, so a reader holding no lock always sees a complete
/// one. Adding an edge twice, or removing one that is absent, changes nothing — so a replay cannot
/// double-add one.</para>
/// </summary>
public sealed class ViewDependencyIndex
{
    public static readonly ViewDependencyIndex Empty = new(
        ImmutableDictionary<string, ImmutableHashSet<string>>.Empty.WithComparers(StringComparer.Ordinal),
        ImmutableDictionary<string, ImmutableHashSet<string>>.Empty.WithComparers(StringComparer.Ordinal),
        ImmutableDictionary<string, ImmutableHashSet<string>>.Empty.WithComparers(StringComparer.Ordinal));

    private readonly ImmutableDictionary<string, ImmutableHashSet<string>> tableEdges;

    private readonly ImmutableDictionary<string, ImmutableHashSet<string>> viewEdges;

    private readonly ImmutableDictionary<string, ImmutableHashSet<string>> columnEdges;

    private ViewDependencyIndex(
        ImmutableDictionary<string, ImmutableHashSet<string>> tableEdges,
        ImmutableDictionary<string, ImmutableHashSet<string>> viewEdges,
        ImmutableDictionary<string, ImmutableHashSet<string>> columnEdges)
    {
        this.tableEdges = tableEdges;
        this.viewEdges = viewEdges;
        this.columnEdges = columnEdges;
    }

    /// <summary>Ids of the definitions that list <paramref name="relationId"/> in <c>DependsOnTableIds</c>.</summary>
    public IReadOnlyCollection<string> DependentsOfRelation(string relationId)
        => tableEdges.TryGetValue(relationId, out ImmutableHashSet<string>? ids) ? ids : [];

    /// <summary>Ids of the definitions that list <paramref name="viewId"/> in <c>DependsOnViewIds</c>.</summary>
    public IReadOnlyCollection<string> DependentsOfView(string viewId)
        => viewEdges.TryGetValue(viewId, out ImmutableHashSet<string>? ids) ? ids : [];

    /// <summary>Ids of the definitions that list <paramref name="columnId"/> in <c>DependsOnColumnIds</c>.</summary>
    public IReadOnlyCollection<string> DependentsOfColumn(string columnId)
        => columnEdges.TryGetValue(columnId, out ImmutableHashSet<string>? ids) ? ids : [];

    /// <summary>
    /// The index with the edges of <paramref name="before"/> taken out and the edges of
    /// <paramref name="after"/> put in, for the definition <paramref name="dependentId"/>. Either may be
    /// null: a create has nothing before, a drop nothing after.
    /// </summary>
    public ViewDependencyIndex Replace(string dependentId, ViewDefinition? before, ViewDefinition? after)
    {
        if (ReferenceEquals(before, after))
            return this;

        return new(
            Move(tableEdges, dependentId, before?.DependsOnTableIds, after?.DependsOnTableIds),
            Move(viewEdges, dependentId, before?.DependsOnViewIds, after?.DependsOnViewIds),
            Move(columnEdges, dependentId, before?.DependsOnColumnIds, after?.DependsOnColumnIds));
    }

    /// <summary>The index of every definition in <paramref name="definitions"/>, from their forward lists.</summary>
    public static ViewDependencyIndex Build(IEnumerable<(string Id, ViewDefinition Definition)> definitions)
    {
        ViewDependencyIndex index = Empty;

        foreach ((string id, ViewDefinition definition) in definitions)
            index = index.Replace(id, null, definition);

        return index;
    }

    /// <summary>
    /// True when both indexes hold the same edges. Used by the check that an index maintained delta by
    /// delta matches one rebuilt from the forward lists.
    /// </summary>
    public bool SameEdgesAs(ViewDependencyIndex other)
        => SameEdges(tableEdges, other.tableEdges)
           && SameEdges(viewEdges, other.viewEdges)
           && SameEdges(columnEdges, other.columnEdges);

    private static bool SameEdges(
        ImmutableDictionary<string, ImmutableHashSet<string>> left,
        ImmutableDictionary<string, ImmutableHashSet<string>> right)
    {
        if (left.Count != right.Count)
            return false;

        foreach ((string key, ImmutableHashSet<string> ids) in left)
        {
            if (!right.TryGetValue(key, out ImmutableHashSet<string>? other) || !ids.SetEquals(other))
                return false;
        }

        return true;
    }

    private static ImmutableDictionary<string, ImmutableHashSet<string>> Move(
        ImmutableDictionary<string, ImmutableHashSet<string>> edges,
        string dependentId,
        List<string>? removed,
        List<string>? added)
    {
        foreach (string target in removed ?? [])
        {
            if (!edges.TryGetValue(target, out ImmutableHashSet<string>? ids))
                continue;

            ImmutableHashSet<string> remaining = ids.Remove(dependentId);
            edges = remaining.IsEmpty ? edges.Remove(target) : edges.SetItem(target, remaining);
        }

        foreach (string target in added ?? [])
        {
            ImmutableHashSet<string> ids = edges.TryGetValue(target, out ImmutableHashSet<string>? existing)
                ? existing
                : ImmutableHashSet<string>.Empty.WithComparer(StringComparer.Ordinal);

            edges = edges.SetItem(target, ids.Add(dependentId));
        }

        return edges;
    }
}
