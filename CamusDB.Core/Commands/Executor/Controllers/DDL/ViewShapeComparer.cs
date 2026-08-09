
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Enforces the rule that <c>CREATE OR REPLACE VIEW</c> may only <b>append</b> columns: the existing
/// column names, types, and order must be preserved exactly.
///
/// <para>This is not pedantry. A dependent view binds to the column names it saw at its own creation;
/// a cached plan binds to positions; a client binds to both. Silently changing any of them would
/// change what those already-created objects mean, with no statement having been issued against
/// them — and the change would surface later, somewhere else, as wrong data rather than as an error.
/// Dropping and recreating the view forces the dependents into the open, which is exactly the point.
/// PostgreSQL enforces the identical rule for the identical reason.</para>
/// </summary>
internal static class ViewShapeComparer
{
    public static void RequireCompatible(string viewName, ViewDefinition existing, ViewDefinition replacement)
    {
        IReadOnlyList<ViewColumnSchema> before = existing.Columns ?? [];
        IReadOnlyList<ViewColumnSchema> after = replacement.Columns ?? [];

        if (after.Count < before.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.CannotChangeViewShape,
                $"Cannot drop columns from view '{viewName}': it has {before.Count} column(s) and the " +
                $"replacement body returns {after.Count}. Drop and recreate the view instead.");

        for (int i = 0; i < before.Count; i++)
        {
            if (!string.Equals(before[i].Name, after[i].Name, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.CannotChangeViewShape,
                    $"Cannot change name of view column '{before[i].Name}' to '{after[i].Name}' in view " +
                    $"'{viewName}'. Drop and recreate the view instead.");

            if (before[i].Type != after[i].Type)
                throw new CamusDBException(
                    CamusDBErrorCodes.CannotChangeViewShape,
                    $"Cannot change data type of view column '{before[i].Name}' from {before[i].Type} to " +
                    $"{after[i].Type} in view '{viewName}'. Drop and recreate the view instead.");
        }
    }
}

/// <summary>
/// Answers the two dependency questions view DDL asks: "would dropping this orphan anything?" and
/// "would creating this close a cycle?".
///
/// <para>Both walk <see cref="ViewDefinition.DependsOnTableIds"/> / <c>DependsOnViewIds</c>, which
/// hold immutable ids. Resolving through names instead would give wrong answers the moment anything
/// was renamed — which is precisely when a dependency check matters most.</para>
/// </summary>
internal static class ViewDependencyGraph
{
    /// <summary>Names of the views that read <paramref name="relationId"/>, directly only.</summary>
    public static List<string> DirectDependentsOfTable(Schema schema, string relationId)
    {
        List<string> dependents = [];

        foreach ((string name, ViewSchema view) in schema.Views)
        {
            if (view.Definition?.DependsOnTableIds?.Contains(relationId, StringComparer.Ordinal) == true)
                dependents.Add(name);
        }

        return dependents;
    }

    /// <summary>Names of the views that read view <paramref name="viewId"/>, directly only.</summary>
    public static List<string> DirectDependentsOfView(Schema schema, string viewId)
    {
        List<string> dependents = [];

        foreach ((string name, ViewSchema view) in schema.Views)
        {
            if (view.Definition?.DependsOnViewIds?.Contains(viewId, StringComparer.Ordinal) == true)
                dependents.Add(name);
        }

        return dependents;
    }

    /// <summary>
    /// Every view that depends on <paramref name="viewId"/>, transitively, ordered dependents-first
    /// so dropping them in order never leaves a view whose dependency is already gone.
    /// </summary>
    public static List<string> TransitiveDependentsOfView(Schema schema, string viewId)
    {
        List<string> ordered = [];
        HashSet<string> seen = new(StringComparer.OrdinalIgnoreCase);

        Visit(viewId);
        return ordered;

        void Visit(string id)
        {
            foreach (string dependentName in DirectDependentsOfView(schema, id))
            {
                if (!seen.Add(dependentName))
                    continue;

                if (schema.Views.TryGetValue(dependentName, out ViewSchema? dependent) && dependent.Id is not null)
                    Visit(dependent.Id);

                // Appended after recursing, so a view is always listed after everything that depends
                // on it — which is the order a cascading drop has to use.
                ordered.Add(dependentName);
            }
        }
    }

    /// <summary>
    /// Rejects a definition that would make <paramref name="viewId"/> reachable from itself.
    /// </summary>
    /// <remarks>
    /// Checked at DDL time so the runtime expansion-depth cap stays a backstop rather than the
    /// defense. <paramref name="viewId"/> is the id the view will have — on a replace that is the
    /// existing id, which is exactly why a replace preserves it: minting a new one would make a
    /// self-referencing replacement look acyclic here and then recurse forever at read time.
    /// </remarks>
    public static void RequireAcyclic(Schema schema, string viewId, string viewName, ViewDefinition definition)
    {
        HashSet<string> visited = new(StringComparer.Ordinal);
        Stack<string> pending = new(definition.DependsOnViewIds ?? []);

        while (pending.Count > 0)
        {
            string id = pending.Pop();

            if (string.Equals(id, viewId, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewRecursionDetected,
                    $"Infinite recursion detected in the definition of view '{viewName}'");

            if (!visited.Add(id))
                continue;

            foreach (ViewSchema candidate in schema.Views.Values)
            {
                if (!string.Equals(candidate.Id, id, StringComparison.Ordinal))
                    continue;

                foreach (string next in candidate.Definition?.DependsOnViewIds ?? [])
                    pending.Push(next);
            }
        }
    }
}
