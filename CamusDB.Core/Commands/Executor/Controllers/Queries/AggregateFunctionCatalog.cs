/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Frozen;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The one place that knows which function names are aggregates and what the engine may do with each:
/// the built-ins the query pipeline computes (<c>count</c>, <c>sum</c>, <c>avg</c>, <c>min</c>,
/// <c>max</c>), the names the engine reserves as aggregates, and the aggregates of the C# extensions
/// installed in the statement's database. The classifier, the aggregator, the planner's decomposition
/// and partial-aggregation rules, CHECK validation and result-type inference all ask here instead of
/// keeping their own name lists.
///
/// <para><b>Built-ins keep their fast paths.</b> A built-in keeps its own
/// <see cref="QueryAggregationType"/>, so its accumulator and the single-aggregate fast paths are
/// unchanged. An extension aggregate is <see cref="QueryAggregationType.Extension"/>, and the
/// aggregator runs it through the general workspace with one state object per group.</para>
///
/// <para><b>Extension names resolve per statement.</b> Like a scalar extension function, an extension
/// aggregate resolves through the ambient <see cref="ExtensionFunctionScope"/>: the extensions installed
/// in the database the statement runs on, as this engine loaded them. A flow with no scope sees
/// built-ins only. Built-in lookups never read the scope, and a process that never loaded an extension
/// never reads it at all (<see cref="ExtensionRegistry.AnyLoaded"/>).</para>
///
/// <para><b>Never shipped.</b> <see cref="IsDecomposable"/> and <see cref="IsPartialMergeable"/> answer
/// for built-ins only. An extension aggregate therefore always runs on the node that runs the statement,
/// over the gathered rows: its state has no wire form, and a peer could run another build.</para>
/// </summary>
internal static class AggregateFunctionCatalog
{
    private static readonly FrozenDictionary<string, QueryAggregationType> BuiltIns =
        new Dictionary<string, QueryAggregationType>(StringComparer.OrdinalIgnoreCase)
        {
            ["count"] = QueryAggregationType.Count,
            ["sum"] = QueryAggregationType.Sum,
            ["avg"] = QueryAggregationType.Average,
            ["min"] = QueryAggregationType.Min,
            ["max"] = QueryAggregationType.Max,
        }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Names that some part of the engine treats as aggregates without computing them in the query
    /// pipeline (the CHECK validator refuses them). An extension may not define a member under one of
    /// them, because a call would never reach the member.
    /// </summary>
    private static readonly FrozenSet<string> ReservedOnly =
        new[] { "count_distinct", "group_concat", "string_agg", "array_agg" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>Built-ins whose per-partition results merge by re-aggregation, for the local-plus-merge split.</summary>
    private static readonly FrozenSet<string> Decomposable =
        new[] { "count", "sum", "min", "max" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>Built-ins that the per-span partial aggregation below a gather can compute (AVG as a SUM and COUNT pair).</summary>
    private static readonly FrozenSet<string> PartialMergeable =
        new[] { "count", "sum", "min", "max", "avg" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>True for a built-in aggregate the query pipeline computes.</summary>
    public static bool IsBuiltIn(string name) => BuiltIns.ContainsKey(name);

    /// <summary>
    /// True when a call to <paramref name="name"/> is an aggregate in this flow: a built-in, or an
    /// aggregate of an extension installed in the statement's database (bound on this node or not).
    /// </summary>
    public static bool IsAggregate(string name) => BuiltIns.ContainsKey(name) || IsExtensionAggregate(name);

    /// <summary>True when <paramref name="name"/> is an aggregate of an extension installed in the statement's database.</summary>
    public static bool IsExtensionAggregate(string name) =>
        ExtensionRegistry.AnyLoaded && (ExtensionFunctionScope.CurrentInstalled?.HasAggregate(name) ?? false);

    /// <summary>
    /// Every name the engine treats as an aggregate whatever the flow: the built-ins and the reserved
    /// names. The extension loader refuses a member under one of them.
    /// </summary>
    public static bool IsReservedName(string name) => BuiltIns.ContainsKey(name) || ReservedOnly.Contains(name);

    /// <summary>
    /// True when a CHECK constraint or a column DEFAULT must refuse a call to <paramref name="name"/> as
    /// an aggregate: a reserved name, or an extension aggregate installed in the statement's database.
    /// </summary>
    public static bool IsRefusedOutsideAggregation(string name) => IsReservedName(name) || IsExtensionAggregate(name);

    /// <summary>The accumulator kind of a call: a built-in's own kind, <see cref="QueryAggregationType.Extension"/>, or None.</summary>
    public static QueryAggregationType GetAggregationType(string name)
    {
        if (BuiltIns.TryGetValue(name, out QueryAggregationType type))
            return type;

        return IsExtensionAggregate(name) ? QueryAggregationType.Extension : QueryAggregationType.None;
    }

    /// <summary>
    /// The extension aggregate <paramref name="name"/> names. Throws
    /// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> when it is installed but this node does
    /// not load its build, and "function not found" when nothing defines it.
    /// </summary>
    public static ExtensionAggregate ResolveExtension(string name)
    {
        if (ExtensionFunctionScope.CurrentInstalled is { } installed && installed.TryGetAggregate(name, out ExtensionAggregate aggregate))
            return aggregate;

        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Function not found '" + name + "'");
    }

    /// <summary>True for a built-in whose per-partition results merge by re-aggregation (COUNT, SUM, MIN, MAX).</summary>
    public static bool IsDecomposable(string? name) => name is not null && Decomposable.Contains(name);

    /// <summary>True for a built-in the per-span partial aggregation can compute (COUNT, SUM, MIN, MAX, AVG).</summary>
    public static bool IsPartialMergeable(string? name) => name is not null && PartialMergeable.Contains(name);
}
