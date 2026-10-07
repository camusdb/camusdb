
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Splits join WHERE clauses into per-source scan filters and post-join residuals.
///
/// <para>A single-alias conjunct is pushed to that alias's scan only when the alias is not
/// <b>null-extended</b>: an alias is null-extended when it sits in the right subtree of any
/// <see cref="JoinKind.LeftOuter"/> join on its path to the root. Pushing a conjunct below the
/// outer join on that side would filter rows before padding and turn the join back into an inner
/// join with no error; a conjunct on the preserved side filters the same rows either way, so it is
/// pushed as for an inner join. Every multi-alias conjunct stays post-join, and a join kind this
/// class does not know gets no pushdown at all (<see cref="AnalyzeWithoutPushdown"/>), which is
/// always correct and never optimal.</para>
/// </summary>
internal static class JoinPredicatePushdown
{
    public sealed class Result
    {
        public IReadOnlyDictionary<string, NodeAst?> ScanFiltersByAlias { get; init; } =
            new Dictionary<string, NodeAst?>(StringComparer.OrdinalIgnoreCase);

        public NodeAst? PostJoinFilter { get; init; }
    }

    public static Result Analyze(BoundSelectQuery bound, NodeAst? where)
    {
        HashSet<string> nullExtended = new(StringComparer.OrdinalIgnoreCase);

        if (!TryCollectNullExtendedAliases(bound.Query.Source, nullExtended, underNullExtendingSide: false))
            return AnalyzeWithoutPushdown(bound, where);

        Dictionary<string, List<NodeAst>> conjunctsByAlias = new(StringComparer.OrdinalIgnoreCase);

        foreach (BoundTableSource source in bound.Sources)
            conjunctsByAlias[source.Alias] = new List<NodeAst>();

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
            conjunctsByAlias[source.Alias] = new List<NodeAst>();

        List<NodeAst> postJoinConjuncts = new();

        if (where is not null)
        {
            List<NodeAst> conjuncts = new();
            PredicateAnalyzer.CollectAndConjuncts(where, conjuncts);

            foreach (NodeAst conjunct in conjuncts)
            {
                HashSet<string> referencedAliases = CollectReferencedAliases(conjunct, bound);

                if (referencedAliases.Count == 1 && !nullExtended.Contains(referencedAliases.First()))
                    conjunctsByAlias[referencedAliases.First()].Add(conjunct);
                else
                    postJoinConjuncts.Add(conjunct);
            }
        }

        Dictionary<string, NodeAst?> scanFilters = new(conjunctsByAlias.Count, StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, List<NodeAst>> entry in conjunctsByAlias)
            scanFilters[entry.Key] = PredicateAnalyzer.CombineConjuncts(entry.Value);

        return new Result
        {
            ScanFiltersByAlias = scanFilters,
            PostJoinFilter = PredicateAnalyzer.CombineConjuncts(postJoinConjuncts),
        };
    }

    /// <summary>
    /// Walks the source tree and adds every alias that is null-extended (see the class summary).
    /// Returns false when a join kind is not one this class knows how to place, so the caller can
    /// fall back to no pushdown at all instead of guessing.
    /// </summary>
    private static bool TryCollectNullExtendedAliases(QuerySource source, HashSet<string> nullExtended, bool underNullExtendingSide)
    {
        switch (source)
        {
            case TableSource ts:
                if (underNullExtendingSide)
                    nullExtended.Add(ts.Alias ?? ts.TableName);
                return true;

            case DerivedTableSource ds:
                if (underNullExtendingSide)
                    nullExtended.Add(ds.Alias);
                return true;

            case JoinSource js:
                if (js.Kind is not (JoinKind.Inner or JoinKind.LeftOuter))
                    return false;

                return TryCollectNullExtendedAliases(js.Left, nullExtended, underNullExtendingSide)
                    && TryCollectNullExtendedAliases(js.Right, nullExtended, underNullExtendingSide || js.Kind == JoinKind.LeftOuter);

            default:
                return false;
        }
    }

    /// <summary>
    /// Fallback for a join tree with a kind the walker does not know: no per-source scan filters
    /// are derived (every alias maps to null = no pushed filter) and the whole WHERE stays a
    /// post-join residual — always correct, never optimal.
    /// </summary>
    private static Result AnalyzeWithoutPushdown(BoundSelectQuery bound, NodeAst? where)
    {
        Dictionary<string, NodeAst?> scanFilters = new(StringComparer.OrdinalIgnoreCase);

        foreach (BoundTableSource source in bound.Sources)
            scanFilters[source.Alias] = null;

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
            scanFilters[source.Alias] = null;

        return new Result
        {
            ScanFiltersByAlias = scanFilters,
            PostJoinFilter = where,
        };
    }

    private static HashSet<string> CollectReferencedAliases(NodeAst node, BoundSelectQuery bound)
    {
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);
        WalkIdentifiers(node, bound, aliases);
        return aliases;
    }

    private static void WalkIdentifiers(NodeAst? node, BoundSelectQuery bound, HashSet<string> aliases)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.Identifier && node.yytext is not null)
        {
            AddIdentifierAlias(node.yytext, bound, aliases);
            return;
        }

        WalkIdentifiers(node.leftAst, bound, aliases);
        WalkIdentifiers(node.rightAst, bound, aliases);
        WalkIdentifiers(node.extendedOne, bound, aliases);
        WalkIdentifiers(node.extendedTwo, bound, aliases);
        WalkIdentifiers(node.extendedThree, bound, aliases);
        WalkIdentifiers(node.extendedFour, bound, aliases);
        WalkIdentifiers(node.extendedFive, bound, aliases);
    }

    private static void AddIdentifierAlias(string identifier, BoundSelectQuery bound, HashSet<string> aliases)
    {
        int dotIndex = identifier.IndexOf('.');

        if (dotIndex > 0 && dotIndex < identifier.Length - 1)
        {
            aliases.Add(identifier[..dotIndex]);
            return;
        }

        List<string> owners = new();

        foreach (BoundTableSource source in bound.Sources)
        {
            if (SourceHasColumn(source, identifier))
                owners.Add(source.Alias);
        }

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
        {
            if (source.HasColumn(identifier))
                owners.Add(source.Alias);
        }

        if (owners.Count == 1)
            aliases.Add(owners[0]);
    }

    private static bool SourceHasColumn(BoundTableSource source, string columnName)
    {
        foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(column))
                return true;
        }

        return false;
    }
}
