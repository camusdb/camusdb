
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
        // Pushdown below a join is only sound for INNER joins: pushing a WHERE conjunct into
        // the null-extending side of an outer join filters rows before null-extension and
        // changes the result (and removing it from the post-join filter loses the NULL checks).
        // Only Inner exists today; this guard makes the first outer-join implementation fail
        // safe (everything stays post-join) instead of silently returning wrong rows.
        if (ContainsNonInnerJoin(bound.Query.Source))
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

                if (referencedAliases.Count == 1)
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

    private static bool ContainsNonInnerJoin(QuerySource source) => source switch
    {
        JoinSource js => js.Kind != JoinKind.Inner
                         || ContainsNonInnerJoin(js.Left)
                         || ContainsNonInnerJoin(js.Right),
        _ => false,
    };

    /// <summary>
    /// Fallback for join trees containing a non-inner join: no per-source scan filters are
    /// derived (every alias maps to null = no pushed filter) and the whole WHERE stays a
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
