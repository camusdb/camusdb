
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
            new Dictionary<string, NodeAst?>();

        public NodeAst? PostJoinFilter { get; init; }
    }

    public static Result Analyze(BoundSelectQuery bound, NodeAst? where)
    {
        Dictionary<string, List<NodeAst>> conjunctsByAlias = new(StringComparer.Ordinal);

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

        Dictionary<string, NodeAst?> scanFilters = new(conjunctsByAlias.Count, StringComparer.Ordinal);

        foreach (KeyValuePair<string, List<NodeAst>> entry in conjunctsByAlias)
            scanFilters[entry.Key] = PredicateAnalyzer.CombineConjuncts(entry.Value);

        return new Result
        {
            ScanFiltersByAlias = scanFilters,
            PostJoinFilter = PredicateAnalyzer.CombineConjuncts(postJoinConjuncts),
        };
    }

    private static HashSet<string> CollectReferencedAliases(NodeAst node, BoundSelectQuery bound)
    {
        HashSet<string> aliases = new(StringComparer.Ordinal);
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
            if (column.Name == columnName && SchemaElementStateRules.IsReadable(column))
                return true;
        }

        return false;
    }
}
