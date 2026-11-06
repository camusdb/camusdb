
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class ExistsSubqueryAnalyzer
{
    public static bool IsCorrelated(SelectQuery innerQuery)
    {
        HashSet<string> innerAliases = CollectSourceAliases(innerQuery.Source);

        if (innerQuery.Where is null)
            return false;

        HashSet<string> references = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(innerQuery.Where.Expression, references);

        foreach (string reference in references)
        {
            if (ReferencesOuterScope(reference, innerAliases))
                return true;
        }

        return false;
    }

    /// <summary>
    /// True when an EXISTS body reads the outer row in any clause. An IN subquery uses
    /// <see cref="IsCorrelated"/>, because it returns its select list and refuses an outer column there.
    /// An EXISTS discards its select list, so <c>EXISTS (SELECT o.v FROM r i)</c> is valid.
    /// </summary>
    public static bool ExistsBodyReadsOuterRow(SelectQuery innerQuery)
    {
        if (IsCorrelated(innerQuery))
            return true;

        HashSet<string> innerAliases = CollectSourceAliases(innerQuery.Source);
        HashSet<string> references = new(StringComparer.Ordinal);

        foreach (ProjectionItem projection in innerQuery.Projections)
            QueryExpressionWalker.CollectColumnReferences(projection.Expression, references);

        if (innerQuery.GroupBy is not null)
            foreach (NodeAst expression in innerQuery.GroupBy)
                QueryExpressionWalker.CollectColumnReferences(expression, references);

        if (innerQuery.OrderBy is not null)
            foreach (OrderByItem item in innerQuery.OrderBy)
                QueryExpressionWalker.CollectColumnReferences(item.Expression, references);

        if (innerQuery.Having is not null)
            QueryExpressionWalker.CollectColumnReferences(innerQuery.Having.Expression, references);

        foreach (string reference in references)
        {
            if (ReferencesOuterScope(reference, innerAliases))
                return true;
        }

        return false;
    }

    public static bool ReferencesOuterScope(string identifier, IReadOnlySet<string> innerAliases)
    {
        if (TrySplitQualified(identifier, out string alias, out _))
            return !innerAliases.Contains(alias);

        return false;
    }

    public static HashSet<string> CollectSourceAliases(QuerySource source)
    {
        // Case-insensitive to match alias binding (QueryRowNameResolver): a reference
        // qualified as "R." must count as the inner alias "r", not as an outer-scope
        // correlation reference.
        HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);
        CollectSourceAliases(source, aliases);
        return aliases;
    }

    private static void CollectSourceAliases(QuerySource source, ISet<string> aliases)
    {
        switch (source)
        {
            case TableSource tableSource:
                aliases.Add(tableSource.Alias ?? tableSource.TableName);
                return;

            case DerivedTableSource derivedSource:
                aliases.Add(derivedSource.Alias);
                return;

            case JoinSource joinSource:
                CollectSourceAliases(joinSource.Left, aliases);
                CollectSourceAliases(joinSource.Right, aliases);
                return;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported subquery source: {source.GetType().Name}");
        }
    }

    private static bool TrySplitQualified(string identifier, out string alias, out string columnName)
    {
        int dotIndex = identifier.IndexOf('.');

        if (dotIndex <= 0 || dotIndex >= identifier.Length - 1)
        {
            alias = "";
            columnName = identifier;
            return false;
        }

        alias = identifier[..dotIndex];
        columnName = identifier[(dotIndex + 1)..];
        return true;
    }
}
