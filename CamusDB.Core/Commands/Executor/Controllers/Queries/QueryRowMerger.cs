
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Merges scanned/joined rows using qualified keys per QP1.1 / <see cref="BoundRow"/> rules.
/// </summary>
internal static class QueryRowMerger
{
    public static Dictionary<string, ColumnValue> QualifyRow(
        IReadOnlyDictionary<string, ColumnValue> row,
        string alias)
    {
        Dictionary<string, ColumnValue> qualified = new(row.Count);

        foreach (KeyValuePair<string, ColumnValue> entry in row)
        {
            string key = IsQualifiedKey(entry.Key)
                ? entry.Key
                : QueryRowNameResolver.FormatQualifiedKey(alias, entry.Key);

            qualified[key] = entry.Value;
        }

        return qualified;
    }

    public static Dictionary<string, ColumnValue> MergeRows(
        IReadOnlyDictionary<string, ColumnValue> leftRow,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias)
    {
        Dictionary<string, ColumnValue> merged = new(leftRow);

        foreach (KeyValuePair<string, ColumnValue> entry in rightRow)
        {
            string key = IsQualifiedKey(entry.Key)
                ? entry.Key
                : QueryRowNameResolver.FormatQualifiedKey(rightAlias, entry.Key);

            if (merged.ContainsKey(key))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column collision on merged join row key '{key}'");
            }

            merged[key] = entry.Value;
        }

        return merged;
    }

    internal static bool IsQualifiedKey(string key) => key.Contains('.');
}