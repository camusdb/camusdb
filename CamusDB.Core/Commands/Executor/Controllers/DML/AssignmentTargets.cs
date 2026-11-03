
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// The one rule for a statement or request that gives values to columns: each target column gets
/// at most one value. A repeated target is refused with <see cref="CamusDBErrorCodes.DuplicateColumn"/>
/// before anything is written, as PostgreSQL refuses <c>UPDATE t SET a = 1, a = 2</c>.
///
/// <para><b>Why a refusal and not "the last value wins".</b> The values are kept in a dictionary keyed
/// by column, so a second assignment used to replace the first without a trace: the statement reported
/// success and wrote a value the author may not have meant. Two values for one column have no single
/// correct reading, so the statement is ambiguous and is refused.</para>
///
/// <para><b>Name comparison.</b> Every form (an UPDATE or ON CONFLICT DO UPDATE SET list, an INSERT
/// column list, the value map of a typed row) resolves a name to its column case-insensitively, and
/// two columns of one table never differ only in case. So two names that differ only in case reach the
/// same column and count as a repeat.</para>
/// </summary>
internal static class AssignmentTargets
{
    /// <summary>
    /// Builds the SET list of an UPDATE or an ON CONFLICT DO UPDATE, keyed case-insensitively by
    /// column, and refuses a column that is assigned more than once.
    /// </summary>
    internal static Dictionary<string, NodeAst> ToSetList(List<(string column, NodeAst value)> items)
    {
        Dictionary<string, NodeAst> values = new(items.Count, StringComparer.OrdinalIgnoreCase);

        foreach ((string column, NodeAst value) in items)
        {
            if (!values.TryAdd(column, value))
                throw MultipleAssignments(column);
        }

        return values;
    }

    /// <summary>
    /// Refuses a value map whose keys name one column more than once under a case-insensitive
    /// comparison. A typed row update arrives as a map whose comparer can be ordinal, so <c>a</c> and
    /// <c>A</c> can both be keys, and both resolve to column <c>a</c>.
    /// </summary>
    internal static void RequireUniqueIgnoreCase<TValue>(IEnumerable<KeyValuePair<string, TValue>> values, int count)
    {
        HashSet<string> seen = new(count, StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, TValue> entry in values)
        {
            if (!seen.Add(entry.Key))
                throw MultipleAssignments(entry.Key);
        }
    }

    /// <summary>The refusal of a second assignment to <paramref name="column"/> in one SET list or value map.</summary>
    internal static CamusDBException MultipleAssignments(string column) =>
        new(CamusDBErrorCodes.DuplicateColumn, $"Multiple assignments to the same column '{column}'");

    /// <summary>The refusal of a column that an INSERT column list or an inserted row names more than once.</summary>
    internal static CamusDBException RepeatedInsertColumn(string column, string where) =>
        new(CamusDBErrorCodes.DuplicateColumn, $"Column '{column}' specified more than once in {where}");
}
