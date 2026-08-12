
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The form a stored view body uses to name a table column it reads: the column's immutable id
/// wrapped in a reserved identifier, <c>__camus_col_{id}</c>.
///
/// <para>The relation counterpart of this is <see cref="StoredRelationRef"/>, and the reasoning is
/// the same — a name is presentation, an id is the binding — but two details differ.</para>
///
/// <para><b>Column ids are ObjectIds, so they are globally unique.</b> A token therefore identifies
/// a column without saying which relation it belongs to, and no table qualifier is needed to make it
/// unambiguous.</para>
///
/// <para><b>The qualifier is nevertheless preserved.</b> A reference written <c>o.total</c> is
/// stored as <c>o.__camus_col_{id}</c>, not as a bare token. Dropping the qualifier would be
/// unambiguous to the engine and wrong for the user: <c>SHOW CREATE VIEW</c> re-renders names, and
/// two relations in a join can expose the same column name, so an unqualified rendering could come
/// back as SQL that no longer parses to the same query.</para>
/// </summary>
public static class StoredColumnRef
{
    /// <summary>The reserved identifier prefix. Changing it invalidates every stored body.</summary>
    public const string Prefix = "__camus_col_";

    /// <summary>The identifier a stored body uses for <paramref name="columnId"/>.</summary>
    public static string Format(string columnId) => Prefix + columnId;

    /// <summary>
    /// The column id carried by <paramref name="identifier"/>, when it is one of these tokens. The
    /// prefix matches case-insensitively as SQL identifiers do; the id comes back verbatim.
    /// </summary>
    public static bool TryGetColumnId(string? identifier, out string columnId)
    {
        if (identifier is not null &&
            identifier.Length > Prefix.Length &&
            identifier.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            columnId = identifier[Prefix.Length..];
            return true;
        }

        columnId = "";
        return false;
    }

    /// <summary>
    /// Whether <paramref name="name"/> is a column name the engine must refuse, because it would
    /// shadow a stored reference.
    /// </summary>
    public static bool IsReservedColumnName(string? name) =>
        name is not null && name.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether a stored body could carry a column reference at all.</summary>
    public static bool MayContainReference(string? sql) =>
        sql is not null && sql.Contains(Prefix, StringComparison.OrdinalIgnoreCase);
}
