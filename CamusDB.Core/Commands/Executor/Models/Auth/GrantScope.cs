/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>The breadth a grant applies to. Ordered <c>Global ⊃ Database ⊃ Table</c>.</summary>
public enum GrantScopeKind
{
    /// <summary><c>*.*</c> — every database and table.</summary>
    Global = 0,

    /// <summary><c>db.*</c> — every table in one database.</summary>
    Database = 1,

    /// <summary><c>db.table</c> — one table.</summary>
    Table = 2,
}

/// <summary>
/// The object a grant targets. Matching is done by <b>immutable id</b>
/// (<see cref="DatabaseId"/>/<see cref="TableId"/>), never by name: a drop-and-recreate of the same
/// name yields a new id, so a stale grant can never silently re-activate on a different object. The
/// display names are retained only so <c>SHOW GRANTS</c> can render the original <c>db</c>/<c>db.table</c>
/// text.
///
/// <para><see cref="ScopeKey"/> is the order-preserving string embedded in the KV key under
/// <c>_system/auth/grant:{user}/</c>. It uses ids, not names, for the same reason matching does.</para>
/// </summary>
public sealed class GrantScope
{
    public GrantScopeKind Kind { get; set; }

    /// <summary>Immutable database id. Empty for <see cref="GrantScopeKind.Global"/>.</summary>
    public string DatabaseId { get; set; } = "";

    /// <summary>Immutable table id. Empty unless <see cref="GrantScopeKind.Table"/>.</summary>
    public string TableId { get; set; } = "";

    /// <summary>Display database name (for <c>SHOW GRANTS</c> only). Empty for global.</summary>
    public string DatabaseName { get; set; } = "";

    /// <summary>Display table name (for <c>SHOW GRANTS</c> only). Empty unless table-scoped.</summary>
    public string TableName { get; set; } = "";

    /// <summary>
    /// The id-based key fragment used to name this scope's grant key. Global is <c>*</c>, a database
    /// is <c>d:{databaseId}</c>, a table is <c>t:{databaseId}:{tableId}</c>. Uniquely identifies the
    /// (user, scope) pair so a repeated GRANT on the same object overwrites rather than duplicates.
    /// </summary>
    public string ScopeKey() => Kind switch
    {
        GrantScopeKind.Global => "*",
        GrantScopeKind.Database => $"d:{DatabaseId}",
        GrantScopeKind.Table => $"t:{DatabaseId}:{TableId}",
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown grant scope kind {Kind}"),
    };

    /// <summary>Human-readable <c>db.table</c> / <c>db.*</c> / <c>*.*</c> form for <c>SHOW GRANTS</c>.</summary>
    public string DisplayObject() => Kind switch
    {
        GrantScopeKind.Global => "*.*",
        GrantScopeKind.Database => $"{DatabaseName}.*",
        GrantScopeKind.Table => $"{DatabaseName}.{TableName}",
        _ => "",
    };
}
