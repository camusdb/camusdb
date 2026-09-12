/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One account and every grant stored for it, as one listing row source.
///
/// <para>The two travel together because the statements that read them need both at once:
/// <c>SHOW USERS</c> reports a grant count per account, and <c>SHOW GRANTS FOR *</c> reports the
/// grants of every account. Splitting them would mean two scans of the same keyspace, and the second
/// scan could observe a different state from the first.</para>
/// </summary>
/// <param name="User">The stored account record. Never null.</param>
/// <param name="Grants">The account's grants, ordered by <c>GrantScope.ScopeKey()</c>. Empty, never null.</param>
public sealed record AuthCatalogEntry(UserRecord User, IReadOnlyList<GrantRecord> Grants);

/// <summary>
/// An immutable, ordered picture of the whole authentication catalog, taken from storage rather than
/// from a cache.
///
/// <para><b>It is complete or it does not exist.</b> The scan that builds it raises rather than
/// returning a short result, so a caller never has to ask whether it saw everything. That property is
/// the reason the listing statements are built on this type instead of on the in-memory maps: an
/// operator taking an inventory before a storage-revision upgrade would believe a short list, and a
/// short list is worse than no statement at all.</para>
///
/// <para><see cref="Generation"/> is the authentication coherence generation the snapshot was read
/// under. It is carried so a caller can reason about how the picture relates to a later change; the
/// listing statements themselves do not use it.</para>
/// </summary>
public sealed class AuthCatalogSnapshot
{
    /// <summary>Every account, ordered by its normalized (lower-cased) name, ascending ordinal.</summary>
    public IReadOnlyList<AuthCatalogEntry> Entries { get; }

    /// <summary>The coherence generation this picture was read under.</summary>
    public long Generation { get; }

    public AuthCatalogSnapshot(IReadOnlyList<AuthCatalogEntry> entries, long generation)
    {
        Entries = entries;
        Generation = generation;
    }
}
