/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One (user, scope) → privilege-mask grant, persisted under
/// <c>_system/auth/grant:{normalizedUser}/{scopeKey}</c>. A repeated <c>GRANT</c> on the same scope
/// unions into <see cref="Privileges"/> and overwrites this record; <c>REVOKE</c> subtracts and
/// deletes the record when the mask reaches <see cref="Privilege.None"/>.
///
/// <para><see cref="User"/> is the normalized user name (the KV-key form). The scope is id-bound —
/// see <see cref="GrantScope"/> — so the grant follows a renamed object and never resurrects on a
/// recreated one. <see cref="UserId"/> binds the other side of the grant the same way, for the same
/// reason.</para>
/// </summary>
public sealed class GrantRecord
{
    /// <summary>Normalized (lower-cased) user name this grant belongs to.</summary>
    public string User { get; set; } = "";

    /// <summary>
    /// The <c>Id</c> of the user record this grant was written for. It exists because the key is
    /// keyed by <see cref="User"/> — a name — and a name can be freed and taken again: drop a user
    /// while any of its grant keys survive the delete, recreate the name, and the new account would
    /// inherit the old one's privileges. A recreated user is a new record with a new id, so a grant
    /// naming the previous id is ignored rather than adopted.
    ///
    /// <para>Null on a grant written before this field existed. Those are honored, because refusing
    /// them would revoke every existing grant on upgrade; they carry the older, name-only binding
    /// until the next <c>GRANT</c> or <c>REVOKE</c> on that scope rewrites them.</para>
    /// </summary>
    public string? UserId { get; set; }

    public GrantScope Scope { get; set; } = new();

    public Privilege Privileges { get; set; }
}
