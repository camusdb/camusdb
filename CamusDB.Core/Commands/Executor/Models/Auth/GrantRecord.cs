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
/// recreated one.</para>
/// </summary>
public sealed class GrantRecord
{
    /// <summary>Normalized (lower-cased) user name this grant belongs to.</summary>
    public string User { get; set; } = "";

    public GrantScope Scope { get; set; } = new();

    public Privilege Privileges { get; set; }
}
