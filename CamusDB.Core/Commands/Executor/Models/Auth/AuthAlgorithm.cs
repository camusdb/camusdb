/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Identifies the key-derivation scheme a stored <see cref="Credential"/> was produced with.
///
/// <para>The SQL surface names an authentication <em>plugin</em> (<c>IDENTIFIED WITH sha256_password</c>)
/// for MySQL compatibility; this enum records the actual KDF used to produce the stored verifier, so
/// the hashing scheme can evolve behind the same SQL keyword. Persisted per credential — never assume
/// a single global algorithm — so an upgrade path can re-hash on next successful login.</para>
/// </summary>
public enum AuthAlgorithm
{
    /// <summary>
    /// Salted PBKDF2-HMAC-SHA256. Reached through the <c>sha256_password</c> plugin name. The stored
    /// <see cref="Credential.Iterations"/> is the work factor that was in force when the password was
    /// set; verification always uses the stored count, not the current config default.
    /// </summary>
    Pbkdf2Sha256 = 0,
}
