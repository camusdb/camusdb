/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// A stored password verifier. Holds only the salted, iterated hash — never the cleartext, which
/// exists on the ticket for the duration of the <c>CREATE/ALTER USER</c> statement and is dropped
/// after hashing.
///
/// <para>A <see cref="UserRecord"/> with a <c>null</c> credential cannot authenticate: the user exists
/// as a grant target but has no password until <c>ALTER USER … IDENTIFIED …</c> sets one.
/// <see cref="Iterations"/> is captured per credential so raising the global work factor does not
/// invalidate existing hashes — verification always uses the stored count.</para>
/// </summary>
public sealed class Credential
{
    public AuthAlgorithm Algorithm { get; set; } = AuthAlgorithm.Pbkdf2Sha256;

    /// <summary>Per-user random salt (base64 in JSON). Regenerated on every password change.</summary>
    public byte[] Salt { get; set; } = [];

    /// <summary>Derived hash bytes (base64 in JSON).</summary>
    public byte[] Hash { get; set; } = [];

    /// <summary>KDF work factor in force when this credential was produced.</summary>
    public int Iterations { get; set; }
}
