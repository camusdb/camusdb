/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Text;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Auth;

/// <summary>
/// Produces and verifies salted PBKDF2-HMAC-SHA256 password verifiers for database users. This is the
/// only place cleartext passwords are turned into stored form; callers must never persist or log the
/// cleartext, and it is dropped immediately after <see cref="Hash"/> returns.
///
/// <para>Each call to <see cref="Hash"/> draws a fresh random salt and stamps the credential with the
/// iteration count currently in force (<see cref="CamusDBConfig.PasswordHashIterations"/>).
/// <see cref="Verify"/> recomputes with the credential's <em>stored</em> salt and iteration count — not
/// the current config — so raising the work factor never invalidates existing hashes. The final
/// comparison is constant-time to avoid leaking timing information about how many leading bytes
/// matched.</para>
/// </summary>
public static class PasswordHasher
{
    private const int SaltBytes = 16;
    private const int HashBytes = 32;

    /// <summary>
    /// Derives a fresh <see cref="Credential"/> from <paramref name="password"/> using a new random
    /// salt and the configured iteration count. Rejects a password larger than
    /// <see cref="CamusDBConfig.MaxPasswordBytes"/> so an oversized input cannot turn one hash into a
    /// denial-of-service lever (the ticket validator enforces the same bound earlier; this is the
    /// last-line guard).
    /// </summary>
    public static Credential Hash(string password)
    {
        ArgumentNullException.ThrowIfNull(password);

        byte[] passwordBytes = Encoding.UTF8.GetBytes(password);
        if (passwordBytes.Length > CamusDBConfig.MaxPasswordBytes)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Password exceeds the maximum of {CamusDBConfig.MaxPasswordBytes} bytes");

        int iterations = CamusDBConfig.PasswordHashIterations;
        byte[] salt = RandomNumberGenerator.GetBytes(SaltBytes);
        byte[] hash = Rfc2898DeriveBytes.Pbkdf2(passwordBytes, salt, iterations, HashAlgorithmName.SHA256, HashBytes);

        return new Credential
        {
            Algorithm = AuthAlgorithm.Pbkdf2Sha256,
            Salt = salt,
            Hash = hash,
            Iterations = iterations,
        };
    }

    /// <summary>
    /// Recomputes the verifier for <paramref name="password"/> against <paramref name="credential"/>'s
    /// stored salt and iteration count and compares it in constant time. Returns false for a null
    /// credential (a user with no password can never authenticate) or an unsupported algorithm rather
    /// than throwing, so an authentication path has a single uniform failure shape.
    /// </summary>
    public static bool Verify(string password, Credential? credential)
    {
        if (credential is null || credential.Algorithm != AuthAlgorithm.Pbkdf2Sha256
            || credential.Hash.Length == 0 || credential.Iterations <= 0)
            return false;

        // Reject an oversized password BEFORE the KDF, so an attacker-sized login input cannot turn one
        // verification into a CPU/allocation denial-of-service lever (the /login path reaches Verify
        // with attacker-controlled input).
        if (Encoding.UTF8.GetByteCount(password) > CamusDBConfig.MaxPasswordBytes)
            return false;

        byte[] passwordBytes = Encoding.UTF8.GetBytes(password);
        byte[] candidate = Rfc2898DeriveBytes.Pbkdf2(
            passwordBytes, credential.Salt, credential.Iterations, HashAlgorithmName.SHA256, credential.Hash.Length);

        return CryptographicOperations.FixedTimeEquals(candidate, credential.Hash);
    }
}
