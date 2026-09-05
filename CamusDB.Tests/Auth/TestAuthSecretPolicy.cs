/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Auth;

namespace CamusDB.Tests.Auth;

/// <summary>
/// The token design's whole premise is that a leaked auth catalog yields nothing usable, because the
/// catalog stores only an HMAC keyed by a server-side secret. A guessable key voids that premise: an
/// attacker holding the catalog forges tokens offline. Before this floor existed the only check was
/// for emptiness, so <c>CAMUSDB_AUTH_TOKEN_KEY=changeme</c> produced a working server.
/// </summary>
[TestFixture]
internal sealed class TestAuthSecretPolicy
{
    [TestCase("")]
    [TestCase(null)]
    public void AnAbsentSecretIsTheCallersDecision(string? secret)
    {
        // Absence means something different for each secret — no token can be issued, versus the peer
        // routes are refused — so the policy does not judge it. Each caller answers for its own.
        Assert.DoesNotThrow(() => AuthSecretPolicy.EnsureStrongEnough(secret, "CAMUSDB_AUTH_TOKEN_KEY"));
    }

    [TestCase("changeme")]
    [TestCase("s3cr3t")]
    [TestCase("0123456789012345678901234567890")]
    public void AShortSecretIsRefused(string secret)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => AuthSecretPolicy.EnsureStrongEnough(secret, "CAMUSDB_AUTH_TOKEN_KEY"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);

        // The operator must be able to act on this without reading the source: it names the variable
        // and the requirement.
        Assert.That(ex.Message, Does.Contain("CAMUSDB_AUTH_TOKEN_KEY"));
        Assert.That(ex.Message, Does.Contain("32"));

        // And it must never echo the value, which is the thing being protected.
        Assert.That(ex.Message, Does.Not.Contain(secret));
    }

    [Test]
    public void ASecretAtExactlyTheFloorIsAccepted()
    {
        string atFloor = new('k', AuthSecretPolicy.MinimumSecretBytes);

        Assert.AreEqual(32, Encoding.UTF8.GetByteCount(atFloor));
        Assert.DoesNotThrow(() => AuthSecretPolicy.EnsureStrongEnough(atFloor, "CAMUSDB_NODE_SECRET"));
    }

    /// <summary>
    /// The floor counts bytes of key material, not characters. Thirty-two emoji are thirty-two
    /// characters and well past the floor; sixteen two-byte characters are also thirty-two bytes and
    /// pass, while eight of them do not — which is the whole reason the measurement is in bytes.
    /// </summary>
    [Test]
    public void TheFloorMeasuresBytesNotCharacters()
    {
        string sixteenTwoByteChars = new('ñ', 16);
        Assert.AreEqual(32, Encoding.UTF8.GetByteCount(sixteenTwoByteChars));
        Assert.DoesNotThrow(() => AuthSecretPolicy.EnsureStrongEnough(sixteenTwoByteChars, "CAMUSDB_NODE_SECRET"));

        // Thirty-one characters of plain ASCII is thirty-one bytes and fails, even though it is longer
        // in characters than the fifteen-character value below.
        string fifteenTwoByteChars = new('ñ', 15);
        Assert.AreEqual(30, Encoding.UTF8.GetByteCount(fifteenTwoByteChars));
        Assert.Throws<CamusDBException>(
            () => AuthSecretPolicy.EnsureStrongEnough(fifteenTwoByteChars, "CAMUSDB_NODE_SECRET"));
    }
}
