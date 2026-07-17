
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

using NUnit.Framework;

using CamusDB.Grpc.Client;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// Tests the certificate-validation policy that <see cref="ChannelFactory"/> derives from
/// <see cref="CamusGrpcOptions"/> — the security-sensitive decision, isolated so it can be verified
/// without opening a real TLS connection.
/// </summary>
[TestFixture]
public class TestChannelFactory
{
    private static X509Certificate2 SelfSigned()
    {
        using ECDsa key = ECDsa.Create();
        CertificateRequest req = new("CN=camus-test", key, HashAlgorithmName.SHA256);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    private static string Sha256Hex(X509Certificate2 cert)
        => Convert.ToHexString(SHA256.HashData(cert.GetRawCertData()));

    [Test]
    public void DefaultOptions_UseOsChainValidation()
    {
        // A null callback tells the handler to use the standard OS certificate chain — the secure default.
        RemoteCertificateValidationCallback? cb = ChannelFactory.BuildCertValidationCallback(new CamusGrpcOptions());
        Assert.That(cb, Is.Null);
    }

    [Test]
    public void InsecureOption_AcceptsAnyCertificate()
    {
        CamusGrpcOptions options = new() { AllowInsecureCertificateValidation = true };
        RemoteCertificateValidationCallback? cb = ChannelFactory.BuildCertValidationCallback(options);

        Assert.That(cb, Is.Not.Null);
        Assert.That(cb!(this, null, null, SslPolicyErrors.RemoteCertificateNotAvailable), Is.True);
    }

    [Test]
    public void ThumbprintPin_AcceptsMatch_RejectsMismatchAndNull()
    {
        using X509Certificate2 cert = SelfSigned();

        CamusGrpcOptions options = new();
        options.TrustedServerCertificateThumbprints.Add(Sha256Hex(cert).ToLowerInvariant());   // case-insensitive
        RemoteCertificateValidationCallback? cb = ChannelFactory.BuildCertValidationCallback(options);
        Assert.That(cb, Is.Not.Null);

        Assert.That(cb!(this, cert, null, SslPolicyErrors.RemoteCertificateChainErrors), Is.True,
            "A pinned thumbprint must be accepted even when the OS chain would reject it");
        Assert.That(cb(this, null, null, SslPolicyErrors.None), Is.False, "A null certificate must be rejected");

        CamusGrpcOptions wrongPin = new();
        wrongPin.TrustedServerCertificateThumbprints.Add(new string('0', 64));
        RemoteCertificateValidationCallback cb2 = ChannelFactory.BuildCertValidationCallback(wrongPin)!;
        Assert.That(cb2(this, cert, null, SslPolicyErrors.None), Is.False, "A non-matching thumbprint must be rejected");
    }

    [Test]
    public void InsecureTakesPrecedenceOverPins()
    {
        CamusGrpcOptions options = new() { AllowInsecureCertificateValidation = true };
        options.TrustedServerCertificateThumbprints.Add(new string('0', 64));

        RemoteCertificateValidationCallback cb = ChannelFactory.BuildCertValidationCallback(options)!;
        Assert.That(cb(this, null, null, SslPolicyErrors.RemoteCertificateNotAvailable), Is.True);
    }
}
