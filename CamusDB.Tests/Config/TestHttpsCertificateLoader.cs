/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;

using CamusDB.App;

namespace CamusDB.Tests.Config;

/// <summary>
/// With <c>http3_enabled</c>, the QUIC library on Linux and macOS gets the HTTPS certificate as a PKCS#12
/// blob that .NET exports from it. A key that cannot be exported makes every QUIC handshake fail with a
/// TLS alert and no server log line, and on macOS a key loaded from a PFX with default flags cannot be
/// exported. These tests pin that the HTTP/3 loader gives a certificate whose key exports, and that it
/// resolves paths the way the web server's own loader does.
/// </summary>
[TestFixture]
public sealed class TestHttpsCertificateLoader
{
    private string directory = "";

    [SetUp]
    public void SetUp()
    {
        directory = Path.Combine(Path.GetTempPath(), "camusdb-https-cert-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(directory);

        using RSA key = RSA.Create(2048);
        CertificateRequest request = new("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));

        File.WriteAllBytes(Path.Combine(directory, "api.pfx"), certificate.Export(X509ContentType.Pfx));
    }

    [TearDown]
    public void TearDown()
    {
        Directory.Delete(directory, recursive: true);
    }

    [Test]
    public void LoadsAKeyThatQuicCanExport()
    {
        using X509Certificate2 certificate = HttpsCertificateLoader.LoadForQuic("/unused", Path.Combine(directory, "api.pfx"));

        Assert.IsTrue(certificate.HasPrivateKey);
        Assert.IsNotEmpty(certificate.Export(X509ContentType.Pkcs12));
    }

    [Test]
    public void ResolvesARelativePathAgainstTheContentRoot()
    {
        using X509Certificate2 certificate = HttpsCertificateLoader.LoadForQuic(directory, "api.pfx");

        Assert.AreEqual("CN=localhost", certificate.Subject);
    }
}
