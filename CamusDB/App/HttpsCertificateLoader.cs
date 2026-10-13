/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography.X509Certificates;

namespace CamusDB.App;

/// <summary>
/// Loads the <c>https_certificate</c> PFX for an HTTPS port that also serves HTTP/3.
///
/// <para><b>Why the web server's own loader is not enough.</b> On Linux and macOS the QUIC library gets
/// the certificate as a PKCS#12 blob that .NET exports from the loaded certificate, so the private key
/// must be exportable. On Linux it always is. On macOS a key loaded from a PFX without
/// <see cref="X509KeyStorageFlags.Exportable"/> is not, and the web server's <c>UseHttps(fileName)</c>
/// loads it that way. The QUIC handshake then fails on every connection: the server sends the TLS alert
/// <c>user_canceled</c> and logs nothing, while HTTP/1.1 and HTTP/2 on the same port keep working.</para>
///
/// <para>The path is resolved the same way <c>UseHttps(fileName)</c> resolves it: a relative path is
/// relative to the content root, and an absolute path is used as it is.</para>
/// </summary>
public static class HttpsCertificateLoader
{
    public static X509Certificate2 LoadForQuic(string contentRootPath, string fileName)
        => X509CertificateLoader.LoadPkcs12FromFile(
            Path.Combine(contentRootPath, fileName),
            password: null,
            X509KeyStorageFlags.Exportable);
}
