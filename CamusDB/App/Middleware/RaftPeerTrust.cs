/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using CamusDB.Core;

namespace CamusDB.App.Middleware;

/// <summary>
/// Decides which callers are this cluster's nodes when <c>raft_mutual_tls_enabled</c> is on.
///
/// <para>The decision is made once per connection, during the TLS handshake on the Raft listener: the
/// listener requires a client certificate and accepts it only if it chains to the CA in
/// <c>raft_peer_ca</c> and allows client authentication. Revocation is not checked; a private cluster CA
/// usually publishes no revocation list, and an offline check would refuse every peer.</para>
///
/// <para>Per request, <see cref="IsTrustedPeer"/> only confirms that the request came over that
/// listener with a certificate. That is enough because no other listener asks for a client
/// certificate, and it is required because a certificate seen on another port was never checked
/// against the cluster CA. Kommander's <c>RaftService</c> and Kahuna's node transport gate then check
/// the same certificate again, with Kommander's validity-window rule.</para>
///
/// <para>This class is the reason those two can run with an empty thumbprint allow-list: Kommander
/// documents that an empty list trusts whatever the host's listener validated, and this is that
/// validation.</para>
/// </summary>
public sealed class RaftPeerTrust
{
    /// <summary>The extended key usage a certificate needs to authenticate a TLS client.</summary>
    private const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";

    private readonly X509Certificate2Collection roots;

    /// <summary>The Raft listener's port, the only port where a client certificate is checked.</summary>
    public int RaftPort { get; }

    public RaftPeerTrust(X509Certificate2Collection roots, int raftPort)
    {
        if (roots.Count == 0)
            throw new ArgumentException("At least one CA certificate is required.", nameof(roots));

        this.roots = roots;
        RaftPort = raftPort;
    }

    /// <summary>
    /// Loads the CA certificates from the PEM file at <paramref name="caPath"/>. Throws
    /// <see cref="CamusDBErrorCodes.InvalidConfig"/> when the file cannot be read or holds no certificate,
    /// so a bad path stops the node at startup instead of failing every peer handshake later.
    /// </summary>
    public static RaftPeerTrust Load(string caPath, int raftPort)
    {
        X509Certificate2Collection roots = [];

        try
        {
            roots.ImportFromPemFile(caPath);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or CryptographicException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'raft_peer_ca' could not be read from '{caPath}': {e.Message}");
        }

        if (roots.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'raft_peer_ca' file '{caPath}' holds no PEM certificate");

        return new RaftPeerTrust(roots, raftPort);
    }

    /// <summary>
    /// Makes an HTTPS listener require a client certificate and accept only one this policy trusts.
    /// Apply it to the Raft listener only: every other listener serves clients, who have no peer
    /// certificate.
    /// </summary>
    public void ConfigureListener(HttpsConnectionAdapterOptions https)
    {
        https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
        https.CheckCertificateRevocation = false;
        https.ClientCertificateValidation = (certificate, _, _) => IsTrustedCertificate(certificate);
    }

    /// <summary>
    /// True when <paramref name="certificate"/> chains to one of the configured CA certificates, is
    /// inside its validity window, and allows client authentication. The system trust store plays no
    /// part, so a publicly trusted certificate is not a peer.
    /// </summary>
    public bool IsTrustedCertificate(X509Certificate2 certificate)
    {
        using X509Chain chain = new();

        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(roots);
        // A CA file may hold an intermediate together with its root; the chain builder looks for
        // intermediates in the extra store.
        chain.ChainPolicy.ExtraStore.AddRange(roots);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.ApplicationPolicy.Add(new Oid(ClientAuthenticationOid));

        try
        {
            return chain.Build(certificate);
        }
        finally
        {
            // The chain hands out its own copies of each certificate; they hold native handles.
            foreach (X509ChainElement element in chain.ChainElements)
                element.Certificate.Dispose();
        }
    }

    /// <summary>
    /// True when the request arrived over TLS on the Raft listener with a client certificate that is
    /// still inside its validity window. The handshake already proved the certificate chains to the
    /// cluster CA; the window is checked again because an HTTP/2 connection can outlive its certificate.
    /// </summary>
    public bool IsTrustedPeer(HttpContext context)
    {
        if (!context.Request.IsHttps || context.Connection.LocalPort != RaftPort)
            return false;

        X509Certificate2? certificate = context.Connection.ClientCertificate;
        if (certificate is null)
            return false;

        DateTime nowUtc = DateTime.UtcNow;
        return nowUtc >= certificate.NotBefore.ToUniversalTime() && nowUtc <= certificate.NotAfter.ToUniversalTime();
    }
}
