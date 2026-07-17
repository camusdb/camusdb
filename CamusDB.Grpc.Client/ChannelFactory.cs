
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net.Security;
using System.Security.Cryptography;

using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;

namespace CamusDB.Grpc.Client;

/// <summary>
/// Builds a <see cref="GrpcChannel"/> from <see cref="CamusGrpcOptions"/>, applying the TLS trust policy
/// and keep-alive tuning. Kept separate from <see cref="CamusConnection"/> so the certificate-validation
/// decision (the security-sensitive part) is a pure, unit-testable function. Mirrors the equivalent
/// channel setup in the Kahuna client.
/// </summary>
internal static class ChannelFactory
{
    public static GrpcChannel Create(string address, CamusGrpcOptions options)
    {
        SocketsHttpHandler handler = new()
        {
            ConnectTimeout                = options.ConnectTimeout,
            PooledConnectionIdleTimeout   = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay            = options.KeepAlivePingDelay,
            KeepAlivePingTimeout          = options.KeepAlivePingTimeout,
            EnableMultipleHttp2Connections = options.EnableMultipleHttp2Connections,
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = BuildCertValidationCallback(options),
            },
        };

        MethodConfig retry = new()
        {
            Names = { MethodName.Default },
            RetryPolicy = new RetryPolicy
            {
                MaxAttempts          = 5,
                InitialBackoff       = TimeSpan.FromSeconds(1),
                MaxBackoff           = TimeSpan.FromSeconds(5),
                BackoffMultiplier    = 1.5,
                RetryableStatusCodes = { StatusCode.Unavailable },
            },
        };

        return GrpcChannel.ForAddress(address, new GrpcChannelOptions
        {
            HttpHandler   = handler,
            ServiceConfig = new ServiceConfig { MethodConfigs = { retry } },
        });
    }

    /// <summary>
    /// Chooses the certificate-validation callback for the given options:
    /// <list type="bullet">
    /// <item><c>null</c> — standard OS chain validation (the secure default).</item>
    /// <item>always-accept — when <see cref="CamusGrpcOptions.AllowInsecureCertificateValidation"/> is set
    /// (dev only; takes precedence over pinning).</item>
    /// <item>SHA-256 thumbprint pinning — when thumbprints are configured and insecure is off.</item>
    /// </list>
    /// </summary>
    internal static RemoteCertificateValidationCallback? BuildCertValidationCallback(CamusGrpcOptions options)
    {
        if (options.AllowInsecureCertificateValidation)
            return static (_, _, _, _) => true;

        if (options.TrustedServerCertificateThumbprints.Count == 0)
            return null;

        // Snapshot the pins so the callback is not affected by later mutation of the options list.
        string[] pins = options.TrustedServerCertificateThumbprints
            .Select(t => t.ToUpperInvariant())
            .ToArray();

        return (_, certificate, _, _) =>
        {
            if (certificate is null)
                return false;
            string thumbprint = Convert.ToHexString(SHA256.HashData(certificate.GetRawCertData()));
            return pins.Contains(thumbprint);
        };
    }
}
