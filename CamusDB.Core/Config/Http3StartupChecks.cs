/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;

namespace CamusDB.Core.Config;

/// <summary>
/// Reports the configurations where <c>http3_enabled: true</c> does not give the HTTPS API port
/// HTTP/3.
///
/// <para>It exists because neither case fails. The node starts, the HTTPS port (if any) keeps
/// serving HTTP/1.1 and HTTP/2, and every client still works, so the operator who asked for HTTP/3
/// has no signal that they do not have it.</para>
///
/// <para>Whether the platform has QUIC is passed in rather than probed here, because the probe is a
/// web-server concern and the answer differs per host: the caller asks
/// <c>System.Net.Quic.QuicListener.IsSupported</c>, and a test passes either value.</para>
/// </summary>
public static class Http3StartupChecks
{
    /// <summary>
    /// Returns one message per reason HTTP/3 is off although the configuration asks for it, empty
    /// when HTTP/3 is not requested or nothing stops it. Each message names what fixes it.
    /// </summary>
    public static IReadOnlyList<string> Inspect(ConfigDefinition config, bool quicSupported)
    {
        List<string> warnings = [];

        if (!config.Http3Enabled)
            return warnings;

        if (string.IsNullOrEmpty(config.HttpsCertificate))
            warnings.Add(
                "http3_enabled is true but https_certificate is not set, so there is no HTTPS port and " +
                "no HTTP/3. HTTP/3 runs only over TLS; the plain HTTP port serves HTTP/1.1 and HTTP/2 " +
                "only. Set https_certificate in config.yml to serve HTTP/3 on https_port.");

        if (!quicSupported)
            warnings.Add(
                "http3_enabled is true but QUIC is not available on this host, so HTTP/3 stays off and " +
                "the HTTPS port serves HTTP/1.1 and HTTP/2 only. Install libmsquic on Linux or macOS; on " +
                "Windows, HTTP/3 needs Windows 11 or Windows Server 2022 or later.");

        return warnings;
    }
}
