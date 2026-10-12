/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using NUnit.Framework;

using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// <c>http3_enabled</c> asks the HTTPS API port to serve HTTP/3 as well. Two things can make that
/// request a silent no-op — no HTTPS port at all, or no QUIC on the host — and in both the node still
/// starts and serves HTTP/1.1 and HTTP/2. These tests pin that the key binds from YAML under the name
/// the docs use, and that each no-op case produces a warning that names its fix.
/// </summary>
[TestFixture]
public sealed class TestHttp3Config
{
    [Test]
    public void IsOffByDefault()
    {
        Assert.IsFalse(new ConfigReader().Read("http_port: 5095").Http3Enabled);
    }

    [Test]
    public void BindsFromYamlKey()
    {
        // The underscored naming convention must map the digit-bearing property name onto exactly
        // the documented key; an unknown-key error or a silently unset property both fail here.
        ConfigDefinition config = new ConfigReader().Read("https_certificate: /tmp/cert.pfx\nhttp3_enabled: true");

        Assert.IsTrue(config.Http3Enabled);
        Assert.IsTrue(config.ProvidedKeys.Contains("http3_enabled"));
    }

    [Test]
    public void NotRequested_WarnsAboutNothing()
    {
        // No certificate and no QUIC, but nobody asked for HTTP/3: nothing to report.
        ConfigDefinition config = new() { Http3Enabled = false };

        Assert.IsEmpty(Http3StartupChecks.Inspect(config, quicSupported: false));
    }

    [Test]
    public void CertificateAndQuic_WarnsAboutNothing()
    {
        ConfigDefinition config = new() { Http3Enabled = true, HttpsCertificate = "/etc/camusdb/api.pfx" };

        Assert.IsEmpty(Http3StartupChecks.Inspect(config, quicSupported: true));
    }

    [Test]
    public void NoCertificate_WarnsAndNamesHttpsCertificate()
    {
        ConfigDefinition config = new() { Http3Enabled = true };

        var warnings = Http3StartupChecks.Inspect(config, quicSupported: true);

        Assert.AreEqual(1, warnings.Count);
        Assert.That(warnings[0], Does.Contain("https_certificate"));
    }

    [Test]
    public void NoQuic_WarnsAndNamesLibmsquic()
    {
        ConfigDefinition config = new() { Http3Enabled = true, HttpsCertificate = "/etc/camusdb/api.pfx" };

        var warnings = Http3StartupChecks.Inspect(config, quicSupported: false);

        Assert.AreEqual(1, warnings.Count);
        Assert.That(warnings[0], Does.Contain("libmsquic"));
    }

    [Test]
    public void NoCertificateAndNoQuic_ReportsBoth()
    {
        // Each cause has its own fix, so reporting only the first would cost the operator a restart.
        ConfigDefinition config = new() { Http3Enabled = true };

        var warnings = Http3StartupChecks.Inspect(config, quicSupported: false);

        Assert.AreEqual(2, warnings.Count);
        Assert.IsTrue(warnings.Any(w => w.Contains("https_certificate")));
        Assert.IsTrue(warnings.Any(w => w.Contains("libmsquic")));
    }
}
