
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using NUnit.Framework;
using CommandLine;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers the transport-security policy flag for authenticated requests across all three layers it
/// travels through: the YAML key, the CLI flag, and the process-wide static the request gates read.
///
/// <para>The flag is secure-by-default, so the tests that matter are the ones proving it can actually
/// be turned <b>off</b> — a node behind a TLS-terminating proxy sees plaintext on the inside hop and
/// would reject every forwarded request otherwise — and that an omitted CLI flag does not silently
/// re-enable it over an operator's YAML choice.</para>
///
/// <para>Marked non-parallelizable because one test mutates the process-wide
/// <see cref="CamusDBOptions.RequireTlsWhenAuthEnabled"/>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestConfigRequireTlsWhenAuthEnabled
{
    private static ConfigCliOverrides ParseCli(params string[] args)
    {
        ParserResult<CamusCommandLineOptions> result =
            Parser.Default.ParseArguments<CamusCommandLineOptions>(args);

        CamusCommandLineOptions? opts = null;
        result.WithParsed(o => opts = o);

        Assert.That(opts, Is.Not.Null, $"CLI failed to parse: {string.Join(' ', args)}");
        return opts!.ToOverrides();
    }

    // ─── YAML ─────────────────────────────────────────────────────────────────

    [Test]
    public void DefaultsToTrueWhenTheKeyIsAbsent()
    {
        ConfigDefinition config = new ConfigReader().Read("http_port: 5095");

        Assert.That(config.RequireTlsWhenAuthEnabled, Is.True, "Secure by default — omitting the key must not relax it");
    }

    [Test]
    public void YamlCanTurnItOff()
    {
        ConfigDefinition config = new ConfigReader().Read("require_tls_when_auth_enabled: false");

        Assert.That(config.RequireTlsWhenAuthEnabled, Is.False);
    }

    [Test]
    public void YamlKeyIsAccepted_NotRejectedAsUnknown()
    {
        // The reader rejects unrecognized root keys outright, so a missing allow-list entry would
        // surface as a startup failure rather than a silently ignored setting.
        Assert.DoesNotThrow(() => new ConfigReader().Read("require_tls_when_auth_enabled: true"));
    }

    // ─── CLI ──────────────────────────────────────────────────────────────────

    [Test]
    public void CliFlagParsesBothValues()
    {
        Assert.That(ParseCli("--require-tls-when-auth-enabled", "false").RequireTlsWhenAuthEnabled, Is.False);
        Assert.That(ParseCli("--require-tls-when-auth-enabled", "true").RequireTlsWhenAuthEnabled, Is.True);
    }

    [Test]
    public void OmittedCliFlagIsNull()
    {
        Assert.That(ParseCli("--http-port", "5095").RequireTlsWhenAuthEnabled, Is.Null,
            "An absent flag must stay null so it cannot override the YAML value");
    }

    [Test]
    public void CliOverridesYaml()
    {
        ConfigDefinition config = new ConfigReader().Read("require_tls_when_auth_enabled: true");

        ConfigResolver.ApplyCliOverrides(config, ParseCli("--require-tls-when-auth-enabled", "false"));

        Assert.That(config.RequireTlsWhenAuthEnabled, Is.False);
    }

    [Test]
    public void OmittedCliFlagLeavesYamlValueIntact()
    {
        ConfigDefinition config = new ConfigReader().Read("require_tls_when_auth_enabled: false");

        ConfigResolver.ApplyCliOverrides(config, ParseCli("--http-port", "5095"));

        Assert.That(config.RequireTlsWhenAuthEnabled, Is.False,
            "A CLI run that says nothing about TLS must not re-enable it over the operator's YAML");
    }

    // ─── Carried onto the resolved options ────────────────────────────────────

    /// <summary>
    /// Both values must survive resolution: the request gates read this from the options an engine was
    /// built with, so a YAML/CLI value the resolver drops silently does nothing.
    /// </summary>
    [Test]
    public void Resolve_PropagatesBothValues()
    {
        CamusDBOptions off = ConfigResolver.Resolve(
            new ConfigReader().Read("require_tls_when_auth_enabled: false"));
        Assert.That(off.RequireTlsWhenAuthEnabled, Is.False);

        CamusDBOptions on = ConfigResolver.Resolve(
            new ConfigReader().Read("require_tls_when_auth_enabled: true"));
        Assert.That(on.RequireTlsWhenAuthEnabled, Is.True);
    }
}
