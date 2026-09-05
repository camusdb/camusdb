/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;

namespace CamusDB.Tests.Config;

/// <summary>
/// This resolver decides the scheme for all three node-to-node HTTP channels, and two of them attach
/// the cluster's shared node secret as a header. It used to interpolate <c>http://</c> into both of
/// its branches, so no configuration could make that secret travel encrypted — and the token HMAC and
/// the constant-time comparisons elsewhere bought nothing against anyone who could read the wire.
/// These tests exist to keep the scheme configurable rather than assumed.
/// </summary>
[TestFixture]
public sealed class TestPeerEndpointResolver
{
    private static readonly IReadOnlyList<string> Peers = ["10.0.0.1:7070", "10.0.0.2:7070"];

    private static PeerEndpointResolver Build(IReadOnlyList<string> httpPeers, bool tls)
        => new(Peers, httpPeers, httpPort: 5095, peerTlsEnabled: tls, NullLogger<ICamusDB>.Instance);

    [Test]
    public void MappedPeer_FollowsTheConfiguredScheme()
    {
        IReadOnlyList<string> httpPeers = ["10.0.0.1:5095", "10.0.0.2:5095"];

        Assert.AreEqual(new Uri("http://10.0.0.1:5095"), Build(httpPeers, tls: false).Resolve("10.0.0.1:7070"));
        Assert.AreEqual(new Uri("https://10.0.0.1:5095"), Build(httpPeers, tls: true).Resolve("10.0.0.1:7070"));
    }

    /// <summary>
    /// The fallback is the branch that used to be missed. A peer absent from <c>http_peers</c> — which
    /// is the default configuration, since that list is optional — took the hard-coded scheme no matter
    /// what the operator asked for.
    /// </summary>
    [Test]
    public void UniformPortFallback_FollowsTheConfiguredScheme()
    {
        Assert.AreEqual(new Uri("http://10.0.0.9:5095"), Build([], tls: false).Resolve("10.0.0.9:7070"));
        Assert.AreEqual(new Uri("https://10.0.0.9:5095"), Build([], tls: true).Resolve("10.0.0.9:7070"));
    }

    /// <summary>
    /// An address that names its own scheme is left alone, so one peer reachable differently from the
    /// rest can be written out in full instead of forcing the whole cluster onto a single setting.
    /// </summary>
    [Test]
    public void AnAddressCarryingItsOwnSchemeWins()
    {
        IReadOnlyList<string> httpPeers = ["https://10.0.0.1:5095", "http://10.0.0.2:5095"];
        PeerEndpointResolver resolver = Build(httpPeers, tls: false);

        Assert.AreEqual(new Uri("https://10.0.0.1:5095"), resolver.Resolve("10.0.0.1:7070"));
        Assert.AreEqual(new Uri("http://10.0.0.2:5095"), resolver.Resolve("10.0.0.2:7070"));
    }

    /// <summary>
    /// The naive way to add a scheme is to prefix it unconditionally, which turns an address that
    /// already has one into <c>http://https://host</c> — a URI that parses, resolves to the wrong host,
    /// and fails at request time rather than at startup.
    /// </summary>
    [Test]
    public void ASchemeIsNeverPrefixedTwice()
    {
        Uri resolved = Build(["https://10.0.0.1:5095", "https://10.0.0.2:5095"], tls: true).Resolve("10.0.0.1:7070");

        Assert.AreEqual("https", resolved.Scheme);
        Assert.AreEqual("10.0.0.1", resolved.Host);
    }
}
