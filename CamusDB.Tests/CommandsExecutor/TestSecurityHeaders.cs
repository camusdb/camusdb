/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using NUnit.Framework;

using CamusDB.App.Middleware;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The host set no browser security headers at all. The dashboard's read-only cookie and
/// <c>SameSite=Strict</c> blunt the impact, so this is defence in depth rather than an open hole —
/// but the page could be framed and its responses could be content-type sniffed, and neither costs
/// anything to close.
///
/// <para>These drive the middleware directly. It has to set the headers before the response starts,
/// which means it decides from the path alone and cannot consult the content type — so the path
/// classification is the whole behavior, and it is what these tests pin.</para>
/// </summary>
[TestFixture]
public sealed class TestSecurityHeaders
{
    private static async Task<DefaultHttpContext> InvokeAsync(string path)
    {
        DefaultHttpContext http = new();
        http.Request.Path = path;

        SecurityHeadersMiddleware mw = new(_ => Task.CompletedTask);
        await mw.Invoke(http);

        return http;
    }

    [TestCase("/")]
    [TestCase("/Index")]
    [TestCase("/SignIn")]
    [TestCase("/Error")]
    public async Task AnHtmlPageCarriesTheHeaders(string path)
    {
        DefaultHttpContext http = await InvokeAsync(path);

        Assert.IsTrue(http.Response.Headers.ContainsKey("Content-Security-Policy"), path);
        Assert.AreEqual("DENY", http.Response.Headers["X-Frame-Options"].ToString(), path);
        Assert.AreEqual("nosniff", http.Response.Headers["X-Content-Type-Options"].ToString(), path);
    }

    /// <summary>
    /// A policy on a JSON response is inert — no browser treats one as a document. Sending it anyway
    /// would be harmless today and a trap later, when a policy written for the dashboard is inherited
    /// by an endpoint nobody re-read.
    /// </summary>
    [TestCase("/execute-sql-query")]
    [TestCase("/v1/dashboard/summary")]
    [TestCase("/login")]
    [TestCase("/health")]
    public async Task AJsonEndpointDoesNotCarryThem(string path)
    {
        DefaultHttpContext http = await InvokeAsync(path);

        Assert.IsFalse(http.Response.Headers.ContainsKey("Content-Security-Policy"), path);
        Assert.IsFalse(http.Response.Headers.ContainsKey("X-Frame-Options"), path);
    }

    /// <summary>
    /// The policy allows inline script only through a per-request nonce, because the dashboard has two
    /// inline scripts it genuinely needs — the theme stamp that must run before the first paint, and
    /// the sign-in handler. Allowing inline script wholesale instead would give up most of what the
    /// policy is for, since injected script is inline script.
    /// </summary>
    [Test]
    public async Task ThePolicyCarriesANoncePublishedToThePage()
    {
        DefaultHttpContext http = await InvokeAsync("/");

        string? nonce = http.Items[SecurityHeadersMiddleware.NonceItemKey] as string;
        Assert.IsNotNull(nonce, "the layout reads the nonce from here to stamp its script tag");
        Assert.IsNotEmpty(nonce);

        string policy = http.Response.Headers["Content-Security-Policy"].ToString();
        Assert.That(policy, Does.Contain($"'nonce-{nonce}'"));

        // The concessions the nonce exists to avoid must not be there anyway.
        Assert.That(policy, Does.Not.Contain("unsafe-inline"));
        Assert.That(policy, Does.Not.Contain("unsafe-eval"));

        // frame-ancestors is the modern half of the anti-framing pair; X-Frame-Options is the legacy
        // half. Both are set, and losing this one silently would leave only the header browsers are
        // steadily de-emphasising.
        Assert.That(policy, Does.Contain("frame-ancestors 'none'"));
    }

    /// <summary>
    /// A nonce that repeated across requests would be as good as none: an injected script could carry
    /// a value harvested from an earlier response.
    /// </summary>
    [Test]
    public async Task EachRequestGetsItsOwnNonce()
    {
        DefaultHttpContext first = await InvokeAsync("/");
        DefaultHttpContext second = await InvokeAsync("/");

        Assert.AreNotEqual(
            first.Items[SecurityHeadersMiddleware.NonceItemKey],
            second.Items[SecurityHeadersMiddleware.NonceItemKey]);
    }
}
