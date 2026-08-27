/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.App.Services;

namespace CamusDB.Tests.Dashboard;

/// <summary>
/// The dashboard's access rules, tested where they are decided.
///
/// <para>These are pure functions, and that is deliberate: they encode a security boundary, so they
/// were written to be checkable without a running node. Two properties matter more than anything
/// else on this page.</para>
///
/// <para><b>The session cookie must never reach a write route.</b> Every non-dashboard route
/// authenticates by the <c>Authorization</c> header alone, and a browser never sends that header by
/// itself — which is what makes cross-site request forgery impossible against them. A cookie is
/// sent automatically, so the moment one is accepted on <c>/insert</c> or
/// <c>/execute-sql-non-query</c>, any web page in the world can drive this database as the signed-in
/// operator. The route classification below is the whole of that boundary.</para>
///
/// <para><b>With authentication off, the dashboard is loopback-only.</b> There is then no principal
/// to gate any panel on, so the page would hand this node's configuration, metrics and database list
/// to whoever reaches the port.</para>
/// </summary>
[TestFixture]
public class TestDashboardAccess
{
    [Test]
    public void DashboardPagesAreRecognized()
    {
        Assert.IsTrue(DashboardSession.IsDashboardPage("/"));
        Assert.IsTrue(DashboardSession.IsDashboardPage("/Index"));
        Assert.IsTrue(DashboardSession.IsDashboardPage("/SignIn"));

        // Razor Pages matches its route case-insensitively, so the classification must too — or a
        // request to /signin would be treated as an ordinary authenticated route and refused.
        Assert.IsTrue(DashboardSession.IsDashboardPage("/signin"));
        Assert.IsTrue(DashboardSession.IsDashboardPage("/index"));
    }

    [Test]
    public void DashboardEndpointsAreRoutesButNotPages()
    {
        foreach (string path in new[]
        {
            "/v1/dashboard/summary",
            "/v1/dashboard/metrics",
            "/v1/dashboard/databases",
            "/v1/dashboard/databases/shop/tables",
            "/v1/dashboard/config",
        })
        {
            Assert.IsTrue(DashboardSession.IsDashboardRoute(path), path);

            // Not a page: a failure here must answer JSON the polling script can branch on, never a
            // redirect the script would follow and then try to parse as JSON.
            Assert.IsFalse(DashboardSession.IsDashboardPage(path), path);
        }
    }

    /// <summary>
    /// The boundary that keeps the session cookie away from every route that can change data. If
    /// this ever passes for one of these paths, the cookie authenticates a write and the database is
    /// open to cross-site request forgery.
    /// </summary>
    [Test]
    public void WriteRoutesAreNeverDashboardRoutes()
    {
        foreach (string path in new[]
        {
            "/execute-sql-query",
            "/execute-sql-non-query",
            "/execute-sql-ddl",
            "/execute-sql-query-stream",
            "/insert",
            "/update",
            "/delete",
            "/query",
            "/query-by-id",
            "/create-db",
            "/drop-db",
            "/create-table",
            "/start-transaction",
            "/commit-transaction",
            "/rollback-transaction",
            "/prepare-sql-statement",
            "/v1/backups",
            "/v1/restore",
            "/v1/cluster/leave",
            "/internal/schema-ddl/create-table",
        })
        {
            Assert.IsFalse(DashboardSession.IsDashboardRoute(path), path);
        }
    }

    /// <summary>
    /// A path that merely starts with the dashboard prefix as a substring must not qualify. Prefix
    /// matching is easy to get subtly wrong, and the cost of getting it wrong is the boundary above.
    /// </summary>
    [Test]
    public void LookalikePathsAreNotDashboardRoutes()
    {
        Assert.IsFalse(DashboardSession.IsDashboardRoute("/v1/dashboardx/summary"));
        Assert.IsFalse(DashboardSession.IsDashboardRoute("/v1/dashboard"));
        Assert.IsFalse(DashboardSession.IsDashboardRoute("/api/v1/dashboard/summary"));
        Assert.IsFalse(DashboardSession.IsDashboardPage("/SignInPage"));
    }

    [Test]
    public void OnlyTheSignInRoutesAreAnonymous()
    {
        Assert.IsTrue(DashboardSession.IsAnonymousDashboardRoute("/SignIn"));
        Assert.IsTrue(DashboardSession.IsAnonymousDashboardRoute("/v1/dashboard/login"));
        Assert.IsTrue(DashboardSession.IsAnonymousDashboardRoute("/v1/dashboard/logout"));

        // The dashboard itself is not: reaching it without a credential is the bug the redirect to
        // the sign-in page exists to prevent.
        Assert.IsFalse(DashboardSession.IsAnonymousDashboardRoute("/"));
        Assert.IsFalse(DashboardSession.IsAnonymousDashboardRoute("/v1/dashboard/summary"));
        Assert.IsFalse(DashboardSession.IsAnonymousDashboardRoute("/v1/dashboard/config"));
    }

    /// <summary>
    /// With authentication on, every caller may reach the surface and each panel is gated by the
    /// statement behind it. The address is then irrelevant.
    /// </summary>
    [Test]
    public void WithAuthenticationOnEveryAddressIsServed()
    {
        CamusDBOptions options = CamusDBOptions.Default with { AuthenticationEnabled = true };

        Assert.IsTrue(DashboardSession.IsServedTo(options, IPAddress.Loopback));
        Assert.IsTrue(DashboardSession.IsServedTo(options, IPAddress.Parse("192.168.10.8")));
        Assert.IsTrue(DashboardSession.IsServedTo(options, IPAddress.IPv6Loopback));

        // A null remote address happens for an in-process or unix-socket caller. With a credential
        // required it changes nothing.
        Assert.IsTrue(DashboardSession.IsServedTo(options, null));
    }

    /// <summary>
    /// With authentication off there is no principal, so only loopback is served. A null remote
    /// address must fail closed: it is not evidence of a local caller.
    /// </summary>
    [Test]
    public void WithAuthenticationOffOnlyLoopbackIsServed()
    {
        CamusDBOptions options = CamusDBOptions.Default with { AuthenticationEnabled = false };

        Assert.IsTrue(DashboardSession.IsServedTo(options, IPAddress.Loopback));
        Assert.IsTrue(DashboardSession.IsServedTo(options, IPAddress.IPv6Loopback));
        Assert.IsTrue(DashboardSession.IsServedTo(options, IPAddress.Parse("127.0.0.2")));

        Assert.IsFalse(DashboardSession.IsServedTo(options, IPAddress.Parse("192.168.10.8")));
        Assert.IsFalse(DashboardSession.IsServedTo(options, IPAddress.Parse("10.0.4.21")));
        Assert.IsFalse(DashboardSession.IsServedTo(options, IPAddress.Parse("::ffff:10.0.4.21")));
        Assert.IsFalse(DashboardSession.IsServedTo(options, null));
    }

    /// <summary>
    /// The defaults an operator gets without writing any configuration: the dashboard is on, and it
    /// refreshes often enough to be useful without being a load.
    /// </summary>
    [Test]
    public void DashboardDefaultsAreOnAndModest()
    {
        CamusDBOptions defaults = CamusDBOptions.Default;

        Assert.IsTrue(defaults.DashboardEnabled);
        Assert.AreEqual(2, defaults.DashboardRefreshSeconds);
    }
}
