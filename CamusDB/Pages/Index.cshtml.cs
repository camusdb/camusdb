/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using CamusDB.Core;
using CamusDB.App.Services;

namespace CamusDB.Pages;

/// <summary>
/// The dashboard shell. It renders frames only; every value is fetched by the page's script from
/// <c>/v1/dashboard/</c> after load.
///
/// <para>Authentication is already settled before this page model runs: the transport-wide
/// <c>AuthenticationMiddleware</c> either resolved a principal from the session cookie or redirected
/// the browser to the sign-in page. Nothing here re-checks it, because a second check that could
/// disagree with the first is worse than no second check.</para>
/// </summary>
public class IndexModel : PageModel
{
    private readonly CamusDBOptions options;

    public IndexModel(CamusDBOptions options) => this.options = options;

    /// <summary>Drives whether the layout offers a sign-out control. With authentication off there is no session to end.</summary>
    public bool AuthenticationEnabled => options.AuthenticationEnabled;

    /// <summary>
    /// Serves the page, or 404 when the dashboard is switched off.
    ///
    /// <para>404 rather than 403 is deliberate: with <c>dashboard_enabled</c> false the page does not
    /// exist on this node, and saying "forbidden" would advertise a surface an operator turned off.</para>
    /// </summary>
    public IActionResult OnGet()
    {
        if (!options.DashboardEnabled)
            return NotFound();

        // The same rule the endpoints apply. Serving the page to the network while every panel
        // answers 403 would look like a broken dashboard rather than a closed one.
        if (!DashboardSession.IsServedTo(options, HttpContext.Connection.RemoteIpAddress))
            return StatusCode(StatusCodes.Status403Forbidden, DashboardSession.NetworkRefusalMessage);

        return Page();
    }
}
