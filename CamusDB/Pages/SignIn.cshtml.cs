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
/// The browser sign-in page. It posts to <c>/v1/dashboard/login</c>, which exchanges the credential
/// for the session cookie; this model renders the form and nothing else.
///
/// <para>It is reachable without a credential — see <see cref="DashboardSession"/> — because it is
/// the page whose whole purpose is to obtain one. It carries no navigation bar: a sign-out control
/// and a link to a dashboard the visitor cannot yet load would both be dead ends.</para>
/// </summary>
public class SignInModel : PageModel
{
    private readonly CamusDBOptions options;

    public SignInModel(CamusDBOptions options) => this.options = options;

    /// <summary>
    /// Serves the form, or redirects when there is nothing to sign in to.
    ///
    /// <para>With the dashboard off the page does not exist (404). With authentication off there is
    /// no credential to give and no principal to resolve, so the dashboard is served to loopback
    /// directly — offering a sign-in form there would ask for a password that nothing checks.</para>
    /// </summary>
    public IActionResult OnGet()
    {
        if (!options.DashboardEnabled)
            return NotFound();

        // The same rule the endpoints apply. Serving the page to the network while every panel
        // answers 403 would look like a broken dashboard rather than a closed one.
        if (!DashboardSession.IsServedTo(options, HttpContext.Connection.RemoteIpAddress))
            return StatusCode(StatusCodes.Status403Forbidden, DashboardSession.NetworkRefusalMessage);

        if (!options.AuthenticationEnabled)
            return Redirect(DashboardSession.DashboardPage);

        return Page();
    }
}
