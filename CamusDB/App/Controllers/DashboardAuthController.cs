/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

/// <summary>
/// Browser sign-in for the dashboard.
///
/// <para>It issues no new kind of credential. <c>/login</c> already exchanges a password for a
/// short-lived opaque bearer token, and this endpoint calls the same code; the only difference is
/// where the token is put. An API client keeps it and sends it in a header. A browser cannot keep a
/// secret safely in script, so the token goes into a cookie the script cannot read.</para>
///
/// <para>The cookie is <c>HttpOnly</c> so a cross-site scripting flaw cannot exfiltrate the token,
/// <c>SameSite=Strict</c> so another origin cannot cause it to be sent, and <c>Secure</c> whenever
/// the connection is TLS. It expires exactly when the token does, so a stale cookie never lingers
/// promising access it no longer has. What actually confines the cookie to read-only routes is
/// <see cref="DashboardSession"/> — read the reasoning there.</para>
/// </summary>
[ApiController]
public sealed class DashboardAuthController : CommandsController
{
    public DashboardAuthController(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        ILogger<ICamusDB> logger,
        CamusDBOptions options)
        : base(executor, transactions, logger, options)
    {
    }

    /// <summary>
    /// Exchanges a user and password for a session cookie.
    /// </summary>
    [HttpPost]
    [Route("/v1/dashboard/login")]
    public async Task<IActionResult> SignIn()
    {
        // A credential answer must never be cached by an intermediary.
        Response.Headers.CacheControl = "no-store";

        try
        {
            // 404, matching the page: with the dashboard switched off this surface does not exist
            // on this node, and "forbidden" would advertise something an operator removed.
            if (!options.DashboardEnabled)
                return NotFound();

            // A password must never travel over a plaintext connection.
            EnsureSecureTransport();

            if (!options.AuthenticationEnabled)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Authentication is disabled on this node; the dashboard is served to loopback without signing in");

            LoginRequest? request = await JsonSerializer
                .DeserializeAsync<LoginRequest>(Request.Body, jsonOptions).ConfigureAwait(false);

            if (request is null || string.IsNullOrEmpty(request.User) || request.Password is null)
                throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

            string source = HttpContext.Connection.RemoteIpAddress?.ToString() ?? "";
            LoginResult result = await executor.LoginAsync(request.User, request.Password, source).ConfigureAwait(false);

            Response.Cookies.Append(DashboardSession.CookieName, result.Token, new CookieOptions
            {
                HttpOnly = true,
                Secure = Request.IsHttps,
                SameSite = SameSiteMode.Strict,
                Path = "/",
                Expires = new DateTimeOffset(result.ExpiresAt, TimeSpan.Zero),
            });

            return new JsonResult(new DashboardLoginResponse { Redirect = DashboardSession.DashboardPage });
        }
        catch (CamusDBException e)
        {
            // Never log the credential. The code is enough to tell a bad password from a locked-out
            // user or a rate limit.
            logger.LogWarning("Dashboard sign-in failed: {Code}", e.Code);

            return new JsonResult(new DashboardLoginResponse
            {
                Status = "failed",
                Code = e.Code,
                Message = e.Message,
            })
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
    }

    /// <summary>
    /// Revokes the session token, then clears the cookie.
    ///
    /// <para>The order matters and the clearing is unconditional. Revocation is what actually ends
    /// the session; deleting the cookie only stops this browser from presenting it. If revocation
    /// fails, the cookie is still cleared — leaving a browser holding a credential the operator
    /// believes they have just given up would be the worse of the two failures.</para>
    /// </summary>
    [HttpPost]
    [Route("/v1/dashboard/logout")]
    public async Task<IActionResult> SignOutSession()
    {
        Response.Headers.CacheControl = "no-store";

        try
        {
            string? token = Request.Cookies[DashboardSession.CookieName];
            if (!string.IsNullOrEmpty(token))
                await executor.LogoutAsync(token).ConfigureAwait(false);

            return new JsonResult(new DashboardLoginResponse { Redirect = DashboardSession.SignInPage });
        }
        catch (CamusDBException e)
        {
            logger.LogWarning("Dashboard sign-out failed: {Code}", e.Code);
            return new JsonResult(new DashboardLoginResponse
            {
                Status = "failed",
                Code = e.Code,
                Message = e.Message,
                Redirect = DashboardSession.SignInPage,
            });
        }
        finally
        {
            Response.Cookies.Delete(DashboardSession.CookieName, new CookieOptions
            {
                HttpOnly = true,
                Secure = Request.IsHttps,
                SameSite = SameSiteMode.Strict,
                Path = "/",
            });
        }
    }
}
