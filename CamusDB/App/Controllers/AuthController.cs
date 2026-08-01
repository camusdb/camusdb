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
/// Authentication endpoints. <c>/login</c> performs the deliberately expensive password verification
/// once and returns a short-lived opaque bearer token; ordinary SQL requests then present that token in
/// the <c>Authorization</c> header. <c>/logout</c> revokes the presented token.
/// </summary>
[ApiController]
public sealed class AuthController : CommandsController
{
    public AuthController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger,
        CamusDBOptions options)
        : base(executor, transactions, logger, options)
    {
    }

    [HttpPost]
    [Route("/login")]
    public async Task<JsonResult> Login()
    {
        // The token must never be cached by intermediaries.
        Response.Headers.CacheControl = "no-store";

        try
        {
            // A password must never travel over a plaintext connection.
            EnsureSecureTransport();

            LoginRequest? request = await JsonSerializer.DeserializeAsync<LoginRequest>(Request.Body, jsonOptions).ConfigureAwait(false);
            if (request is null || string.IsNullOrEmpty(request.User) || request.Password is null)
                throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

            string source = HttpContext.Connection.RemoteIpAddress?.ToString() ?? "";
            LoginResult result = await executor.LoginAsync(request.User, request.Password, source).ConfigureAwait(false);
            return new JsonResult(new LoginResponse("ok", token: result.Token)
            {
                ExpiresAtUnixMs  = new DateTimeOffset(result.ExpiresAt, TimeSpan.Zero).ToUnixTimeMilliseconds(),
                ExpiresInSeconds = result.SecondsUntilExpiry(DateTime.UtcNow),
            });
        }
        catch (CamusDBException e)
        {
            // Do not log credentials; log only the code.
            logger.LogWarning("Login failed: {Code}", e.Code);
            return new JsonResult(new LoginResponse("failed", code: e.Code, message: e.Message))
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code)
            };
        }
    }

    [HttpPost]
    [Route("/logout")]
    public async Task<JsonResult> Logout()
    {
        try
        {
            string authorization = Request.Headers.Authorization.ToString();
            string? bearer = authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
                ? authorization["Bearer ".Length..].Trim()
                : null;

            await executor.LogoutAsync(bearer).ConfigureAwait(false);
            return new JsonResult(new LoginResponse("ok"));
        }
        catch (CamusDBException e)
        {
            return new JsonResult(new LoginResponse("failed", code: e.Code, message: e.Message))
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code)
            };
        }
    }
}
