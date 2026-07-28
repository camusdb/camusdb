
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Grpc;

namespace CamusDB.App.Grpc;

/// <summary>
/// gRPC credential exchange — the counterpart of the REST <c>/login</c> and <c>/logout</c> routes, so a
/// gRPC-only deployment no longer has to expose the HTTP port just to obtain a token.
///
/// <para>These are the only RPCs that must work <b>without</b> a bearer token, since a client has none
/// until <see cref="Login"/> returns one. That exemption is declared in one place — the transport-wide
/// <c>AuthenticationMiddleware</c> — and the methods here validate their own input instead.</para>
///
/// <para>A password crosses the wire on <see cref="Login"/>, so the TLS gate runs <b>before</b> the
/// credentials are read: refusing after deserializing would still have accepted the password in the
/// clear. Failures are deliberately uniform — an unknown user, an account with no password, and a wrong
/// password all surface as the same <c>UNAUTHENTICATED</c> with the same code, so replies cannot be used
/// to enumerate accounts — and nothing derived from the credentials is ever logged.</para>
/// </summary>
public sealed class CamusAuthService : CamusAuth.CamusAuthBase
{
    private readonly CommandExecutor executor;
    private readonly ILogger<ICamusDB> logger;

    public CamusAuthService(CommandExecutor executor, ILogger<ICamusDB> logger)
    {
        this.executor = executor;
        this.logger   = logger;
    }

    /// <summary>
    /// Exchanges a password for a short-lived bearer token, reporting the session's absolute expiry and
    /// the equivalent seconds-from-now so a client can renew ahead of it rather than discovering the
    /// expiry from a failed statement.
    ///
    /// <para>The peer address is passed through as the rate-limiting source, matching REST: the limiter
    /// is per (account, source), so dropping it would let a login flood from many peers share — and
    /// exhaust — one bucket.</para>
    /// </summary>
    public override async Task<LoginReply> Login(LoginRequest request, ServerCallContext context)
    {
        try
        {
            // Before touching the credentials: a password must never travel in the clear.
            GrpcTransportSecurity.EnsureSecureTransport(context);

            if (string.IsNullOrEmpty(request.User) || request.Password is null)
                throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

            string source = SourceOf(context);
            LoginResult result = await executor.LoginAsync(request.User, request.Password, source).ConfigureAwait(false);

            return new LoginReply
            {
                Token            = result.Token,
                ExpiresAtUnixMs  = new DateTimeOffset(result.ExpiresAt, TimeSpan.Zero).ToUnixTimeMilliseconds(),
                ExpiresInSeconds = result.SecondsUntilExpiry(DateTime.UtcNow),
            };
        }
        catch (CamusDBException e)
        {
            // Log the code only — never the user or anything derived from the password.
            logger.LogWarning("Login failed: {Code}", e.Code);
            throw GrpcErrorMapper.ToRpcException(e);
        }
    }

    /// <summary>
    /// Revokes the token presented in the <c>authorization</c> metadata. Idempotent: an absent, malformed,
    /// or already-revoked token still succeeds, because the requested end state — that token no longer
    /// being usable — holds either way, and reporting the difference would tell an unauthenticated caller
    /// whether a token was valid.
    /// </summary>
    public override async Task<LogoutReply> Logout(LogoutRequest request, ServerCallContext context)
    {
        try
        {
            await executor.LogoutAsync(BearerOf(context)).ConfigureAwait(false);
            return new LogoutReply();
        }
        catch (CamusDBException e)
        {
            throw GrpcErrorMapper.ToRpcException(e);
        }
    }

    /// <summary>Extracts the bearer token from the call's <c>authorization</c> metadata, or null.</summary>
    private static string? BearerOf(ServerCallContext context)
    {
        string? authorization = context.RequestHeaders.GetValue("authorization");
        return authorization is not null
               && authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : null;
    }

    /// <summary>
    /// The caller's origin for per-source login rate limiting. Falls back to the empty string when there
    /// is no ASP.NET connection (an in-process test call), which the limiter treats as one shared bucket.
    /// </summary>
    private static string SourceOf(ServerCallContext context)
    {
        try
        {
            return context.GetHttpContext()?.Connection.RemoteIpAddress?.ToString() ?? "";
        }
        catch
        {
            return "";
        }
    }
}
