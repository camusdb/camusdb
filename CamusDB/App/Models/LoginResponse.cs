/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>Response for <c>/login</c> and <c>/logout</c>. On success carries the opaque bearer
/// <see cref="Token"/> and its expiry (login only); on failure carries the error
/// <see cref="Code"/>/<see cref="Message"/>.</summary>
public sealed class LoginResponse
{
    public string Status { get; set; } = "ok";

    public string? Token { get; set; }

    /// <summary>
    /// Unix epoch milliseconds (UTC) after which <see cref="Token"/> is rejected. Null on logout and on
    /// failure. Authoritative: a client should renew before this instant rather than assume a lifetime,
    /// since the server's token TTL is configurable and may be shorter than any value a driver defaults to.
    /// </summary>
    public long? ExpiresAtUnixMs { get; set; }

    /// <summary>
    /// Whole seconds until the token expires, as measured by the <b>server</b> when it issued the reply.
    /// Redundant with <see cref="ExpiresAtUnixMs"/> on purpose: it lets a client whose clock disagrees
    /// with the server's renew on a monotonic timer instead of comparing absolute instants.
    /// </summary>
    public long? ExpiresInSeconds { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public LoginResponse() { }

    public LoginResponse(string status, string? token = null, string? code = null, string? message = null)
    {
        Status = status;
        Token = token;
        Code = code;
        Message = message;
    }
}
