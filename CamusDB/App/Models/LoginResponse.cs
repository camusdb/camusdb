/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>Response for <c>/login</c> and <c>/logout</c>. On success carries the opaque bearer
/// <see cref="Token"/> (login only); on failure carries the error <see cref="Code"/>/<see cref="Message"/>.</summary>
public sealed class LoginResponse
{
    public string Status { get; set; } = "ok";

    public string? Token { get; set; }

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
