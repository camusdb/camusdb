/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>Request body for <c>/login</c>: a user name and cleartext password (over TLS).</summary>
public sealed class LoginRequest
{
    public string? User { get; set; }

    public string? Password { get; set; }
}
