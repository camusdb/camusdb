/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.RegularExpressions;

namespace CamusDB.Core.Auth;

/// <summary>
/// Redacts the cleartext password out of <c>CREATE USER</c> / <c>ALTER USER … IDENTIFIED … BY '…'</c>
/// SQL before it reaches any log, trace, or metric. A statement text is otherwise safe to log, but the
/// password literal is a credential — a request body containing <c>IDENTIFIED BY 'secret'</c> must never
/// place that secret in application logs. Every transport must pass SQL through this before
/// <c>LogExecutingSql</c>/request-body logging.
///
/// <para>Only the quoted literal after <c>BY</c> is replaced (with <c>'***'</c>); a parameterized
/// password (<c>BY @p</c>) has no literal and is left as-is. The regex tolerates the optional
/// <c>WITH &lt;plugin&gt;</c> clause and doubled single quotes inside the literal.</para>
/// </summary>
public static partial class SqlCredentialRedactor
{
    [GeneratedRegex(@"(IDENTIFIED\s+(?:WITH\s+[A-Za-z0-9_]+\s+)?BY\s+)'(?:[^']|'')*'",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex IdentifiedByLiteral();

    public static string Redact(string? sql)
    {
        if (string.IsNullOrEmpty(sql))
            return sql ?? "";

        // Cheap guard: only pay for the regex when the statement could carry a credential literal.
        if (sql.IndexOf("identified", StringComparison.OrdinalIgnoreCase) < 0)
            return sql;

        return IdentifiedByLiteral().Replace(sql, "$1'***'");
    }
}
