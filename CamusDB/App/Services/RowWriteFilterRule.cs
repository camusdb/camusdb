/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;

namespace CamusDB.App.Services;

/// <summary>
/// The typed row APIs (HTTP <c>/update</c> and <c>/delete</c>, gRPC <c>UpdateRows</c> and
/// <c>DeleteRows</c>) refuse a request that carries no filter. The SQL grammar refuses UPDATE and
/// DELETE without WHERE, so that one statement cannot change or delete every row by accident; a typed
/// request with no filter would otherwise get around that rule, because a ticket with no filter
/// matches every row.
/// </summary>
/// <remarks>
/// The rule lives at the API edge and not in the ticket validators on purpose: engine code builds
/// filterless tickets for deliberate full-table writes. The refusal stages nothing. In an explicit
/// transaction, call it inside the path that rolls back on error, so a refused request ends the
/// transaction exactly as a refused SQL statement does.
/// </remarks>
internal static class RowWriteFilterRule
{
    public const string Update = "UPDATE";

    public const string Delete = "DELETE";

    /// <summary>Throws <see cref="CamusDBErrorCodes.InvalidInput"/> when <paramref name="filterCount"/> is zero.</summary>
    public static void Require(int filterCount, string statement)
    {
        if (filterCount > 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            statement + " requires at least one filter, so that one request cannot change every row by accident. " +
            "To " + (statement == Delete ? "delete" : "update") + " every row, use SQL with WHERE TRUE");
    }
}
