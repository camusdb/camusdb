/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Body of <c>/prepare-sql-statement</c>: the database and SQL a handle will stand for. Both are
/// captured once and reused by every execution of the resulting handle, so neither travels again.
/// </summary>
public sealed class PrepareStatementRequest
{
    public string? DatabaseName { get; set; }

    public string? Sql { get; set; }
}

/// <summary>
/// Body of <c>/close-sql-statement</c>. Closing a handle that is unknown or already closed succeeds:
/// the caller asked for it to be gone, and it is.
/// </summary>
public sealed class CloseStatementRequest
{
    public string? StatementId { get; set; }
}
