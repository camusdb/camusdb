/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Request to empty one base table by replacing the key-space its rows live in.
///
/// <para>Deliberately minimal: <c>TRUNCATE</c> takes exactly one target. The multi-table form and
/// <c>CASCADE</c> remain out of scope, so there is no flag here that could be misread as supporting
/// either.</para>
/// </summary>
public readonly struct TruncateTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    /// <summary>
    /// True when <c>RESTART IDENTITY</c> was written: every sequence <b>owned by</b> a column of
    /// this relation returns to its recorded start value.
    ///
    /// <para>Owned only. A sequence a column merely defaults from is shared — other tables may draw
    /// from it — and silently resetting one would be a data-loss-shaped surprise. That is also
    /// PostgreSQL's rule.</para>
    /// </summary>
    public bool RestartIdentity { get; }

    public TruncateTableTicket(string databaseName, string tableName, bool restartIdentity = false)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        RestartIdentity = restartIdentity;
    }
}
