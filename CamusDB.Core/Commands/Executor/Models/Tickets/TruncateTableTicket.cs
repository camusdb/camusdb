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
/// <para>Deliberately minimal: <c>TRUNCATE</c> takes exactly one target and has no options. The
/// multi-table form, <c>RESTART IDENTITY</c> and <c>CASCADE</c> are all out of scope, so there is no
/// flag here that could be misread as supporting one of them.</para>
/// </summary>
public readonly struct TruncateTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public TruncateTableTicket(string databaseName, string tableName)
    {
        DatabaseName = databaseName;
        TableName = tableName;
    }
}
