
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct ExecuteNonSQLResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int ModifiedRows { get; }

    /// <summary>
    /// A human-readable note about an outcome that succeeded but may not be what the caller intended
    /// — today, a time-travel copy that read no rows, which can mean the requested history was already
    /// reclaimed. Null when there is nothing to report. See
    /// <see cref="ExecuteDDLSQLResult.Warning"/> for why this is carried in the result rather than
    /// only logged.
    /// </summary>
    public string? Warning { get; }

    public ExecuteNonSQLResult(DatabaseDescriptor database, TableDescriptor table, int modifiedRows, string? warning = null)
    {
        Database = database;
        Table = table;
        ModifiedRows = modifiedRows;
        Warning = warning;
    }
}
