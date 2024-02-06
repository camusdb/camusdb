
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

    public ExecuteNonSQLResult(DatabaseDescriptor database, TableDescriptor table, int modifiedRows)
    {
        Database = database;
        Table = table;
        ModifiedRows = modifiedRows;
    }
}
