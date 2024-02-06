
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct ExecuteDDLSQLResult
{
    public DatabaseDescriptor Database { get; }    

    public bool Success { get; }

    public ExecuteDDLSQLResult(DatabaseDescriptor database, bool success)
    {
        Database = database;
        Success = success;
    }
}
