
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct DeleteResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int DeletedRows { get; }

    public DeleteResult(DatabaseDescriptor database, TableDescriptor table, int deletedRows)
    {
        Database = database;
        Table = table;
        DeletedRows = deletedRows;
    }
}
