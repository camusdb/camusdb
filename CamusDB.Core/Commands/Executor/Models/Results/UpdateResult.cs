
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct UpdateResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int UpdatedRows { get; }

    public UpdateResult(DatabaseDescriptor database, TableDescriptor table, int updatedRows)
    {
        Database = database;
        Table = table;
        UpdatedRows = updatedRows;
    }
}

