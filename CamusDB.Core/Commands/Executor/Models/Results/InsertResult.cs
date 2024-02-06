
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct InsertResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int InsertedRows { get; }

    public InsertResult(DatabaseDescriptor database, TableDescriptor table, int insertedRows)
    {
        Database = database;
        Table = table;
        InsertedRows = insertedRows;
    }
}
