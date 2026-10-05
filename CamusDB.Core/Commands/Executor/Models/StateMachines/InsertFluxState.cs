
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public sealed class InsertFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public InsertTicket Ticket { get; }

    //public InsertFluxIndexState Indexes { get; }

    //public List<BufferPageOperation> ModifiedPages { get; } = new();

    //public List<IDisposable> Locks { get; } = new();

    public int InsertedRows { get; set; }

    /// <summary>
    /// Collects the parent keys the inserted rows reference. <see cref="Controllers.ForeignKeyStatementChecker.None"/>
    /// when the table owns no enforced foreign key.
    /// </summary>
    internal Controllers.ForeignKeyStatementChecker ForeignKeys { get; init; } = Controllers.ForeignKeyStatementChecker.None;

    /// <summary>
    /// Receives each written row as a (row id, shaped values) pair for an <c>INSERT … RETURNING</c>,
    /// or null when the statement returns no rows. The values are the dictionary the row was encoded
    /// from — after coercion, defaults and sequence draws — so RETURNING reports what was stored. A
    /// failed write throws out of the statement, and the caller then discards this list with the
    /// statement, so it never reaches a client holding a row that was not stored.
    /// </summary>
    internal List<QueryResultRow>? ReturningRows { get; init; }

    public InsertFluxState(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        //Indexes = indexes;
    }
}
