
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

internal sealed class UpdateFluxState
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public UpdateTicket Ticket { get; }

    public QueryExecutor QueryExecutor { get; }

    /// <summary>
    /// Matched rows buffered before update (Halloween problem). Uses <see cref="SpillableRowList"/>
    /// so that very large matched sets spill to disk rather than exhausting the process heap.
    /// Owned by <c>RowUpdater.UpdateInternal</c>, which disposes it after the mutation phase.
    /// </summary>
    public SpillableRowList? RowsToUpdate { get; set; }

    /// <summary>
    /// The ticket the locate scan ran with. The write phase re-evaluates the statement's predicate on
    /// each row it read under lock, and evaluates it with this ticket so the parameters and the filter
    /// rules are the ones the scan used. Set by the locate step.
    /// </summary>
    public QueryTicket? LocateTicket { get; set; }

    public int ModifiedRows { get; set; }

    /// <summary>
    /// Collects the keys the updated rows changed. <see cref="ForeignKeyStatementChecker.None"/> when the
    /// statement assigns no column of an enforced foreign key.
    /// </summary>
    internal ForeignKeyStatementChecker ForeignKeys { get; init; } = ForeignKeyStatementChecker.None;

    /// <summary>
    /// Where the statement's retryable Kahuna abort is recorded instead of thrown, or null to throw.
    /// Each step tests it after every call it passes it to and aborts the machine when it is set.
    /// See <see cref="Transactions.RetryableAbortSink"/>.
    /// </summary>
    public Transactions.RetryableAbortSink? RetryableAborts { get; }

    public UpdateFluxState(
        DatabaseDescriptor database,
        TableDescriptor table,
        UpdateTicket ticket,
        QueryExecutor queryExecutor,
        Transactions.RetryableAbortSink? retryableAborts = null
    )
    {
        Database = database;
        Table = table;
        Ticket = ticket;
        QueryExecutor = queryExecutor;
        RetryableAborts = retryableAborts;
    }
}
