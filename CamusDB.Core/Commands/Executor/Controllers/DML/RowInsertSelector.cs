
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Executes <c>INSERT INTO t [(c1, …)] SELECT …</c>: drains the source query's rows, maps them onto
/// the target columns, and writes them through the ordinary <see cref="RowInserter"/> so index
/// maintenance, unique/CHECK/NOT NULL enforcement and TTL behave exactly as for
/// <c>INSERT … VALUES</c>.
///
/// <para><b>Every source row is read before the first row is written.</b> This is not an
/// optimization choice, it is what makes <c>INSERT INTO t SELECT … FROM t</c> terminate: within one
/// transaction a scan observes that transaction's own staged writes, so a streaming copy would
/// re-read the rows it had just inserted and never finish. The buffer is bounded because rows-read
/// equals rows-inserted here and a transaction may not insert more than
/// <see cref="CamusDBOptions.MaxMutationsPerTransaction"/> rows — the drain enforces that ceiling as
/// it reads rather than after materialising an unbounded set.</para>
/// </summary>
internal sealed class RowInsertSelector
{
    /// <summary>
    /// Maps the source's output columns onto the target columns <b>positionally</b> and inserts them.
    /// </summary>
    /// <param name="sourceColumns">
    /// The source query's output columns, in projection order — the same schema the query would
    /// report to a client, so a column's type here is the type a <c>SELECT</c> of it would return.
    /// </param>
    /// <param name="cursor">
    /// The unexecuted source row cursor. It is lazy, so nothing has been read yet and the arity
    /// check below still runs before the query does any work.
    /// </param>
    public async Task<int> InsertSelect(
        RowInserter rowInserter,
        StatisticsManager statisticsManager,
        DatabaseDescriptor database,
        TableDescriptor table,
        InsertSelectTicket ticket,
        IReadOnlyList<DerivedColumnSchema> sourceColumns,
        IAsyncEnumerable<QueryResultRow> cursor)
    {
        // Target columns: the explicit list, or every column in schema order. The implicit list
        // deliberately mirrors INSERT … VALUES (which also takes every column) so the two forms
        // cannot disagree about what "no column list" means.
        List<string> targetColumns;

        if (ticket.TargetColumns is not null)
        {
            targetColumns = new(ticket.TargetColumns);
        }
        else
        {
            targetColumns = new(table.Schema.Columns!.Count);
            foreach (TableColumnSchema column in table.Schema.Columns!)
                targetColumns.Add(column.Name);
        }

        if (sourceColumns.Count != targetColumns.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The number of target columns is not equal to the number of source columns. " +
                $"Columns={targetColumns.Count} != Source={sourceColumns.Count}"
            );

        InsertRowShaper shaper = InsertRowShaper.Create(table.Schema, targetColumns);

        int mutationLimit = database.Options.MaxMutationsPerTransaction;

        // Read everything first (see the class summary), failing fast at the mutation ceiling.
        List<Dictionary<string, ColumnValue>> rows = new();
        ColumnValue?[] slots = new ColumnValue?[targetColumns.Count];

        await foreach (QueryResultRow sourceRow in cursor.ConfigureAwait(false))
        {
            if (rows.Count >= mutationLimit)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMutationLimitExceeded,
                    $"INSERT ... SELECT would insert more than {mutationLimit} rows, which exceeds the " +
                    $"per-transaction mutation limit. Narrow the source query (for example with WHERE or " +
                    $"LIMIT) and run it in several transactions."
                );

            for (int i = 0; i < sourceColumns.Count; i++)
            {
                // Resolved by the source's output name — the same name the projection publishes and,
                // for a join, the {alias}.{column} form the join executor keys its rows by. A name the
                // cursor did not produce contributes no value, so the target column takes its default.
                slots[i] = sourceRow.Row.TryGetValue(sourceColumns[i].Name, out ColumnValue? value)
                    ? value
                    : null;
            }

            rows.Add(shaper.ShapeRow(slots));
        }

        if (rows.Count == 0)
            return 0;

        // Write in pages so a large copy does not hold every serialized row at once. All pages share
        // the caller's transaction, so a failure in a later page rolls back the earlier ones.
        int pageSize = Math.Max(1, database.Options.SpillEffectiveThreshold);
        int inserted = 0;

        for (int offset = 0; offset < rows.Count; offset += pageSize)
        {
            List<Dictionary<string, ColumnValue>> page = rows.GetRange(offset, Math.Min(pageSize, rows.Count - offset));

            InsertTicket pageTicket = new(
                txnState: ticket.TxnState,
                databaseName: ticket.DatabaseName,
                tableName: ticket.TableName,
                values: page
            );

            inserted += await rowInserter.Insert(database, table, pageTicket).ConfigureAwait(false);
            statisticsManager.TrackInsert(database, table, page.Count, page);
        }

        return inserted;
    }
}
