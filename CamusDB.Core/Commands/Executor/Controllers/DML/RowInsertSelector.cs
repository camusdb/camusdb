
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
    /// <param name="sequenceBinder">
    /// Reserves the values a sequence-backed column default will supply. The reservation is made
    /// after the drain and before the first row is shaped, so its size is the exact source row
    /// count rather than a guess — which is possible only because this path already reads every
    /// source row before it writes any.
    /// </param>
    /// <param name="statementTicket">
    /// The statement the insert came from, carrying the parameter dictionary the reservation is
    /// installed in. Distinct from <paramref name="ticket"/>, which describes the insert itself.
    /// </param>
    public async Task<int> InsertSelect(
        RowInserter rowInserter,
        StatisticsManager statisticsManager,
        Functions.SequenceStatementBinder sequenceBinder,
        DatabaseDescriptor database,
        TableDescriptor table,
        InsertSelectTicket ticket,
        ExecuteSQLTicket statementTicket,
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

        // GENERATED ALWAYS means the column's values can only come from its sequence, so a
        // statement that names it — including the implicit all-columns form — is refused.
        Functions.SequenceDefaults.RequireNoValueForAlwaysIdentity(table.Schema, targetColumns);

        InsertRowShaper shaper = InsertRowShaper.Create(table.Schema, targetColumns);

        int mutationLimit = database.Options.MaxMutationsPerTransaction;

        // Read everything first (see the class summary), failing fast at the mutation ceiling.
        // The source values are buffered as raw slot arrays rather than shaped rows: shaping is
        // what applies a sequence-backed default, and the run those defaults draw from can only be
        // reserved once the row count is known — which is after this drain.
        List<ColumnValue?[]> sourceRows = new();

        await foreach (QueryResultRow sourceRow in cursor.ConfigureAwait(false))
        {
            if (sourceRows.Count >= mutationLimit)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMutationLimitExceeded,
                    $"INSERT ... SELECT would insert more than {mutationLimit} rows, which exceeds the " +
                    $"per-transaction mutation limit. Narrow the source query (for example with WHERE or " +
                    $"LIMIT) and run it in several transactions."
                );

            ColumnValue?[] slots = new ColumnValue?[targetColumns.Count];

            for (int i = 0; i < sourceColumns.Count; i++)
            {
                // Resolved by the source column's row key — the key the projection stores the cell
                // under and, for a join, the {alias}.{column} form the join executor keys its rows by.
                // The display name is not used: two source columns may share it. A key the cursor did
                // not produce contributes no value, so the target column takes its default.
                slots[i] = sourceRow.Row.TryGetValue(sourceColumns[i].RowKey, out ColumnValue? value)
                    ? value
                    : null;
            }

            sourceRows.Add(slots);
        }

        if (sourceRows.Count == 0)
            return 0;

        // One reservation per sequence for the whole statement, sized to the exact row count, and
        // made before the first row is shaped so a sequence failure cannot fail the transaction
        // after rows have been written.
        ExecuteSQLTicket bound = await sequenceBinder.BindAsync(
            database,
            statementTicket,
            // Nothing of the source query is scanned here. It was bound and position-checked where
            // it was executed — a projection over a relation refuses a sequence call there, and a
            // FROM-less source is allowed one and has already drawn it. Re-scanning it from this
            // side would refuse the legal form and reserve a second run for the illegal one.
            statementAst: null,
            allowedRegion: null,
            rowCount: sourceRows.Count,
            defaultSequenceIds: shaper.DefaultSequenceIds,
            statementKind: "INSERT ... SELECT",
            statementTicket.CancellationToken).ConfigureAwait(false);

        List<Dictionary<string, ColumnValue>> rows = new(sourceRows.Count);

        foreach (ColumnValue?[] slots in sourceRows)
            rows.Add(shaper.ShapeRow(slots, bound.Parameters));

        Functions.SequenceStatementBinder.RecordDrawnValues(bound.Parameters, statementTicket.TxnState);

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
