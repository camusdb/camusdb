
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Runs the <see cref="LockRowsNode"/> of a <c>SELECT … FOR UPDATE</c>: lock, then read again, then
/// re-check, then emit. The order is the lost-update rule of <c>RowUpdater.UpdateLocatedRowsAsync</c>:
/// the scan below read each row without a lock (at the statement timestamp under Read Committed), so a
/// value returned from that read could already be stale when the lock is granted. The step therefore
/// returns the values of the read it makes <em>after</em> it holds the lock, and drops a row that a
/// concurrent commit deleted or moved out of the WHERE clause (<see cref="MutationRowRecheck"/>).
///
/// <para><b>Pages sized by the LIMIT.</b> Each page asks the input for at most
/// <see cref="CamusDBOptions.IndexScanFetchBatchSize"/> candidates, and never for more than the rows the
/// LIMIT (plus OFFSET) still needs, so the last page locks no row past the LIMIT. A dropped or skipped
/// row does not count, so the next page asks for the rows still missing. A page costs one batched lock
/// round trip (plus one resend per held row under SKIP LOCKED) and one batched read.</para>
///
/// <para><b>Only row ids cross the page.</b> The step keeps the id of each candidate and nothing else,
/// so a candidate decoded with borrowed bytes never outlives its scan iteration here.</para>
/// </summary>
internal static class QueryRowLocker
{
    internal static async IAsyncEnumerable<QueryResultRow> LockRowsAsync(
        QueryPlan plan,
        QueryExecutor queryExecutor,
        IAsyncEnumerable<QueryResultRow> input,
        LockRowsNode node,
        [EnumeratorCancellation] CancellationToken enumeratorToken = default)
    {
        TableDescriptor table = plan.Table
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "A row-locking plan has no table");

        QueryTicket ticket = plan.Ticket;
        KvTransaction tx = ticket.TxnState;
        DatabaseDescriptor database = plan.Database;
        CancellationToken cancellationToken = ticket.CancellationToken;

        MutationRowRecheck recheck = MutationRowRecheck.Build(table.Schema, ticket.Where, ticket.Filters);
        IReadOnlySet<string>? decodeColumns = UnionOrAll(plan.ScanRequiredColumns, recheck.Columns, recheck.IsEmpty);

        RowEncoder.RowDecodeState decodeState = new() { SlotBackedDecode = database.Options.SlotBackedDecode };
        int pageCap = Math.Max(1, database.Options.IndexScanFetchBatchSize);
        long? budget = node.RowBudget;
        long emitted = 0;

        // Set only under EXPLAIN ANALYZE; the counters are read after the cursor is drained.
        PlanNodeStats? stats = node.Stats;

        List<ObjectIdValue> page = new(Math.Min(pageCap, 64));

        await using IAsyncEnumerator<QueryResultRow> candidates = input.GetAsyncEnumerator(enumeratorToken);
        bool inputLeft = true;

        while (inputLeft && (budget is null || emitted < budget.Value))
        {
            int want = budget is null ? pageCap : (int)Math.Min(pageCap, budget.Value - emitted);

            page.Clear();
            while (page.Count < want)
            {
                if (!await candidates.MoveNextAsync().ConfigureAwait(false))
                {
                    inputLeft = false;
                    break;
                }

                page.Add(candidates.Current.RowId);
            }

            if (page.Count == 0)
                break;

            // Test-only interleaving point; see QueryExecutor.TestBeforeRowLockHook. Read once so a
            // concurrent clear cannot fault it.
            if (queryExecutor.TestBeforeRowLockHook is { } beforeLockHook)
                await beforeLockHook().ConfigureAwait(false);

            // The lock and the read are the write side of the statement: like the write phase of an
            // UPDATE they run to completion once started, so they do not take the read's token. A
            // cancel lands at the next candidate pull instead.
            (ReadOnlyMemory<byte>?[] rows, bool[]? skipped) =
                await table.Store.LockAndReadRowsForLockingReadAsync(tx, page, node.WaitPolicy).ConfigureAwait(false);

            if (stats is not null)
            {
                int skippedCount = 0;
                if (skipped is not null)
                {
                    for (int i = 0; i < skipped.Length; i++)
                    {
                        if (skipped[i])
                            skippedCount++;
                    }
                }

                stats.RowsSkippedHeld += skippedCount;
                stats.RowsLocked += page.Count - skippedCount;
            }

            for (int i = 0; i < page.Count; i++)
            {
                // Deleted by a concurrent commit before the lock, or held by another transaction under
                // SKIP LOCKED: either way there is no row to return.
                if (rows[i] is not { Length: > 0 } data)
                {
                    if (stats is not null && skipped?[i] != true)
                        stats.RowsDroppedByRecheck++;

                    continue;
                }

                QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema,
                    tx.TransactionId,
                    page[i],
                    data,
                    database.Options,
                    decodeColumns,
                    plan.TableSchemaVersion,
                    decodeState).ConfigureAwait(false);

                // A concurrent commit changed the row after the scan chose it, and it no longer matches.
                if (!recheck.IsEmpty && !await recheck.MatchesAsync(queryExecutor, database, ticket, row).ConfigureAwait(false))
                {
                    if (stats is not null)
                        stats.RowsDroppedByRecheck++;

                    continue;
                }

                emitted++;
                yield return new QueryResultRow(page[i], row);
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    /// <summary>
    /// The columns the re-read decodes: what the scan decoded (the projection and the filter need
    /// them) plus what the re-check reads. A conjunct that an index seek answered, such as
    /// <c>id = 5</c> on a unique index, is not in the scan's residual filter, so the scan's set alone
    /// can miss a column the re-check reads. Null (every column) when either side needs every column.
    /// </summary>
    private static IReadOnlySet<string>? UnionOrAll(IReadOnlySet<string>? scanColumns, IReadOnlySet<string>? recheckColumns, bool recheckIsEmpty)
    {
        if (scanColumns is null)
            return null;

        if (recheckIsEmpty)
            return scanColumns;

        if (recheckColumns is null)
            return null;

        HashSet<string> union = new(scanColumns, StringComparer.OrdinalIgnoreCase);
        union.UnionWith(recheckColumns);
        return union;
    }
}
