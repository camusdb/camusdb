/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Cache;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Reads a whole query cursor into memory for a transport that sends its response only after the
/// last row: the buffered REST endpoints, and the RETURNING rows that the stream endpoints collect
/// before their commit. It stops with <see cref="CamusDBErrorCodes.ResultSetTooLarge"/> as soon as the
/// rows pass <see cref="CamusDBOptions.MaxBufferedResultRows"/> or their estimated memory passes
/// <see cref="CamusDBOptions.MaxBufferedResultBytes"/>.
///
/// <para>Why a transport bound and not an engine bound: the same cursor feeds the streaming
/// transports, which hold one row at a time and must stay unbounded. Only a consumer that keeps
/// every row can run the node out of memory, so the consumer enforces the cap. Without it, one
/// statement of a few bytes, such as <c>SELECT * FROM generate_series(1, 1000000000)</c>, grows the
/// managed heap until the process dies, with no error to the caller and no rollback.</para>
///
/// <para>Callers must collect <b>before</b> they commit, as they already do so that a serializable
/// retry can restart. The refusal then rolls the statement back, and a client is never told that a
/// committed write failed.</para>
///
/// <para>The byte estimate is <see cref="QueryResultCache.EstimateRowBytes"/>, which errs high. It
/// reads every cell of a lazy row; the JSON writer reads the same cells later, so the extra cost is
/// a walk, not a decode.</para>
/// </summary>
public static class BufferedResultCollector
{
    /// <summary>
    /// Reads every row of <paramref name="cursor"/> into a list, with the caps of
    /// <paramref name="options"/>. Disposes the cursor's enumerator on every path, so a refusal
    /// releases the scan at once.
    /// </summary>
    /// <exception cref="CamusDBException"><see cref="CamusDBErrorCodes.ResultSetTooLarge"/>.</exception>
    public static async Task<List<QueryResultRow>> CollectAsync(
        IAsyncEnumerable<QueryResultRow> cursor,
        CamusDBOptions options,
        CancellationToken cancellationToken = default)
    {
        int maxRows = options.MaxBufferedResultRows;
        long maxBytes = options.MaxBufferedResultBytes;

        List<QueryResultRow> rows = [];
        long bytes = 0;

        await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (maxRows > 0 && rows.Count >= maxRows)
                throw new CamusDBException(
                    CamusDBErrorCodes.ResultSetTooLarge,
                    $"The result has more than {maxRows} rows, the most one buffered response may hold " +
                    "(max_buffered_result_rows). Use /execute-sql-query-stream or the gRPC query stream, " +
                    "or page the result with LIMIT.");

            if (maxBytes > 0)
            {
                bytes += QueryResultCache.EstimateRowBytes(row);
                if (bytes > maxBytes)
                    throw new CamusDBException(
                        CamusDBErrorCodes.ResultSetTooLarge,
                        $"The result needs more than {maxBytes} bytes, the most one buffered response may hold " +
                        "(max_buffered_result_bytes). Use /execute-sql-query-stream or the gRPC query stream, " +
                        "or page the result with LIMIT.");
            }

            rows.Add(row);
        }

        return rows;
    }
}
