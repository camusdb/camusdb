/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Kahuna;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Turns a range scan that Kahuna could not complete into the retryable, correctly worded failure
/// the SQL layer expects.
///
/// <para>Kahuna's streaming scan never truncates: a page it cannot serve — an <c>Aborted</c> answer,
/// a page that kept answering transient past its retry budget, an undecodable continuation cursor —
/// ends the enumeration with a <see cref="KahunaServerException"/>. Left alone that exception reaches
/// the API boundary as an internal error on every path that is not the two read-only query
/// endpoints, and on the write-side paths that catch <c>Aborted</c> it used to be reported with a
/// message written for a batched write. Neither is right for a read: nothing was applied, the scan is
/// idempotent, and the caller's correct move is to retry the statement from <c>BEGIN</c>. Every scan
/// the table store issues therefore streams through <see cref="Translate{T}"/>, which maps the
/// exception to <see cref="CamusDBErrorCodes.TransactionMustRetry"/> with a message naming the scan
/// and Kahuna's own response type.</para>
/// </summary>
internal static class KvScanFailure
{
    /// <summary>
    /// Re-yields <paramref name="source"/> unchanged and converts a <see cref="KahunaServerException"/>
    /// raised while advancing it into a <see cref="CamusDBException"/> carrying
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/>. <paramref name="what"/> names the scan in
    /// the message ("row scan of table orders").
    /// </summary>
    internal static async IAsyncEnumerable<T> Translate<T>(
        IAsyncEnumerable<T> source,
        string what,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // The catch sits around MoveNextAsync alone: C# forbids a yield inside a try block that has a
        // catch, so the element is handed out after the guarded advance.
        await using IAsyncEnumerator<T> enumerator = source.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            bool moved;
            try
            {
                moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
            }
            catch (KahunaServerException ex)
            {
                throw Describe(ex, what);
            }

            if (!moved)
                yield break;

            yield return enumerator.Current;
        }
    }

    /// <summary>
    /// The retryable failure for a scan Kahuna gave up on: <see cref="CamusDBErrorCodes.TransactionMustRetry"/>,
    /// worded for a read, carrying Kahuna's response type and its own message.
    /// </summary>
    internal static CamusDBException Describe(KahunaServerException ex, string what)
    {
        string verdict = ex.ResponseType is KeyValueResponseType type ? type.ToString() : "no response type";
        return new CamusDBException(
            CamusDBErrorCodes.TransactionMustRetry,
            $"The {what} could not be completed by Kahuna ({verdict}); nothing was applied — " +
            $"retry the statement from BeginAsync. {ex.Message}");
    }
}
