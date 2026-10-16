/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Grpc.Core;
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
/// the table store issues therefore streams through <see cref="Translate{T}"/>, or advances its
/// Kahuna enumerator through <see cref="MoveNextAsync{T}"/>, which map the exception to
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> with a message naming the scan and Kahuna's
/// own response type.</para>
///
/// <para>A page whose forward the partition leader's dead process refused is the other way a scan ends
/// without answering: Kahuna's routed scan waits out a page that answers <c>MustRetry</c> but lets 
/// the raw <see cref="RpcException"/> escape, and from here it
/// reached the client as a generic internal error, some ten thousand times per surviving node in the
/// seconds before the new leader was elected. It is translated the same way: nothing was applied and
/// the scan is idempotent.</para>
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
        await using IAsyncEnumerator<T> enumerator = source.GetAsyncEnumerator(cancellationToken);

        while (await MoveNextAsync(enumerator, what, cancellationToken).ConfigureAwait(false))
            yield return enumerator.Current;
    }

    /// <summary>
    /// One guarded advance of a Kahuna scan enumerator: the two translations of <see cref="Translate{T}"/>
    /// around one <c>MoveNextAsync</c>, for a caller that holds the enumerator itself and so needs no
    /// wrapper enumerator of its own. An index probe opens one scan per outer row, which made the
    /// wrapper a per-row allocation. The catch sits around the advance alone: C# forbids a yield inside
    /// a try block that has a catch, so the caller hands the element out after this returns.
    /// </summary>
    internal static async ValueTask<bool> MoveNextAsync<T>(IAsyncEnumerator<T> enumerator, string what, CancellationToken cancellationToken)
    {
        try
        {
            return await enumerator.MoveNextAsync().ConfigureAwait(false);
        }
        catch (KahunaServerException ex)
        {
            throw Describe(ex, what);
        }
        catch (RpcException ex) when (KahunaRetryPolicy.IsTransientTransportFailure(ex, cancellationToken))
        {
            throw KahunaRetryPolicy.ToMustRetry(ex, what);
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
