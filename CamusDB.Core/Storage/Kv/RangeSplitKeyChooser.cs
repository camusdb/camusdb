/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Picks the key at which an operator-driven range split divides a key space: the median key of the
/// space, so that both halves hold rows (Kahuna refuses a split that would leave a half empty), moved
/// one key up when the median is the start key of the range that covers it (Kahuna refuses a split at
/// a range's own start).
///
/// <para>Why a scan and not an estimate: a CamusDB row key is <c>{dbId}:{tableId}|r/{rowIdHex24}</c>,
/// ordered by the stored row id, which no client can compute from the row's primary key. A fault-injection
/// harness that wants a cutover at a chosen moment therefore cannot name the key itself, and a key chosen
/// from an estimate could land outside the data or on a range start. The scan reads keys only, pages at
/// the store's default page size, and stops at <paramref name="maxKeys"/>; the caller refuses a space
/// larger than that rather than scanning an unbounded table on an admin call.</para>
///
/// <para>The scan runs outside any transaction, at the latest committed state. A row that commits while
/// the scan runs can move the median by one key; that does not change what the split is for.</para>
///
/// <para>Kahuna ends a scan it cannot complete with an exception. A page that keeps answering
/// <c>MustRetry</c> past its retry budget does that, and a key that holds an unresolved write intent
/// causes it for some seconds after a node dies, which is when a fault-injection harness asks for a
/// split. Nothing was chosen and the same scan can succeed a moment later, so the chooser reports it in
/// <see cref="Choice.ScanFailure"/> and the caller answers a retryable outcome. A damaged store is not
/// retryable and still throws.</para>
/// </summary>
internal static class RangeSplitKeyChooser
{
    /// <summary>
    /// What the chooser found: the split key, or why there is none. <see cref="ScanFailure"/> carries
    /// Kahuna's message when the scan ended before its last page; the other fields then describe the
    /// keys read up to that point and no key was chosen.
    /// </summary>
    internal sealed record Choice(string? SplitKey, int KeysSeen, bool Truncated, string? ScanFailure = null);

    /// <summary>
    /// Scans <paramref name="keySpace"/> and picks its median key as the split key.
    /// <paramref name="rangeStartKeys"/> are the start keys of the space's current ranges; a median
    /// equal to one of them moves up to the next key, because a split at a range start would create
    /// an empty half. <see cref="Choice.SplitKey"/> is null when the space holds fewer than two keys
    /// or when every candidate from the median up is a range start; <see cref="Choice.Truncated"/> is
    /// true when the space holds more than <paramref name="maxKeys"/> keys and nothing was chosen;
    /// <see cref="Choice.ScanFailure"/> is set when Kahuna gave up on the scan and a retry can succeed.
    /// </summary>
    internal static async Task<Choice> PickMedianAsync(
        IKahuna kahuna,
        string keySpace,
        IReadOnlySet<string> rangeStartKeys,
        int maxKeys,
        CancellationToken cancellationToken)
    {
        List<string> keys = [];

        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                HLCTimestamp.Zero,
                keySpace,
                null, true,
                null, true,
                KvStoreConstants.DefaultPageSize,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                cancellationToken).ConfigureAwait(false))
            {
                // A scan of the bucket prefix stops at its end on its own; the check keeps a key of a
                // neighbouring space out of the count should the scan ever answer one.
                if (!key.StartsWith(keySpace + "/", StringComparison.Ordinal))
                    continue;

                keys.Add(key);

                if (keys.Count > maxKeys)
                    return new Choice(null, keys.Count, Truncated: true);
            }
        }
        catch (KahunaServerException ex)
        {
            // KvScanFailure decides which of Kahuna's answers a retry can clear. The other answers
            // (a damaged store, a snapshot below the kept history) keep their own error code.
            CamusDBException failure = KvScanFailure.Describe(ex, $"median scan of key space '{keySpace}'");

            if (failure.Code != CamusDBErrorCodes.TransactionMustRetry)
                throw failure;

            return new Choice(null, keys.Count, Truncated: false, ScanFailure: ex.Message);
        }
        catch (RpcException ex) when (KahunaRetryPolicy.IsTransientTransportFailure(ex, cancellationToken))
        {
            // The forward of a page to a partition leader that is gone throws the transport error.
            return new Choice(null, keys.Count, Truncated: false,
                ScanFailure: $"A page could not reach its partition leader ({ex.StatusCode}: {ex.Status.Detail}).");
        }

        if (keys.Count < 2)
            return new Choice(null, keys.Count, Truncated: false);

        // The scan answers in key order, which is the order the ranges divide. A split key must sort
        // strictly above the start of the range that covers it, so a median that is itself a range start
        // moves up; the keys above it are inside the same range until the next start.
        for (int i = keys.Count / 2; i < keys.Count; i++)
        {
            if (!rangeStartKeys.Contains(keys[i]))
                return new Choice(keys[i], keys.Count, Truncated: false);
        }

        return new Choice(null, keys.Count, Truncated: false);
    }
}
