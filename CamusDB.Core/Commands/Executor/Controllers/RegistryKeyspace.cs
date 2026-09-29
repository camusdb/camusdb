/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The persistent keyspace every <see cref="DatabaseRegistry"/> component writes to: the
/// <c>dbregistry</c> bucket under the registry's key prefix, the KV client it lives on, and the
/// retrying single-key primitives the registry's side records (drop markers, pending-create markers,
/// orphan records) share.
/// </summary>
internal sealed class RegistryKeyspace
{
    public const int MaxRetries = 10;

    /// <summary>
    /// How many times a full registry bucket scan is attempted before a transient failure is allowed
    /// to surface. A whole-scan restart is safe because these scans only read, and every consumer
    /// re-confirms what it acts on (the reclaimer under its per-id fence, relink under the drop
    /// intent). Kept small: one failed attempt can already have spent the store's per-page settle
    /// budget (seconds), so a large count would turn a persistent fault into a very long stall.
    /// </summary>
    private const int MaxScanAttempts = 3;

    public IKahuna Kahuna { get; }

    public KvTransactionsManager Transactions { get; }

    /// <summary>KV bucket that holds every registry key.</summary>
    public string Bucket { get; }

    public RegistryKeyspace(IKahuna kahuna, KvTransactionsManager transactions, string keyPrefix)
    {
        Kahuna = kahuna;
        Transactions = transactions;
        Bucket = $"{keyPrefix}dbregistry";
    }

    /// <summary>The full key for <paramref name="suffix"/> inside the registry bucket.</summary>
    public string Key(string suffix) => $"{Bucket}/{suffix}";

    /// <summary>
    /// Unconditionally writes <paramref name="value"/> to <paramref name="key"/> outside any
    /// transaction, retrying transient replication statuses with a bounded backoff. Throws
    /// <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/> with <paramref name="failureMessage"/> and the
    /// final status when the write cannot be confirmed.
    /// </summary>
    public async Task SetAsync(string key, byte[] value, string failureMessage)
    {
        KeyValueResponseType type;
        int retries = 0;

        do
        {
            if (retries > 0)
                await Task.Delay(retries * 10).ConfigureAwait(false);

            (type, _, _) = await Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, value, null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++retries < MaxRetries);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"{failureMessage}: {type}");
    }

    /// <summary>Deletes <paramref name="key"/> outside any transaction. Best-effort: failures are swallowed.</summary>
    public async Task DeleteBestEffortAsync(string key)
    {
        try
        {
            await Kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, key,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        catch
        {
            // best-effort
        }
    }

    /// <summary>
    /// Scans the registry bucket inside a read-committed transaction and returns every key starting
    /// with <paramref name="prefix"/>, with its value.
    /// </summary>
    public async Task<List<(string Key, byte[]? Value)>> ScanPrefixAsync(string prefix)
    {
        List<(string, byte[]?)> matches = [];

        KvTransaction tx = await Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in Kahuna.LocateAndScanRange(
                tx.TransactionId,
                Bucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (key.StartsWith(prefix, StringComparison.Ordinal))
                    matches.Add((key, kve.Value));
            }
        }
        finally
        {
            await Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return matches;
    }

    /// <summary>
    /// Runs one registry bucket scan, restarting it a bounded number of times when it fails with a
    /// transient store response. Under registry write contention (relink versus GC is the ordinary
    /// case) a scan page can fail loudly instead of truncating: <c>Aborted</c> on a read conflict,
    /// or <c>MustRetry</c>/<c>WaitingForReplication</c> when a page's settle budget runs out while
    /// a key holds an unresolved write intent. The scan is idempotent, so the whole scan is simply
    /// re-run; any other failure — and a transient one that persists across every attempt — still
    /// propagates.
    /// </summary>
    public static async Task<T> RetryTransientScanAsync<T>(Func<Task<T>> scan)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await scan().ConfigureAwait(false);
            }
            catch (KahunaServerException ex) when (
                attempt < MaxScanAttempts - 1 &&
                ex.ResponseType is KeyValueResponseType.Aborted
                    or KeyValueResponseType.MustRetry
                    or KeyValueResponseType.WaitingForReplication)
            {
                await Task.Delay((attempt + 1) * 25).ConfigureAwait(false);
            }
        }
    }
}
