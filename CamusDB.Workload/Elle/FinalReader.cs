/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Operations;

namespace CamusDB.Workload.Elle;

/// <summary>
/// Reads the final state of many keys after the writers stopped, a bounded number at a time over the
/// run's read connections, retrying the transient failures a settling cluster still produces.
///
/// <para>Post-run, so never measured, and never allowed to throw for one key's transient error: a key
/// that cannot be read inside the budget fails the whole read, and the caller reports the final
/// state as unavailable rather than judging a partial one.</para>
/// </summary>
public static class FinalReader
{
    /// <summary>Concurrent reads in flight; a quiesced cluster answers point reads in about a
    /// millisecond, so this reads a few hundred thousand keys in well under a minute.</summary>
    public const int Parallelism = 16;

    private const int MaxAttemptsPerKey = 30;

    public static async Task<IReadOnlyList<T>> ReadAllAsync<T>(
        ConnectionSet connections, IReadOnlyList<long> keys, Func<CamusConnection, long, CancellationToken, Task<T>> read,
        TimeSpan budget, CancellationToken ct)
    {
        T[] results = new T[keys.Count];
        Stopwatch elapsed = Stopwatch.StartNew();
        using SemaphoreSlim gate = new(Parallelism);

        async Task ReadOne(int index)
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                int attempt = 0;
                while (true)
                {
                    try
                    {
                        results[index] = await read(connections.NextRead(), keys[index], ct).ConfigureAwait(false);
                        return;
                    }
                    catch (Exception ex) when (!ct.IsCancellationRequested && ErrorClassifier.IsRetryableForIdempotentRead(ex))
                    {
                        attempt++;
                        if (attempt >= MaxAttemptsPerKey || elapsed.Elapsed > budget)
                            throw new InvalidOperationException(
                                $"key {keys[index]} could not be read after {attempt} attempt(s) in {elapsed.Elapsed.TotalSeconds:F0}s: {ex.Message}", ex);
                        await Task.Delay(Math.Min(100 * attempt, 2000), ct).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                gate.Release();
            }
        }

        Task[] tasks = new Task[keys.Count];
        for (int i = 0; i < keys.Count; i++)
            tasks[i] = ReadOne(i);
        await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }

    /// <summary>One scalar read with the same retry rule, for the counts.</summary>
    public static async Task<long> ScalarAsync(
        ConnectionSet connections, Func<CamusConnection, CancellationToken, Task<long>> read, TimeSpan budget, CancellationToken ct)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        int attempt = 0;
        while (true)
        {
            try
            {
                return await read(connections.NextRead(), ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && ErrorClassifier.IsRetryableForIdempotentRead(ex))
            {
                attempt++;
                if (attempt >= MaxAttemptsPerKey || elapsed.Elapsed > budget)
                    throw new InvalidOperationException(
                        $"the count could not be read after {attempt} attempt(s) in {elapsed.Elapsed.TotalSeconds:F0}s: {ex.Message}", ex);
                await Task.Delay(Math.Min(100 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Reads the single value a point read returns: the value and the row count, so a read
    /// that returned two rows for one key is reported instead of silently taking the first.</summary>
    public static async Task<(long? Value, int Rows)> PointAsync(CamusCommand command, CancellationToken ct)
    {
        using CamusDataReader reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        long? value = null;
        int rows = 0;
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (rows == 0)
                value = reader.GetInt64(0);
            rows++;
        }
        return (value, rows);
    }
}
