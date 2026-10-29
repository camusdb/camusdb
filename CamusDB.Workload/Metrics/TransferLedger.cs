/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Metrics;

/// <summary>
/// A per-attempt journal of every bank transfer the workload drove, written to the run artifacts so a
/// conservation deficit can be attributed to exact rows and transactions after the fact. The aggregate
/// <c>SUM(balance)</c> invariant can prove that value was lost but not where: this ledger closes that gap —
/// combined with the seeded per-row baseline, each row's expected final balance is its baseline plus the sum
/// of its committed deltas, rows touched by indeterminate attempts carry a known ambiguity, and any row whose
/// observed balance still disagrees identifies the victim transfer (and its wall-clock moment, joinable with
/// node logs) precisely.
///
/// <para>The same <see cref="Record"/> call also feeds <see cref="Attribution"/>, which performs that join
/// in-process at the end of the run instead of leaving it to an offline script. Routing both through one
/// call is deliberate: the journal a reader inspects and the expectation the run is failed against are then
/// derived from the same event, and cannot drift apart.</para>
///
/// <para>Recording is lock-free (a concurrent queue of pre-formatted lines) so the hot path pays one string
/// format and one enqueue per terminal attempt; the file is written once, after the run.</para>
/// </summary>
public sealed class TransferLedger
{
    private readonly ConcurrentQueue<string> lines = new();

    public TransferLedger(RowAttribution? attribution = null) => Attribution = attribution;

    /// <summary>The per-row expectation built from the same attempts this journal records, or null when
    /// per-row attribution is off for this run.</summary>
    public RowAttribution? Attribution { get; }

    /// <summary>Records one terminal transfer attempt. The deltas are signed and always sum to zero across
    /// the two rows; only the low row's delta is recorded, the high row's is its negation.</summary>
    public void Record(long lowIndex, long highIndex, long lowDelta, int attempt, TransferOutcome outcome, string? code)
    {
        long wallMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        lines.Enqueue(string.Create(CultureInfo.InvariantCulture,
            $"{wallMs},{lowIndex},{highIndex},{lowDelta},{attempt},{outcome.ToLedgerToken()},{code ?? ""}"));

        Attribution?.Record(lowIndex, highIndex, lowDelta, outcome);
    }

    /// <summary>Writes the journal plus the seeded per-row baseline into <paramref name="outputDir"/> as
    /// <c>transfer-ledger.csv</c> and <c>row-baseline.csv</c>. The baseline is the dataset's deterministic
    /// seeded balance per row index — the run starts from a freshly seeded cluster, so it is also the run's
    /// starting balance — and carries the row id so ledger rows join to the final table state.</summary>
    public async Task WriteAsync(string outputDir, Workload.Dataset dataset, long rows, CancellationToken ct)
    {
        // Streamed line by line: a two-hour run at several thousand write transactions per second
        // journals tens of millions of attempts, and one string holding them all passes the 2 GB
        // object limit (a 25-million-line ledger is ~2.3 GB of UTF-16) and fails the write with
        // OutOfMemoryException after the measured window completed.
        await using (StreamWriter ledger = new(Path.Combine(outputDir, "transfer-ledger.csv"), append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 16))
        {
            await ledger.WriteLineAsync("wall_ms,low_index,high_index,low_delta,attempt,outcome,code").ConfigureAwait(false);
            while (lines.TryDequeue(out string? line))
            {
                ct.ThrowIfCancellationRequested();
                await ledger.WriteLineAsync(line).ConfigureAwait(false);
            }
        }

        await using StreamWriter baseline = new(Path.Combine(outputDir, "row-baseline.csv"), append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 16);
        await baseline.WriteLineAsync("index,id,seeded_balance").ConfigureAwait(false);
        for (long index = 0; index < rows; index++)
        {
            (string id, _, long seededBalance, _) = dataset.RowFor(index);
            await baseline.WriteLineAsync(string.Create(CultureInfo.InvariantCulture, $"{index},{id},{seededBalance}")).ConfigureAwait(false);
        }
    }
}
