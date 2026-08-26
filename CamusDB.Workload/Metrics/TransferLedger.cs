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
/// <para>Recording is lock-free (a concurrent queue of pre-formatted lines) so the hot path pays one string
/// format and one enqueue per terminal attempt; the file is written once, after the run.</para>
/// </summary>
public sealed class TransferLedger
{
    private readonly ConcurrentQueue<string> lines = new();

    /// <summary>Records one terminal transfer attempt. <paramref name="outcome"/> is one of
    /// <c>committed</c>, <c>indeterminate</c>, <c>conflict-retry</c> (the caller retries from BEGIN),
    /// <c>conflict-final</c> (retry budget exhausted), or <c>error</c>. The deltas are signed and always sum
    /// to zero across the two rows; only the low row's delta is recorded, the high row's is its negation.</summary>
    public void Record(long lowIndex, long highIndex, long lowDelta, int attempt, string outcome, string? code)
    {
        long wallMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        lines.Enqueue(string.Create(CultureInfo.InvariantCulture,
            $"{wallMs},{lowIndex},{highIndex},{lowDelta},{attempt},{outcome},{code ?? ""}"));
    }

    /// <summary>Writes the journal plus the seeded per-row baseline into <paramref name="outputDir"/> as
    /// <c>transfer-ledger.csv</c> and <c>row-baseline.csv</c>. The baseline is the dataset's deterministic
    /// seeded balance per row index — the run starts from a freshly seeded cluster, so it is also the run's
    /// starting balance — and carries the row id so ledger rows join to the final table state.</summary>
    public async Task WriteAsync(string outputDir, Workload.Dataset dataset, long rows, CancellationToken ct)
    {
        StringBuilder ledger = new(lines.Count * 48 + 64);
        ledger.AppendLine("wall_ms,low_index,high_index,low_delta,attempt,outcome,code");
        while (lines.TryDequeue(out string? line))
            ledger.AppendLine(line);
        await File.WriteAllTextAsync(Path.Combine(outputDir, "transfer-ledger.csv"), ledger.ToString(), ct).ConfigureAwait(false);

        StringBuilder baseline = new((int)rows * 48 + 32);
        baseline.AppendLine("index,id,seeded_balance");
        for (long index = 0; index < rows; index++)
        {
            (string id, _, long seededBalance, _) = dataset.RowFor(index);
            baseline.AppendLine(string.Create(CultureInfo.InvariantCulture, $"{index},{id},{seededBalance}"));
        }
        await File.WriteAllTextAsync(Path.Combine(outputDir, "row-baseline.csv"), baseline.ToString(), ct).ConfigureAwait(false);
    }
}
