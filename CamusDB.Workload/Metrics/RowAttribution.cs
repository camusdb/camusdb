/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Serialization;
using CamusDB.Client;
using CamusDB.Workload.Util;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Metrics;

/// <summary>Why the per-row check did or did not produce a verdict.</summary>
public enum RowAttributionStatus
{
    /// <summary>The caller turned the check off. The run keeps only the aggregate invariants.</summary>
    Disabled,

    /// <summary>The check could not be trusted to produce a verdict — the baseline scan did not
    /// complete, the dataset was too large to attribute, or writes could still land after the
    /// verification scan. Reported as "could not verify", never as a pass.</summary>
    Unavailable,

    /// <summary>Every row was scanned and compared against the journal.</summary>
    Verified,
}

/// <summary>One row whose durable state contradicts the transfer journal.</summary>
/// <param name="RowIndex">Dataset row index, which also identifies the table that holds it.</param>
/// <param name="Kind">Short machine-readable label: <c>balance</c>, <c>version-high</c>,
/// <c>version-low</c>, <c>duplicate</c>, <c>missing</c> or <c>foreign</c>.</param>
public sealed record RowViolation(
    long RowIndex, string Id, string Table, string Kind, string Detail);

/// <summary>
/// The verdict of the per-row check. <see cref="Violations"/> is a bounded sample for
/// <c>reconciliation.json</c>; the complete list goes to <c>row-violations.csv</c>.
/// </summary>
public sealed record RowAttributionResult(
    RowAttributionStatus Status,
    string? Reason,
    long RowsScanned,
    long RowsInAmbiguityBand,
    long BalanceViolations,
    long UncountedWriteRows,
    long LostWriteRows,
    long RowsMissing,
    long RowsDuplicated,
    long RowsForeign,
    long HalfAppliedTransfers,
    IReadOnlyList<RowViolation> Violations)
{
    /// <summary>
    /// Every violation the scan retained, for <c>row-violations.csv</c>. Kept out of
    /// <c>reconciliation.json</c> — a badly broken run can produce a hundred thousand of these, and the
    /// JSON verdict has to stay something a person reads.
    /// </summary>
    [JsonIgnore]
    public IReadOnlyList<RowViolation> AllViolations { get; init; } = [];

    /// <summary>
    /// True when the leg-pairing check could not see every unknown-outcome transfer, because the run
    /// produced more than the retained cap. Violations it did find are still real; absence of them is
    /// no longer proof of absence, so a caller must not read a zero count as clean.
    /// </summary>
    [JsonIgnore]
    public bool HalfAppliedCheckTruncated { get; init; }

    /// <summary>Total contradictions with the journal, by any of the checks.</summary>
    public long TotalViolations =>
        BalanceViolations + UncountedWriteRows + LostWriteRows + RowsMissing + RowsDuplicated + RowsForeign
        + HalfAppliedTransfers;

    /// <summary>
    /// A disabled check passes (the operator opted out knowingly); an unavailable one does not, because
    /// "we could not look" must never read the same as "we looked and it was clean".
    /// </summary>
    public bool Passed => Status switch
    {
        RowAttributionStatus.Disabled => true,
        RowAttributionStatus.Verified => TotalViolations == 0,
        _ => false,
    };

    public static RowAttributionResult Disabled(string reason) =>
        new(RowAttributionStatus.Disabled, reason, 0, 0, 0, 0, 0, 0, 0, 0, 0, []);

    public static RowAttributionResult Unavailable(string reason) =>
        new(RowAttributionStatus.Unavailable, reason, 0, 0, 0, 0, 0, 0, 0, 0, 0, []);
}

/// <summary>Row count and column totals produced by one full scan of the dataset.</summary>
public sealed record RowScanTotals(long Rows, long BalanceSum, long VersionSum);

/// <summary>The three columns the per-row check reads from a workload table.</summary>
public readonly record struct ScannedRow(string Id, long Balance, long Version);

/// <summary>
/// Streams one table's rows. This is the seam the per-row check reads through, so its comparison logic
/// can be exercised against a hand-built dataset — including durable states no healthy engine would
/// ever produce — without a running cluster. Production always supplies
/// <see cref="RowAttribution.RowsFrom"/>, which issues the real query.
/// </summary>
public delegate IAsyncEnumerable<ScannedRow> TableRowSource(string table, CancellationToken ct);

/// <summary>
/// Per-row attribution of the transfer workload's durable effect, the check that catches an atomicity
/// break the aggregate invariants cannot see.
///
/// <para><c>SUM(balance)</c> conservation only proves that the <em>net</em> of every stray write is
/// zero. An even number of leaked legs that cancel — one row credited, another debited, neither
/// commit ever acknowledged to the client — leaves the total untouched, and a run that leaked exactly
/// that way has been reported as PASS. The same is true of <c>SUM(version)</c>: it is a total, so a
/// row that gained an uncounted increment is hidden by any row that lost one.</para>
///
/// <para>This class closes that gap by holding the whole dataset per row rather than in aggregate.
/// A pre-run scan captures each row's starting balance and version; every terminal transfer attempt
/// reports its effect here as it happens; a post-run scan then compares each row against the exact
/// value the journal predicts. Two independent checks fall out:</para>
/// <list type="number">
/// <item><description><b>Balance attribution</b> — a row's final balance must equal its baseline plus
/// the deltas of the transfers that committed against it.</description></item>
/// <item><description><b>Version accounting</b> — each committed leg increments <c>version</c> by
/// exactly one, so a row whose version exceeds its journalled write count carries a write the client
/// never committed. This is the sharper of the two: it fires even when the stray writes happen to
/// leave the balance correct.</description></item>
/// </list>
///
/// <para>Rows touched by an <see cref="TransferOutcome.Indeterminate"/> attempt are genuinely
/// unknowable — that transaction may have applied or not — so each such attempt widens that row's
/// admissible band by its own delta and by one version increment, and the row is reported inside the
/// ambiguity band instead of as a violation. The band is per row and per attempt, so it stays tight:
/// an unhealthy cluster makes the check quieter, never blind. A row outside even its widened band is
/// still a violation.</para>
///
/// <para>Recording is lock-free (interlocked adds into flat arrays) so the hot path pays two atomics
/// per leg. Verification runs after the workload stops, outside any measured window.</para>
/// </summary>
public sealed class RowAttribution
{
    /// <summary>
    /// Largest dataset this check will attribute. The bookkeeping is about 52 bytes per row, so five
    /// million rows costs roughly a quarter of a gigabyte in the driver process — past that the check
    /// would trade a memory blow-up in the load generator for a correctness signal, which is the wrong
    /// bargain. Above the cap the run keeps the aggregate invariants and says so.
    /// </summary>
    public const long MaxAttributedRows = 5_000_000;

    /// <summary>Violations retained in full for <c>row-violations.csv</c>. A run that breaks this badly
    /// has already made its point; the count keeps reporting the true total.</summary>
    private const int MaxRetainedViolations = 100_000;

    /// <summary>Violations copied into <c>reconciliation.json</c>, which is meant to stay readable.</summary>
    private const int MaxSampledViolations = 50;

    /// <summary>
    /// Unknown-outcome transfers whose legs are retained for the pairing check. At 8 bytes a pair this
    /// caps the cost at about 8 MB, far above what any healthy run produces — a run that exceeds it has
    /// a pathological indeterminate rate, and the check reports itself truncated rather than silently
    /// scanning a subset.
    /// </summary>
    private const int MaxRetainedPairs = 1_000_000;

    private readonly Dataset _dataset;
    private readonly ulong _seed;
    private readonly int _rows;

    private readonly long[] _baselineBalance;
    private readonly long[] _baselineVersion;
    private readonly long[] _committedDelta;
    private readonly int[] _committedWrites;
    private readonly long[] _ambiguousPositive;
    private readonly long[] _ambiguousNegative;
    private readonly int[] _ambiguousTouches;
    private readonly int[] _observed;

    // The two rows of every unknown-outcome transfer, kept as pairs rather than folded into the
    // per-row totals above. A transfer moves value between its two rows, so it is zero-sum: whether it
    // applied or not, it leaves SUM(balance) unchanged and leaves each row inside its own band. Only
    // the pairing shows an attempt that applied to one row and not the other, which is the atomicity
    // break the other checks are structurally unable to see.
    private readonly ConcurrentQueue<(int Low, int High)> _ambiguousPairs = new();
    private int _ambiguousPairsSeen;

    // Per-row count of unknown-outcome transfers that demonstrably applied, derived from the scanned
    // version, and whether that count can be trusted for this row. A row whose version fell outside its
    // band is already reported by its own check and tells the pairing pass nothing reliable.
    private readonly int[] _ambiguousApplied;
    private readonly bool[] _ambiguousAppliedKnown;

    private bool _baselineCaptured;

    private RowAttribution(Dataset dataset, ulong seed, int rows)
    {
        _dataset = dataset;
        _seed = seed;
        _rows = rows;
        _baselineBalance = new long[rows];
        _baselineVersion = new long[rows];
        _committedDelta = new long[rows];
        _committedWrites = new int[rows];
        _ambiguousPositive = new long[rows];
        _ambiguousNegative = new long[rows];
        _ambiguousTouches = new int[rows];
        _observed = new int[rows];
        _ambiguousApplied = new int[rows];
        _ambiguousAppliedKnown = new bool[rows];
    }

    /// <summary>
    /// Builds an attribution for a dataset, or explains why one cannot exist. Returning a reason rather
    /// than throwing keeps an over-sized dataset a reportable downgrade of the verdict instead of a
    /// crashed run.
    /// </summary>
    public static RowAttribution? TryCreate(Dataset dataset, ulong seed, out string? reason)
    {
        if (dataset.Rows < 1)
        {
            reason = "the dataset has no rows to attribute.";
            return null;
        }
        if (dataset.Rows > MaxAttributedRows)
        {
            reason = $"the dataset has {dataset.Rows} rows, above the {MaxAttributedRows}-row limit " +
                     "for per-row attribution; the aggregate invariants still apply.";
            return null;
        }

        reason = null;
        return new RowAttribution(dataset, seed, (int)dataset.Rows);
    }

    /// <summary>True once a baseline scan has completed and the check can produce a verdict.</summary>
    public bool HasBaseline => _baselineCaptured;

    /// <summary>
    /// Records a transfer attempt's effect on the two rows it touched. Only the low row's delta is
    /// passed, as in the journal; the high row's is its negation. Called from
    /// <see cref="TransferLedger.Record"/> so the journal and the attribution can never disagree about
    /// what happened.
    /// </summary>
    public void Record(long lowIndex, long highIndex, long lowDelta, TransferOutcome outcome)
    {
        switch (outcome)
        {
            case TransferOutcome.Committed:
                ApplyCommitted(lowIndex, lowDelta);
                ApplyCommitted(highIndex, -lowDelta);
                break;

            case TransferOutcome.Indeterminate:
                ApplyAmbiguous(lowIndex, lowDelta);
                ApplyAmbiguous(highIndex, -lowDelta);
                RetainPair(lowIndex, highIndex);
                break;

            // A conflict or a definite server abort applied nothing, so it moves no row's expected
            // state. That is what makes the check sharp: if such a transfer's legs turn up in the
            // durable data anyway, the row falls outside its band and is reported.
            default:
                break;
        }
    }

    private void ApplyCommitted(long index, long delta)
    {
        if (!InRange(index))
            return;
        Interlocked.Add(ref _committedDelta[index], delta);
        Interlocked.Increment(ref _committedWrites[index]);
    }

    private void ApplyAmbiguous(long index, long delta)
    {
        if (!InRange(index))
            return;
        if (delta > 0)
            Interlocked.Add(ref _ambiguousPositive[index], delta);
        else if (delta < 0)
            Interlocked.Add(ref _ambiguousNegative[index], delta);
        Interlocked.Increment(ref _ambiguousTouches[index]);
    }

    /// <summary>
    /// Keeps one unknown-outcome transfer's two rows for the pairing pass, up to the retention cap.
    /// Past the cap the pair is dropped and the check reports itself truncated, because a pairing pass
    /// over an arbitrary subset would turn "found nothing" into a claim it cannot support.
    /// </summary>
    private void RetainPair(long lowIndex, long highIndex)
    {
        if (!InRange(lowIndex) || !InRange(highIndex))
            return;
        if (Interlocked.Increment(ref _ambiguousPairsSeen) > MaxRetainedPairs)
            return;
        _ambiguousPairs.Enqueue(((int)lowIndex, (int)highIndex));
    }

    private bool InRange(long index) => index >= 0 && index < _rows;

    /// <summary>
    /// Scans every table once and records each row's starting balance and version, so the check
    /// measures this run's effect rather than assuming the dataset starts at its freshly-seeded values.
    /// That assumption is wrong on any dataset a previous run already moved, and <c>version</c> in
    /// particular is only zero on a dataset nobody has written yet.
    ///
    /// <para>Returns the same totals the aggregate baselines need, so a transfer run pays one scan here
    /// instead of a scan plus two aggregate queries. Throws if the scan cannot complete; the caller
    /// falls back to the aggregate baselines and marks the row check unavailable.</para>
    /// </summary>
    public Task<RowScanTotals> CaptureBaselineAsync(
        CamusConnection conn, CancellationToken ct, TimeSpan? retryBudget = null)
        => CaptureBaselineAsync(RowsFrom(conn), ct, retryBudget);

    /// <inheritdoc cref="CaptureBaselineAsync(CamusConnection, CancellationToken, TimeSpan?)"/>
    public async Task<RowScanTotals> CaptureBaselineAsync(
        TableRowSource source, CancellationToken ct, TimeSpan? retryBudget = null)
    {
        long balanceSum = 0, versionSum = 0, foreign = 0, duplicated = 0;

        await ScanDatasetAsync(source, ct, retryBudget,
            reset: () =>
            {
                Array.Clear(_observed);
                Array.Clear(_ambiguousApplied);
                Array.Clear(_ambiguousAppliedKnown);
                Array.Clear(_baselineBalance);
                Array.Clear(_baselineVersion);
                balanceSum = 0;
                versionSum = 0;
                foreign = 0;
                duplicated = 0;
            },
            onRow: (_, id, balance, version) =>
            {
                if (!RowIdFactory.TryRowIndex(_seed, id, out long index) || !InRange(index))
                {
                    foreign++;
                    return;
                }
                if (++_observed[index] > 1)
                {
                    duplicated++;
                    return;
                }
                _baselineBalance[index] = balance;
                _baselineVersion[index] = version;
                balanceSum += balance;
                versionSum += version;
            }).ConfigureAwait(false);

        long seen = 0;
        for (int i = 0; i < _rows; i++)
        {
            if (_observed[i] > 0)
                seen++;
        }

        // An incomplete or contaminated baseline would make every later comparison meaningless, so it
        // is refused outright rather than silently narrowed to the rows that happened to be present.
        if (seen != _rows || foreign != 0 || duplicated != 0)
        {
            throw new InvalidOperationException(
                $"baseline scan saw {seen} of {_rows} dataset row(s), {duplicated} duplicate(s) and " +
                $"{foreign} row(s) from outside this dataset.");
        }

        _baselineCaptured = true;
        return new RowScanTotals(seen, balanceSum, versionSum);
    }

    /// <summary>
    /// Scans every table again and compares each row against the balance and version the journal
    /// predicts for it. Throws if the scan cannot complete; the caller reports "could not verify".
    /// </summary>
    public Task<RowAttributionResult> VerifyAsync(
        CamusConnection conn, CancellationToken ct, TimeSpan? retryBudget = null)
        => VerifyAsync(RowsFrom(conn), ct, retryBudget);

    /// <inheritdoc cref="VerifyAsync(CamusConnection, CancellationToken, TimeSpan?)"/>
    public async Task<RowAttributionResult> VerifyAsync(
        TableRowSource source, CancellationToken ct, TimeSpan? retryBudget = null)
    {
        if (!_baselineCaptured)
            return RowAttributionResult.Unavailable("no per-row baseline was captured before the run.");

        List<RowViolation> violations = new();
        long scanned = 0, ambiguous = 0, balanceBad = 0, versionHigh = 0, versionLow = 0;
        long duplicated = 0, foreign = 0;

        await ScanDatasetAsync(source, ct, retryBudget,
            reset: () =>
            {
                Array.Clear(_observed);
                violations.Clear();
                scanned = 0;
                ambiguous = 0;
                balanceBad = 0;
                versionHigh = 0;
                versionLow = 0;
                duplicated = 0;
                foreign = 0;
            },
            onRow: (table, id, balance, version) =>
            {
                scanned++;

                if (!RowIdFactory.TryRowIndex(_seed, id, out long index) || !InRange(index))
                {
                    foreign++;
                    Add(violations, new RowViolation(-1, id, table, "foreign",
                        "row id does not belong to this seed's dataset."));
                    return;
                }

                if (++_observed[index] > 1)
                {
                    duplicated++;
                    Add(violations, new RowViolation(index, id, table, "duplicate",
                        $"the id was returned {_observed[index]} times by the scan."));
                    return;
                }

                // The band a row is allowed to land in. With no indeterminate attempt against it both
                // ends collapse onto the journalled value and the comparison is exact.
                long expected = _baselineBalance[index] + _committedDelta[index];
                long low = expected + _ambiguousNegative[index];
                long high = expected + _ambiguousPositive[index];

                long expectedVersion = _baselineVersion[index] + _committedWrites[index];
                long maxVersion = expectedVersion + _ambiguousTouches[index];

                if (_ambiguousTouches[index] > 0)
                    ambiguous++;

                if (balance < low || balance > high)
                {
                    balanceBad++;
                    Add(violations, new RowViolation(index, id, table, "balance",
                        $"balance {balance} outside [{low}, {high}] (baseline {_baselineBalance[index]}, " +
                        $"journalled delta {_committedDelta[index]}, {_ambiguousTouches[index]} indeterminate attempt(s))."));
                }

                if (version > maxVersion)
                {
                    versionHigh++;
                    Add(violations, new RowViolation(index, id, table, "version-high",
                        $"version {version} exceeds {maxVersion} (baseline {_baselineVersion[index]} + " +
                        $"{_committedWrites[index]} journalled write(s) + {_ambiguousTouches[index]} indeterminate); " +
                        "the row carries a write the client never committed."));
                }
                else if (version < expectedVersion)
                {
                    versionLow++;
                    Add(violations, new RowViolation(index, id, table, "version-low",
                        $"version {version} below {expectedVersion} (baseline {_baselineVersion[index]} + " +
                        $"{_committedWrites[index]} journalled write(s)); a committed write was lost."));
                }

                // Inside its band, the version says exactly how many of this row's unknown-outcome
                // transfers applied. Outside it, the row already has its own violation and the count
                // would be meaningless, so the pairing pass is told not to trust it.
                if (version >= expectedVersion && version <= maxVersion)
                {
                    _ambiguousApplied[index] = (int)(version - expectedVersion);
                    _ambiguousAppliedKnown[index] = true;
                }
            }).ConfigureAwait(false);

        long missing = 0;
        for (int i = 0; i < _rows; i++)
        {
            if (_observed[i] != 0)
                continue;
            missing++;
            Add(violations, new RowViolation(
                i, RowIdFactory.ForRow(_seed, i), _dataset.TableOf(i), "missing",
                "the seeded row was not returned by the final scan."));
        }

        long halfApplied = CheckLegPairing(violations);

        return new RowAttributionResult(
            RowAttributionStatus.Verified, null, scanned, ambiguous,
            balanceBad, versionHigh, versionLow, missing, duplicated, foreign, halfApplied,
            violations.Take(MaxSampledViolations).ToArray())
        {
            AllViolations = violations,
            HalfAppliedCheckTruncated = _ambiguousPairsSeen > MaxRetainedPairs,
        };
    }

    /// <summary>
    /// Reports every unknown-outcome transfer proven to have applied to one of its rows and not the
    /// other. Returns how many were found.
    ///
    /// <para>Only provable cases are counted. For a transfer's row, the scanned version says how many
    /// of that row's unknown-outcome transfers applied: a count equal to the row's total proves this
    /// transfer applied there, and a count of zero proves it did not. When one row proves "applied" and
    /// the other proves "not applied", the transfer moved value into one row without moving it out of
    /// the other — a broken transfer, whatever the totals say. A row whose count is neither extreme
    /// leaves this transfer's fate genuinely open and is passed over, so the check never guesses.</para>
    ///
    /// <para>Neither of the other checks can see this. Both legs stay inside their own bands, because a
    /// row touched by an unknown-outcome transfer is allowed to have applied it or not; and
    /// <c>SUM(balance)</c> only shows the net, so two half-applied transfers of opposite sign cancel
    /// and the total reads as conserved.</para>
    /// </summary>
    private long CheckLegPairing(List<RowViolation> violations)
    {
        long halfApplied = 0;

        foreach ((int low, int high) in _ambiguousPairs)
        {
            if (!_ambiguousAppliedKnown[low] || !_ambiguousAppliedKnown[high])
                continue;

            bool provenApplied =
                _ambiguousApplied[low] == _ambiguousTouches[low] ||
                _ambiguousApplied[high] == _ambiguousTouches[high];
            bool provenNotApplied =
                _ambiguousApplied[low] == 0 || _ambiguousApplied[high] == 0;

            if (!provenApplied || !provenNotApplied)
                continue;

            halfApplied++;
            int applied = _ambiguousApplied[low] == 0 ? high : low;
            int skipped = applied == low ? high : low;
            Add(violations, new RowViolation(
                applied, RowIdFactory.ForRow(_seed, applied), _dataset.TableOf(applied), "half-applied",
                $"a transfer between rows {low} and {high} applied to row {applied} but not to row " +
                $"{skipped}, so value moved without its counterpart; the client was never told the " +
                "transfer committed."));
        }

        return halfApplied;
    }

    private static void Add(List<RowViolation> violations, RowViolation violation)
    {
        if (violations.Count < MaxRetainedViolations)
            violations.Add(violation);
    }

    /// <summary>
    /// Streams <c>id, balance, version</c> from every table in the dataset, retrying the <b>whole</b>
    /// scan on a retryable failure.
    ///
    /// <para>Retrying the whole scan, not the failed table, is what makes <paramref name="onRow"/> safe
    /// to write as a plain accumulator: a scan that dies half way through has already fed rows to the
    /// callback, and resuming would count them twice. <paramref name="reset"/> discards everything the
    /// abandoned attempt accumulated before the next one starts. The scans run after the workload has
    /// stopped, so nothing is writing underneath them and two attempts see the same data.</para>
    /// </summary>
    private async Task ScanDatasetAsync(
        TableRowSource source, CancellationToken ct, TimeSpan? retryBudget,
        Action reset, Action<string, string, long, long> onRow)
    {
        TimeSpan budget = retryBudget ?? Reconciliation.DefaultRetryBudget;
        long startedAt = Stopwatch.GetTimestamp();

        for (int attempt = 1; ; attempt++)
        {
            reset();
            try
            {
                foreach (string table in _dataset.TableNames)
                {
                    await foreach (ScannedRow row in source(table, ct).WithCancellation(ct).ConfigureAwait(false))
                        onRow(table, row.Id, row.Balance, row.Version);
                }
                return;
            }
            catch (Exception ex)
            {
                Operations.OperationStatus status = Operations.ErrorClassifier.Classify(ex).Status;
                bool retryable = status is Operations.OperationStatus.Conflict or Operations.OperationStatus.Transient;
                bool budgetSpent = Stopwatch.GetElapsedTime(startedAt) >= budget;
                if (!retryable || budgetSpent || attempt >= MaxScanAttempts || ct.IsCancellationRequested)
                    throw;

                await Task.Delay(Math.Min(200 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Safety net only; the wall-clock retry budget is the real bound. A full scan is far too
    /// expensive to repeat thousands of times, so this cap is deliberately small.</summary>
    private const int MaxScanAttempts = 50;

    /// <summary>
    /// The production row source: a full primary scan of one table, streamed row by row. Both scans run
    /// after the workload has stopped writing (the baseline before the first worker starts, the
    /// verification after the last one finishes), so an autocommit read sees a settled table and needs
    /// no snapshot transaction of its own.
    /// </summary>
    public static TableRowSource RowsFrom(CamusConnection conn) => (table, ct) => StreamTableAsync(conn, table, ct);

    private static async IAsyncEnumerable<ScannedRow> StreamTableAsync(
        CamusConnection conn, string table, [EnumeratorCancellation] CancellationToken ct)
    {
        using CamusCommand cmd = conn.CreateCamusCommand($"SELECT id, balance, version FROM {table}");
        using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            yield return new ScannedRow(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    /// <summary>
    /// Writes every recorded violation to <c>row-violations.csv</c> so the offending rows can be joined
    /// against <c>transfer-ledger.csv</c> and the node logs. Writes nothing when the run was clean.
    /// </summary>
    public static async Task WriteViolationsAsync(
        string outputDir, RowAttributionResult result, CancellationToken ct)
    {
        IReadOnlyList<RowViolation> all = result.AllViolations;
        if (all.Count == 0)
            return;

        StringBuilder csv = new(all.Count * 96 + 64);
        csv.AppendLine("row_index,id,table,kind,detail");
        foreach (RowViolation v in all)
        {
            csv.AppendLine(string.Create(CultureInfo.InvariantCulture,
                $"{v.RowIndex},{v.Id},{v.Table},{v.Kind},\"{v.Detail.Replace("\"", "\"\"")}\""));
        }
        await File.WriteAllTextAsync(Path.Combine(outputDir, "row-violations.csv"), csv.ToString(), ct)
            .ConfigureAwait(false);
    }
}
