/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Metrics;

/// <summary>
/// The single thread-safe sink every worker reports to during the measured interval. It separates the
/// three numbers coordinated omission tends to conflate: how many operations were <em>offered</em>
/// (scheduled), how many <em>started</em>, and how many <em>completed</em> — plus scheduling delay
/// kept apart from response latency, so an overloaded server cannot look fast. Counters are bounded
/// dimensions (kind + outcome); latency lives in mergeable histograms, never per-sample lists.
/// Only results produced inside the measured window are recorded; warm-up and drain feed a separate
/// instance that is discarded.
/// </summary>
public sealed class RunMetrics
{
    private long _offered;      // operations the scheduler intended to submit
    private long _started;      // operations actually dispatched
    private long _completedRead;
    private long _completedWrite;
    private long _failed;
    private long _conflicts;
    private long _transient;
    private long _indeterminate;
    private long _domainErrors;
    private long _internalErrors;
    private long _scheduleDrops; // open-loop: schedule slots dropped due to backpressure
    private long _inFlight;
    private long _rowsRead;
    private long _writeRowsCommitted;

    public readonly LatencyHistogram ReadLatency = new();
    public readonly LatencyHistogram WriteLatency = new();
    public readonly LatencyHistogram BeginLatency = new();
    public readonly LatencyHistogram WriteReadLatency = new();
    public readonly LatencyHistogram UpdateLatency = new();
    public readonly LatencyHistogram CommitLatency = new();
    public readonly LatencyHistogram ScheduleDelay = new();
    public readonly ErrorSampler Errors = new();

    /// <summary>
    /// Wall-clock UTC at which the measured window began (the instant the interval recorder started).
    /// Stamped once by <see cref="IntervalRecorder.Start"/>. This is the anchor that lets an external
    /// tool align the per-second <c>intervals.csv</c> (whose <c>second</c> column is 0 at this instant)
    /// with wall-clock events such as a chaos harness's fault timeline. <see cref="DateTime.MinValue"/>
    /// until the measured window starts.
    /// </summary>
    public DateTime MeasureStartUtc { get; set; } = DateTime.MinValue;

    /// <summary>
    /// Operations still running when the scheduler's drain budget expired. Non-zero only in open-loop
    /// mode, which dispatches without waiting. It matters to correctness checking, not to throughput:
    /// a transfer still in flight can commit <em>after</em> the verification scan has read its rows, so
    /// any post-run comparison against the client's journal would see a write the journal does not yet
    /// carry. A run that ends with work outstanding cannot have its per-row attribution trusted, and
    /// says so instead of reporting a violation it manufactured itself.
    /// </summary>
    public long UnfinishedAtDrain { get; set; }

    private readonly int _writesPerTransaction;

    public RunMetrics(int writesPerTransaction)
    {
        _writesPerTransaction = Math.Max(1, writesPerTransaction);
    }

    public long Offered => Interlocked.Read(ref _offered);
    public long Started => Interlocked.Read(ref _started);
    public long CompletedRead => Interlocked.Read(ref _completedRead);
    public long CompletedWrite => Interlocked.Read(ref _completedWrite);
    public long Completed => CompletedRead + CompletedWrite;
    public long Failed => Interlocked.Read(ref _failed);
    public long Conflicts => Interlocked.Read(ref _conflicts);
    public long Transient => Interlocked.Read(ref _transient);
    public long Indeterminate => Interlocked.Read(ref _indeterminate);
    public long DomainErrors => Interlocked.Read(ref _domainErrors);
    public long InternalErrors => Interlocked.Read(ref _internalErrors);
    public long ScheduleDrops => Interlocked.Read(ref _scheduleDrops);
    public long InFlight => Interlocked.Read(ref _inFlight);
    public long RowsRead => Interlocked.Read(ref _rowsRead);
    public long WriteRowsCommitted => Interlocked.Read(ref _writeRowsCommitted);

    public void MarkOffered() => Interlocked.Increment(ref _offered);
    public void MarkScheduleDrop() => Interlocked.Increment(ref _scheduleDrops);
    public void MarkStarted() => Interlocked.Increment(ref _started);
    public void IncInFlight() => Interlocked.Increment(ref _inFlight);
    public void DecInFlight() => Interlocked.Decrement(ref _inFlight);

    public void RecordScheduleDelay(double ms) => ScheduleDelay.Record(ms);

    /// <summary>Records a completed (or failed) operation together with its whole-operation latency.</summary>
    public void RecordResult(in OperationResult result, double totalLatencyMs)
    {
        if (result.IsSuccess)
        {
            if (result.Kind == OperationKind.Read)
            {
                Interlocked.Increment(ref _completedRead);
                Interlocked.Increment(ref _rowsRead);
                ReadLatency.Record(totalLatencyMs);
            }
            else
            {
                Interlocked.Increment(ref _completedWrite);
                Interlocked.Add(ref _writeRowsCommitted, _writesPerTransaction);
                WriteLatency.Record(totalLatencyMs);
                BeginLatency.Record(result.BeginMs);
                WriteReadLatency.Record(result.ReadMs);
                UpdateLatency.Record(result.UpdateMs);
                CommitLatency.Record(result.CommitMs);
            }
            return;
        }

        Interlocked.Increment(ref _failed);
        switch (result.Status)
        {
            case OperationStatus.Conflict: Interlocked.Increment(ref _conflicts); break;
            case OperationStatus.Transient: Interlocked.Increment(ref _transient); break;
            case OperationStatus.Indeterminate: Interlocked.Increment(ref _indeterminate); break;
            case OperationStatus.DomainError: Interlocked.Increment(ref _domainErrors); break;
            default: Interlocked.Increment(ref _internalErrors); break;
        }
        Errors.Record(result.ErrorCode ?? "UNKNOWN", $"{result.Kind}:{result.Status}");
    }
}
