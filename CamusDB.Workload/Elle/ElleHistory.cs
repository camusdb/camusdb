/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Elle;

/// <summary>How one transaction attempt ended, in Jepsen's terms.</summary>
public enum ElleOutcome
{
    /// <summary>Committed; the completion carries every read's observed list.</summary>
    Ok,

    /// <summary>Definitely did not commit: aborted, rolled back, or never reached commit.</summary>
    Fail,

    /// <summary>May or may not have committed: the commit was sent and no verdict came back.</summary>
    Info,
}

/// <summary>An invocation that has been written and still waits for its completion.</summary>
public sealed class ElleInvocation
{
    internal ElleInvocation(int process, IReadOnlyList<ElleMicroOp> txn)
    {
        Process = process;
        Txn = txn;
    }

    public int Process { get; }

    public IReadOnlyList<ElleMicroOp> Txn { get; }

    /// <summary>Set once the completion is written, so a second completion or the close-out cannot
    /// write another one.</summary>
    internal bool Completed;
}

/// <summary>Counts of what the history holds, for the run's own console and for <c>elle.json</c>.</summary>
public sealed record ElleHistoryStats(long Invocations, long Ok, long Fail, long Info, long OpenAtClose, int Processes);

/// <summary>
/// Writes a Jepsen-format history, one EDN operation map per line, for Elle's list-append checker.
///
/// <para>Elle reads a history as a set of single-threaded logical processes: a process has at most one
/// operation in flight, and a process whose operation ended <c>:info</c> is never used again, because
/// that operation may still take effect at any later time. The workload does not have such processes —
/// in open-loop mode one worker has many operations in flight at once — so the recorder allocates them
/// itself: an invocation takes a free process id, an <c>:ok</c> or <c>:fail</c> gives it back, and an
/// <c>:info</c> retires it.</para>
///
/// <para>The line order in the file is the <c>:index</c> order, and Elle takes real-time order from it:
/// an operation whose completion line comes before another's invocation line finished first. Both are
/// written under one lock, so the file order is the order the recorder saw the events.</para>
///
/// <para>An operation still open when the recorder closes gets an <c>:info</c> completion, since it may
/// have committed. A completion that arrives after the close is dropped: the <c>:info</c> already
/// covers both outcomes.</para>
/// </summary>
public sealed class ElleHistory : IDisposable
{
    private readonly object _gate = new();
    private readonly StreamWriter _writer;
    private readonly ConcurrentStack<int> _freeProcesses = new();
    private readonly HashSet<ElleInvocation> _open = new(ReferenceEqualityComparer.Instance);
    private readonly long _startTimestamp = Stopwatch.GetTimestamp();
    private int _nextProcess;
    private long _index;
    private long _invocations;
    private long _ok;
    private long _fail;
    private long _info;
    private long _openAtClose;
    private bool _closed;

    public ElleHistory(string path)
        : this(new StreamWriter(path, append: false, new UTF8Encoding(false)))
    {
    }

    /// <summary>Writes to a caller-supplied writer; tests use this to read the history back.</summary>
    public ElleHistory(StreamWriter writer)
    {
        _writer = writer;
    }

    /// <summary>Writes the invocation of one transaction attempt and returns its handle.</summary>
    public ElleInvocation Invoke(IReadOnlyList<ElleMicroOp> txn)
    {
        int process = _freeProcesses.TryPop(out int free) ? free : Interlocked.Increment(ref _nextProcess) - 1;
        ElleInvocation invocation = new(process, txn);

        lock (_gate)
        {
            if (_closed)
            {
                // Nothing is written after the close, so this attempt never exists in the history. The
                // caller still runs it; an attempt the checker never sees cannot corrupt the check.
                invocation.Completed = true;
                return invocation;
            }

            WriteLine(":invoke", process, txn);
            _open.Add(invocation);
            _invocations++;
        }

        return invocation;
    }

    /// <summary>
    /// Writes the completion of an attempt. <paramref name="observed"/> is the transaction with its read
    /// results filled in, and is used only for <see cref="ElleOutcome.Ok"/>; a failed or indeterminate
    /// attempt repeats the invoked value, as Jepsen does.
    /// </summary>
    public void Complete(ElleInvocation invocation, ElleOutcome outcome, IReadOnlyList<ElleMicroOp>? observed = null)
    {
        lock (_gate)
        {
            if (invocation.Completed)
                return;
            invocation.Completed = true;
            _open.Remove(invocation);

            switch (outcome)
            {
                case ElleOutcome.Ok:
                    WriteLine(":ok", invocation.Process, observed ?? invocation.Txn);
                    _ok++;
                    break;
                case ElleOutcome.Fail:
                    WriteLine(":fail", invocation.Process, invocation.Txn);
                    _fail++;
                    break;
                default:
                    WriteLine(":info", invocation.Process, invocation.Txn);
                    _info++;
                    break;
            }
        }

        // Outside the lock: the id goes back only after its completion line is in the file, so the next
        // invocation on this process can never be written before it.
        if (outcome != ElleOutcome.Info)
            _freeProcesses.Push(invocation.Process);
    }

    /// <summary>
    /// Ends the history: every attempt still open gets an <c>:info</c> completion, and the file is
    /// flushed. Later calls to <see cref="Invoke"/> and <see cref="Complete"/> write nothing.
    /// </summary>
    public ElleHistoryStats Close()
    {
        lock (_gate)
        {
            if (!_closed)
            {
                _closed = true;
                foreach (ElleInvocation open in _open)
                {
                    open.Completed = true;
                    WriteLine(":info", open.Process, open.Txn);
                    _info++;
                    _openAtClose++;
                }
                _open.Clear();
                _writer.Flush();
            }

            return new ElleHistoryStats(_invocations, _ok, _fail, _info, _openAtClose, Volatile.Read(ref _nextProcess));
        }
    }

    public void Dispose()
    {
        Close();
        _writer.Dispose();
    }

    private void WriteLine(string type, int process, IReadOnlyList<ElleMicroOp> txn)
    {
        // A TimeSpan tick is 100 ns. Elle uses :time only to draw plots; order comes from :index.
        long nanos = Stopwatch.GetElapsedTime(_startTimestamp).Ticks * 100;

        StringBuilder sb = new(64 + txn.Count * 24);
        sb.Append("{:index ").Append(_index.ToString(CultureInfo.InvariantCulture))
          .Append(", :type ").Append(type)
          .Append(", :f :txn, :process ").Append(process.ToString(CultureInfo.InvariantCulture))
          .Append(", :time ").Append(nanos.ToString(CultureInfo.InvariantCulture))
          .Append(", :value [");
        for (int i = 0; i < txn.Count; i++)
        {
            if (i > 0)
                sb.Append(' ');
            txn[i].WriteEdn(sb);
        }
        sb.Append("]}");

        _writer.WriteLine(sb.ToString());
        _index++;
    }
}
