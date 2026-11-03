/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Elle;

/// <summary>Which checker reads the history, and so what a key's final state must look like.</summary>
public enum ElleModel
{
    /// <summary>Lists: an acknowledged append can never leave the list.</summary>
    ListAppend,

    /// <summary>Registers: an acknowledged write may be overwritten, but the final value must be a
    /// value some acknowledged or unresolved write of this run produced.</summary>
    RwRegister,
}

/// <summary>The final state of one register as the quiescent check read it. <see cref="Value"/> is
/// the row's value by primary key (null for no row), <see cref="Rows"/> how many rows the primary-key
/// read returned, and <see cref="Alternates"/> the same key read by other access paths (a secondary
/// index, a join), each of which must agree with the primary-key read.</summary>
public sealed record FinalRegister(long Key, long? Value, int Rows, IReadOnlyList<FinalAlternate> Alternates);

/// <summary>One other access path's view of a key: the path's name, its value (null for no row) and
/// its row count.</summary>
public sealed record FinalAlternate(string Path, long? Value, int Rows);

/// <summary>The final state of one list as the quiescent check read it; null for no row.</summary>
public sealed record FinalList(long Key, IReadOnlyList<long>? Values, int Rows);

/// <summary>
/// The quiescent check's verdict, written to <c>reconciliation.json</c> beside the dataset checks.
///
/// <para><see cref="RealtimeStaleFinals"/> counts keys whose final value is an acknowledged write that
/// another acknowledged write of the same key started after; under serializability that order is
/// allowed (the later-started write may be serialized first), so it is reported, and a strict
/// serializability claim is what makes it a failure. Every other count is a failure under any
/// isolation level the workload runs at.</para>
/// </summary>
public sealed record QuiescentResult(
    string Status,
    string Model,
    long Keys,
    long KeysRead,
    long OkWrites,
    long InfoWrites,
    long FailWrites,
    long RowsPresent,
    long LostWrites,
    long AbortedVisible,
    long GarbageValues,
    long DuplicateRows,
    long PathMismatches,
    long RealtimeStaleFinals,
    long CountMismatches,
    IReadOnlyList<string> Failures)
{
    public bool Passed => Status == "verified" && Failures.Count == 0;

    public static QuiescentResult Unavailable(string reason, ElleModel model, ElleWriteLedger ledger) => new(
        "unavailable", ElleWriteLedger.ModelName(model), ledger.KeyCount, 0, ledger.OkWrites, ledger.InfoWrites,
        ledger.FailWrites, 0, 0, 0, 0, 0, 0, 0, 0, [$"the final state could not be read: {reason}"]);
}

/// <summary>
/// Records every write an Elle run attempted, by key and outcome, and judges the table's final state
/// against them once the writers have stopped.
///
/// <para>Elle judges the history; this judges the end state, which the history cannot: a history whose
/// last operation on a key is a write says nothing about whether that write is still there, and a
/// write whose completion was lost (<c>:info</c>) is unresolved in the history for ever. The rules are
/// the model's. A list keeps every acknowledged append, so one that is missing was lost for good. A
/// register is overwritten by design, so a missing acknowledged value is normal and the check is on
/// the value that is there: it must be one this run's acknowledged or unresolved writes produced, and
/// never one from a write the server refused. An unresolved write may or may not be there; the check
/// never classes it as aborted.</para>
///
/// <para>Memory is one record per acknowledged write and one value per unresolved or failed write,
/// which is tens of megabytes for a million transactions.</para>
/// </summary>
public sealed class ElleWriteLedger
{
    /// <summary>An acknowledged write with the Stopwatch ticks of its invocation and its completion, so
    /// the real-time order of two writes on one key is known when they did not overlap.</summary>
    private readonly record struct OkWrite(long Value, long InvokeTicks, long CompleteTicks);

    private sealed class KeyLedger
    {
        public readonly List<OkWrite> Ok = [];
        public List<long>? Info;
        public List<long>? Fail;
    }

    private readonly object _gate = new();
    private readonly Dictionary<long, KeyLedger> _keys = [];
    private readonly ElleModel _model;
    private long _ok;
    private long _info;
    private long _fail;

    public ElleWriteLedger(ElleModel model)
    {
        _model = model;
    }

    public ElleModel Model => _model;

    public static string ModelName(ElleModel model) => model == ElleModel.ListAppend ? "list-append" : "rw-register";

    /// <summary>Keys with at least one attempted write, whatever its outcome.</summary>
    public long KeyCount
    {
        get
        {
            lock (_gate)
                return _keys.Count;
        }
    }

    public long OkWrites => Interlocked.Read(ref _ok);

    public long InfoWrites => Interlocked.Read(ref _info);

    public long FailWrites => Interlocked.Read(ref _fail);

    /// <summary>Every key with an attempted write, for the final reads.</summary>
    public long[] TouchedKeys()
    {
        lock (_gate)
            return [.. _keys.Keys];
    }

    /// <summary>Records the mutations of a committed transaction.</summary>
    public void RecordOk(IReadOnlyList<ElleMicroOp> txn, long invokeTicks, long completeTicks)
    {
        lock (_gate)
        {
            foreach (ElleMicroOp op in txn)
            {
                if (!op.IsMutation)
                    continue;
                Ledger(op.Key).Ok.Add(new OkWrite(op.Value, invokeTicks, completeTicks));
                _ok++;
            }
        }
    }

    /// <summary>Records the mutations of a transaction whose commit got no verdict.</summary>
    public void RecordInfo(IReadOnlyList<ElleMicroOp> txn)
    {
        lock (_gate)
        {
            foreach (ElleMicroOp op in txn)
            {
                if (!op.IsMutation)
                    continue;
                KeyLedger ledger = Ledger(op.Key);
                (ledger.Info ??= []).Add(op.Value);
                _info++;
            }
        }
    }

    /// <summary>Records the mutations of a transaction the server refused or that rolled back.</summary>
    public void RecordFail(IReadOnlyList<ElleMicroOp> txn)
    {
        lock (_gate)
        {
            foreach (ElleMicroOp op in txn)
            {
                if (!op.IsMutation)
                    continue;
                KeyLedger ledger = Ledger(op.Key);
                (ledger.Fail ??= []).Add(op.Value);
                _fail++;
            }
        }
    }

    private KeyLedger Ledger(long key)
    {
        if (!_keys.TryGetValue(key, out KeyLedger? ledger))
        {
            ledger = new KeyLedger();
            _keys[key] = ledger;
        }
        return ledger;
    }

    /// <summary>
    /// Judges the final registers. <paramref name="countMismatches"/> is the predicate shape's count of
    /// group reads whose <c>COUNT(*)</c> disagreed with the rows the same transaction selected; the
    /// other shapes pass 0. It fails the state only when <paramref name="countMismatchesFail"/> is set:
    /// one snapshot per transaction (serializable) must answer both statements alike, one snapshot per
    /// statement (read committed) need not, and the count is then a measurement carried in the result.
    /// <paramref name="extraFailures"/> are findings the reader made itself, such as a group whose
    /// access paths disagreed as sets.
    /// </summary>
    public QuiescentResult JudgeRegisters(
        IReadOnlyList<FinalRegister> finals, long countMismatches = 0, IReadOnlyList<string>? extraFailures = null,
        bool countMismatchesFail = true)
    {
        List<string> failures = extraFailures is null ? [] : [.. extraFailures];
        long present = 0, lost = 0, aborted = 0, garbage = 0, duplicates = 0, mismatches = 0, stale = 0;
        HashSet<long> read = [];

        lock (_gate)
        {
            foreach (FinalRegister final in finals)
            {
                read.Add(final.Key);
                _keys.TryGetValue(final.Key, out KeyLedger? ledger);

                if (final.Rows > 1)
                {
                    duplicates++;
                    Note(failures, $"key {final.Key}: the primary-key read returned {final.Rows} rows");
                }

                foreach (FinalAlternate alternate in final.Alternates)
                {
                    if (alternate.Rows != final.Rows || alternate.Value != final.Value)
                    {
                        mismatches++;
                        Note(failures,
                            $"key {final.Key}: read by {alternate.Path} gave {Describe(alternate.Value, alternate.Rows)}, " +
                            $"by primary key {Describe(final.Value, final.Rows)}");
                    }
                }

                if (final.Value is not long value)
                {
                    if (ledger is { Ok.Count: > 0 })
                    {
                        lost++;
                        Note(failures, $"key {final.Key}: {ledger.Ok.Count} acknowledged write(s), but no row");
                    }
                    continue;
                }

                present++;
                if (ledger is null)
                {
                    garbage++;
                    Note(failures, $"key {final.Key}: a row holds {value}, but this run never wrote the key");
                    continue;
                }

                int okAt = ledger.Ok.FindIndex(w => w.Value == value);
                if (okAt >= 0)
                {
                    OkWrite final0 = ledger.Ok[okAt];
                    if (ledger.Ok.Any(w => w.InvokeTicks > final0.CompleteTicks))
                        stale++;
                    continue;
                }

                if (ledger.Info is not null && ledger.Info.Contains(value))
                    continue;

                if (ledger.Fail is not null && ledger.Fail.Contains(value))
                {
                    aborted++;
                    Note(failures, $"key {final.Key}: holds {value}, which the server refused to commit");
                    continue;
                }

                garbage++;
                Note(failures, $"key {final.Key}: holds {value}, which no write of this run produced");
            }

            foreach (long key in _keys.Keys)
            {
                if (!read.Contains(key))
                    Note(failures, $"key {key}: was written but never read back");
            }

            if (countMismatches > 0 && countMismatchesFail)
                failures.Add($"{countMismatches:N0} group read(s) whose COUNT(*) disagreed with the rows the same transaction selected");

            return new QuiescentResult(
                "verified", ModelName(_model), _keys.Count, read.Count, _ok, _info, _fail, present,
                lost, aborted, garbage, duplicates, mismatches, stale, countMismatches, failures);
        }
    }

    /// <summary>Judges the final lists.</summary>
    public QuiescentResult JudgeLists(IReadOnlyList<FinalList> finals)
    {
        List<string> failures = [];
        long present = 0, lost = 0, aborted = 0, garbage = 0, duplicates = 0, stale = 0;
        HashSet<long> read = [];

        lock (_gate)
        {
            foreach (FinalList final in finals)
            {
                read.Add(final.Key);
                _keys.TryGetValue(final.Key, out KeyLedger? ledger);

                if (final.Rows > 1)
                {
                    duplicates++;
                    Note(failures, $"key {final.Key}: the primary-key read returned {final.Rows} rows");
                }

                if (final.Values is null)
                {
                    if (ledger is { Ok.Count: > 0 })
                    {
                        lost++;
                        Note(failures, $"key {final.Key}: {ledger.Ok.Count} acknowledged append(s), but no row");
                    }
                    continue;
                }

                present++;
                if (ledger is null)
                {
                    garbage++;
                    Note(failures, $"key {final.Key}: a list exists, but this run never appended to the key");
                    continue;
                }

                Dictionary<long, int> position = [];
                for (int i = 0; i < final.Values.Count; i++)
                {
                    long element = final.Values[i];
                    if (!position.TryAdd(element, i))
                    {
                        duplicates++;
                        Note(failures, $"key {final.Key}: element {element} appears more than once");
                        continue;
                    }

                    bool known = ledger.Ok.Any(w => w.Value == element)
                        || (ledger.Info is not null && ledger.Info.Contains(element));
                    if (known)
                        continue;

                    if (ledger.Fail is not null && ledger.Fail.Contains(element))
                    {
                        aborted++;
                        Note(failures, $"key {final.Key}: holds {element}, which the server refused to commit");
                    }
                    else
                    {
                        garbage++;
                        Note(failures, $"key {final.Key}: holds {element}, which no append of this run produced");
                    }
                }

                foreach (OkWrite ok in ledger.Ok)
                {
                    if (!position.ContainsKey(ok.Value))
                    {
                        lost++;
                        Note(failures, $"key {final.Key}: acknowledged append {ok.Value} is missing from the list");
                    }
                }

                // Real-time order of appends that did not overlap: the one that finished first should
                // come first. Serializability alone does not promise it, so it is counted, not failed.
                for (int a = 0; a < ledger.Ok.Count; a++)
                {
                    if (!position.TryGetValue(ledger.Ok[a].Value, out int posA))
                        continue;
                    for (int b = 0; b < ledger.Ok.Count; b++)
                    {
                        if (a == b || ledger.Ok[b].InvokeTicks <= ledger.Ok[a].CompleteTicks)
                            continue;
                        if (position.TryGetValue(ledger.Ok[b].Value, out int posB) && posB < posA)
                        {
                            stale++;
                            break;
                        }
                    }
                }
            }

            foreach (long key in _keys.Keys)
            {
                if (!read.Contains(key))
                    Note(failures, $"key {key}: was appended to but never read back");
            }

            return new QuiescentResult(
                "verified", ModelName(_model), _keys.Count, read.Count, _ok, _info, _fail, present,
                lost, aborted, garbage, duplicates, 0, stale, 0, failures);
        }
    }

    /// <summary>Keeps the failure list readable: the counts carry the totals, the list the first examples.</summary>
    private const int MaxNotedFailures = 50;

    private static void Note(List<string> failures, string failure)
    {
        if (failures.Count < MaxNotedFailures)
            failures.Add(failure);
        else if (failures.Count == MaxNotedFailures)
            failures.Add("further findings omitted; the counts carry the totals");
    }

    private static string Describe(long? value, int rows)
        => rows == 0 ? "no row" : rows == 1 ? $"{value}" : $"{rows} rows";
}
