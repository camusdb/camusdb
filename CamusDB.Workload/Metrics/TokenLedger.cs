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

/// <summary>How a token move ended, as the journal records it.</summary>
public enum TokenMoveOutcome
{
    Committed,
    Indeterminate,
    ConflictRetry,
    ConflictFinal,
    Error,
}

/// <summary>How a move names the successor row it inserts.</summary>
public enum TokenMoveKind
{
    /// <summary>A key never used before.</summary>
    Fresh,

    /// <summary>The key the move just deleted, inside the same transaction.</summary>
    Same,

    /// <summary>The key the previous move of the slot deleted, so a key deleted by an earlier, committed
    /// transaction is inserted again.</summary>
    Back,
}

/// <summary>One row of the token table, as a verification read returned it.</summary>
public sealed record TokenRow(string Id, long Slot, long Gen, string PrevId);

/// <summary>
/// The token workload's verdict, written into <c>reconciliation.json</c>. Every count of a violation is
/// never waived: a row that came back after its delete committed, a move that committed and is not in the
/// table, and two moves committed from one generation are each the failure the workload exists to catch.
/// </summary>
public sealed record TokenResult(
    string Status,
    long Tokens,
    long Rows,
    long Committed,
    long Indeterminate,
    long ConflictRetries,
    long ConflictFinal,
    long Errors,
    long Reads,
    long ReadViolations,
    long MissingSlots,
    long DuplicateSlots,
    long ForeignRows,
    long GenBelow,
    long GenAbove,
    long DoubleCommits,
    long IndexMissing,
    long IndexExtra,
    IReadOnlyDictionary<string, long> MovesByKind,
    IReadOnlyList<string> Failures)
{
    public bool Passed => Status == "verified" && Failures.Count == 0;

    public static TokenResult Unavailable(string reason, TokenLedger ledger, long tokens) => new(
        "unavailable", tokens, 0, ledger.CommittedMoves, ledger.IndeterminateMoves, ledger.ConflictRetries,
        ledger.ConflictFinal, ledger.Errors, ledger.Reads, ledger.ReadViolations,
        0, 0, 0, 0, 0, ledger.DoubleCommits.Count, 0, 0, ledger.MovesByKind(),
        [$"tokens: the final state could not be read, so the run is unverified: {reason}"]);
}

/// <summary>
/// The journal of every token move and every token read the workload drove, and the expectation the final
/// table is judged against.
///
/// <para>Per slot it keeps the moves that committed and the moves whose commit had no verdict. A slot's
/// generation counts the moves applied to it, so its final <c>gen</c> must lie between the committed
/// moves and the committed plus the indeterminate ones. Below means a move the client saw commit is not in
/// the table (a lost write, or a lost delete-and-insert); above means a move the client saw fail was
/// applied. Every committed move also records the generation it read: two commits from one generation are
/// a double spend, which the table alone can hide when the second insert collided with nothing.</para>
///
/// <para>Recording is lock-free on the hot path. The journal is written to <c>token-ledger.csv</c> after
/// the run, one line per terminal attempt and per read violation.</para>
/// </summary>
public sealed class TokenLedger
{
    private const int SampleLimit = 10;

    private readonly long _tokens;
    private readonly long[] _committed;
    private readonly long[] _indeterminate;
    private readonly ConcurrentDictionary<(long Slot, long Gen), byte> _committedFrom = new();
    private readonly ConcurrentQueue<string> _doubleCommits = new();
    private readonly ConcurrentQueue<string> _readViolationSamples = new();
    private readonly ConcurrentQueue<string> _lines = new();
    private readonly long[] _kinds = new long[3];
    private long _conflictRetries;
    private long _conflictFinal;
    private long _errors;
    private long _reads;
    private long _readViolations;

    public TokenLedger(long tokens)
    {
        _tokens = tokens;
        _committed = new long[tokens];
        _indeterminate = new long[tokens];
    }

    public long Tokens => _tokens;

    public long CommittedMoves => _committed.Sum();

    public long IndeterminateMoves => _indeterminate.Sum();

    public long ConflictRetries => Interlocked.Read(ref _conflictRetries);

    public long ConflictFinal => Interlocked.Read(ref _conflictFinal);

    public long Errors => Interlocked.Read(ref _errors);

    public long Reads => Interlocked.Read(ref _reads);

    public long ReadViolations => Interlocked.Read(ref _readViolations);

    public IReadOnlyList<string> DoubleCommits => _doubleCommits.ToArray();

    public long CommittedOf(long slot) => Interlocked.Read(ref _committed[slot]);

    public long IndeterminateOf(long slot) => Interlocked.Read(ref _indeterminate[slot]);

    public IReadOnlyDictionary<string, long> MovesByKind() => new Dictionary<string, long>
    {
        ["fresh"] = Interlocked.Read(ref _kinds[(int)TokenMoveKind.Fresh]),
        ["same"] = Interlocked.Read(ref _kinds[(int)TokenMoveKind.Same]),
        ["back"] = Interlocked.Read(ref _kinds[(int)TokenMoveKind.Back]),
    };

    /// <summary>Records one terminal attempt of a move of <paramref name="slot"/> that read generation
    /// <paramref name="fromGen"/> (-1 when the attempt failed before it read the row).</summary>
    public void RecordMove(long slot, long fromGen, TokenMoveKind kind, int attempt, TokenMoveOutcome outcome, string? code)
    {
        long wallMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _lines.Enqueue(string.Create(CultureInfo.InvariantCulture,
            $"{wallMs},move,{slot},{fromGen},{kind.ToString().ToLowerInvariant()},{attempt},{Token(outcome)},{code ?? ""}"));

        switch (outcome)
        {
            case TokenMoveOutcome.Committed:
                Interlocked.Increment(ref _committed[slot]);
                Interlocked.Increment(ref _kinds[(int)kind]);
                if (!_committedFrom.TryAdd((slot, fromGen), 0))
                    _doubleCommits.Enqueue($"slot {slot}: a second move committed from generation {fromGen} at {wallMs} ms");
                break;
            case TokenMoveOutcome.Indeterminate:
                Interlocked.Increment(ref _indeterminate[slot]);
                break;
            case TokenMoveOutcome.ConflictRetry:
                Interlocked.Increment(ref _conflictRetries);
                break;
            case TokenMoveOutcome.ConflictFinal:
                Interlocked.Increment(ref _conflictFinal);
                break;
            case TokenMoveOutcome.Error:
                Interlocked.Increment(ref _errors);
                break;
        }
    }

    /// <summary>Records one read-only lookup of a slot. A lookup that does not return exactly one row saw a
    /// half-applied move, a lost row or a resurrected one, inside a snapshot that should hold exactly one.</summary>
    public void RecordRead(long slot, IReadOnlyList<(string Id, long Gen)> found)
    {
        Interlocked.Increment(ref _reads);
        if (found.Count == 1)
            return;

        Interlocked.Increment(ref _readViolations);
        long wallMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        string rows = found.Count == 0 ? "no row" : string.Join(" ", found.Select(f => $"{f.Id}@{f.Gen}"));
        _lines.Enqueue(string.Create(CultureInfo.InvariantCulture, $"{wallMs},read,{slot},,,,{found.Count}_rows,{rows}"));
        if (_readViolationSamples.Count < SampleLimit)
            _readViolationSamples.Enqueue($"slot {slot} at {wallMs} ms: {rows}");
    }

    private static string Token(TokenMoveOutcome outcome) => outcome switch
    {
        TokenMoveOutcome.Committed => "committed",
        TokenMoveOutcome.Indeterminate => "indeterminate",
        TokenMoveOutcome.ConflictRetry => "conflict_retry",
        TokenMoveOutcome.ConflictFinal => "conflict_final",
        _ => "error",
    };

    /// <summary>
    /// Judges the final table. <paramref name="byPrimaryKey"/> is every row read through the primary key;
    /// <paramref name="idsByIndex"/> is every id read through the slot index.
    /// </summary>
    public TokenResult Judge(IReadOnlyList<TokenRow> byPrimaryKey, IReadOnlyList<string> idsByIndex)
    {
        List<string> failures = [];
        Dictionary<long, List<TokenRow>> bySlot = [];
        List<TokenRow> foreign = [];
        foreach (TokenRow row in byPrimaryKey)
        {
            if (row.Slot < 0 || row.Slot >= _tokens)
            {
                foreign.Add(row);
                continue;
            }
            if (!bySlot.TryGetValue(row.Slot, out List<TokenRow>? rows))
                bySlot[row.Slot] = rows = [];
            rows.Add(row);
        }

        List<string> missing = [];
        List<string> duplicate = [];
        List<string> below = [];
        List<string> above = [];
        for (long slot = 0; slot < _tokens; slot++)
        {
            if (!bySlot.TryGetValue(slot, out List<TokenRow>? rows))
            {
                missing.Add($"slot {slot} (committed {CommittedOf(slot)}, indeterminate {IndeterminateOf(slot)})");
                continue;
            }

            if (rows.Count > 1)
            {
                duplicate.Add($"slot {slot}: {string.Join(", ", rows.OrderBy(r => r.Gen).Select(r => $"{r.Id} gen {r.Gen}"))}");
                continue;
            }

            long gen = rows[0].Gen;
            long committed = CommittedOf(slot);
            long ceiling = committed + IndeterminateOf(slot);
            if (gen < committed)
                below.Add($"slot {slot}: gen {gen}, but {committed} move(s) committed ({committed - gen} lost)");
            else if (gen > ceiling)
                above.Add($"slot {slot}: gen {gen}, but at most {ceiling} move(s) could have applied ({committed} committed, {IndeterminateOf(slot)} indeterminate)");
        }

        HashSet<string> pkIds = byPrimaryKey.Select(r => r.Id).ToHashSet(StringComparer.Ordinal);
        HashSet<string> indexIds = idsByIndex.ToHashSet(StringComparer.Ordinal);
        List<string> indexMissing = pkIds.Where(id => !indexIds.Contains(id)).Order(StringComparer.Ordinal).ToList();
        List<string> indexExtra = indexIds.Where(id => !pkIds.Contains(id)).Order(StringComparer.Ordinal).ToList();
        int indexDuplicates = idsByIndex.Count - indexIds.Count;

        if (byPrimaryKey.Count != _tokens)
            failures.Add($"tokens: the table holds {byPrimaryKey.Count} row(s), expected exactly {_tokens}");
        Add(failures, missing, "slot(s) have no row: a committed move's insert is lost, or its delete applied without it");
        Add(failures, duplicate, "slot(s) have more than one row: a committed delete came back, or two moves of one generation both applied");
        Add(failures, foreign.Select(r => $"{r.Id} (slot {r.Slot})").ToList(), "row(s) belong to no slot of this run");
        Add(failures, below, "slot(s) hold fewer moves than committed: a committed move was lost");
        Add(failures, above, "slot(s) hold more moves than could have applied: a move the client saw fail was applied");
        Add(failures, DoubleCommits.ToList(), "move(s) committed from a generation another committed move had already consumed (double spend)");
        Add(failures, indexMissing, "row(s) are missing from the slot index");
        Add(failures, indexExtra, "slot index entr(ies) point at rows the primary key does not hold");
        if (indexDuplicates > 0)
            failures.Add($"tokens: the slot index returned {indexDuplicates} row(s) more than once");
        if (ReadViolations > 0)
            failures.Add($"tokens: {ReadViolations} of {Reads} read-only slot lookup(s) did not return exactly one row " +
                         $"(a snapshot saw a half-applied, lost or resurrected token). First: {string.Join("; ", _readViolationSamples.Take(5))}");

        return new TokenResult(
            "verified", _tokens, byPrimaryKey.Count, CommittedMoves, IndeterminateMoves, ConflictRetries, ConflictFinal, Errors,
            Reads, ReadViolations, missing.Count, duplicate.Count, foreign.Count, below.Count, above.Count,
            DoubleCommits.Count, indexMissing.Count, indexExtra.Count + indexDuplicates, MovesByKind(), failures);
    }

    private static void Add(List<string> failures, List<string> found, string what)
    {
        if (found.Count > 0)
            failures.Add($"tokens: {found.Count} {what}. First: {string.Join("; ", found.Take(5))}");
    }

    /// <summary>Writes <c>token-ledger.csv</c>: one line per terminal move attempt and per read violation.</summary>
    public async Task WriteAsync(string outputDir, CancellationToken ct)
    {
        // Streamed for the same reason as the transfer ledger: one string of every attempt of a long
        // run can pass the 2 GB object limit.
        await using StreamWriter csv = new(Path.Combine(outputDir, "token-ledger.csv"), append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 16);
        await csv.WriteLineAsync("wall_ms,event,slot,from_gen,kind,attempt,outcome,code").ConfigureAwait(false);
        while (_lines.TryDequeue(out string? line))
        {
            ct.ThrowIfCancellationRequested();
            await csv.WriteLineAsync(line).ConfigureAwait(false);
        }
    }
}
