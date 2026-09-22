/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Diagnostics;
using Kahuna;
using Kahuna.Shared.Sequences;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// One consecutive run of sequence values, handed out with no further storage traffic.
/// </summary>
/// <param name="Start">The first value of the run, inclusive.</param>
/// <param name="End">The last value of the run, inclusive.</param>
/// <param name="Count">How many values the run holds.</param>
internal readonly record struct SequenceRun(long Start, long End, int Count);

/// <summary>
/// Where a sequence stands, as far as the sequencer can say.
/// </summary>
/// <param name="ReservedCeiling">
/// The reserved high-water mark: an upper bound on the values issued, never the last one issued.
/// After four draws on a fresh sequence with the default block size it reads 1000.
/// </param>
/// <param name="HasIssuedAValue">
/// False while the counter still sits exactly where it was seeded, which means nothing has been
/// drawn from it since it was created or last moved. A caller showing the position to a person
/// reports nothing at all in that case, rather than the seed — the seed is one increment below
/// the first value the sequence will issue, and printing it invites the reader to mistake it for
/// a value that was handed out.
/// </param>
internal readonly record struct SequencePosition(long ReservedCeiling, bool HasIssuedAValue);

/// <summary>
/// The only code in the engine that calls Kahuna's sequencer for a <b>user</b> sequence. Nothing
/// else invokes <c>LocateAnd*Sequence*</c> for one, so the naming rule, the response mapping and
/// the retry budget are stated once.
///
/// <para><b>The counter is named after the sequence's immutable id, never the user's name.</b> A
/// rename must not move the counter, and a drop-plus-recreate of the same name must not inherit
/// the previous counter's reserved block. <see cref="MetaKeys.KahunaSequenceName"/> builds the
/// name; no caller spells it out.</para>
///
/// <para><b>A sequence call is not a KV write.</b> It takes no row lock, registers no key range,
/// and joins no transaction's modified-key set. It must stay out of <c>KvTransaction</c>
/// entirely — which is also why an advance cannot be rolled back, and why the values a rolled-back
/// statement drew are gone for good.</para>
///
/// <para><b>Gaps are normal and are not an error.</b> The owning node reserves a block per durable
/// commit and abandons whatever is left of it on restart, on an ownership change, and on eviction
/// once more than <c>SequencerMaxSequencesPerActor</c> sequences are resident. Many rarely-used
/// sequences therefore means more gaps, not a failure; nothing here tries to prevent it.</para>
/// </summary>
internal sealed class SequenceAllocator
{
    private CamusDBOptions options;

    internal SequenceAllocator(CamusDBOptions options)
    {
        this.options = options;
    }

    /// <summary>
    /// Adopts a published configuration swap. The retry budget is read per call from the current
    /// snapshot, so a change to it takes effect on the next allocation rather than at restart.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Creates the counter behind <paramref name="sequence"/>. <c>AlreadyExists</c> is success: the
    /// counter is created by whichever node proposes the DDL, and a re-run of a create that already
    /// landed must not fail the statement.
    /// </summary>
    internal async Task CreateAsync(IKahuna kahuna, string dbId, SequenceSchema sequence, CancellationToken cancellationToken)
    {
        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        string name = MetaKeys.KahunaSequenceName(dbId, sequence.Id!);

        // The counter is created holding StartValue - Increment, so the first value it issues is
        // StartValue itself. Kahuna's next value is always "current + increment", and a counter
        // created at StartValue would issue StartValue + Increment first, skipping the value the
        // user asked to start from.
        (SequenceResponseType type, _) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndCreateSequence(
                name,
                initialValue: sequence.StartValue - sequence.Increment,
                increment: sequence.Increment,
                maxValue: sequence.MaxValue,
                blockSize: sequence.CacheSize,
                SequenceDurability.Persistent,
                cancellationToken),
            elapsed,
            options.SequenceRetryBudgetMs
        ).ConfigureAwait(false);

        if (type is SequenceResponseType.Success or SequenceResponseType.AlreadyExists)
            return;

        throw Failure(type, sequence.Name ?? name, "create the sequence");
    }

    /// <summary>
    /// Deletes the counter behind a dropped sequence. <c>NotFound</c> is success: the delete runs
    /// after the catalog record is already gone, so a retry, a crash-resume, or a create that never
    /// completed all reach this with nothing left to remove.
    /// </summary>
    internal async Task DeleteAsync(IKahuna kahuna, string dbId, string sequenceId, CancellationToken cancellationToken)
    {
        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        string name = MetaKeys.KahunaSequenceName(dbId, sequenceId);

        SequenceResponseType type = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndDeleteSequence(name, SequenceDurability.Persistent, cancellationToken),
            elapsed,
            options.SequenceRetryBudgetMs
        ).ConfigureAwait(false);

        if (type is SequenceResponseType.Success or SequenceResponseType.NotFound)
            return;

        // A delete that answered MustRetry has done nothing. Reporting it matters: the caller — the
        // database purge — clears its recovery marker only on a verified result, and treating this
        // as success would leak the counter with nothing left that names it.
        throw Failure(type, name, "delete the sequence");
    }

    /// <summary>
    /// Reserves <paramref name="count"/> consecutive values in one round trip, creating the counter
    /// first if it is missing.
    ///
    /// <para>The missing-counter path is the documented repair for a create that replicated its
    /// catalog record and then died before the counter existed. The record carries the start value,
    /// so the first use can rebuild the counter rather than fail the user's statement. It is not a
    /// fallback for an unknown sequence: the caller resolved <paramref name="sequence"/> from the
    /// catalog, so the sequence does exist.</para>
    /// </summary>
    /// <param name="idempotencyKey">
    /// Makes the reservation replayable, so a retry after a timeout returns the same run instead of
    /// consuming a second one. Replay is guaranteed only inside Kahuna's retention window
    /// (256 entries per sequence, 10 minutes), so a later retry allocates fresh values — acceptable,
    /// because a sequence may gap. A key replayed with a <b>different</b> count is refused outright,
    /// which is why a chunked load must vary the key per chunk rather than per statement.
    /// </param>
    internal async Task<SequenceRun> ReserveAsync(
        IKahuna kahuna,
        string dbId,
        SequenceSchema sequence,
        int count,
        string? idempotencyKey,
        CancellationToken cancellationToken)
    {
        if (count <= 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"A sequence reservation must ask for at least one value; got {count}");

        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        string name = MetaKeys.KahunaSequenceName(dbId, sequence.Id!);

        (SequenceResponseType type, SequenceAllocation allocation) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndReserveSequenceRange(
                name, count, idempotencyKey, SequenceDurability.Persistent, cancellationToken),
            elapsed,
            options.SequenceRetryBudgetMs
        ).ConfigureAwait(false);

        if (type == SequenceResponseType.NotFound)
        {
            await CreateAsync(kahuna, dbId, sequence, cancellationToken).ConfigureAwait(false);

            (type, allocation) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
                () => kahuna.LocateAndReserveSequenceRange(
                    name, count, idempotencyKey, SequenceDurability.Persistent, cancellationToken),
                elapsed,
                options.SequenceRetryBudgetMs
            ).ConfigureAwait(false);
        }

        if (type != SequenceResponseType.Success)
            throw Failure(type, sequence.Name ?? name, $"reserve {count} value(s)");

        return new SequenceRun(allocation.Start, allocation.End, allocation.Count);
    }

    /// <summary>
    /// Rewrites the counter's parameters — the operation behind <c>setval</c>,
    /// <c>ALTER SEQUENCE … RESTART</c> and <c>TRUNCATE … RESTART IDENTITY</c>.
    ///
    /// <para><b>This call takes about one block-lease period (five seconds by default) to answer,
    /// by design.</b> Kahuna withholds success until a block reserved from the replaced incarnation
    /// can no longer be served anywhere, and refuses allocations from the new incarnation for the
    /// same interval. That is what makes moving a live counter safe rather than a five-second hole
    /// in the uniqueness guarantee. Every caller is a DDL-shaped statement, so the delay is the
    /// right price — but a client deadline shorter than the lease turns a correct update into a
    /// timeout.</para>
    ///
    /// <para><paramref name="currentValue"/> sets the reserved high-water mark, so the next value
    /// issued is that plus the increment. A caller that wants the next value to be <c>n</c> passes
    /// <c>n - increment</c>.</para>
    /// </summary>
    internal async Task UpdateAsync(
        IKahuna kahuna,
        string dbId,
        SequenceSchema sequence,
        long? currentValue,
        long? increment,
        long? initialValue,
        long? maxValue,
        bool removeMaxValue,
        int? blockSize,
        bool removeBlockSize,
        CancellationToken cancellationToken)
    {
        SequenceUpdate update = new(
            CurrentValue: currentValue,
            Increment: increment,
            InitialValue: initialValue,
            MaxValue: maxValue,
            RemoveMaxValue: removeMaxValue,
            BlockSize: blockSize,
            RemoveBlockSize: removeBlockSize);

        // Nothing to do rather than an error: Kahuna refuses an empty change set, and an ALTER that
        // named only fields CamusDB keeps in its own catalog (MINVALUE) legitimately produces one.
        if (update.IsEmpty)
            return;

        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        string name = MetaKeys.KahunaSequenceName(dbId, sequence.Id!);

        (SequenceResponseType type, _) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndUpdateSequence(name, update, SequenceDurability.Persistent, cancellationToken),
            elapsed,
            options.SequenceRetryBudgetMs
        ).ConfigureAwait(false);

        if (type == SequenceResponseType.NotFound)
        {
            // The counter never existed — a create whose record replicated before the counter was
            // made. Create it at the value the update asked for, which reaches the same end state
            // without a second round trip through the update path.
            SequenceSchema seeded = sequence.Clone();
            seeded.StartValue = (currentValue ?? (sequence.StartValue - sequence.Increment)) + (increment ?? sequence.Increment);
            seeded.Increment = increment ?? sequence.Increment;
            seeded.MaxValue = removeMaxValue ? null : maxValue ?? sequence.MaxValue;
            seeded.CacheSize = removeBlockSize ? null : blockSize ?? sequence.CacheSize;

            await CreateAsync(kahuna, dbId, seeded, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (type != SequenceResponseType.Success)
            throw Failure(type, sequence.Name ?? name, "change the sequence");
    }

    /// <summary>
    /// Reads the counter's <b>reserved high-water mark</b> — an upper bound on the values issued,
    /// not the last value issued. After four <c>nextval</c> calls on a fresh sequence with the
    /// default block size this reads 1000, because a thousand values were reserved to issue four.
    ///
    /// <para>The name says ceiling for that reason. Printing this number in a column called
    /// <c>last_value</c> publishes a value that is wrong by up to a whole block, and a caller that
    /// compares two readings to decide whether anything changed is comparing block boundaries, not
    /// activity. Returns null when the counter does not exist yet, which is the honest answer for a
    /// sequence nothing has drawn from.</para>
    /// </summary>
    internal Task<long?> ReadReservedCeilingAsync(
        IKahuna kahuna, string dbId, string sequenceId, CancellationToken cancellationToken)
        => ReadReservedCeilingAsync(kahuna, dbId, sequenceId, options.SequenceRetryBudgetMs, cancellationToken);

    /// <summary>
    /// Reads where a sequence stands, for a caller that will show it to a person. Null when the
    /// counter does not exist yet.
    /// </summary>
    internal async Task<SequencePosition?> ReadPositionAsync(
        IKahuna kahuna, string dbId, string sequenceId, CancellationToken cancellationToken)
    {
        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        string name = MetaKeys.KahunaSequenceName(dbId, sequenceId);

        (SequenceResponseType type, ReadOnlySequenceEntry? entry) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndGetSequence(name, SequenceDurability.Persistent, cancellationToken),
            elapsed,
            options.SequenceRetryBudgetMs
        ).ConfigureAwait(false);

        if (type == SequenceResponseType.NotFound)
            return null;

        if (type != SequenceResponseType.Success || entry is null)
            throw Failure(type, name, "read the sequence");

        // The record keeps the value the counter was seeded with. While the two are equal nothing
        // has been drawn since the seed was set, so there is no issued value to report.
        return new SequencePosition(entry.CurrentValue, entry.CurrentValue != entry.InitialValue);
    }

    /// <summary>
    /// The same read, for a caller that holds no allocator instance and supplies the budget itself
    /// — the branch fork, which runs from a static copier.
    /// </summary>
    internal static async Task<long?> ReadReservedCeilingAsync(
        IKahuna kahuna, string dbId, string sequenceId, int retryBudgetMs, CancellationToken cancellationToken)
    {
        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        string name = MetaKeys.KahunaSequenceName(dbId, sequenceId);

        (SequenceResponseType type, ReadOnlySequenceEntry? entry) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndGetSequence(name, SequenceDurability.Persistent, cancellationToken),
            elapsed,
            retryBudgetMs
        ).ConfigureAwait(false);

        // Only NotFound means the counter is absent. Every other unsuccessful answer means the
        // read did not happen — MustRetry, in particular, is what an unavailable owner returns, and
        // it comes back with a null entry. Treating that as absence is how a branch fork seeded a
        // counter at its start value and began reissuing numbers its inherited rows already held.
        if (type == SequenceResponseType.NotFound)
            return null;

        if (type != SequenceResponseType.Success)
            throw Failure(type, name, "read the sequence");

        // Success with no entry is not a shape the sequencer produces. Refused rather than read as
        // absence, for the same reason: absence is the one answer that lets a caller start over.
        if (entry is null)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"The sequencer reported success with no record for sequence '{name}'");

        return entry.CurrentValue;
    }

    /// <summary>
    /// Creates a branch's own counter for one inherited sequence, seeded above every value the
    /// source ever issued.
    /// </summary>
    /// <remarks>
    /// <para>The seed is the source's <b>reserved ceiling</b>, and that is correct precisely
    /// because it is a ceiling: it sits above everything the source issued, so the branch cannot
    /// reissue a number its inherited rows already hold. The cost is a gap of up to one block, and
    /// a gap is allowed.</para>
    ///
    /// <para>A source that never drew from the sequence has no counter at all, and the branch may
    /// then start exactly where the source would have — one increment below the recorded start
    /// value.</para>
    ///
    /// <para><c>AlreadyExists</c> is success: a retried fork reaches this with the counter already
    /// made. Static because the branch copier holds no allocator, and here rather than there so
    /// the sequencer's naming and response mapping stay in one class.</para>
    /// </remarks>
    internal static async Task SeedBranchCounterAsync(
        IKahuna kahuna,
        string sourceDbId,
        string branchDbId,
        SequenceSchema sequence,
        int retryBudgetMs,
        CancellationToken cancellationToken)
    {
        long? ceiling = await ReadReservedCeilingAsync(
            kahuna, sourceDbId, sequence.Id!, retryBudgetMs, cancellationToken).ConfigureAwait(false);

        long seed = ceiling ?? sequence.StartValue - sequence.Increment;

        (SequenceResponseType type, _) = await SequenceRetryPolicy.RetryWhileMustRetryAsync(
            () => kahuna.LocateAndCreateSequence(
                MetaKeys.KahunaSequenceName(branchDbId, sequence.Id!),
                initialValue: seed,
                increment: sequence.Increment,
                maxValue: sequence.MaxValue,
                blockSize: sequence.CacheSize,
                SequenceDurability.Persistent,
                cancellationToken),
            ValueStopwatch.StartNew(),
            retryBudgetMs
        ).ConfigureAwait(false);

        if (type is SequenceResponseType.Success or SequenceResponseType.AlreadyExists)
            return;

        throw Failure(type, sequence.Name ?? sequence.Id!, "create the branch's counter for the sequence");
    }

    /// <summary>
    /// Maps a sequencer response to the error the user should see. Each distinct response gets its
    /// own code: a caller who exhausted a <c>MAXVALUE</c> must not read "internal error", and a
    /// partition without a leader must not read "corrupt".
    /// </summary>
    private static CamusDBException Failure(SequenceResponseType type, string sequenceName, string what) => type switch
    {
        SequenceResponseType.NotFound => new CamusDBException(
            CamusDBErrorCodes.SequenceDoesntExist,
            $"Cannot {what} '{sequenceName}': the sequence does not exist"),

        SequenceResponseType.AlreadyExists => new CamusDBException(
            CamusDBErrorCodes.SequenceAlreadyExists,
            $"Cannot {what} '{sequenceName}': the sequence already exists"),

        SequenceResponseType.MaxValueExceeded => new CamusDBException(
            CamusDBErrorCodes.SequenceExhausted,
            $"Sequence '{sequenceName}' has no values left to issue: the next value would pass its maximum. " +
            "Raise MAXVALUE or restart the sequence."),

        SequenceResponseType.InvalidInput => new CamusDBException(
            CamusDBErrorCodes.InvalidSequenceDefinition,
            $"Cannot {what} '{sequenceName}': the sequencer refused the parameters"),

        // An exhausted MustRetry means the partition owning the counter stayed leaderless for the
        // whole retry window. Nothing was allocated and nothing is damaged, so it is transient
        // unavailability the caller can simply re-issue.
        SequenceResponseType.MustRetry => new CamusDBException(
            CamusDBErrorCodes.SequenceUnavailable,
            $"Cannot {what} '{sequenceName}': no confirmed leader for the sequence's partition within the retry budget"),

        SequenceResponseType.Aborted => new CamusDBException(
            CamusDBErrorCodes.SequenceUnavailable,
            $"Cannot {what} '{sequenceName}': the sequencer aborted the operation"),

        _ => new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Cannot {what} '{sequenceName}': the sequencer answered {type}"),
    };
}
