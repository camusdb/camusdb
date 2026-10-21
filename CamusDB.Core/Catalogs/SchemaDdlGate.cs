/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// The DDL gate of one database on one node: the semaphore that serializes schema proposals, and the
/// tables that a staged schema change holds while its long phase runs outside that semaphore.
///
/// <para><b>Two things are protected, at two granularities.</b> The <see cref="Semaphore"/> makes
/// "resolve the target, then propose the delta" atomic against every other DDL of the database, and
/// keeps two proposals from being built against the same schema version. Every proposal is short:
/// one Raft round trip and its acks. The staged changes are not. An index build or a column fill
/// visits every row of its table, and a validation pass reads every row; at the rate a loaded table
/// allows, that is minutes. Holding the semaphore for that long made every DDL of the database,
/// a <c>CREATE TABLE</c> of an unrelated table included, wait for the slowest backfill in flight.</para>
///
/// <para>So a staged change holds its <em>table</em> instead (<see cref="HoldTable"/>), and takes the
/// semaphore only around each delta it proposes. The hold is what a DDL that names that table waits
/// for: <see cref="EnterAsync(string, ILogger?)"/> takes the semaphore and, when one of the named
/// tables is held, releases it again and waits for the hold to end before it tries once more. A DDL
/// on another table enters at once. The hold is taken and looked up under the semaphore, so a DDL
/// that saw no hold and a change that is about to take one cannot both proceed.</para>
///
/// <para><b>Why same-table DDL still waits.</b> A running job names its table and its element by name
/// and assumes both exist until it is done. A <c>RENAME TABLE</c> would make its next step miss the
/// table and its compensation delete the job while the element stays half-built; a <c>DROP TABLE</c>
/// or <c>TRUNCATE</c> would purge rows under a batch that is writing their entries; a <c>DROP INDEX</c>
/// of the index being built would leave the entries the last batches write. A second staged change on
/// the same table would run two backfills over the same rows at once. None of that is worth
/// allowing; a DDL on the same table waits exactly as it did before, and a DDL on another table does
/// not.</para>
///
/// <para>The gate is node-local, like the semaphore it wraps. Only the schema leader runs staged
/// changes and proposes deltas, so one node's gate is the one that matters; a follower forwards its
/// DDL and holds nothing while it waits for the leader's answer.</para>
/// </summary>
public sealed class SchemaDdlGate : IDisposable
{
    private readonly string databaseName;

    private readonly object sync = new();

    /// <summary>Held tables by name. Table names are case-insensitive, as in the schema.</summary>
    private readonly Dictionary<string, StagedTableHold> heldByName = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Serializes the schema proposals of the database on this node, and every "resolve, then mutate"
    /// sequence of a DDL statement. Exposed for the callers that hold it directly around a short
    /// metadata change; a DDL that names a table enters through <see cref="EnterAsync(string, ILogger?)"/>.
    /// </summary>
    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public SchemaDdlGate(string databaseName)
    {
        this.databaseName = databaseName;
    }

    /// <summary>
    /// Takes the <see cref="Semaphore"/> for a DDL that touches <paramref name="tableName"/>, after
    /// every staged change in progress on that table has finished. Released with <see cref="Exit"/>.
    /// </summary>
    public Task EnterAsync(string tableName, ILogger? logger = null) => EnterAsync([tableName], logger);

    /// <summary>
    /// Takes the <see cref="Semaphore"/> for a DDL that touches every table in
    /// <paramref name="tableNames"/>, after every staged change in progress on any of them has
    /// finished. A table that does not exist, or a name that is not yet taken, cannot be held, so a
    /// <c>CREATE TABLE</c> or a <c>RENAME TABLE</c> may name its new table here too. Released with
    /// <see cref="Exit"/>.
    /// </summary>
    public async Task EnterAsync(IReadOnlyList<string> tableNames, ILogger? logger = null)
    {
        while (true)
        {
            await Semaphore.WaitAsync().ConfigureAwait(false);

            StagedTableHold? hold = FindHold(tableNames);
            if (hold is null)
                return;

            // Released while waiting: the staged change takes the semaphore for each delta it
            // proposes, and a waiter that kept it would stop the change it is waiting for.
            Semaphore.Release();

            if (logger is not null)
                Log.LogDdlWaitsForStagedChange(logger, hold.TableName, hold.Element, databaseName);

            await hold.Completion.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Takes the <see cref="Semaphore"/> after every staged change of the database has finished, for an
    /// operation on the database as a whole, such as dropping it. Released with <see cref="Exit"/>.
    /// </summary>
    public async Task EnterExclusiveAsync(ILogger? logger = null)
    {
        while (true)
        {
            await Semaphore.WaitAsync().ConfigureAwait(false);

            StagedTableHold? hold = AnyHold();
            if (hold is null)
                return;

            Semaphore.Release();

            if (logger is not null)
                Log.LogDdlWaitsForStagedChange(logger, hold.TableName, hold.Element, databaseName);

            await hold.Completion.ConfigureAwait(false);
        }
    }

    /// <summary>Releases the <see cref="Semaphore"/> taken by one of the <c>Enter</c> methods.</summary>
    public void Exit() => Semaphore.Release();

    /// <summary>
    /// Marks <paramref name="tableName"/> as held by a staged change to <paramref name="element"/>,
    /// so that a DDL naming the table waits until the returned hold is disposed. <b>The caller must
    /// hold the <see cref="Semaphore"/></b>: that is what makes the hold visible to every DDL that
    /// enters after it, with no window in which one could resolve the table and then act while the
    /// change runs. A table already held by another change is refused; the gate exists so that two
    /// changes to one table never run at once, and a caller reaches this point only after it entered
    /// for that table.
    /// </summary>
    public StagedTableHold HoldTable(string tableId, string tableName, string element)
    {
        if (Semaphore.CurrentCount != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"A staged change to {element} of table '{tableName}' tried to hold the table without the DDL gate of database '{databaseName}'");

        lock (sync)
        {
            if (heldByName.TryGetValue(tableName, out StagedTableHold? existing))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Table '{tableName}' of database '{databaseName}' is already held by a staged change to {existing.Element}; a change to {element} cannot start on it");

            StagedTableHold hold = new(this, tableId, tableName, element);
            heldByName[tableName] = hold;
            return hold;
        }
    }

    /// <summary>True when a staged change holds <paramref name="tableName"/> right now.</summary>
    public bool IsTableHeld(string tableName)
    {
        lock (sync)
            return heldByName.ContainsKey(tableName);
    }

    /// <summary>The number of tables held by a staged change right now.</summary>
    public int HeldTableCount
    {
        get
        {
            lock (sync)
                return heldByName.Count;
        }
    }

    private StagedTableHold? FindHold(IReadOnlyList<string> tableNames)
    {
        lock (sync)
        {
            if (heldByName.Count == 0)
                return null;

            foreach (string tableName in tableNames)
            {
                if (heldByName.TryGetValue(tableName, out StagedTableHold? hold))
                    return hold;
            }

            return null;
        }
    }

    private StagedTableHold? AnyHold()
    {
        lock (sync)
        {
            foreach (StagedTableHold hold in heldByName.Values)
                return hold;

            return null;
        }
    }

    private void Release(StagedTableHold hold)
    {
        lock (sync)
        {
            if (heldByName.TryGetValue(hold.TableName, out StagedTableHold? current) && ReferenceEquals(current, hold))
                heldByName.Remove(hold.TableName);
        }

        hold.Complete();
    }

    /// <summary>
    /// Disposes the semaphore and ends every hold, so that a DDL waiting for a staged change of a
    /// database that is closing is woken rather than left waiting forever.
    /// </summary>
    public void Dispose()
    {
        StagedTableHold[] holds;
        lock (sync)
        {
            holds = [.. heldByName.Values];
            heldByName.Clear();
        }

        foreach (StagedTableHold hold in holds)
            hold.Complete();

        Semaphore.Dispose();
    }

    /// <summary>
    /// A table held by a staged schema change. Disposing it ends the hold and wakes every DDL waiting
    /// for the table; disposing it twice is harmless.
    /// </summary>
    public sealed class StagedTableHold : IDisposable
    {
        private readonly SchemaDdlGate gate;

        private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string TableId { get; }

        public string TableName { get; }

        /// <summary>The element under change, for log lines: for example <c>index 'acct_balance'</c>.</summary>
        public string Element { get; }

        /// <summary>Completes when the hold ends.</summary>
        public Task Completion => completion.Task;

        internal StagedTableHold(SchemaDdlGate gate, string tableId, string tableName, string element)
        {
            this.gate = gate;
            TableId = tableId;
            TableName = tableName;
            Element = element;
        }

        internal void Complete() => completion.TrySetResult();

        public void Dispose() => gate.Release(this);
    }
}

/// <summary>
/// The DDL gate as one staged change sees it: held or released, with idempotent transitions, so the
/// coordinator can release the gate for its long phase and take it again for each delta without
/// tracking who took it last. A caller that already held the gate when it handed the job over gets
/// it back held; one that did not gets it back released. See <see cref="SchemaChangeCoordinator"/>.
/// </summary>
public sealed class SchemaDdlGateHold
{
    private readonly SchemaDdlGate gate;

    /// <summary>Whether the caller held the gate when the job started, and must find it held after.</summary>
    public bool HeldByCaller { get; }

    /// <summary>Whether this flow holds the gate right now.</summary>
    public bool IsHeld { get; private set; }

    public SchemaDdlGateHold(SchemaDdlGate gate, bool heldByCaller)
    {
        this.gate = gate;
        HeldByCaller = heldByCaller;
        IsHeld = heldByCaller;
    }

    /// <summary>Takes the gate unless this flow already holds it.</summary>
    public async Task AcquireAsync()
    {
        if (IsHeld)
            return;

        await gate.Semaphore.WaitAsync().ConfigureAwait(false);
        IsHeld = true;
    }

    /// <summary>Releases the gate if this flow holds it.</summary>
    public void Release()
    {
        if (!IsHeld)
            return;

        IsHeld = false;
        gate.Semaphore.Release();
    }

    /// <summary>
    /// Leaves the gate as the caller expects to find it: held when the caller held it on entry, and
    /// released otherwise. Called last, after the job's own work and compensation.
    /// </summary>
    public async Task RestoreAsync()
    {
        if (HeldByCaller)
            await AcquireAsync().ConfigureAwait(false);
        else
            Release();
    }
}
