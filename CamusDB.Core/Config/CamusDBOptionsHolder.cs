/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// Atomically-swapped holder of the engine's immutable <see cref="CamusDBOptions"/> record — the
/// indirection that makes runtime configuration change possible without giving up the record's
/// immutability.
///
/// <para>The record itself stays <c>init</c>-only and is never mutated: a change builds a
/// <em>new</em> record and hands it to <see cref="Publish"/>. Readers pin one snapshot at a
/// well-defined boundary — a statement, a transaction begin, one iteration of a background loop —
/// by reading <see cref="Current"/> (or their component's swapped field) once at the top of the
/// operation and passing that snapshot down, exactly as options are passed today. A component that
/// re-reads mid-operation reintroduces the torn-read hazard the immutable record exists to prevent
/// (a planner costing with one threshold while the executor enforces another), which is why the
/// holder is handed only to boundary owners; everything below a boundary keeps receiving the plain
/// record.</para>
///
/// <para><see cref="Publish"/> is serialized behind a lock so two concurrent applies cannot
/// interleave their read-modify-write of whatever produced the new record; in cluster mode Raft
/// commit order decides which change is second, and this lock only ensures the local swap and its
/// subscriber notifications happen in that order. Each publish also refreshes the process-wide
/// ambient value (<see cref="CamusDBConfig.SetAmbient"/>) so the static regex helpers — the one
/// remaining ambient consumer — follow along without growing the ambient surface.</para>
/// </summary>
public sealed class CamusDBOptionsHolder
{
    private CamusDBOptions current;

    private readonly object publishSync = new();

    private readonly List<Action<CamusDBOptions>> subscribers = [];

    public CamusDBOptionsHolder(CamusDBOptions initial)
    {
        ArgumentNullException.ThrowIfNull(initial);
        current = initial;
    }

    /// <summary>
    /// The latest published snapshot. Read it once per boundary and pass the result down; do not
    /// re-read it in the middle of an operation.
    /// </summary>
    public CamusDBOptions Current => Volatile.Read(ref current);

    /// <summary>
    /// Installs <paramref name="next"/> as the current snapshot, keeps the ambient value in sync,
    /// and notifies subscribers in registration order. Serialized: concurrent publishers queue
    /// behind the lock rather than racing, so subscribers always observe swaps in publish order.
    /// Subscriber callbacks run inline under the lock — keep them cheap (swap a field, re-arm a
    /// timer); anything expensive belongs on the subscriber's own schedule.
    /// </summary>
    public void Publish(CamusDBOptions next)
    {
        ArgumentNullException.ThrowIfNull(next);

        lock (publishSync)
        {
            Volatile.Write(ref current, next);
            CamusDBConfig.SetAmbient(next);

            foreach (Action<CamusDBOptions> subscriber in subscribers)
                subscriber(next);
        }
    }

    /// <summary>
    /// Registers a callback invoked on every subsequent <see cref="Publish"/>. Dispose the handle
    /// to unsubscribe — components shorter-lived than the holder (an executor in a test, a
    /// disposed engine) must unhook or the holder keeps them alive.
    /// </summary>
    public IDisposable Subscribe(Action<CamusDBOptions> onSwap)
    {
        ArgumentNullException.ThrowIfNull(onSwap);

        lock (publishSync)
            subscribers.Add(onSwap);

        return new Subscription(this, onSwap);
    }

    private void Unsubscribe(Action<CamusDBOptions> onSwap)
    {
        lock (publishSync)
            subscribers.Remove(onSwap);
    }

    private sealed class Subscription(CamusDBOptionsHolder owner, Action<CamusDBOptions> onSwap) : IDisposable
    {
        private bool disposed;

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            owner.Unsubscribe(onSwap);
        }
    }
}
