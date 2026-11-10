/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// The inline SQL functions of one database: the replicated records (<see cref="Functions"/>) and,
/// per engine, the bindings that run them (<see cref="Resolve"/>). One instance belongs to one
/// <c>Schema</c> and is never replaced, so a statement's function scope can hold it without pinning the
/// rest of the schema. It mirrors <see cref="InstalledExtensionCatalog"/>.
///
/// <para><b>Published snapshots.</b> The schema apply replaces the whole map (<see cref="Publish"/>)
/// under the schema lock; a statement reads it with no lock and always sees one complete map. A record
/// is never changed after it is published: a replace publishes a new record under the same name.</para>
///
/// <para><b>Stamps.</b> <see cref="Stamp"/> is 0 while the database has no function and otherwise the
/// schema version of the last publish. <see cref="LastPublished"/> keeps that version even when the map
/// became empty. The caches that hold work derived from function calls compare one value that the
/// schema forms from the stamps of this catalog and of the extension catalog
/// (<c>Schema.CallableStamp</c>), so they never serve a result that an older function body computed.</para>
///
/// <para><b>Bindings follow the snapshot.</b> A binding holds the compiled code of one record on one
/// engine. When a publish replaces or removes a record, its binding is retired at once: it gives back
/// its reference on the compiled unit, and the engine can unload the code. A binding whose record did
/// not change moves to the next snapshot with its code, so a <c>CREATE FUNCTION</c> does not make the
/// other functions compile again. Both happen under <see cref="gate"/>, so a reader that resolves
/// while a publish runs cannot leave a set behind that nobody retires.</para>
///
/// <para><b>Removed names.</b> The catalog remembers which names it held and no longer holds
/// (<see cref="WasRemoved"/>), for this process. The proposer-side reference check uses it to tell "a
/// function that was dropped while the statement ran" from a name that never was a function.</para>
/// </summary>
internal sealed class InlineFunctionCatalog : IDisposable
{
    private static readonly IReadOnlyDictionary<string, InlineFunctionDefinition> None =
        new Dictionary<string, InlineFunctionDefinition>(StringComparer.OrdinalIgnoreCase);

    /// <summary>The most removed names the catalog remembers; older ones are forgotten together.</summary>
    private const int MaxRemovedNames = 4096;

    private readonly object gate = new();

    private volatile IReadOnlyDictionary<string, InlineFunctionDefinition> functions = None;

    private volatile HashSet<string> removed = new(StringComparer.OrdinalIgnoreCase);

    private volatile ResolvedSet? resolved;

    private long stamp;

    private long lastPublished;

    /// <summary>The function records keyed by name (case-insensitive). An immutable snapshot.</summary>
    public IReadOnlyDictionary<string, InlineFunctionDefinition> Functions => functions;

    /// <summary>0 while the database has no inline function, and otherwise the schema version of the last publish.</summary>
    public long Stamp => Interlocked.Read(ref stamp);

    /// <summary>The schema version of the last publish, also when that publish left no function.</summary>
    public long LastPublished => Interlocked.Read(ref lastPublished);

    /// <summary>
    /// Replaces the function records. Called by the schema apply (under the schema lock, with the
    /// delta's target version) and by the schema load. The map must not be changed after it is
    /// published.
    /// </summary>
    public void Publish(IReadOnlyDictionary<string, InlineFunctionDefinition> next, long version)
    {
        lock (gate)
        {
            IReadOnlyDictionary<string, InlineFunctionDefinition> previous = functions;

            RememberRemoved(previous, next);

            functions = next;
            Interlocked.Exchange(ref stamp, next.Count == 0 ? 0 : version);
            Interlocked.Exchange(ref lastPublished, version);

            if (next.Count > 0)
                ExtensionRegistry.MarkLoaded();

            // Retire the bindings of the records that changed now, not at the next statement: a
            // dropped function must give its code back even when nothing calls a function again.
            if (resolved is { } current)
                resolved = new ResolvedSet(next, current.Runtime, current.DatabaseName, InlineFunctionSet.Build(next, current.Runtime, current.DatabaseName, current.Set));
        }
    }

    /// <summary>
    /// The functions of the current snapshot bound for <paramref name="runtime"/>. Built once per
    /// snapshot; later statements compare two references. Nothing is compiled here: a function is
    /// compiled at its first call.
    /// </summary>
    public InlineFunctionSet Resolve(InlineFunctionRuntime runtime, string? databaseName)
    {
        IReadOnlyDictionary<string, InlineFunctionDefinition> snapshot = functions;

        if (snapshot.Count == 0 && resolved is null)
            return InlineFunctionSet.Empty;

        ResolvedSet? cached = resolved;

        if (cached is not null && ReferenceEquals(cached.Snapshot, snapshot) && ReferenceEquals(cached.Runtime, runtime))
            return cached.Set;

        lock (gate)
        {
            snapshot = functions;
            cached = resolved;

            if (cached is not null && ReferenceEquals(cached.Snapshot, snapshot) && ReferenceEquals(cached.Runtime, runtime))
                return cached.Set;

            // Another engine's set over the same catalog cannot happen in the server; a test that
            // shares a schema between engines gets the old set retired, which is correct and slow.
            InlineFunctionSet? previous = cached is not null && ReferenceEquals(cached.Runtime, runtime) ? cached.Set : null;

            if (cached is not null && previous is null)
                cached.Set.RetireAll();

            InlineFunctionSet set = InlineFunctionSet.Build(snapshot, runtime, databaseName, previous);
            resolved = new ResolvedSet(snapshot, runtime, databaseName, set);
            return set;
        }
    }

    /// <summary>
    /// True when this catalog held a function named <paramref name="name"/> and holds none now: the
    /// function was dropped, or the schema was reloaded without it, since this process opened the
    /// database.
    /// </summary>
    public bool WasRemoved(string name) => removed.Contains(name) && !functions.ContainsKey(name);

    /// <summary>True when this catalog saw at least one function removed. Lets a check that needs <see cref="WasRemoved"/> skip its work.</summary>
    public bool HasRemovedNames => removed.Count > 0;


    /// <summary>
    /// Makes this catalog a copy of <paramref name="source"/> for a dry run: the same records, the same
    /// stamps and the same removed names, and no binding. The schema validation applies a delta to a
    /// copy of the schema, and the copy must answer <see cref="WasRemoved"/> as the live schema does.
    /// </summary>
    public void CopyFrom(InlineFunctionCatalog source)
    {
        lock (gate)
        {
            functions = source.functions;
            removed = source.removed;
            Interlocked.Exchange(ref stamp, source.Stamp);
            Interlocked.Exchange(ref lastPublished, source.LastPublished);
        }
    }

    private void RememberRemoved(IReadOnlyDictionary<string, InlineFunctionDefinition> previous, IReadOnlyDictionary<string, InlineFunctionDefinition> next)
    {
        HashSet<string>? updated = null;

        foreach (string name in previous.Keys)
        {
            if (!next.ContainsKey(name) && !removed.Contains(name))
                (updated ??= new HashSet<string>(removed.Count >= MaxRemovedNames ? [] : removed, StringComparer.OrdinalIgnoreCase)).Add(name);
        }

        // The set is replaced, never changed in place: a reader holds the one it read.
        if (updated is not null)
            removed = updated;
    }

    /// <summary>Retires every binding, so the engine can unload their code. Called when the database closes.</summary>
    public void Dispose()
    {
        lock (gate)
        {
            resolved?.Set.RetireAll();
            resolved = null;
        }
    }

    private sealed record ResolvedSet(
        IReadOnlyDictionary<string, InlineFunctionDefinition> Snapshot, InlineFunctionRuntime Runtime, string? DatabaseName, InlineFunctionSet Set);
}

/// <summary>
/// The inline functions a statement can call on this engine: one <see cref="InlineFunctionBinding"/>
/// for each function record of a snapshot, by name. Immutable; a publish builds the next one.
/// </summary>
internal sealed class InlineFunctionSet
{
    public static readonly InlineFunctionSet Empty = new();

    private readonly Dictionary<string, InlineFunctionBinding> bindings = new(StringComparer.OrdinalIgnoreCase);

    private InlineFunctionSet() { }

    /// <summary>
    /// Builds the set of <paramref name="snapshot"/>. A binding of <paramref name="previous"/> whose
    /// record is the same object is taken over with its compiled code; every other binding of
    /// <paramref name="previous"/> is retired.
    /// </summary>
    internal static InlineFunctionSet Build(
        IReadOnlyDictionary<string, InlineFunctionDefinition> snapshot, InlineFunctionRuntime runtime, string? databaseName, InlineFunctionSet? previous)
    {
        InlineFunctionSet set = snapshot.Count == 0 ? Empty : new InlineFunctionSet();

        foreach (InlineFunctionDefinition record in snapshot.Values)
        {
            if (previous is not null && previous.bindings.TryGetValue(record.Name, out InlineFunctionBinding? kept) && ReferenceEquals(kept.Record, record))
                set.bindings[record.Name] = kept;
            else
                set.bindings[record.Name] = new InlineFunctionBinding(record, runtime, (databaseName ?? "") + "." + record.Name);
        }

        if (previous is not null)
        {
            foreach (InlineFunctionBinding old in previous.bindings.Values)
            {
                if (!set.bindings.TryGetValue(old.Record.Name, out InlineFunctionBinding? current) || !ReferenceEquals(current, old))
                    old.Retire();
            }
        }

        return set;
    }

    /// <summary>Resolves a function by name. Never compiles: the descriptor compiles at its first call.</summary>
    public bool TryGetFunction(string name, out ScalarFunctionDescriptor descriptor)
    {
        if (bindings.TryGetValue(name, out InlineFunctionBinding? binding))
        {
            descriptor = binding.Descriptor;
            return true;
        }

        descriptor = null!;
        return false;
    }

    /// <summary>True when the database has an inline function of this name.</summary>
    public bool HasFunction(string name) => bindings.ContainsKey(name);

    /// <summary>The binding of a function, for <c>SHOW FUNCTIONS</c>, or null.</summary>
    public InlineFunctionBinding? Find(string name) => bindings.GetValueOrDefault(name);

    internal void RetireAll()
    {
        foreach (InlineFunctionBinding binding in bindings.Values)
            binding.Retire();
    }
}

/// <summary>
/// One inline function record on one engine: the descriptor that the expression evaluator calls, and
/// the compiled code behind it.
///
/// <para><b>Lazy.</b> The descriptor is built from the record alone: the name, the number of
/// parameters and the declared result type. Resolving a name, checking a statement and planning a
/// query therefore compile nothing. The first <b>call</b> compiles the body, or takes the unit that
/// another function with the same source already compiled. A node that never calls a function never
/// compiles it, and the schema apply, which runs inside the commit pipeline, never compiles.</para>
///
/// <para><b>Deterministic.</b> <see cref="ScalarFunctionDescriptor.IsVolatile"/> is false for every
/// inline function: the allowed list admits only members whose result depends on their inputs.</para>
///
/// <para><b>Retired.</b> After the record was replaced or dropped, the binding has given back its
/// reference on the compiled unit. A statement that resolved the function before that can still call
/// it: the adapter it holds keeps the code alive until the statement ends.</para>
/// </summary>
internal sealed class InlineFunctionBinding
{
    private readonly InlineFunctionRuntime runtime;

    private readonly string label;

    private readonly object gate = new();

    private volatile ScalarFunctionEvaluatorDelegate? evaluator;

    private InlineFunctionUnit? unit;

    private bool retired;

    internal InlineFunctionBinding(InlineFunctionDefinition record, InlineFunctionRuntime runtime, string label)
    {
        this.runtime = runtime;
        this.label = label;
        Record = record;

        ColumnType returnType = record.ReturnType;

        Descriptor = new ScalarFunctionDescriptor
        {
            Name = record.Name,
            MinArity = record.Parameters.Count,
            MaxArity = record.Parameters.Count,
            Evaluator = Invoke,
            InferReturnType = _ => returnType,
            ArrayElementType = record.ReturnElementType,
            IsVolatile = false,
        };
    }

    /// <summary>The record this binding runs.</summary>
    public InlineFunctionDefinition Record { get; }

    /// <summary>The descriptor the expression evaluator resolves the function's name to.</summary>
    public ScalarFunctionDescriptor Descriptor { get; }

    /// <summary>
    /// What this node has done with the function, for <c>SHOW FUNCTIONS</c>: <c>not compiled</c> before
    /// the first call, <c>compiled</c>, or <c>failed</c> with the reason.
    /// </summary>
    public string CompileState
    {
        get
        {
            lock (gate)
            {
                if (unit is null)
                    return "not compiled";

                return unit.Evaluator is not null ? "compiled" : "failed: " + (unit.Failure?.Message ?? "unknown");
            }
        }
    }

    private ColumnValue Invoke(string calledName, IReadOnlyList<ColumnValue> arguments) =>
        (evaluator ?? Bind())(calledName, arguments);

    private ScalarFunctionEvaluatorDelegate Bind()
    {
        lock (gate)
        {
            if (evaluator is { } bound)
                return bound;

            if (unit is null)
            {
                unit = runtime.Acquire(Record, label);

                // A call that arrives after the record was replaced: run it, and keep no reference.
                if (retired)
                    runtime.Release(unit, label);
            }
            else
                runtime.EnsureBuilt(unit, Record, label);

            if (unit.Evaluator is not { } compiled)
            {
                CamusDBException failure = unit.Failure
                    ?? new CamusDBException(CamusDBErrorCodes.InlineFunctionUnavailableOnNode, $"Function '{Record.Name}' has no compiled code on this node");

                // A new exception for each call: the kept one is shared between statements.
                throw new CamusDBException(failure.Code, failure.Message);
            }

            return evaluator = runtime.Guard(compiled);
        }
    }

    /// <summary>Gives back the compiled code. Idempotent.</summary>
    internal void Retire()
    {
        lock (gate)
        {
            if (retired)
                return;

            retired = true;

            if (unit is not null)
                runtime.Release(unit, label);
        }
    }
}
