/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Extensions;
using CamusDB.Extensions.Inline;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// A compile that failed for a reason that can pass: no compile slot was free in time, or the compile
/// ran over its time limit while the node was busy. A node remembers this failure for a short time
/// only (<see cref="InlineFunctionRuntime.BusyRetryMs"/>) and then compiles again.
/// </summary>
internal sealed class InlineFunctionCompileBusyException(string message)
    : CamusDBException(CamusDBErrorCodes.InlineFunctionCompileFailed, message);

/// <summary>
/// The compiled code of one inline function source on this engine, shared by every function record
/// that compiles to it (see <c>InlineFunctionCompiler.UnitKey</c>) and counted by them.
/// </summary>
internal sealed class InlineFunctionUnit(string key)
{
    /// <summary>The identity of the source this unit was compiled from.</summary>
    public string Key { get; } = key;

    /// <summary>The adapter over the compiled method, without the budgets. Null until the compile ended well.</summary>
    public ScalarFunctionEvaluatorDelegate? Evaluator { get; set; }

    /// <summary>Why this node cannot run the source. Kept, so the node compiles a bad source once.</summary>
    public CamusDBException? Failure { get; set; }

    /// <summary>
    /// For a failure that can pass (<see cref="InlineFunctionCompileBusyException"/>): the tick count
    /// from which the next call compiles again. Zero for a failure that the source itself causes.
    /// </summary>
    public long RetryAt { get; set; }

    /// <summary>How many bindings hold this unit. Guarded by the runtime's unit table.</summary>
    public int References { get; set; }

    /// <summary>The load context of the code; an <see cref="IDisposable"/>-free handle the runtime unloads.</summary>
    public object? Context { get; set; }
}

/// <summary>
/// What one engine needs to run inline SQL functions: the settings it was built with, the compiler,
/// and the compiled units. One instance per <c>CommandExecutor</c>, created in its constructor, so
/// two engines in one process (tests, the in-process cluster) share nothing.
///
/// <para><b>Settings are latched.</b> The budgets of a call, the compile limits and the on/off switch
/// are read once, here. Every one is restart-class; a compiled function's adapter keeps the budgets it
/// was built with.</para>
///
/// <para><b>Units.</b> A unit is compiled at the first call of a function that needs it, under the
/// unit's own lock, so two statements that call a new function at the same time compile it once. A
/// unit lives while a binding of a published function record holds it. When the last one lets go
/// (<c>DROP FUNCTION</c>, <c>CREATE OR REPLACE</c>, the database closes) the runtime removes the unit
/// and asks its load context to unload. The runtime then only watches the context through a weak
/// reference: the code leaves the process when the last statement that still runs it ends and the
/// garbage collector runs, and <see cref="Unloading"/> reports it until then.</para>
///
/// <para><b>The guard around a call</b> (<see cref="Guard"/>) starts the budgets, makes the thread's
/// culture invariant, and turns a budget exception into
/// <see cref="CamusDBErrorCodes.InlineFunctionBudgetExceeded"/>. The culture is set only when the
/// thread is not invariant already; the server host always is.</para>
/// </summary>
internal sealed class InlineFunctionRuntime : IDisposable
{
    /// <summary>How long a node waits before it compiles again after a compile that ran out of time.</summary>
    internal const int BusyRetryMs = 30_000;

    private readonly ILogger? logger;

    private readonly Dictionary<string, InlineFunctionUnit> units = new(StringComparer.Ordinal);

    private readonly List<(string Label, WeakReference Context)> unloading = [];

    private readonly int callTimeoutMs;

    private readonly long allocationLimitBytes;

    private readonly int arrayLimit;

#if !CAMUSDB_BROWSER
    private readonly InlineFunctionCompiler compiler;
#endif

    private volatile bool disposed;

    public InlineFunctionRuntime(CamusDBOptions options, ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.logger = logger;

        Enabled = options.InlineFunctionsEnabled;
        MaxFunctionsPerDatabase = options.MaxInlineFunctionsPerDatabase;
        callTimeoutMs = options.InlineFunctionCallTimeoutMs;
        allocationLimitBytes = options.InlineFunctionAllocationLimitBytes;
        arrayLimit = options.InlineFunctionArrayLimit;

#if !CAMUSDB_BROWSER
        compiler = new InlineFunctionCompiler(
            options.InlineFunctionSourceLimitBytes,
            options.InlineFunctionCompileTimeoutMs,
            options.InlineFunctionCompileConcurrency,
            options.InlineFunctionArrayLimit,
            logger);

        if (Enabled)
            _ = Task.Run(Warm);
#endif
    }

    /// <summary>True when this engine accepts <c>CREATE FUNCTION</c>. Functions that exist run either way.</summary>
    public bool Enabled { get; }

    /// <summary>The most inline functions one database may hold; zero or less for no limit.</summary>
    public int MaxFunctionsPerDatabase { get; }

    /// <summary>The C# language version a new function record is stamped with.</summary>
    public static string LanguageVersion =>
#if CAMUSDB_BROWSER
        "";
#else
        InlineFunctionCompiler.LanguageName;
#endif

    /// <summary>How many compiles this engine ran. For tests and diagnostics.</summary>
    public long Compilations =>
#if CAMUSDB_BROWSER
        0;
#else
        compiler.Compilations;
#endif

    /// <summary>The most compiles that ran at the same time on this engine.</summary>
    public int PeakConcurrentCompilations =>
#if CAMUSDB_BROWSER
        0;
#else
        compiler.PeakConcurrentCompilations;
#endif

    /// <summary>The lowercase hex SHA-256 of a body, as a function record stores it.</summary>
    public static string SourceHash(string source) =>
        Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(source)));

    /// <summary>
    /// Compiles <paramref name="function"/> all the way to bound code and throws what a refusal or a
    /// compiler error throws. <c>CREATE FUNCTION</c> calls it on the proposing node before it takes the
    /// schema lock, so a body that does not compile never reaches the schema log. The code is unloaded
    /// again: every node, this one too, compiles a function at its first call.
    /// </summary>
    public void Validate(InlineFunctionDefinition function, CancellationToken cancellation)
    {
#if CAMUSDB_BROWSER
        throw NoCompiler();
#else
        CompiledInlineFunction compiled = compiler.Compile(function, cancellation);

        try
        {
            Bind(compiled);
        }
        finally
        {
            compiled.Context.Unload();
        }
#endif
    }

    /// <summary>
    /// Takes a reference on the unit of <paramref name="function"/>, and compiles it when this is the
    /// first one. A compile error does not throw here: the unit carries it
    /// (<see cref="InlineFunctionUnit.Failure"/>), so the caller reports it for each call and the node
    /// compiles the bad source once. The caller gives the reference back with <see cref="Release"/>.
    /// </summary>
    internal InlineFunctionUnit Acquire(InlineFunctionDefinition function, string label)
    {
#if CAMUSDB_BROWSER
        return new InlineFunctionUnit("") { Failure = NoCompiler() };
#else
        string key = InlineFunctionCompiler.UnitKey(function);
        InlineFunctionUnit unit;

        lock (units)
        {
            if (!units.TryGetValue(key, out unit!))
                units[key] = unit = new InlineFunctionUnit(key);

            unit.References++;
        }

        EnsureBuilt(unit, function, label);
        return unit;
#endif
    }

    /// <summary>
    /// Compiles <paramref name="unit"/> when it has no code yet: at its first use, and again after a
    /// failure that can pass once <see cref="BusyRetryMs"/> went by. Under the unit's lock, so
    /// statements that call a new function at the same time compile it once.
    /// </summary>
    internal void EnsureBuilt(InlineFunctionUnit unit, InlineFunctionDefinition function, string label)
    {
#if !CAMUSDB_BROWSER
        if (unit.Evaluator is not null)
            return;

        lock (unit)
        {
            if (unit.Evaluator is not null)
                return;

            if (unit.Failure is null || (unit.RetryAt != 0 && Environment.TickCount64 >= unit.RetryAt))
                Build(unit, function, label);
        }
#endif
    }

    /// <summary>
    /// Gives back a reference from <see cref="Acquire"/>. The last one removes the unit and starts the
    /// unload of its code.
    /// </summary>
    internal void Release(InlineFunctionUnit unit, string label)
    {
#if !CAMUSDB_BROWSER
        InlineFunctionLoadContext? context;

        lock (units)
        {
            if (--unit.References > 0)
                return;

            if (units.TryGetValue(unit.Key, out InlineFunctionUnit? current) && ReferenceEquals(current, unit))
                units.Remove(unit.Key);

            context = unit.Context as InlineFunctionLoadContext;
            unit.Context = null;

            if (context is not null)
                unloading.Add((label, new WeakReference(context)));
        }

        // Asks, does not force: the code stays until the last delegate into it is gone.
        context?.Unload();
#endif
    }

    /// <summary>
    /// The labels (<c>database.function</c>) of the units this engine asked to unload whose code is
    /// still in memory. A unit leaves the list when the garbage collector has taken its load context.
    /// </summary>
    public IReadOnlyList<string> Unloading()
    {
        lock (units)
        {
            unloading.RemoveAll(entry => !entry.Context.IsAlive);

            if (unloading.Count == 0)
                return [];

            string[] labels = new string[unloading.Count];

            for (int i = 0; i < labels.Length; i++)
                labels[i] = unloading[i].Label;

            return labels;
        }
    }

    /// <summary>
    /// Wraps the adapter of a compiled function with what every call needs: the budgets, an invariant
    /// culture, and the error for an exhausted budget. The limits are the ones this engine was built
    /// with.
    /// </summary>
    internal ScalarFunctionEvaluatorDelegate Guard(ScalarFunctionEvaluatorDelegate inner)
    {
        int timeout = callTimeoutMs;
        long allocation = allocationLimitBytes;
        int array = arrayLimit;

        return (calledName, arguments) =>
        {
            CultureInfo culture = CultureInfo.CurrentCulture;
            CultureInfo uiCulture = CultureInfo.CurrentUICulture;

            // The server host is invariant, so this write happens in other hosts only. A body that
            // formats a number or reads an exception message then gives the same text everywhere.
            bool pinned = culture.Name.Length != 0 || uiCulture.Name.Length != 0;

            if (pinned)
            {
                CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
                CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
            }

            // The scalar evaluator carries no cancellation token, so the deadline is what ends a call
            // of a cancelled statement.
            long deadline = timeout > 0 ? Environment.TickCount64 + timeout : long.MaxValue;
            CallBudgetFrame frame = CallBudget.Enter(timeout, allocation, array, CancellationToken.None);

            try
            {
                // The frame of the body is taken before its first budget check can run. This check
                // leaves room for it: the limits on the body's types keep one frame far under what
                // the runtime calls sufficient.
                System.Runtime.CompilerServices.RuntimeHelpers.EnsureSufficientExecutionStack();

                return inner(calledName, arguments);
            }
            catch (InsufficientExecutionStackException deep)
            {
                throw BudgetExceeded(calledName, deep, timeout, allocation);
            }
            catch (CamusDBException failed) when (failed.InnerException is { } cause and not OperationCanceledException && CallBudget.IsBudgetException(cause))

            {
                throw BudgetExceeded(calledName, cause, timeout, allocation);
            }
            catch (CamusDBException failed) when (failed.InnerException is System.Text.RegularExpressions.RegexMatchTimeoutException && Environment.TickCount64 >= deadline)
            {
                // The match had the time the call had left as its timeout, and used all of it.
                throw BudgetExceeded(calledName, new CallBudgetExceededException(CallBudgetKind.Time, failed.Message), timeout, allocation);
            }

            finally
            {
                CallBudget.Exit(frame);

                if (pinned)
                {
                    CultureInfo.CurrentCulture = culture;
                    CultureInfo.CurrentUICulture = uiCulture;
                }
            }
        };
    }

    private static CamusDBException BudgetExceeded(string function, Exception cause, int timeoutMs, long allocationBytes)
    {
        string reason = cause switch
        {
            CallBudgetExceededException { Kind: CallBudgetKind.Time } =>
                $"one call ran longer than {timeoutMs} ms (inline_function_call_timeout_ms)",
            CallBudgetExceededException { Kind: CallBudgetKind.Allocation } =>
                $"one call allocated more than {allocationBytes} bytes (inline_function_allocation_limit_bytes)",
            CallBudgetExceededException { Kind: CallBudgetKind.ArrayLength } exceeded =>
                exceeded.Message + " (inline_function_array_limit)",
            InsufficientExecutionStackException => "its calls are nested too deeply for the stack",
            OutOfMemoryException => "the node could not give the call the memory it asked for",
            _ => cause.Message,
        };

        return new CamusDBException(CamusDBErrorCodes.InlineFunctionBudgetExceeded, $"Function '{function}' was stopped: {reason}", cause);
    }

#if !CAMUSDB_BROWSER
    private void Build(InlineFunctionUnit unit, InlineFunctionDefinition function, string label)
    {
        try
        {
            CompiledInlineFunction compiled = compiler.Compile(function, CancellationToken.None);

            try
            {
                unit.Evaluator = Bind(compiled);
                unit.Context = compiled.Context;
                unit.Failure = null;
                unit.RetryAt = 0;
            }
            catch
            {
                compiled.Context.Unload();
                throw;
            }
        }
        catch (InlineFunctionCompileBusyException busy)
        {
            // Remembered for a short time: a call for each row must not start a compile for each row.
            unit.Failure = Unavailable(function, busy.Message);
            unit.RetryAt = Environment.TickCount64 + BusyRetryMs;
        }
        catch (CamusDBException refused)
        {
            unit.RetryAt = 0;
            unit.Failure = Unavailable(function, refused.Message);


            logger?.LogError(
                "Inline function {Function} cannot run on this node: {Reason}. Statements that call it fail here until its owner replaces or drops it",
                label, refused.Message);
        }
        catch (Exception unexpected)
        {
            unit.Failure = Unavailable(function, unexpected.Message);
            logger?.LogError(unexpected, "Inline function {Function} cannot run on this node", label);
        }
    }

    /// <summary>
    /// Binds the compiled method through the binder of extension functions, so an inline function has
    /// the same value mapping and the same NULL rules as a function from an extension. The attribute is
    /// made here and is not in the compiled code; the binder reads only the method's signature from the
    /// code.
    /// </summary>
    private static ScalarFunctionEvaluatorDelegate Bind(CompiledInlineFunction compiled) =>
        ExtensionFunctionBinder.Bind(compiled.Method, new CamusFunctionAttribute("inline")).Evaluator;

    private static CamusDBException Unavailable(InlineFunctionDefinition function, string reason) =>
        new(CamusDBErrorCodes.InlineFunctionUnavailableOnNode,
            $"Function '{function.Name}' cannot run on this node: {reason}. " +
            (function.RuleRevision != InlineFunctionAllowList.Revision
                ? $"It was accepted under rule revision {function.RuleRevision}, and this node has revision {InlineFunctionAllowList.Revision}. "
                : "") +
            "Its owner can replace it with CREATE OR REPLACE FUNCTION, or drop it");

    /// <summary>
    /// Compiles one small function and drops it, so the first statement after a start does not pay for
    /// loading the compiler (about half a second). Runs once, on a pool thread, when inline functions
    /// are on. A failure only means the first statement pays instead.
    /// </summary>
    private void Warm()
    {
        if (disposed)
            return;

        try
        {
            compiler.Compile(
                new InlineFunctionDefinition
                {
                    Name = "warm",
                    Parameters = [new InlineFunctionParameter { Name = "s", Type = ColumnType.String }],
                    ReturnType = ColumnType.String,
                    Source = "var parts = new List<string>(); foreach (var c in s) parts.Add($\"{c}\"); return string.Join(\"-\", parts.Select(p => p.Trim()));",
                },
                CancellationToken.None).Context.Unload();
        }
        catch (Exception failed) when (failed is CamusDBException or ObjectDisposedException)
        {
            logger?.LogDebug(failed, "The warm-up compile for inline functions did not finish");
        }
    }
#else
    private static CamusDBException NoCompiler() =>
        new(CamusDBErrorCodes.InlineFunctionsDisabled, "Inline functions are not available in this build of the server: it has no C# compiler");
#endif

    public void Dispose()
    {
        disposed = true;

#if !CAMUSDB_BROWSER
        List<InlineFunctionLoadContext> contexts = [];

        lock (units)
        {
            foreach (InlineFunctionUnit unit in units.Values)
            {
                if (unit.Context is InlineFunctionLoadContext context)
                    contexts.Add(context);

                unit.Context = null;
            }

            units.Clear();
        }

        foreach (InlineFunctionLoadContext context in contexts)
            context.Unload();
#endif
    }
}
