/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;
using CamusDB.Core.Extensions.Inline;
using CamusDB.Extensions;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// What an inline SQL function costs for each call, against the same C# code bound the way a
/// startup-loaded extension function is bound.
///
/// <para>
/// <see cref="ExtensionSlugify"/> and <see cref="InlineSlugify"/> run one body with no loop, so the
/// difference between them is the guard around an inline call: the budget frame, the culture check
/// and one more delegate. <see cref="ExtensionLoop"/> and <see cref="InlineLoop"/> run a loop of
/// <see cref="Iterations"/> steps, so their difference, divided by the steps, is the cost of one
/// budget check.
/// </para>
///
/// <para>
/// <c>dotnet run -c Release -- probe inlinefn</c> prints the numbers a benchmark job cannot give: the
/// cold and the warm compile time, and the memory a node keeps for each replace and after an unload.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class InlineFunctionBenchmarks
{
    private static readonly int Rounds = int.TryParse(Environment.GetEnvironmentVariable("INLINE_PROBE_ROUNDS"), out int rounds) ? rounds : 2;

    private const string SlugifyBody = "return s.Trim().ToLowerInvariant().Replace(' ', '-');";

    private const string LoopBody = "long t = 0; for (long i = 0; i < n; i++) t += i ^ 3; return t;";

    [Params(1000)]
    public long Iterations { get; set; }

    private InlineFunctionRuntime runtime = null!;

    private readonly List<InlineFunctionUnit> units = [];

    private ScalarFunctionEvaluatorDelegate inlineSlugify = null!;

    private ScalarFunctionEvaluatorDelegate inlineLoop = null!;

    private ScalarFunctionEvaluatorDelegate extensionSlugify = null!;

    private ScalarFunctionEvaluatorDelegate extensionLoop = null!;

    private ColumnValue[] text = null!;

    private ColumnValue[] count = null!;

    /// <summary>The same body as <see cref="SlugifyBody"/>, compiled with this assembly.</summary>
    public static string? Slugify(string s) => s.Trim().ToLowerInvariant().Replace(' ', '-');

    /// <summary>The same body as <see cref="LoopBody"/>, compiled with this assembly.</summary>
    public static long? Loop(long n)
    {
        long t = 0;

        for (long i = 0; i < n; i++)
            t += i ^ 3;

        return t;
    }

    [GlobalSetup]
    public void Setup()
    {
        runtime = new InlineFunctionRuntime(CamusDBOptions.Default, logger: null);

        inlineSlugify = Bind(Function(SlugifyBody, ColumnType.String, ("s", ColumnType.String)));
        inlineLoop = Bind(Function(LoopBody, ColumnType.Integer64, ("n", ColumnType.Integer64)));

        extensionSlugify = ExtensionFunctionBinder.Bind(typeof(InlineFunctionBenchmarks).GetMethod(nameof(Slugify))!, new CamusFunctionAttribute("slugify")).Evaluator;
        extensionLoop = ExtensionFunctionBinder.Bind(typeof(InlineFunctionBenchmarks).GetMethod(nameof(Loop))!, new CamusFunctionAttribute("loop")).Evaluator;

        text = [new ColumnValue(ColumnType.String, "  The Quick Brown Fox  ")];
        count = [new ColumnValue(ColumnType.Integer64, Iterations)];
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (InlineFunctionUnit unit in units)
            runtime.Release(unit, "bench.f");

        runtime.Dispose();
    }

    private ScalarFunctionEvaluatorDelegate Bind(InlineFunctionDefinition function)
    {
        InlineFunctionUnit unit = runtime.Acquire(function, "bench.f");
        units.Add(unit);

        return runtime.Guard(unit.Evaluator ?? throw unit.Failure!);
    }

    internal static InlineFunctionDefinition Function(string body, ColumnType returnType, params (string Name, ColumnType Type)[] parameters) => new()
    {
        Name = "f",
        Source = body,
        ReturnType = returnType,
        Parameters = parameters.Select(p => new InlineFunctionParameter { Name = p.Name, Type = p.Type }).ToList(),
        LanguageVersion = InlineFunctionRuntime.LanguageVersion,
        RuleRevision = InlineFunctionAllowList.Revision,
    };

    [Benchmark(Baseline = true)]
    public ColumnValue ExtensionSlugify() => extensionSlugify("slugify", text);

    [Benchmark]
    public ColumnValue InlineSlugify() => inlineSlugify("slugify", text);

    [Benchmark]
    public ColumnValue ExtensionLoop() => extensionLoop("loop", count);

    [Benchmark]
    public ColumnValue InlineLoop() => inlineLoop("loop", count);

    /// <summary>
    /// Loads <paramref name="count"/> functions with different bodies, calls each once, measures the
    /// memory while they are loaded, and gives every unit back. In its own method, so no local of the
    /// caller keeps a unit alive.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static long LoadAndRelease(InlineFunctionRuntime runtime, int count, int round, out long whileLoaded)
    {
        List<InlineFunctionUnit> held = [];

        for (int i = 0; i < count; i++)
        {
            InlineFunctionDefinition version = Function("return s + \"" + round + "-" + i + "\";", ColumnType.String, ("s", ColumnType.String));
            InlineFunctionUnit unit = runtime.Acquire(version, "probe.f");
            runtime.Guard(unit.Evaluator!)("f", [new ColumnValue(ColumnType.String, "x")]);
            held.Add(unit);
        }

        for (int i = 0; i < 5; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        whileLoaded = GC.GetTotalMemory(forceFullCollection: true);

        foreach (InlineFunctionUnit unit in held)
            runtime.Release(unit, "probe.f");

        return whileLoaded;
    }

    /// <summary>
    /// The one-shot numbers: compile times, the cost of one call and of one budget check measured with
    /// a stopwatch (for a quick look without a benchmark job), and retained memory.
    /// </summary>
    public static int RunProbe()
    {
        InlineFunctionRuntime runtime = new(CamusDBOptions.Default, logger: null);
        InlineFunctionDefinition slugify = Function(SlugifyBody, ColumnType.String, ("s", ColumnType.String));

        Stopwatch watch = Stopwatch.StartNew();
        runtime.Validate(slugify, default);
        Console.WriteLine($"cold compile (loads the compiler): {watch.Elapsed.TotalMilliseconds:F1} ms");

        const int warmRounds = 50;
        watch.Restart();

        for (int i = 0; i < warmRounds; i++)
            runtime.Validate(Function(SlugifyBody + " // " + i, ColumnType.String, ("s", ColumnType.String)), default);

        Console.WriteLine($"warm compile: {watch.Elapsed.TotalMilliseconds / warmRounds:F2} ms for each function");

        // One call, and one budget check.
        InlineFunctionBenchmarks bench = new() { Iterations = 1000 };
        bench.Setup();

        const int calls = 2_000_000;

        static double Nanoseconds(Func<ColumnValue> call, int rounds)
        {
            for (int i = 0; i < rounds / 10; i++)
                call();

            Stopwatch timer = Stopwatch.StartNew();

            for (int i = 0; i < rounds; i++)
                call();

            return timer.Elapsed.TotalNanoseconds / rounds;
        }

        // The server host runs with the invariant culture, where the guard writes no culture. Another
        // host pays for two culture writes before the call and two after it.
        System.Globalization.CultureInfo hostCulture = System.Globalization.CultureInfo.CurrentCulture;
        System.Globalization.CultureInfo hostUiCulture = System.Globalization.CultureInfo.CurrentUICulture;

        foreach (bool invariant in new[] { true, false })
        {
            System.Globalization.CultureInfo.CurrentCulture = invariant ? System.Globalization.CultureInfo.InvariantCulture : System.Globalization.CultureInfo.GetCultureInfo("de-DE");
            System.Globalization.CultureInfo.CurrentUICulture = System.Globalization.CultureInfo.CurrentCulture;

            double extensionCall = Nanoseconds(bench.ExtensionSlugify, calls);
            double inlineCall = Nanoseconds(bench.InlineSlugify, calls);

            Console.WriteLine(
                $"one call, no loop, {(invariant ? "invariant culture (the server host)" : "another culture")}: " +
                $"extension {extensionCall:F0} ns, inline {inlineCall:F0} ns (the guard costs {inlineCall - extensionCall:F0} ns)");
        }

        System.Globalization.CultureInfo.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
        System.Globalization.CultureInfo.CurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;

        double extensionLoop = Nanoseconds(bench.ExtensionLoop, calls / 100);
        double inlineLoop = Nanoseconds(bench.InlineLoop, calls / 100);

        Console.WriteLine($"one budget check: {(inlineLoop - extensionLoop) / bench.Iterations:F2} ns (loop of {bench.Iterations}: extension {extensionLoop:F0} ns, inline {inlineLoop:F0} ns)");
        bench.Cleanup();

        System.Globalization.CultureInfo.CurrentCulture = hostCulture;
        System.Globalization.CultureInfo.CurrentUICulture = hostUiCulture;

        // Retained memory: replace a function many times, then let every unit go.
        static long Settled()
        {
            for (int i = 0; i < 5; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            return GC.GetTotalMemory(forceFullCollection: true);
        }

        // Two rounds: the compiler fills its own fixed-size pools during the first, so the second
        // shows what one function costs once those are full.
        for (int round = 1; round <= Rounds; round++)
        {
            long before = Settled();
            const int replaces = 200;
            long loaded = LoadAndRelease(runtime, replaces, round, out long whileLoaded);
            _ = loaded;

            long unloaded = Settled();

            Console.WriteLine(
                $"round {round}: {(whileLoaded - before) / replaces:N0} bytes for each of {replaces} loaded functions; " +
                $"{(unloaded - before) / replaces:N0} bytes for each kept after the unload; still unloading: {runtime.Unloading().Count}");
        }

        runtime.Dispose();
        return 0;
    }
}
