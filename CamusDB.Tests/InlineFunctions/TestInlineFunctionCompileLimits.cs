/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The limits that protect the compiler itself: the source of a function comes from a user, and a
/// short text can make a compiler work for a long time. Each limit fails the statement, never the
/// node.
/// </summary>
[TestFixture]
public sealed class TestInlineFunctionCompileLimits
{
    [Test]
    public void ABodyLongerThanTheLimit_IsRefusedBeforeItIsParsed()
    {
        using InlineFunctionProbe probe = new(CamusDBOptions.Default with { InlineFunctionSourceLimitBytes = 1024 });

        string atLimit = "return 1; /*" + new string('x', 1024 - 14) + "*/";
        Assert.AreEqual(1024, Encoding.UTF8.GetByteCount(atLimit));
        probe.Validate(InlineFunctionProbe.Number(atLimit));

        CamusDBException tooLong = probe.Refusal(InlineFunctionProbe.Number(atLimit + " "));
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, tooLong.Code);
        StringAssert.Contains("longer than 1024 bytes", tooLong.Message);
        Assert.AreEqual(1, probe.Runtime.Compilations, "the long body never reached the compiler");

        // The limit counts UTF-8 bytes: 400 characters of three bytes each are 1200 bytes.
        CamusDBException wide = probe.Refusal(InlineFunctionProbe.Number("return 1; /*" + new string('€', 400) + "*/"));
        StringAssert.Contains("longer than 1024 bytes", wide.Message);
    }

    [Test]
    public void ABodyWithANulCharacter_IsRefused()
    {
        using InlineFunctionProbe probe = new();

        StringAssert.Contains("NUL", probe.Refusal(InlineFunctionProbe.Number("return 1; //\0")).Message);
    }

    [Test]
    public void ABodyNestedDeeperThanTheLimit_IsRefused()
    {
        using InlineFunctionProbe probe = new();

        static string Nested(int blocks) => string.Concat(Enumerable.Repeat("{", blocks)) + "return 1;" + string.Concat(Enumerable.Repeat("}", blocks));

        // The body is one block, and the return statement and its literal are two more levels.
        probe.Validate(InlineFunctionProbe.Number(Nested(InlineFunctionSyntaxChecker.MaxDepth - 3)));

        CamusDBException tooDeep = probe.Refusal(InlineFunctionProbe.Number(Nested(InlineFunctionSyntaxChecker.MaxDepth)));
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, tooDeep.Code);
        StringAssert.Contains("nested more than 128 levels deep", tooDeep.Message);

        // Far past what a recursive walk survives: the depth is measured with a loop.
        CamusDBException veryDeep = probe.Refusal(InlineFunctionProbe.Number("return " + new string('(', 20_000) + "1" + new string(')', 20_000) + ";"));
        Assert.That(veryDeep.Code, Is.EqualTo(CamusDBErrorCodes.InlineFunctionRefused).Or.EqualTo(CamusDBErrorCodes.InlineFunctionCompileFailed));
    }

    /// <summary>
    /// A body whose levels are lambdas given to <c>Sum</c>. <c>Sum</c> has ten overloads that take a
    /// selector, so the compiler binds each level once for each overload of the level around it.
    /// The value is three to the power of the levels.
    /// </summary>
    private static string NestedSum(int levels)
    {
        string expression = "1L";

        for (int i = levels; i >= 1; i--)
            expression = $"xs.Sum(a{i} => {expression})";

        return "var xs = new List<long> { 1, 2, 3 }; return " + expression + ";";
    }

    private static string Lines(int count, Func<int, string> line) => string.Concat(Enumerable.Range(1, count).Select(line));

    private static CamusDBException Refused(InlineFunctionProbe probe, string body, string reason)
    {
        Stopwatch watch = Stopwatch.StartNew();
        CamusDBException refused = probe.Refusal(InlineFunctionProbe.Number(body));
        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, refused.Code, refused.Message);
        StringAssert.Contains(reason, refused.Message);
        Assert.Less(watch.ElapsedMilliseconds, 10_000, "a refusal must not wait for the compiler");

        return refused;
    }

    [Test]
    public void ACompileThatTakesTooLong_FailsTheStatement()
    {
        using InlineFunctionProbe probe = new(CamusDBOptions.Default with { InlineFunctionCompileTimeoutMs = 50 });

        // The slowest shape that the checks before the compiler accept.
        CamusDBException timedOut = probe.Refusal(InlineFunctionProbe.Number(NestedSum(4)));

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, timedOut.Code, timedOut.Message);
        StringAssert.Contains("did not compile within 50 ms", timedOut.Message);

        // The node is fine: the next function compiles.
        using InlineFunctionProbe next = new();
        Assert.AreEqual(3, next.Call(InlineFunctionProbe.Number("return n + 1;"), "x", 2).LongValue);
    }

    [Test]
    public void ACompileThatDoesNotStop_IsLeftByItsCaller_AndKeepsItsSlotUntilItEnds()
    {
        // No wait past the time limit: the caller leaves the compile thread at once. With the wait a
        // server has, no body that the checks accept keeps the compiler busy for long enough.
        using InlineFunctionCompiler compiler = new(
            sourceLimitBytes: 65_536, compileTimeoutMs: 20, concurrency: 1, arrayLimit: 1_000_000, logger: null, stopGraceMs: 0);

        CamusDBException left = Assert.Throws<CamusDBException>(
            () => compiler.Compile(InlineFunctionProbe.Number(NestedSum(4)), CancellationToken.None))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, left.Code, left.Message);
        StringAssert.Contains("did not compile within 20 ms", left.Message);

        StringAssert.Contains("the compiler did not stop", left.Message);
        Assert.IsNotInstanceOf<InlineFunctionCompileBusyException>(left, "a compile that does not stop is not tried again");
        Assert.AreEqual(1, compiler.AbandonedCompilations);

        // The thread still holds the only slot until it ends; then the compiler works again.
        Assert.That(() => compiler.Compilations, Is.EqualTo(1).After(30_000, 20), "the compile thread ends");
        Assert.AreEqual(1, compiler.PeakConcurrentCompilations);

        using InlineFunctionCompiler patient = new(sourceLimitBytes: 65_536, compileTimeoutMs: 30_000, concurrency: 1, arrayLimit: 1_000_000);
        CompiledInlineFunction compiled = patient.Compile(InlineFunctionProbe.Number("return n + 1;"), CancellationToken.None);
        compiled.Context.Unload();
    }

    [Test]
    public void LambdasNestedInOverloadedCalls_AreRefusedBeforeTheCompiler()
    {
        using InlineFunctionProbe probe = new();

        Assert.AreEqual(81, probe.Call(InlineFunctionProbe.Number(NestedSum(4)), "x", 0).LongValue, "four levels compile");

        // Each more level costs the compiler about seven times more, and it does not look at the
        // cancellation token while it binds them: fourteen levels would hold a thread for hours.
        foreach (int levels in new[] { 5, 14 })
            Refused(probe, NestedSum(levels), "the lambdas are nested too deeply, or there are too many of them");

        // Many pairs side by side cost as much as one deep nest.
        Refused(
            probe,
            "var xs = new List<long> { 1, 2, 3 }; long r = 0;\n" + Lines(200, _ => "r += xs.Sum(a => xs.Sum(b => 1L));\n") + "return r;",
            "the lambdas are nested too deeply, or there are too many of them");

        // Hundreds of lambdas that hold no other lambda are ordinary code.
        Assert.AreEqual(
            1800,
            probe.Call(InlineFunctionProbe.Number("var xs = new List<long> { 1, 2, 3 }; long r = 0;\n" + Lines(600, _ => "r += xs.Sum(a => 1L);\n") + "return r;"), "x", 0).LongValue);

        // A local function is bound one time wherever it is called from, so six levels pass when
        // each level is one.
        const string flat =
            "var xs = new List<long> { 1, 2, 3 }; long A(long a) => xs.Sum(b => B(b)); long B(long b) => xs.Sum(c => C(c)); " +
            "long C(long c) => xs.Sum(d => D(d)); long D(long d) => xs.Sum(e => xs.Sum(f => 1L)); return xs.Sum(a => A(a));";

        Assert.AreEqual(729, probe.Call(InlineFunctionProbe.Number(flat), "x", 0).LongValue);
    }

    [Test]
    public void ATypeThatGrowsWithEachStep_IsRefusedBeforeTheCompiler()
    {
        using InlineFunctionProbe probe = new();

        const string tooLarge = "too large to compile";

        // The type of each local holds the type of the one before it twice: sixty lines ask the
        // compiler for a type of 2^61 elements, and it walks a type element by element.
        Refused(probe, "var t0 = (n, n);\n" + Lines(60, i => $"var t{i} = (t{i - 1}, t{i - 1});\n") + "return n;", tooLarge);

        // The same in one expression, through the lambda parameters of a chain.
        Refused(
            probe,
            "var xs = new List<long> { 1, 2, 3 }; return xs" + Lines(30, i => $".Select(a{i} => (a{i}, a{i}))") + ".Count();",
            tooLarge);

        // And through type arguments that a call infers from two lambdas.
        Refused(
            probe,
            "var d0 = new Dictionary<long, long>();\n" + Lines(30, i => $"var d{i} = new[] {{ d{i - 1} }}.ToDictionary(k => k, v => v);\n") + "return d30.Count;",
            tooLarge);

        // A written type stops the growth: the bound of a local is then what was written.
        Assert.AreEqual(
            7,
            probe.Call(
                InlineFunctionProbe.Number("(long, long) t0 = (n, n);\n" + Lines(60, i => $"(long, long) t{i} = (t{i - 1}.Item1, t{i - 1}.Item2);\n") + "return t60.Item1;"),
                "x", 7).LongValue);
    }

    [Test]
    public void OrdinaryLongExpressions_PassTheChecksBeforeTheCompiler()
    {
        using InlineFunctionProbe probe = new();

        const string chain =
            "var xs = new List<long> { 1, 2, 3 }; " +
            "return xs.Where(x => x > 0).Select(x => (x, x * 2)).Where(p => p.Item2 > 1).OrderBy(p => p.Item1).ThenBy(p => p.Item2)" +
            ".Select(p => (p.Item1 + 1, p.Item2)).GroupBy(p => p.Item1 % 2).Select(g => (g.Key, g.Count())).OrderByDescending(p => p.Item2)" +
            ".Select(p => p.Item1).Distinct().Take(5).ToList().Count;";

        Assert.AreEqual(2, probe.Call(InlineFunctionProbe.Number(chain), "x", 0).LongValue);

        const string joins =
            "var xs = new List<long> { 1, 2, 3 }; var ys = new List<long> { 2, 3 }; " +
            "return xs.Join(ys, x => x, y => y, (x, y) => (x, y)).GroupBy(p => p.x, p => p.y, (k, vs) => (k, vs.Sum())).ToDictionary(p => p.k, p => p.Item2).Count;";

        Assert.AreEqual(2, probe.Call(InlineFunctionProbe.Number(joins), "x", 0).LongValue);

        // Sixty calls in one chain, and 150 inferred locals that each come from the one before.
        Assert.AreEqual(
            6,
            probe.Call(InlineFunctionProbe.Number("return s" + Lines(30, _ => ".Trim().ToUpperInvariant()") + ".Length;"), "b, a;c", 0).LongValue);

        Assert.AreEqual(
            6,
            probe.Call(
                InlineFunctionProbe.Number("var s0 = s;\n" + Lines(150, i => $"var s{i} = s{i - 1}.Replace(\"a\", \"b\");\n") + "return s150.Length;"),
                "b, a;c", 0).LongValue);

        Assert.AreEqual(
            1000,
            probe.Call(
                InlineFunctionProbe.Number("var sb = new StringBuilder();\n" + Lines(20, _ => "sb" + Lines(25, _ => ".Append(n).Append(',')") + ";\n") + "return sb.Length;"),
                "x", 7).LongValue);
    }

    [Test]
    public void ATypeOverTheLimits_IsRefused_WithTheReason()
    {
        using InlineFunctionProbe probe = new();

        static string Doubling(int levels) => "var t0 = (n, n);\n" + Lines(levels, i => $"var t{i} = (t{i - 1}, t{i - 1});\n") + "return n;";
        static string Nesting(int levels) => "var t0 = (n, n);\n" + Lines(levels, i => $"var t{i} = (n, t{i - 1});\n") + $"return t{levels}.ToString().Length;";

        Assert.AreEqual(7, probe.Call(InlineFunctionProbe.Number(Doubling(6)), "x", 7).LongValue, "a tuple of 128 numbers is small enough");
        Refused(probe, Doubling(7), "the type is too large: it holds more than 256 type names");

        Assert.AreEqual(156, probe.Call(InlineFunctionProbe.Number(Nesting(30)), "x", 7).LongValue);
        Refused(probe, Nesting(40), "the type is nested too deeply");

        // 800 levels are more than a compiler survives on the stack of a pool thread; the compile has a
        // stack of its own, and the type is refused when the compiler is done.
        Refused(probe, Nesting(800), "the type is nested too deeply");
        Refused(probe, "var a0 = new[] { n };\n" + Lines(800, i => $"var a{i} = new[] {{ a{i - 1} }};\n") + "return a800.Length;", "the type is nested too deeply");

        static string Numbers(int count) => string.Join(", ", Enumerable.Repeat("n", count));
        static string Types(int count) => string.Join(", ", Enumerable.Repeat("long", count));

        Assert.AreEqual(7, probe.Call(InlineFunctionProbe.Number($"var t = ({Numbers(100)}); return t.Item100;"), "x", 7).LongValue, "800 bytes");
        Refused(probe, $"var t = ({Numbers(200)}); return t.Item200;", "a value of this type is larger than 1024 bytes");
        Refused(probe, $"var l = new List<({Types(200)})>(); return l.Count;", "a value of this type is larger than 1024 bytes");
    }

    [Test]
    public void AMethodThatWorksWithTooManyLargeValues_IsRefused_AndALocalFunctionIsTheWayOut()
    {
        using InlineFunctionProbe probe = new();

        const string step = "d = (d * 1.5m + 2m - d / 3m) % 1000m;\n";

        Assert.AreEqual(
            ColumnType.Integer64,
            probe.Call(InlineFunctionProbe.Number("decimal d = n;\n" + Lines(150, _ => step) + "return (long)d;"), "x", 7).Type,
            "750 decimal operations are an ordinary body");

        Refused(probe, "decimal d = n;\n" + Lines(750, _ => step) + "return (long)d;", "the body works with more than 32768 bytes of values at one time");

        Refused(
            probe,
            "Func<decimal, decimal> f = d => {\n" + Lines(750, _ => step) + "return d; };\nreturn (long)f(n);",
            "this lambda works with more than 32768 bytes of values at one time");

        Refused(
            probe,
            "decimal F(decimal d) {\n" + Lines(750, _ => step) + "return d; }\nreturn (long)F(n);",
            "this local function works with more than 32768 bytes of values at one time");

        // The same work in ten local functions: each has a frame of its own.
        string split =
            "decimal d = n;\n" +
            Lines(10, k => $"decimal F{k}(decimal d) {{\n" + Lines(75, _ => step) + "return d; }\n") +
            Lines(10, k => $"d = F{k}(d);\n") +
            "return (long)d;";

        Assert.AreEqual(ColumnType.Integer64, probe.Call(InlineFunctionProbe.Number(split), "x", 7).Type);

        // Locals count too, one register each or more.
        Refused(probe, "long v0 = n" + Lines(4200, i => $", v{i} = n") + "; return v4200;", "the body works with more than 32768 bytes of values at one time");
    }

    [Test]
    public async Task CompilesRunOneAtATime_WhenTheConcurrencyIsOne()
    {
        using InlineFunctionProbe probe = new(CamusDBOptions.Default with { InlineFunctionCompileConcurrency = 1 });

        Task[] compiles = Enumerable.Range(0, 6)
            .Select(i => Task.Run(() => probe.Validate(InlineFunctionProbe.Number($"long t = {i}; foreach (char c in s) t += c; return t + n;"))))
            .ToArray();

        await Task.WhenAll(compiles);

        Assert.AreEqual(6, probe.Runtime.Compilations);
        Assert.AreEqual(1, probe.Runtime.PeakConcurrentCompilations, "no two compiles ran at the same time");
    }

    [Test]
    public async Task ConcurrentFirstCalls_CompileASourceOnce()
    {
        using InlineFunctionProbe probe = new();
        InlineFunctionDefinition function = InlineFunctionProbe.Number("return n * 2;");

        InlineFunctionUnit[] units = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(_ => Task.Run(() => probe.Runtime.Acquire(function, "t.f"))));

        try
        {
            Assert.AreEqual(1, probe.Runtime.Compilations, "eight statements that call a new function at once compile it once");
            Assert.IsTrue(units.All(unit => ReferenceEquals(unit, units[0])));
        }
        finally
        {
            foreach (InlineFunctionUnit unit in units)
                probe.Runtime.Release(unit, "t.f");
        }
    }

    [Test]
    public void TwoFunctionsWithTheSameSource_ShareOneUnit_AndTheNameIsNotInTheCode()
    {
        using InlineFunctionProbe probe = new();

        InlineFunctionDefinition first = InlineFunctionProbe.Number("return n + 1;");
        InlineFunctionDefinition second = InlineFunctionProbe.Number("return n + 1;");
        second.Name = "another_name";

        InlineFunctionUnit a = probe.Runtime.Acquire(first, "db1.f");
        InlineFunctionUnit b = probe.Runtime.Acquire(second, "db2.another_name");

        try
        {
            Assert.AreSame(a, b);
            Assert.AreEqual(1, probe.Runtime.Compilations);

            // Another signature is other code, even with the same body text.
            InlineFunctionDefinition third = InlineFunctionProbe.Define("return n + 1;", ColumnType.Integer64, ("n", ColumnType.Integer64));
            InlineFunctionUnit c = probe.Runtime.Acquire(third, "db1.g");
            Assert.AreNotSame(a, c);
            probe.Runtime.Release(c, "db1.g");
        }
        finally
        {
            probe.Runtime.Release(a, "db1.f");
            probe.Runtime.Release(b, "db2.another_name");
        }
    }

    [Test]
    public void TheCodeOfAReleasedUnit_LeavesTheProcess()
    {
        using InlineFunctionProbe probe = new();

        WeakReference Run()
        {
            InlineFunctionDefinition function = InlineFunctionProbe.Number("return n + 41;");
            InlineFunctionUnit unit = probe.Runtime.Acquire(function, "t.gone");
            Assert.AreEqual(42, probe.Runtime.Guard(unit.Evaluator!)("f", [new ColumnValue(ColumnType.String, "x"), new ColumnValue(ColumnType.Integer64, 1)]).LongValue);

            WeakReference context = new(unit.Context);
            probe.Runtime.Release(unit, "t.gone");
            return context;
        }

        WeakReference released = Run();

        for (int i = 0; i < 20 && released.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.IsFalse(released.IsAlive, "the load context of a released unit is collected");
        CollectionAssert.IsEmpty(probe.Runtime.Unloading(), "and the runtime stops reporting it as unloading");
    }
}
