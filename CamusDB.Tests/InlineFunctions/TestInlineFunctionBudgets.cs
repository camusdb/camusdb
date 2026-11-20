/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The budgets of one call of an inline function. Each case is a body that would hold a statement
/// for ever, or end the process, without them: it must fail the call with
/// <see cref="CamusDBErrorCodes.InlineFunctionBudgetExceeded"/>, soon, and the process must go on (the
/// next test runs, which is the proof for the recursion cases).
/// </summary>
[TestFixture]
public sealed class TestInlineFunctionBudgets
{
    private const int TimeoutMs = 100;

    /// <summary>How long after the limit a stopped call may return: the check runs between framework calls.</summary>
    private const int SlackMs = 2000;

    /// <summary>
    /// The time limit of <see cref="stackProbe"/>. It is far above the time a recursion takes to fill
    /// the stack, also on a slow CI runner under the load of the whole suite.
    /// </summary>
    private const int StackTimeoutMs = 60_000;

    /// <summary>The stack of a thread pool thread on Linux: the smallest stack a call of a function runs on.</summary>
    private const int PoolStackBytes = 1_572_864;

    private InlineFunctionProbe probe = null!;

    /// <summary>
    /// A probe where only the stack can end a recursion. With the 100 ms limit of <see cref="probe"/>,
    /// a recursion races the deadline: a slow runner reaches the deadline before the stack check, and
    /// the call stops with the time reason instead of the stack reason.
    /// </summary>
    private InlineFunctionProbe stackProbe = null!;

    [OneTimeSetUp]
    public void CreateProbes()
    {
        probe = new InlineFunctionProbe(CamusDBOptions.Default with
        {
            InlineFunctionCallTimeoutMs = TimeoutMs,
            InlineFunctionAllocationLimitBytes = 4 * 1024 * 1024,
            InlineFunctionArrayLimit = 10_000,
        });

        stackProbe = new InlineFunctionProbe(CamusDBOptions.Default with
        {
            InlineFunctionCallTimeoutMs = StackTimeoutMs,
            InlineFunctionAllocationLimitBytes = 1L << 30,
            InlineFunctionArrayLimit = 10_000,
        });
    }

    [OneTimeTearDown]
    public void DisposeProbes()
    {
        probe.Dispose();
        stackProbe.Dispose();
    }

    private CamusDBException Stopped(string body, string reason) =>
        Stopped(probe, TimeoutMs + SlackMs, body, reason);

    private static CamusDBException Stopped(InlineFunctionProbe target, long withinMs, string body, string reason)
    {
        Stopwatch watch = Stopwatch.StartNew();

        CamusDBException stopped = Assert.Throws<CamusDBException>(
            () => target.Call(InlineFunctionProbe.Number(body), "x", 5))!;

        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped.Code, stopped.Message);
        StringAssert.Contains(reason, stopped.Message);
        Assert.Less(watch.ElapsedMilliseconds, withinMs, "the call must end soon after its limit");

        return stopped;
    }

    /// <summary>
    /// Calls <paramref name="body"/> on a thread with the stack of a pool thread. Returns the error of
    /// the call when it is a <see cref="CamusDBException"/>, and any other exception apart.
    /// </summary>
    private static (CamusDBException? Stopped, Exception? Other) CallOnAPoolStack(InlineFunctionProbe target, string body)
    {
        CamusDBException? stopped = null;
        Exception? other = null;

        Thread thread = new(
            () =>
            {
                try
                {
                    target.Call(InlineFunctionProbe.Number(body), "x", 5);
                }
                catch (CamusDBException failure)
                {
                    stopped = failure;
                }
                catch (Exception failure)
                {
                    other = failure;
                }
            },
            maxStackSize: PoolStackBytes);

        thread.Start();
        thread.Join();

        return (stopped, other);
    }

    private static void StoppedOnAPoolStack(InlineFunctionProbe target, string body, string reason)
    {
        (CamusDBException? stopped, Exception? other) = CallOnAPoolStack(target, body);

        Assert.IsNull(other, other?.ToString());
        Assert.IsNotNull(stopped, "the call must fail");
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped!.Code, stopped.Message);
        StringAssert.Contains(reason, stopped.Message);
    }

    [TestCase("while (true) { }")]
    [TestCase("for (;;) { }")]
    [TestCase("do { } while (true);")]
    [TestCase("long i = 0; while (true) i++;")]
    [TestCase("var l = new List<long> { 1 }; foreach (var x in l) { while (true) { } } return 1;")]
    public void AnInfiniteLoop_IsStoppedAtTheDeadline(string body) =>
        Stopped(body, "inline_function_call_timeout_ms");

    /// <summary>Without the filter the server adds to each catch, the body would catch the tick's exception and run on.</summary>
    [TestCase("while (true) { try { long.Parse(\"x\"); } catch { } }")]
    [TestCase("while (true) { try { long.Parse(\"x\"); } catch (Exception) { } }")]
    [TestCase("while (true) { try { long.Parse(\"x\"); } catch (Exception e) when (e.Message.Length >= 0) { } }")]
    [TestCase("try { while (true) { } } catch { } return 1;")]
    [TestCase("try { while (true) { } } catch (Exception) { } return 1;")]
    [TestCase("try { try { while (true) { } } catch { } } catch { } return 1;")]
    [TestCase("try { return 1; } finally { while (true) { } }")]
    [TestCase("Func<long> spin = () => { while (true) { } }; try { return spin(); } catch { return 2; }")]
    public void ACatchClause_CannotCatchTheBudget(string body) =>
        // The time budget or the memory budget: a loop that throws and catches an exception with
        // each pass allocates one each time. Which of the two ends the call first is not the point.
        Stopped(body, "was stopped: one call");

    [TestCase("long Deeper(long x) => Deeper(x + 1) + 1; return Deeper(n);")]
    [TestCase("long Deeper(long x) { return Deeper(x + 1) + 1; } return Deeper(n);")]
    [TestCase("Func<long, long> deeper = null!; deeper = x => deeper(x + 1) + 1; return deeper(n);")]
    [TestCase("long A(long x) => B(x) + 1; long B(long x) => A(x) + 1; return A(n);")]
    [TestCase("long Deeper(long x) { try { return Deeper(x + 1) + 1; } catch { return 0; } } return Deeper(n);")]
    public void UnboundedRecursion_FailsTheCallAndNotTheProcess(string body) =>
        Stopped(stackProbe, StackTimeoutMs, body, "nested too deeply for the stack");

    [TestCase("var l = new List<string>(); while (true) { l.Add(new string(new char[1000])); }")]
    [TestCase("string x = \"ab\"; while (true) { x = x + x; }")]
    [TestCase("var l = new List<long> { 1 }; while (true) { l.AddRange(l); }")]
    [TestCase("var sb = new StringBuilder(\"abcdefgh\"); while (true) { sb.Append(sb.ToString()); }")]
    public void AnAllocationLoop_IsStoppedAtTheMemoryLimit(string body) =>
        Stopped(body, "inline_function_allocation_limit_bytes");

    [Test]
    public void ASizedArray_IsCheckedBeforeItIsAllocated()
    {
        InlineFunctionDefinition bytes = InlineFunctionProbe.Number("var a = new byte[n]; return a.Length;");

        Assert.AreEqual(10_000, probe.Call(bytes, "x", 10_000).LongValue, "the limit itself is allowed");

        CamusDBException tooLong = Assert.Throws<CamusDBException>(() => probe.Call(bytes, "x", 10_001))!;
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, tooLong.Code);
        StringAssert.Contains("inline_function_array_limit", tooLong.Message);

        // Under the element limit, over the memory limit: 10,000 elements of 480 bytes each.
        InlineFunctionDefinition wide = InlineFunctionProbe.Number(
            "var a = new (" + string.Join(", ", Enumerable.Repeat("decimal", 30)) + ")[n]; return a.Length;");

        CamusDBException tooWide = Assert.Throws<CamusDBException>(() => probe.Call(wide, "x", 10_000))!;
        StringAssert.Contains("inline_function_allocation_limit_bytes", tooWide.Message);

        // A negative size is the body's own error, the one "new T[-1]" gives.
        CamusDBException negative = Assert.Throws<CamusDBException>(() => probe.Call(bytes, "x", -1))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, negative.Code);
    }

    [Test]
    public void AnArrayWithConstantSizesInTwoDimensions_IsCheckedWhenItCompiles()
    {
        Assert.AreEqual(6, probe.Call(InlineFunctionProbe.Number("var m = new long[2, 3]; return m.Length;"), "x", 1).LongValue);

        CamusDBException tooLarge = probe.Refusal(InlineFunctionProbe.Number("var m = new long[200, 200]; return m.Length;"));
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, tooLarge.Code);
        StringAssert.Contains("larger than the limit of 10000 elements", tooLarge.Message);

        CamusDBException notConstant = probe.Refusal(InlineFunctionProbe.Number("var m = new long[n, 3]; return m.Length;"));
        StringAssert.Contains("needs constant sizes", notConstant.Message);
    }

    [Test]
    public void AFinallyBlockWithALoopThatEnds_Ends()
    {
        Assert.AreEqual(
            1,
            probe.Call(InlineFunctionProbe.Number("try { return 1; } finally { long i = 0; while (i < n) i++; }"), "x", 1000).LongValue);
    }

    [Test]
    public void ARegularExpression_CannotOutliveTheCall()
    {
        // The body asks for a 30 second match timeout. The server limits it to the time the call has
        // left, so the backtracking stops at the deadline of the call.
        const string body =
            "return Regex.IsMatch(new string(new char[40]).Replace('\\0', 'a') + \"!\", \"^(a|aa)+$\", RegexOptions.None, TimeSpan.FromSeconds(30)) ? 1 : 0;";

        Stopped(body, "inline_function_call_timeout_ms");

        // The same in a loop that reads one match at a time and swallows the timeout of each.
        const string loop =
            "var r = new Regex(\"^(a|aa)+$\", RegexOptions.None, TimeSpan.FromSeconds(30)); string text = new string(new char[40]).Replace('\\0', 'a') + \"!\"; " +
            "while (true) { try { r.IsMatch(text); } catch (RegexMatchTimeoutException) { } }";

        Stopped(loop, "inline_function_call_timeout_ms");
    }

    /// <summary>
    /// The stack check of the budget runs at the start of a lambda or a local function, after the
    /// frame of that call is taken. These bodies have the largest frames the type limits allow, and
    /// they run on a thread with the stack of a pool thread: a frame that does not fit in what the
    /// check before it left would end the process here, with no test result at all.
    /// </summary>
    [Test]
    public void RecursionWithTheLargestFrames_IsStopped_OnTheStackOfAPoolThread()
    {
        string tuple = "(" + string.Join(", ", Enumerable.Repeat("n", 120)) + ")";
        string tupleType = "(" + string.Join(", ", Enumerable.Repeat("long", 120)) + ")";
        string Many(int count, Func<int, string> line) => string.Concat(Enumerable.Range(0, count).Select(line));
        string sum = string.Join(" + ", Enumerable.Range(0, 15).Select(i => $"a{i}.Item{i + 1}"));

        string[] bodies =
        [
            // Fifteen locals of 960 bytes each and the fifteen tuples that fill them, in a local function and in a lambda.
            "long F(long k) {\n" + Many(15, i => $"var a{i} = {tuple};\n") + $"return F(k + 1) + {sum}; }}\nreturn F(0);",
            "Func<long, long> f = null!;\nf = k => {\n" + Many(15, i => $"var a{i} = {tuple};\n") + $"return f(k + 1) + {sum}; }};\nreturn f(0);",

            // 1,900 decimal operations in one frame.
            "decimal F(decimal d, long k) {\n" + Many(475, _ => "d = d * 1.0000001m + 2m - d / 3m;\n") + "return F(d % 100, k + 1); }\nreturn (long)F(n, 0);",

            // Large values that the framework copies and compares in frames of its own.
            $"var set = new HashSet<{tupleType}>();\nlong F(long k) {{\n" + Many(12, i => $"var a{i} = {tuple}; set.Add(a{i});\n") +
                "return F(k + 1) + (set.Contains(a0) ? 1 : 0); }\nreturn F(0);",

            // A value nested as deep as the limit allows: Equals, CompareTo and ToString recurse once for each level.
            "var t0 = (n, n);\n" + Many(29, i => $"var t{i + 1} = (n, t{i}, n, n);\n") +
                "long F(long k) { var a = t29; var b = t29; return F(k + 1) + (a.Equals(b) && a.CompareTo(b) == 0 && a.ToString().Length > 0 ? 1 : 0); }\nreturn F(0);",
        ];

        // Room in the other budgets, so that the stack is the one that ends each call.
        using InlineFunctionProbe roomy = new(CamusDBOptions.Default with
        {
            InlineFunctionCallTimeoutMs = 60_000,
            InlineFunctionAllocationLimitBytes = 1L << 30,
        });

        foreach (string body in bodies)
            StoppedOnAPoolStack(roomy, body, "nested too deeply for the stack");
    }

    /// <summary>
    /// The runtime runs a <c>catch</c> or a <c>finally</c> block on top of the stack of the frame that
    /// threw, and gives that stack back only when a <c>catch</c> block ends. A body that throws from
    /// such a block at every level of a recursion goes deeper with each level on its way out. Each of
    /// these bodies ended the process: the first two through the budget check of a <c>finally</c>
    /// block, the last two with no check at all on the way out.
    /// </summary>
    [TestCase("long Deeper(long x) { try { return Deeper(x + 1) + 1; } finally { for (long i = 0; i < 1; i++) { } } } return Deeper(n);")]
    [TestCase("long Leaf(long x) => x; long Deeper(long x) { try { return Deeper(x + 1) + 1; } finally { Leaf(x); } } return Deeper(n);")]
    [TestCase("long Deeper(long x) { try { if (x > 3000) throw new InvalidOperationException(\"end\"); return Deeper(x + 1) + 1; } catch { throw; } } return Deeper(n);")]
    [TestCase("long Deeper(long x) { try { if (x > 3000) throw new InvalidOperationException(\"end\"); return Deeper(x + 1) + 1; } finally { if (x >= 0) throw new InvalidOperationException(\"again\"); } } return Deeper(n);")]
    public void ARecursionThatThrowsAgainOnItsWayOut_FailsTheCallAndNotTheProcess(string body) =>
        StoppedOnAPoolStack(stackProbe, body, "nested too deeply for the stack");

    /// <summary>
    /// The same growth with a stack that is nearly empty: 600 frames, each with a <c>finally</c> block
    /// that has a budget check, and the time limit ends the call at the bottom. Every check on the
    /// way out threw again, and 600 exceptions below each other do not fit in the stack.
    /// </summary>
    [Test]
    public void ATimeStopUnderManyFinallyBlocks_FailsTheCallAndNotTheProcess() =>
        StoppedOnAPoolStack(
            probe,
            "long Deeper(long x) { try { if (x > 600) { while (true) { } } return Deeper(x + 1) + 1; } finally { for (long i = 0; i < 1; i++) { } } } return Deeper(n);",
            "inline_function_call_timeout_ms");

    /// <summary>
    /// The error of a stopped recursion is thrown from the frame of the guard, after the stack of the
    /// recursion is given back. The filter below runs while the runtime looks for the handler of that
    /// error, on top of the frame that threw it: thrown from inside a <c>catch</c> block, that frame
    /// would be at the bottom of the recursion, where the stack is not sufficient.
    /// </summary>
    [Test]
    public void TheErrorOfAStoppedRecursion_IsThrownAfterTheStackIsGivenBack()
    {
        bool? roomWhenThrown = null;
        CamusDBException? stopped = null;

        bool Observe()
        {
            roomWhenThrown = System.Runtime.CompilerServices.RuntimeHelpers.TryEnsureSufficientExecutionStack();
            return true;
        }

        Thread thread = new(
            () =>
            {
                try
                {
                    stackProbe.Call(InlineFunctionProbe.Number("long Deeper(long x) => Deeper(x + 1) + 1; return Deeper(n);"), "x", 5);
                }
                catch (CamusDBException failure) when (Observe())
                {
                    stopped = failure;
                }
            },
            maxStackSize: PoolStackBytes);

        thread.Start();
        thread.Join();

        Assert.IsNotNull(stopped, "the recursion has no end, so the call must fail");
        StringAssert.Contains("nested too deeply for the stack", stopped!.Message);
        Assert.AreEqual(true, roomWhenThrown, "the error was thrown while the stack of the recursion was still in use");
    }

    /// <summary>
    /// The server makes a <c>finally</c> block do nothing after a budget stopped the call. A block
    /// that can be skipped assigns nothing for certain, so these bodies would not compile after the
    /// rewrite without its other half: each reads a variable that only its <c>finally</c> block wrote.
    /// </summary>
    [TestCase("long y; try { y = n; } finally { y = 7; } return y;", 7)]
    [TestCase("long y; void Set() { y = 9; } try { } finally { Set(); } return y;", 9)]
    [TestCase("(long A, long B) t; try { } finally { t.A = 1; t.B = n; } return t.A + t.B;", 6)]
    [TestCase("long a; long b; try { } finally { (a, b) = (n, 2); } return a + b;", 7)]
    [TestCase("long y; try { } finally { long.TryParse(\"12\", out y); } return y;", 12)]
    [TestCase("string @class; try { } finally { @class = \"ab\"; } return @class.Length;", 2)]
    [TestCase("long y; try { } finally { try { } finally { y = n + 1; } } return y;", 6)]
    [TestCase("long Inner(long k) { long y; try { } finally { y = k; long z = 1; y += z; } return y; } return Inner(n);", 6)]
    [TestCase("void Fill(out long y) { try { } finally { y = n + 3; } } Fill(out long v); return v;", 8)]
    [TestCase("void Fill(out long y, out long z) { try { } finally { try { } finally { y = 1; z = n; } } } Fill(out long v, out long w); return v + w;", 6)]
    [TestCase("for (long i, j; ; ) { try { } finally { i = n; j = 4; } return i + j; }", 9)]
    public void AVariableThatAFinallyBlockAssigns_IsStillAssignedAfterIt(string body, long expected) =>
        Assert.AreEqual(expected, probe.Call(InlineFunctionProbe.Number(body), "x", 5).LongValue);

    /// <summary>
    /// The end of a <c>finally</c> block that always throws cannot be reached, so the compiler asks
    /// for no <c>return</c> after its <c>try</c> statement. That must hold after the rewrite too.
    /// </summary>
    [TestCase("long Fail(long k) { try { k++; } finally { throw new InvalidOperationException(\"always \" + k); } } try { return Fail(n); } catch (InvalidOperationException e) { return e.Message.Length; }", 8)]
    [TestCase("try { n++; } finally { throw new InvalidOperationException(\"always\"); }", -1)]
    public void AFinallyBlockThatAlwaysThrows_StillEndsItsMethod(string body, long expected)
    {
        if (expected >= 0)
        {
            Assert.AreEqual(expected, probe.Call(InlineFunctionProbe.Number(body), "x", 5).LongValue);
            return;
        }

        CamusDBException failed = Assert.Throws<CamusDBException>(() => probe.Call(InlineFunctionProbe.Number(body), "x", 5))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, failed.Code, failed.Message);
        StringAssert.Contains("always", failed.Message);
    }

    [Test]
    public void AFinallyBlock_RunsWhenNoBudgetStoppedTheCall()
    {
        const string body =
            "long c = 0; try { try { throw new InvalidOperationException(\"x\"); } finally { c += 10; } } " +
            "catch (InvalidOperationException) { c += 1; } return c + n;";

        Assert.AreEqual(16, probe.Call(InlineFunctionProbe.Number(body), "x", 5).LongValue);
    }

    /// <summary>
    /// The runtime drops an exception that a filter throws and reads the filter as false. The budget
    /// check that failed inside the filter is therefore lost, and the second <c>catch</c> takes the
    /// body's own exception. The check at the start of that block throws the first failure again.
    /// </summary>
    [Test]
    public void ABudgetStopInsideAFilter_StillFailsTheCall() =>
        Stopped(
            "bool Spin() { while (true) { } } try { throw new InvalidOperationException(\"x\"); } " +
            "catch (Exception) when (Spin()) { return 1; } catch { return 2; }",
            "inline_function_call_timeout_ms");

    [Test]
    public void AfterAStoppedCall_TheNextCallHasItsOwnBudget()
    {
        Stopped("while (true) { }", "inline_function_call_timeout_ms");

        Assert.AreEqual(499_500, probe.Call(InlineFunctionProbe.Number("long t = 0; for (long i = 0; i < n; i++) t += i; return t;"), "x", 1000).LongValue);
    }
}
