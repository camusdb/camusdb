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

    private InlineFunctionProbe probe = null!;

    [OneTimeSetUp]
    public void CreateProbe() => probe = new InlineFunctionProbe(CamusDBOptions.Default with
    {
        InlineFunctionCallTimeoutMs = TimeoutMs,
        InlineFunctionAllocationLimitBytes = 4 * 1024 * 1024,
        InlineFunctionArrayLimit = 10_000,
    });

    [OneTimeTearDown]
    public void DisposeProbe() => probe.Dispose();

    private CamusDBException Stopped(string body, string reason)
    {
        Stopwatch watch = Stopwatch.StartNew();

        CamusDBException stopped = Assert.Throws<CamusDBException>(
            () => probe.Call(InlineFunctionProbe.Number(body), "x", 5))!;

        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped.Code, stopped.Message);
        StringAssert.Contains(reason, stopped.Message);
        Assert.Less(watch.ElapsedMilliseconds, TimeoutMs + SlackMs, "the call must end soon after its limit");

        return stopped;
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
        Stopped(body, "nested too deeply");

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
        {
            CamusDBException? stopped = null;
            Exception? other = null;

            Thread thread = new(
                () =>
                {
                    try
                    {
                        roomy.Call(InlineFunctionProbe.Number(body), "x", 5);
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
                maxStackSize: 1_572_864);

            thread.Start();
            thread.Join();

            Assert.IsNull(other, other?.ToString());
            Assert.IsNotNull(stopped, "the recursion has no end, so the call must fail");
            Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped!.Code, stopped.Message);
            StringAssert.Contains("nested too deeply for the stack", stopped.Message);
        }
    }

    [Test]
    public void AfterAStoppedCall_TheNextCallHasItsOwnBudget()
    {
        Stopped("while (true) { }", "inline_function_call_timeout_ms");

        Assert.AreEqual(499_500, probe.Call(InlineFunctionProbe.Number("long t = 0; for (long i = 0; i < n; i++) t += i; return t;"), "x", 1000).LongValue);
    }
}
