/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// What the body of an inline function may not do. Each case is a body that a non-superuser could
/// send, and the statement must refuse it before any of its code is loaded. The first case is the
/// body that passes every rule of the check for operator extensions and reads a file of the server:
/// it is the reason this feature has a positive list.
/// </summary>
[TestFixture]
public sealed class TestInlineFunctionRefusals
{
    private InlineFunctionProbe probe = null!;

    [OneTimeSetUp]
    public void CreateProbe() => probe = new InlineFunctionProbe();

    [OneTimeTearDown]
    public void DisposeProbe() => probe.Dispose();

    private CamusDBException Refused(string body, params string[] fragments)
    {
        CamusDBException refusal = probe.Refusal(InlineFunctionProbe.Text(body));

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, refusal.Code, refusal.Message);

        foreach (string fragment in fragments)
            StringAssert.Contains(fragment, refusal.Message);

        return refusal;
    }

    [Test]
    public void TheFileReadThroughReflection_IsRefused()
    {
        const string body = """
            Type file = typeof(string).Module.GetType("System.IO.File");
            var read = file.GetMethod("ReadAllText", new[] { typeof(string) });
            var f = (Func<string, string>) read.CreateDelegate(typeof(Func<string, string>));
            return f("/etc/passwd");
            """;

        // The syntax stage stops at typeof.
        CamusDBException atSyntax = Refused(body, "typeof is not allowed");
        StringAssert.Contains("(1,13)", atSyntax.Message, "the finding carries the line and the column in the body");

        // Without typeof the same idea reaches the symbol stage, which names every type on the way.
        Refused(
            "var module = s.GetType().Module; var read = module.GetType(\"System.IO.File\").GetMethod(\"ReadAllText\"); return read.Name;",
            "object.GetType is not allowed", "the type Type is not on the list of allowed types");
    }

    /// <summary>The members that the check for operator extensions lets through, and an untrusted author must not get.</summary>
    [TestCase("return Environment.GetEnvironmentVariable(\"HOME\");", "Environment")]
    [TestCase("return Environment.CommandLine;", "Environment")]
    [TestCase("return System.Runtime.CompilerServices.Unsafe.SizeOf<int>().ToString();", "Unsafe")]
    [TestCase("new System.Threading.Thread(() => { }).Start(); return s;", "Thread")]
    [TestCase("System.Threading.Tasks.Parallel.For(0, 4, i => { }); return s;", "Parallel")]
    [TestCase("var a = System.Buffers.ArrayPool<byte>.Shared.Rent(10); System.Buffers.ArrayPool<byte>.Shared.Return(a); return s;", "ArrayPool")]
    [TestCase("GC.TryStartNoGCRegion(1000); return s;", "GC")]
    [TestCase("System.Diagnostics.Debugger.Launch(); return s;", "Debugger")]
    [TestCase("return System.IO.File.ReadAllText(s);", "File")]
    [TestCase("return new System.Net.Http.HttpClient().ToString();", "CS0234")]
    [TestCase("return System.Diagnostics.Process.GetCurrentProcess().Id.ToString();", "CS0234")]
    [TestCase("return AppDomain.CurrentDomain.BaseDirectory;", "AppDomain")]
    [TestCase("return Activator.CreateInstance<System.Text.StringBuilder>().ToString();", "Activator")]
    [TestCase("Console.WriteLine(s); return s;", "CS0103")]
    public void ADangerousFrameworkMember_IsRefused(string body, string named)
    {
        CamusDBException refusal = probe.Refusal(InlineFunctionProbe.Text(body));

        // A type outside the compiler's references does not bind at all, which is a compile error;
        // a type inside them binds and is refused by the list. Both stop the statement.
        Assert.That(
            refusal.Code,
            Is.EqualTo(CamusDBErrorCodes.InlineFunctionRefused).Or.EqualTo(CamusDBErrorCodes.InlineFunctionCompileFailed),
            refusal.Message);

        StringAssert.Contains(named, refusal.Message);
    }

    [TestCase("return s.GetType().Name;", "object.GetType is not allowed")]
    [TestCase("return typeof(string).Name;", "typeof is not allowed")]
    [TestCase("dynamic d = s; return d.Length.ToString();", "dynamic is not allowed")]
    [TestCase("Span<int> a = stackalloc int[(int)n]; return s;", "stackalloc is not allowed")]
    [TestCase("[System.Runtime.InteropServices.DllImport(\"libc\")] static extern int getpid(); return getpid().ToString();", "extern is not allowed")]
    [TestCase("[Obsolete] string Local() => s; return Local();", "attributes are not allowed")]
    [TestCase("lock (s) { return s; }", "lock is not allowed")]
    [TestCase("using var e = s.GetEnumerator(); return s;", "using declarations are not allowed")]
    [TestCase("using (var e = s.GetEnumerator()) { return s; }", "using is not allowed")]
    [TestCase("await System.Threading.Tasks.Task.Delay(1); return s;", "await is not allowed")]
    [TestCase("yield return s;", "yield is not allowed")]
    [TestCase("again: goto again;", "goto and labels are not allowed")]
    [TestCase("switch (n) { case 1: goto case 1; } return s;", "goto and labels are not allowed")]
    [TestCase("return string.Concat(from c in s select c);", "query expressions")]
    [TestCase("#pragma warning disable\nreturn s;", "preprocessor directives are not allowed")]
    [TestCase("#if DEBUG\nreturn s;\n#endif\nreturn s;", "preprocessor directives are not allowed")]
    [TestCase("#nullable disable\nreturn s;", "preprocessor directives are not allowed")]
    [TestCase("#line 100\nreturn s;", "preprocessor directives are not allowed")]
    [TestCase("#region x\nreturn s;\n#endregion", "preprocessor directives are not allowed")]
    [TestCase("var x = new { A = 1 }; return s;", "anonymous types are not allowed")]
    [TestCase("return s[1..2];", "ranges")]
    [TestCase("return s[^1].ToString();", "index from the end")]
    [TestCase("T Same<T>(T x) => x; return Same(s);", "generic local function")]
    [TestCase("string __camus_x = s; return __camus_x;", "reserved")]
    public void ARefusedLanguageForm_IsRefused(string body, string reason) => Refused(body, reason);

    [TestCase("return string.Format(\"{0,10}\", s);", "string.Format is not allowed")]
    [TestCase("return s.PadLeft(100000000);", "string.PadLeft is not allowed")]
    [TestCase("return new string('x', (int)n);", "string.new is not allowed")]
    [TestCase("var l = new List<int>((int)n); return s;", "List<int>.new is not allowed")]
    [TestCase("var d = new Dictionary<string, int>((int)n); return s;", "Dictionary<string, int>.new is not allowed")]
    [TestCase("var b = new StringBuilder((int)n); return s;", "StringBuilder.new is not allowed")]
    [TestCase("var b = new StringBuilder(); b.Append('x', (int)n); return s;", "StringBuilder.Append is not allowed")]
    [TestCase("var b = new StringBuilder(); b.Length = (int)n; return s;", "StringBuilder.Length is not allowed")]
    [TestCase("var l = new List<int>(); l.Capacity = (int)n; return s;", "List<int>.Capacity is not allowed")]
    [TestCase("return Enumerable.Range(0, (int)n).Count().ToString();", "Enumerable.Range is not allowed")]
    [TestCase("return Enumerable.Repeat(s, (int)n).Count().ToString();", "Enumerable.Repeat is not allowed")]
    [TestCase("var a = new int[2]; Array.Resize(ref a, (int)n); return s;", "Array.Resize is not allowed")]
    [TestCase("return string.Concat(new object[] { s, n });", "object cannot be an element type")]
    [TestCase("return string.Intern(s);", "string.Intern is not allowed")]
    public void AMemberWhoseCostTheInputsDoNotBound_IsRefused(string body, string reason) => Refused(body, reason);

    [TestCase("return DateTime.Now.Ticks.ToString();", "DateTime.Now is not allowed")]
    [TestCase("return DateTime.UtcNow.Ticks.ToString();", "DateTime.UtcNow is not allowed")]
    [TestCase("return DateTime.Today.Ticks.ToString();", "DateTime.Today is not allowed")]
    [TestCase("return new DateTime(2020, 1, 1).ToLocalTime().ToString();", "DateTime.ToLocalTime is not allowed")]
    [TestCase("return DateTime.Parse(s).ToString();", "DateTime.Parse is not allowed")]
    [TestCase("return new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Local).ToString();", "DateTimeKind")]
    [TestCase("return DateTimeOffset.UtcNow.ToString();", "DateTimeOffset")]
    [TestCase("return new Random().Next().ToString();", "Random")]
    [TestCase("return Guid.NewGuid().ToString();", "Guid.NewGuid is not allowed")]
    [TestCase("return s.GetHashCode().ToString();", "string.GetHashCode is not allowed")]
    [TestCase("return n.GetHashCode().ToString();", "long.GetHashCode is not allowed")]
    [TestCase("return HashCode.Combine(s, n).ToString();", "HashCode")]
    [TestCase("return new DateTime(2020, 1, 1).ToString(\"zzz\");", "prints the time zone of the machine")]
    [TestCase("return s.Normalize();", "string.Normalize is not allowed")]
    public void AMemberWhoseResultDependsOnTheNode_IsRefused(string body, string reason) => Refused(body, reason);

    [TestCase("ReadOnlySpan<char> x = s; return x.Length.ToString();", "ReadOnlySpan<char> is not on the list")]
    [TestCase("return s.AsSpan().Length.ToString();", "ReadOnlySpan<char>")]
    [TestCase("return CamusDB.Extensions.Inline.CallBudget.IsBudgetException(null).ToString();", "CS0234")]
    [TestCase("return Body(s, n);", "a body cannot call the function it defines")]
    [TestCase("Action a = () => { }; a += () => { }; a(); return s;", "delegates cannot be combined")]
    [TestCase("Func<int> f = () => 1; return f.Method.Name;", "the type Delegate is not on the list")]

    public void ATypeOrMemberOutsideTheList_IsRefused(string body, string reason)
    {
        CamusDBException refusal = probe.Refusal(InlineFunctionProbe.Text(body));
        StringAssert.Contains(reason, refusal.Message);
    }

    [TestCase("return $\"{s,1025}\";", "alignment must be a number from -1024 to 1024")]
    [TestCase("return $\"{s,-1025}\";", "alignment must be a number from -1024 to 1024")]
    [TestCase("const int w = 5; return $\"{s,w}\";", "alignment must be a number")]
    [TestCase("return n.ToString(s);", "a format must be a constant text")]
    [TestCase("return n.ToString(\"D999\");", "precision of more than two digits")]
    [TestCase("return $\"{n:N123}\";", "precision of more than two digits")]
    [TestCase("object o = n; return $\"{o:N2}\";", "a format is allowed only for")]
    public void AFormatThatCanMakeAHugeText_IsRefused(string body, string reason) => Refused(body, reason);

    [Test]
    public void AFormatInsideTheLimits_IsAccepted()
    {
        Assert.AreEqual("  x|7.00|0007", probe.Call(InlineFunctionProbe.Text("return $\"{s,3}|{n:F2}|\" + n.ToString(\"D4\");"), "x", 7).StrValue);
        Assert.AreEqual(new string(' ', 1023) + "x", probe.Call(InlineFunctionProbe.Text("return $\"{s,1024}\";"), "x", 7).StrValue);
    }

    /// <summary>The rules that keep a value from being nested deeper than the stack at run time.</summary>
    [TestCase("var q = s.Where(c => c != 'a'); return q.Count().ToString();", "a lazy sequence")]
    [TestCase("IEnumerable<char> q = s; return q.Count().ToString();", "a lazy sequence")]
    [TestCase("var qs = new List<IEnumerable<char>>(); return s;", "a lazy sequence")]
    [TestCase("int Count(IEnumerable<char> q) => q.Count(); return Count(s).ToString();", "a lazy sequence")]
    [TestCase("var parts = s.Split(',').Select(p => p.Where(c => c != 'a')).ToList(); return s;", "a lazy sequence")]
    [TestCase("object o = n; var t = (o, 1); return s;", "object cannot be an element type or a type argument")]
    [TestCase("var l = new List<object>(); return s;", "object cannot be an element type or a type argument")]
    [TestCase("var d = new Dictionary<object, int>(); return s;", "object cannot be an element type or a type argument")]
    [TestCase("object o = s; var e = (IEnumerable<char>)o; return s;", "a cast to the interface")]
    [TestCase("object o = s; var e = o as IReadOnlyList<char>; return s;", "a cast to the interface")]
    [TestCase("object o = s; return o is IReadOnlyList<char> ? s : s;", "a test for the interface")]
    [TestCase("const string a = \"aa\"; const string b = a + a; return b;", "a const string is not allowed")]
    public void AFormThatCanNestWithoutLimit_IsRefused(string body, string reason) => Refused(body, reason);

    /// <summary>An exception's text holds the server's stack, so an exception stays an exception.</summary>
    [TestCase("try { throw new Exception(\"x\"); } catch (Exception e) { return e.ToString(); }", "Exception.ToString is not allowed")]
    [TestCase("try { throw new Exception(\"x\"); } catch (Exception e) { return e.StackTrace; }", "Exception.StackTrace is not allowed")]
    [TestCase("try { throw new Exception(\"x\"); } catch (Exception e) { object o = e; return o.ToString(); }", "an exception cannot be converted to object")]
    [TestCase("try { throw new Exception(\"x\"); } catch (Exception e) { return $\"{e}\"; }", "an exception cannot be put in a string")]
    [TestCase("try { throw new Exception(\"x\"); } catch (Exception e) { return \"a\" + e; }", "an exception cannot be converted")]
    [TestCase("var l = new List<Exception>(); return s;", "an exception cannot be an element or a type argument")]
    [TestCase("throw new Exception(\"a\", new Exception(\"b\"));", "an exception cannot hold another exception")]
    public void AnExceptionCannotBecomeText(string body, string reason) => Refused(body, reason);

    [TestCase("return Regex.IsMatch(s, \"a+\").ToString();", "Regex.IsMatch needs a match timeout")]
    [TestCase("return new Regex(\"a+\").IsMatch(s).ToString();", "Regex.new needs a match timeout")]
    [TestCase("return Regex.Replace(s, \"a\", \"b\");", "Regex.Replace needs a match timeout")]
    [TestCase("return new Regex(\"a\", RegexOptions.None, Regex.InfiniteMatchTimeout).IsMatch(s).ToString();", "Regex.InfiniteMatchTimeout is not allowed")]
    [TestCase("return new Regex(\"a\", RegexOptions.Compiled, TimeSpan.FromSeconds(1)).IsMatch(s).ToString();", "RegexOptions.Compiled is not allowed")]
    [TestCase("return new Regex(\"a\", RegexOptions.None, TimeSpan.FromSeconds(1)).Matches(s).Count.ToString();", "MatchCollection.Count is not allowed")]
    [TestCase("return new Regex(\"a\", RegexOptions.None, TimeSpan.FromSeconds(1)).Matches(s).Select(m => m.Value).Count().ToString();", "the result of Matches can only be read with foreach")]
    public void ARegularExpressionWithoutALimit_IsRefused(string body, string reason) => Refused(body, reason);

    [Test]
    public void TextThatLeavesTheBlock_IsNotCompiled()
    {
        // The body closes the method and writes a second one. The body is parsed alone as one block,
        // so the second method is text after the block, and nothing of it is ever a member.
        CamusDBException escaped = probe.Refusal(InlineFunctionProbe.Text("return s; } public static string Evil() { return \"x\";"));

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, escaped.Code, escaped.Message);

        Assert.AreEqual(
            CamusDBErrorCodes.InlineFunctionCompileFailed,
            probe.Refusal(InlineFunctionProbe.Text("return s; /* never closed")).Code);

        Assert.AreEqual(
            CamusDBErrorCodes.InlineFunctionCompileFailed,
            probe.Refusal(InlineFunctionProbe.Text("return \"never closed;")).Code);
    }

    [Test]
    public void ACompilerError_CarriesThePositionInTheBody()
    {
        CamusDBException error = probe.Refusal(InlineFunctionProbe.Text("var x = 1;\nreturn missing;"));

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, error.Code);
        StringAssert.Contains("(2,8)", error.Message);
        StringAssert.Contains("CS0103", error.Message);
        StringAssert.DoesNotContain("Fn_", error.Message, "the message shows the body, not the code around it");
    }

    [Test]
    public void EveryFindingIsListed()
    {
        CamusDBException refusal = Refused("var a = DateTime.Now;\nvar b = s.PadLeft(10);\nreturn Guid.NewGuid().ToString();");

        StringAssert.Contains("(1,", refusal.Message);
        StringAssert.Contains("DateTime.Now", refusal.Message);
        StringAssert.Contains("(2,", refusal.Message);
        StringAssert.Contains("string.PadLeft", refusal.Message);
        StringAssert.Contains("(3,", refusal.Message);
        StringAssert.Contains("Guid.NewGuid", refusal.Message);
    }

    [Test]
    public void ASignatureThatTheBodyCannotUse_IsRefused()
    {
        InlineFunctionDefinition keyword = InlineFunctionProbe.Define("return 1;", ColumnType.Integer64, ("class", ColumnType.String));
        StringAssert.Contains("cannot be a parameter name", probe.Refusal(keyword).Message);

        InlineFunctionDefinition twice = InlineFunctionProbe.Define("return 1;", ColumnType.Integer64, ("a", ColumnType.String), ("A", ColumnType.String));
        StringAssert.Contains("two parameters are named", probe.Refusal(twice).Message);

        InlineFunctionDefinition date = InlineFunctionProbe.Define("return 1;", ColumnType.Integer64, ("d", ColumnType.Date));
        StringAssert.Contains("not accepted", probe.Refusal(date).Message);

        InlineFunctionDefinition bytes = InlineFunctionProbe.Define("return 1;", ColumnType.Integer64);
        bytes.Parameters.Add(new InlineFunctionParameter { Name = "b", Type = ColumnType.Array, ElementType = ColumnType.Bytes });
        StringAssert.Contains("not accepted", probe.Refusal(bytes).Message);
    }
}
