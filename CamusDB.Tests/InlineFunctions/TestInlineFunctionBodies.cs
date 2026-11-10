/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// What the body of an inline function can do: one body for each ordinary language form, compiled,
/// loaded and called. A body here that stops compiling means the compiler began to emit a helper that
/// the image check does not list (see <see cref="InlineFunctionImageVerifier.HelperMembers"/>), or
/// that a rule of the symbol check became too wide. The cases are the pin for both lists.
/// </summary>
[TestFixture]
public sealed class TestInlineFunctionBodies
{
    private InlineFunctionProbe probe = null!;

    [OneTimeSetUp]
    public void CreateProbe() => probe = new InlineFunctionProbe();

    [OneTimeTearDown]
    public void DisposeProbe() => probe.Dispose();

    private const string Text = "b, a;c";

    [TestCase("return s.Trim().ToLowerInvariant().Replace(' ', '-');", "b,-a;c")]
    [TestCase("return $\"{s}|{n}|{n:N2}|{new DateTime(2024, 1, 2):yyyy-MM-dd}|{1.5m,8:F3}|{true}|{'c'}|{1.5}|{Guid.Empty:N}\";", "b, a;c|7|7.00|2024-01-02|   1.500|True|c|1.5|00000000000000000000000000000000")]
    [TestCase("object o = n; return $\"{o}-{s.Length}\";", "7-6")]
    [TestCase("return s + n + 'c' + 1.5 + true + s;", "b, a;c7c1.5Trueb, a;c")]
    [TestCase("char c = 'z'; return s + c;", "b, a;cz")]
    [TestCase("return s + s + s + s + s + s;", "b, a;cb, a;cb, a;cb, a;cb, a;cb, a;c")]
    [TestCase("return string.Join(\",\", s, \"x\", \"y\", \"z\", \"w\");", "b, a;c,x,y,z,w")]
    [TestCase("return string.Concat(s, \"x\", \"y\", \"z\", \"w\");", "b, a;cxyzw")]
    [TestCase("return s.Trim(' ', 'b').TrimStart('a', ',').TrimEnd(new[] { 'c' });", " a;")]
    [TestCase("return string.Join(\"|\", s.Split(',', ';').Select(p => p.Trim()).OrderBy(p => p));", "a|b|c")]
    [TestCase("return string.Join(\";\", s.Where(char.IsLetter).GroupBy(c => c).OrderBy(g => g.Key).Select(g => g.Key + \"=\" + g.Count()));", "a=1;b=1;c=1")]
    [TestCase("var sb = new StringBuilder(s); sb.Append(n).Append('x').Insert(0, \"<\").Replace(\"a\", \"A\").Remove(0, 1); sb[0] = 'Z'; return sb.ToString() + sb.Length;", "Z, A;c7x8")]
    [TestCase("return Regex.Replace(s, \"[a-c]\", m => m.Value.ToUpperInvariant(), RegexOptions.None, TimeSpan.FromMilliseconds(500));", "B, A;C")]
    [TestCase("var g = Guid.Parse(\"6f9619ff-8b86-d011-b42d-00c04fc964ff\"); return g.ToString(\"N\") + (g == Guid.Empty);", "6f9619ff8b86d011b42d00c04fc964ffFalse")]
    [TestCase("var d = new DateTime(2024, 1, 31); var e = d + TimeSpan.FromHours(n); return e.ToString(\"o\") + \"|\" + DateOnly.FromDateTime(e).ToString(\"yyyy-MM-dd\") + \"|\" + (e - d).TotalMinutes + \"|\" + e.DayOfWeek;", "2024-01-31T07:00:00.0000000|2024-01-31|420|Wednesday")]
    [TestCase("var day = new DateTime(2024, 1, 1).DayOfWeek; return day.ToString() + (int)day + (day == DayOfWeek.Monday) + Enum.GetName(day) + ((DayOfWeek)3);", "Monday1TrueMondayWednesday")]
    [TestCase("return n switch { < 0 => \"neg\", 0 => \"zero\", > 0 and < 10 => \"small\", _ => \"big\" };", "small")]
    [TestCase("var xs = new List<string> { \"bb\", \"a\", \"ccc\" }; xs.Sort((x, y) => y.Length.CompareTo(x.Length)); return xs[0];", "ccc")]
    [TestCase("decimal d = n; return (d / 3m).ToString(\"F4\");", "2.3333")]
    public void ABodyThatReturnsText(string body, string expected) =>
        Assert.AreEqual(expected, probe.Call(InlineFunctionProbe.Text(body), Text, 7).StrValue);

    [TestCase("long t = 0; for (long i = 0; i < n; i++) t += i; return t;", 21)]
    [TestCase("var xs = new List<long>(); for (long i = 0; i < n; i++) xs.Add(i); return xs.Where(x => x % 2 == 0).Select(x => x * x).Sum();", 56)]
    [TestCase("List<long> xs = [1, 2, 3, n]; return xs.Max();", 7)]
    [TestCase("IReadOnlyList<long> xs = [1, 2, n]; return xs.Count + xs[2];", 10)]
    [TestCase("long[] xs = [1, 2, n]; return xs.Length;", 3)]
    [TestCase("long[] xs = [1, 2]; List<long> ys = [..xs, n, ..xs]; return ys.Count;", 5)]
    [TestCase("HashSet<long> xs = [1, 2, n, n]; return xs.Count;", 3)]
    [TestCase("List<string> xs = []; long[] ys = []; return xs.Count + ys.Length;", 0)]
    [TestCase("long[] xs = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }; return xs.Sum();", 210)]
    [TestCase("byte[] xs = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }; return xs.Length;", 10)]
    [TestCase("long[] xs = { 1, 2, 3 }; int[] ys = new int[3]; ys[0] = (int)n; return xs[1] + ys[0];", 9)]
    [TestCase("var a = new long[n][]; a[0] = new long[2]; return a.Length + a[0].Length;", 9)]
    [TestCase("var m = new long[3, 4]; m[1, 2] = n; return m[1, 2] + m.GetLength(1);", 11)]
    [TestCase("switch (s) { case \"a\": return 1; case \"b\": return 2; case \"c\": return 3; case \"d\": return 4; case \"e\": return 5; case \"f\": return 6; case \"b, a;c\": return 7; default: return 0; }", 7)]
    [TestCase("switch (n) { case 1: case 2: return 10; case 7: return 70; default: return 0; }", 70)]
    [TestCase("object o = n; if (o is long l && l is > 5 and < 10) return l; if (o is string str) return str.Length; return -1;", 7)]
    [TestCase("return s is { Length: > 3 } ? 1 : 0;", 1)]
    [TestCase("long[] xs = [1, n, 3]; return xs is [1, var mid, 3] ? mid : -1;", 7)]
    [TestCase("var (a, b) = (n, n * 2); (a, b) = (b, a); return a - b;", 7)]
    [TestCase("(long sum, long count) Acc((long sum, long count) acc, long x) => (acc.sum + x, acc.count + 1); var r = Acc((0, 0), n); return r.sum + r.count;", 8)]
    [TestCase("var t = (n, s); return t == (7, \"b, a;c\") ? 1 : 0;", 1)]
    [TestCase("long? x = n > 5 ? n : null; long? y = null; return (x ?? 0) + (y ?? 1) + (x.HasValue ? x.Value : 0) + x.GetValueOrDefault();", 22)]
    [TestCase("string? t = n > 100 ? s : null; return (t?.Length ?? -1) + (s?.Trim().Length ?? 0);", 5)]
    [TestCase("try { checked { long big = long.MaxValue; return big + n; } } catch (OverflowException) { return -1; }", -1)]
    [TestCase("var fs = new List<Func<long>>(); for (long i = 0; i < 3; i++) { long j = i; fs.Add(() => j * n); } return fs.Sum(f => f());", 21)]
    [TestCase("long Sum(params long[] xs) { long t = 0; foreach (var x in xs) t += x; return t; } return Sum(1, 2, n);", 10)]
    [TestCase("void Twice(ref long x, out long old) { old = x; x *= 2; } long v = n; Twice(ref v, out long o); return v + o;", 21)]
    [TestCase("return long.TryParse(s, out long v) ? v : int.TryParse(\"12\", out var w) ? w : 0;", 12)]
    [TestCase("long[] xs = [3, 1, n]; Array.Sort(xs); Array.Reverse(xs); return xs[0];", 7)]
    [TestCase("static long Fact(long x) => x <= 1 ? 1 : x * Fact(x - 1); return Fact(n);", 5040)]
    [TestCase("long k = n; Func<long, long> add = x => x + k; return add(1);", 8)]
    [TestCase("var xs = new List<List<long>> { new() { 1, 2 }, new() { n } }; return xs.SelectMany(x => x).Sum() + xs.SelectMany(x => x.Where(y => y > 1).ToList()).Count();", 12)]
    [TestCase("long t = 0; foreach (char c in s) t += c; foreach (var x in new long[] { 1, 2 }) t += x; foreach (var x in new List<long> { 3 }) t += x; foreach (var x in new HashSet<long> { 4 }) t += x; foreach (var k in new Dictionary<long, long> { [5] = 6 }.Keys) t += k; foreach (var x in new Queue<long>(new long[] { 7 })) t += x; foreach (var x in new Stack<long>(new long[] { 8 })) t += x; foreach (var x in new SortedSet<long> { 9 }) t += x; foreach (var kv in new SortedDictionary<long, long> { [10] = 11 }) t += kv.Key + kv.Value; return t;", 489)]
    [TestCase("var d = new Dictionary<string, long> { [\"a\"] = 1, [\"b\"] = n }; long t = 0; foreach (var kv in d) t += kv.Value; foreach (var (k, v) in d) t += k.Length; return t;", 10)]
    [TestCase("var d = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase); d[\"A\"] = 1; d.Add(\"b\", 2); d.TryAdd(\"a\", 9); return d[\"a\"] + (d.TryGetValue(\"B\", out var v) ? v : 0) + d.Count + d.GetValueOrDefault(\"q\", 5) + d.Keys.Count + d.Values.Sum();", 15)]
    [TestCase("var r = new Regex(\"[a-c]\", RegexOptions.IgnoreCase, TimeSpan.FromMilliseconds(500)); long t = 0; foreach (Match m in r.Matches(s)) { t += m.Index; foreach (Group g in m.Groups) t += g.Length; } var m0 = r.Match(s); return t + (m0.Success ? m0.Value.Length : 0);", 12)]
    [TestCase("return (long)(Math.Sqrt(n) * 1000) + Math.Abs(-n) + Math.Max(n, 3) + (long)Math.Round(2.5, MidpointRounding.AwayFromZero) + (long)Math.Pow(2, 10) + long.Clamp(n, 0, 5);", 3691)]
    [TestCase("return Convert.ToInt64(\"42\") + Convert.ToInt32(3.7) + (long)Convert.ToDouble(\"1.5\", CultureInfo.InvariantCulture) + Convert.ToString(255, 16).Length + Convert.ToBase64String(new byte[] { 1, 2, 3 }).Length + Convert.ToHexString(BitConverter.GetBytes(n)).Length;", 69)]
    [TestCase("long t = 0, i = 0; while (i < n) { i++; if (i == 2) continue; t += i; } do { t--; } while (t > 20); for (int j = 0; j < 3; j++) { if (j == 2) break; t += j; } return t;", 21)]
    [TestCase("Func<long, long> f = x => x > 0 ? x : throw new ArgumentOutOfRangeException(\"x\"); Func<long, long> g = x => throw new InvalidOperationException(\"no\"); try { return g(1); } catch (InvalidOperationException e) { return f(n) + e.Message.Length; }", 9)]
    [TestCase("try { try { throw new FormatException(\"a\"); } catch (FormatException) { try { throw new ArgumentException(\"b\"); } catch { return 5; } } finally { n.ToString(); } } catch (Exception e) when (e.Message == \"zz\") { return 6; }", 5)]
    [TestCase("long t = 0; Action<long> add = x => t += x; new List<long> { 1, 2, n }.ForEach(add); Action bump = () => t++; bump(); return t;", 11)]
    [TestCase("Func<long, long> f = delegate (long x) { return x + 1; }; Predicate<long> p = x => x > 3; Comparison<long> c = (x, y) => x.CompareTo(y); return f(n) + (p(n) ? 1 : 0) + c(1, 2);", 8)]
    [TestCase("long d = default; string? z = default(string); return d + nameof(n).Length + (z is null ? 1 : 0);", 2)]
    [TestCase("var q = new Queue<long>(); q.Enqueue(n); q.Enqueue(2); var st = new Stack<long>(); st.Push(q.Dequeue()); st.Push(q.Peek()); return st.Pop() * 10 + st.Peek() + q.Count;", 28)]
    [TestCase("var xs = new List<long> { 1, 2, 3, n }; return xs.Distinct().Skip(1).Take(2).Reverse().Aggregate(0L, (acc, x) => acc * 10 + x) + xs.Zip(xs, (x, y) => x * y).Max() + (xs.Any(x => x > 5) ? 1 : 0) + xs.ToDictionary(x => x, x => x * 2)[n] + xs.ToHashSet().Count + xs.First() + xs.Last() + (long)xs.Average();", 111)]
    public void ABodyThatReturnsANumber(string body, long expected) =>
        Assert.AreEqual(expected, probe.Call(InlineFunctionProbe.Number(body), Text, 7).LongValue);

    [Test]
    public void NullArguments_FollowTheNullRule()
    {
        InlineFunctionDefinition strict = InlineFunctionProbe.Text("return \"called\";");

        Assert.AreEqual(
            ColumnType.Null,
            probe.Call(strict, ColumnValue.Null, new ColumnValue(ColumnType.Integer64, 1)).Type,
            "RETURNS NULL ON NULL INPUT: a NULL argument gives NULL and the body does not run");

        InlineFunctionDefinition called = InlineFunctionProbe.Text("return s is null ? \"null:\" + (n ?? -1) : s;");
        called.NullInput = InlineFunctionNullInput.Called;

        Assert.AreEqual("null:-1", probe.Call(called, ColumnValue.Null, ColumnValue.Null).StrValue, "CALLED ON NULL INPUT: the body sees null");
        Assert.AreEqual(ColumnType.Null, probe.Call(InlineFunctionProbe.Text("return null;"), "x", 1).Type, "return null is SQL NULL");
    }

    [Test]
    public void AnException_FailsTheCallWithItsMessageAndNoStack()
    {
        CamusDBException failed = Assert.Throws<CamusDBException>(
            () => probe.Call(InlineFunctionProbe.Text("throw new ArgumentException(\"bad \" + s);"), "input", 1))!;

        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, failed.Code);
        Assert.AreEqual("Function 'f' failed: bad input", failed.Message);
    }

    [Test]
    public void ArraysAndDates_CrossTheBoundary()
    {
        InlineFunctionDefinition sum = InlineFunctionProbe.Define("return xs.Sum(x => x ?? 0) + xs.Count;", ColumnType.Integer64);
        sum.Parameters.Add(new InlineFunctionParameter { Name = "xs", Type = ColumnType.Array, ElementType = ColumnType.Integer64 });

        ColumnValue array = ColumnValue.FromArray(
            ColumnType.Integer64,
            [new ColumnValue(ColumnType.Integer64, 1), ColumnValue.Null, new ColumnValue(ColumnType.Integer64, 5)]);

        Assert.AreEqual(9, probe.Call(sum, array).LongValue, "a NULL element reaches the body as null");

        InlineFunctionDefinition split = InlineFunctionProbe.Define("return s.Split(',');", ColumnType.Array, ("s", ColumnType.String));
        split.ReturnElementType = ColumnType.String;

        ColumnValue parts = probe.Call(split, new ColumnValue(ColumnType.String, "a,b"));
        Assert.AreEqual(ColumnType.Array, parts.Type);
        Assert.AreEqual(2, parts.ArrayValues!.Count);

        // A date the body builds has no time zone of its own. It is stored digit for digit, on every
        // node, whatever the zone of the machine.
        InlineFunctionDefinition date = InlineFunctionProbe.Define(
            "return new DateTime(2024, 2, 29, 10, 30, 0).AddDays(n);", ColumnType.DateTime, ("n", ColumnType.Integer64));

        ColumnValue stored = probe.Call(date, new ColumnValue(ColumnType.Integer64, 1));
        Assert.AreEqual(new DateTime(2024, 3, 1, 10, 30, 0, DateTimeKind.Utc).Ticks, stored.LongValue);
    }

    [Test]
    public void TheResult_DoesNotDependOnTheCultureOfTheThread()
    {
        InlineFunctionDefinition format = InlineFunctionProbe.Define(
            "return (n / 4.0).ToString() + \"|\" + (1234567.5m).ToString(\"N2\") + \"|\" + s.ToUpper() + \"|\" + new DateTime(2024, 3, 1).ToString() + \"|\" + double.Parse(\"1.5\");",
            ColumnType.String, ("s", ColumnType.String), ("n", ColumnType.Integer64));

        string invariant = probe.Call(format, "title", 10).StrValue!;
        Assert.AreEqual("2.5|1,234,567.50|TITLE|03/01/2024 00:00:00|1.5", invariant);

        CultureInfo before = Thread.CurrentThread.CurrentCulture;

        try
        {
            // A comma for the decimal point, a dotless capital i, and day before month.
            foreach (string culture in new[] { "de-DE", "tr-TR", "fr-FR" })
            {
                Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo(culture);
                Assert.AreEqual(invariant, probe.Call(format, "title", 10).StrValue, culture);
                Assert.AreEqual(culture, Thread.CurrentThread.CurrentCulture.Name, "the culture of the thread is put back after the call");
            }
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = before;
        }
    }

    [Test]
    public void TwoEngines_GiveTheSameBytes()
    {
        using InlineFunctionProbe other = new();

        string[] bodies =
        [
            "return (n / 7.0).ToString(\"R\") + Math.Sqrt(n) + (1m / 3m) + s.ToUpperInvariant();",
            "var d = new Dictionary<string, long>(); foreach (var p in s.Split(',')) d[p] = p.Length; return string.Join(\"|\", d.Select(kv => kv.Key + kv.Value));",
            "var h = new HashSet<string>(s.Split(',')); h.Remove(\"b\"); h.Add(\"zz\"); return string.Join(\"|\", h);",
        ];

        foreach (string body in bodies)
        {
            InlineFunctionDefinition function = InlineFunctionProbe.Text(body);
            Assert.AreEqual(probe.Call(function, "a,b,c,dd", 22).StrValue, other.Call(function, "a,b,c,dd", 22).StrValue, body);
        }
    }
}
