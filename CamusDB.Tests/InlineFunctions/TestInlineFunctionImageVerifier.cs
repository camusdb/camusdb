/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using NUnit.Framework;

using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The check of a compiled image, on images built here and not by the server's pipeline: this check is
/// the line behind the source check, so it is tested with code that the source check would never let
/// through.
/// </summary>
[TestFixture]
public sealed class TestInlineFunctionImageVerifier
{
    private static byte[] Compile(string source, bool allowUnsafe = false)
    {
        string runtime = Path.GetDirectoryName(typeof(object).Assembly.Location)!;

        CSharpCompilation compilation = CSharpCompilation.Create(
            "image-under-test",
            [CSharpSyntaxTree.ParseText(source)],
            Directory.GetFiles(runtime, "System*.dll").Select(path => (MetadataReference)MetadataReference.CreateFromFile(path)),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: allowUnsafe));

        using MemoryStream stream = new();
        Microsoft.CodeAnalysis.Emit.EmitResult result = compilation.Emit(stream);
        Assert.IsTrue(result.Success, string.Join("\n", result.Diagnostics));

        return stream.ToArray();
    }

    private static string Class(string body) =>
        "using System; using System.Collections.Generic; using System.Linq;\npublic static class C { public static string Run(string s) { " + body + " } }";

    [Test]
    public void AnImageThatOnlyUsesListedMembers_Passes()
    {
        IReadOnlyList<string> findings = InlineFunctionImageVerifier.Verify(Compile(Class(
            "var parts = new List<string>(); foreach (char c in s) parts.Add($\"{c}\"); return string.Join(\"-\", parts.Select(p => p.Trim()));")));

        CollectionAssert.IsEmpty(findings);
    }

    [Test]
    public void TheReflectionPathToAFile_IsNamedMemberByMember()
    {
        IReadOnlyList<string> findings = InlineFunctionImageVerifier.Verify(Compile(Class(
            "Type file = typeof(string).Module.GetType(\"System.IO.File\"); " +
            "var read = file.GetMethod(\"ReadAllText\", new[] { typeof(string) }); " +
            "var f = (Func<string, string>) read.CreateDelegate(typeof(Func<string, string>)); return f(s);")));

        string all = string.Join("\n", findings);

        StringAssert.Contains("type System.Type", all);
        StringAssert.Contains("type System.Reflection.Module", all);
        StringAssert.Contains("type System.Reflection.MethodInfo", all);
        StringAssert.Contains("member System.Reflection.Module.GetType", all);
        StringAssert.Contains("member System.Type.GetMethod", all);
        StringAssert.Contains("member System.Reflection.MethodInfo.CreateDelegate", all);
    }

    [TestCase("return System.IO.File.ReadAllText(s);", "type System.IO.File")]
    [TestCase("return Environment.GetEnvironmentVariable(s);", "type System.Environment")]
    [TestCase("new System.Threading.Thread(() => { }).Start(); return s;", "type System.Threading.Thread")]
    [TestCase("return s.GetHashCode().ToString();", "member System.Object.GetHashCode")]
    [TestCase("return s.PadLeft(10);", "member System.String.PadLeft")]
    [TestCase("return new string('x', s.Length);", "member System.String..ctor(char,int)")]
    [TestCase("return new System.Text.StringBuilder(s.Length).ToString();", "member System.Text.StringBuilder..ctor(int)")]
    [TestCase("return string.Format(s, 1);", "member System.String.Format")]
    [TestCase("return DateTime.Now.ToString();", "member System.DateTime.get_Now")]
    [TestCase("return Enumerable.Range(0, s.Length).Count().ToString();", "member System.Linq.Enumerable.Range")]
    [TestCase("return System.Runtime.InteropServices.Marshal.SizeOf<int>().ToString();", "type System.Runtime.InteropServices.Marshal")]
    [TestCase("return System.Runtime.CompilerServices.Unsafe.SizeOf<int>().ToString();", "type System.Runtime.CompilerServices.Unsafe")]
    [TestCase("return System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(s).ToString();", "member System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode")]
    [TestCase("return System.Runtime.InteropServices.CollectionsMarshal.AsSpan(new List<int>()).ToArray().Length.ToString();", "member System.Span`1.ToArray")]
    public void AForbiddenReference_IsFound(string body, string finding)
    {
        IReadOnlyList<string> findings = InlineFunctionImageVerifier.Verify(Compile(Class(body)));

        Assert.That(findings, Has.Some.StartsWith(finding), string.Join("\n", findings));
    }

    [Test]
    public void ANativeImport_IsFound()
    {
        IReadOnlyList<string> findings = InlineFunctionImageVerifier.Verify(Compile(
            "public static class C { [System.Runtime.InteropServices.DllImport(\"libc\")] public static extern int getpid(); }"));

        Assert.That(findings, Has.Some.Contains("a native import"));
    }

    [Test]
    public void AnAssemblyOutsideTheList_IsFound()
    {
        IReadOnlyList<string> findings = InlineFunctionImageVerifier.Verify(Compile(Class(
            "return new System.Collections.Concurrent.ConcurrentDictionary<string, string>().Count.ToString();")));

        Assert.That(findings, Has.Some.EqualTo("assembly reference System.Collections.Concurrent"));
    }

    [Test]
    public void TheBudgetMembersOfTheServer_AreNotOpenToTheBody()
    {
        // The rewrite calls Tick, NewArray, IsBudgetException and RegexTimeout. Enter and Exit start
        // and end a budget; a body that could call them could give itself a new one.
        string budget = typeof(CamusDB.Extensions.Inline.CallBudget).Assembly.Location;
        string runtime = Path.GetDirectoryName(typeof(object).Assembly.Location)!;

        CSharpCompilation compilation = CSharpCompilation.Create(
            "image-under-test",
            [CSharpSyntaxTree.ParseText(
                "public static class C { public static void Run() { " +
                "CamusDB.Extensions.Inline.CallBudget.Tick(); " +
                "var f = CamusDB.Extensions.Inline.CallBudget.Enter(0, 0, 0, default); CamusDB.Extensions.Inline.CallBudget.Exit(f); } }")],
            Directory.GetFiles(runtime, "System*.dll").Select(path => (MetadataReference)MetadataReference.CreateFromFile(path))
                .Append(MetadataReference.CreateFromFile(budget)),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        using MemoryStream stream = new();
        Assert.IsTrue(compilation.Emit(stream).Success);

        IReadOnlyList<string> findings = InlineFunctionImageVerifier.Verify(stream.ToArray());

        Assert.That(findings, Has.Some.StartsWith("member CamusDB.Extensions.Inline.CallBudget.Enter"));
        Assert.That(findings, Has.Some.StartsWith("member CamusDB.Extensions.Inline.CallBudget.Exit"));
        Assert.That(findings, Has.None.Contains("CallBudget.Tick"));
    }

    [Test]
    public void EveryHelperMember_NamesAHelperTypeOrAListedType()
    {
        // A helper member whose type is on neither list could never match a reference, so it would be
        // a dead entry that reads as if it allowed something.
        foreach (string member in InlineFunctionImageVerifier.HelperMembers)
        {
            string withoutSignature = member.Contains('(') ? member[..member.IndexOf('(')] : member;
            string type = withoutSignature.EndsWith("..ctor", StringComparison.Ordinal)
                ? withoutSignature[..^6]
                : withoutSignature[..withoutSignature.LastIndexOf('.')];

            Assert.IsTrue(
                InlineFunctionImageVerifier.HelperTypes.Contains(type) || InlineFunctionAllowList.IsListed(type) || type == "System.Array",
                $"helper member {member} names the type {type}, which is on no list");
        }
    }
}
