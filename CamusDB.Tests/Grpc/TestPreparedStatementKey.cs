/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;

using NUnit.Framework;

using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Guards the client's registration cache key.
///
/// <para>The key once joined database and SQL with a separator character, on the assumption that some
/// character neither could contain existed. None does: both arrive from the caller unvalidated, and
/// SQL can carry almost anything inside a line comment (the lexer's <c>DotChr</c> is "any character
/// except CR/LF"). Two different statements could therefore share one registration, so a statement
/// executed SQL its caller never wrote. These tests pin the structured key that replaced it.</para>
/// </summary>
public sealed class TestPreparedStatementKey
{
    [Test]
    public void PairsThatWouldCollideUnderAnyDelimiterStayDistinct()
    {
        // For every plausible separator, the two pairs below concatenate identically. A structured
        // key compares the parts, so no separator can make them equal.
        foreach (char separator in new[] { '\0', '|', ' ', '\n', ':', '\u001f' })
        {
            PreparedStatementKey first = new("a", $"SELECT 1 --{separator}SELECT 2");
            PreparedStatementKey second = new($"a{separator}SELECT 1 --", "SELECT 2");

            Assert.That(first, Is.Not.EqualTo(second),
                $"pairs separated by U+{(int)separator:X4} must not alias");
        }
    }

    [Test]
    public void EqualPairsAreEqualAndHashAlike()
    {
        PreparedStatementKey one = new("db", "SELECT name FROM robots WHERE year = @year");
        PreparedStatementKey two = new("db", "SELECT name FROM robots WHERE year = @year");

        Assert.That(one, Is.EqualTo(two));
        Assert.That(one.GetHashCode(), Is.EqualTo(two.GetHashCode()));
    }

    [Test]
    public void ComparisonIsOrdinalNotCultureSensitive()
    {
        // Culture-sensitive comparison would treat these as equal in some locales; SQL text and
        // database names are byte-for-byte identities, not natural-language strings.
        Assert.That(
            new PreparedStatementKey("db", "SELECT 1"),
            Is.Not.EqualTo(new PreparedStatementKey("db", "SELECT 11")));

        Dictionary<PreparedStatementKey, int> map = new()
        {
            [new PreparedStatementKey("db", "SELECT a")] = 1,
        };

        Assert.That(map.ContainsKey(new PreparedStatementKey("db", "SELECT A")), Is.False);
        Assert.That(map.ContainsKey(new PreparedStatementKey("DB", "SELECT a")), Is.False);
    }

    [Test]
    public void NoSourceFileContainsANulByte()
    {
        // A NUL in a .cs file makes git and grep classify it as binary, so its diff stops rendering
        // and normal review misses whatever changed in it. That is how the delimiter above survived
        // review in the first place.
        string root = FindRepositoryRoot();
        List<string> offenders = [];

        foreach (string path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}") ||
                path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}"))
                continue;

            if (File.ReadAllBytes(path).Contains((byte)0))
                offenders.Add(Path.GetRelativePath(root, path));
        }

        Assert.That(offenders, Is.Empty, "source files must be text: " + string.Join(", ", offenders));
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? dir = new(TestContext.CurrentContext.TestDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "CamusDB.sln")))
            dir = dir.Parent;

        Assert.That(dir, Is.Not.Null, "could not locate the repository root");
        return dir!.FullName;
    }
}
