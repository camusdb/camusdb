
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Tests.Queries;

/// <summary>
/// Keeps the function list in <c>CamusDB.Core/Commands/Executor/Controllers/Functions/README.md</c>
/// honest. The list once named <c>DATE</c>, <c>YEAR</c>, <c>DATEDIFF</c> and <c>DATEADD</c> while
/// none of them was registered. Every lowercase name in backticks in the second column of the
/// table must resolve in the default registry.
/// </summary>
public sealed class TestFunctionsReadme
{
    private static readonly Regex FunctionName = new("`([a-z][a-z0-9_]*)`", RegexOptions.CultureInvariant);

    [Test]
    public void EveryListedNameIsRegistered()
    {
        string readme = Path.Combine(
            FindRepositoryRoot(), "CamusDB.Core", "Commands", "Executor", "Controllers", "Functions", "README.md");

        ScalarFunctionRegistry registry = ScalarFunctionRegistry.CreateDefault();
        List<string> listed = new();
        List<string> missing = new();

        foreach (string line in File.ReadAllLines(readme))
        {
            if (!line.StartsWith("| `"))
                continue;

            string[] cells = line.Split('|');
            if (cells.Length < 3)
                continue;

            foreach (Match match in FunctionName.Matches(cells[2]))
            {
                string name = match.Groups[1].Value;
                listed.Add(name);
                if (!registry.IsRegisteredScalarFunction(name))
                    missing.Add(name);
            }
        }

        Assert.That(listed.Count, Is.GreaterThan(50), "the README table was not found or not parsed");
        Assert.That(missing, Is.Empty, "names listed in the Functions README but not registered: " + string.Join(", ", missing));
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
