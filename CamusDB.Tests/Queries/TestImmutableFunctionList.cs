/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Tests.Queries;

/// <summary>
/// Every built-in scalar function is classified for expression indexes: it is on the immutable list
/// (an index expression may call it) or on the not-immutable list (it reads the clock, the session, a
/// sequence or a random source). A new built-in fails here until someone decides which, so no function
/// reaches an index without that decision.
/// </summary>
internal sealed class TestImmutableFunctionList
{
    [Test]
    public void EveryBuiltInFunction_IsClassified()
    {
        string[] unclassified = [.. ScalarFunctionEvaluator.BuiltInDescriptors
            .Select(d => d.Name)
            .Where(name => !ScalarFunctionRegistry.ImmutableFunctionNames.Contains(name)
                && !ScalarFunctionRegistry.NotImmutableFunctionNames.Contains(name))
            .OrderBy(name => name, System.StringComparer.Ordinal)];

        Assert.That(unclassified, Is.Empty,
            "Add each function to ScalarFunctionRegistry.ImmutableFunctionNames or NotImmutableFunctionNames: " + string.Join(", ", unclassified));
    }

    [Test]
    public void NoFunction_IsOnBothLists()
    {
        string[] both = [.. ScalarFunctionRegistry.ImmutableFunctionNames.Where(ScalarFunctionRegistry.NotImmutableFunctionNames.Contains)];
        Assert.That(both, Is.Empty);
    }

    [Test]
    public void EveryListedName_IsARegisteredFunction()
    {
        string[] registered = [.. ScalarFunctionEvaluator.BuiltInDescriptors.Select(d => d.Name)];

        string[] stale = [.. ScalarFunctionRegistry.ImmutableFunctionNames
            .Concat(ScalarFunctionRegistry.NotImmutableFunctionNames)
            .Where(name => !registered.Contains(name, System.StringComparer.OrdinalIgnoreCase))];

        Assert.That(stale, Is.Empty, "Names that no built-in function has: " + string.Join(", ", stale));
    }

    [Test]
    public void EveryImmutableFunction_CarriesNoVolatileFlag()
    {
        string[] contradicted = [.. ScalarFunctionEvaluator.BuiltInDescriptors
            .Where(d => ScalarFunctionRegistry.ImmutableFunctionNames.Contains(d.Name) && !ScalarFunctionRegistry.IsImmutable(d))
            .Select(d => d.Name)];

        Assert.That(contradicted, Is.Empty, "On the immutable list but flagged volatile, clock or session: " + string.Join(", ", contradicted));
    }
}
