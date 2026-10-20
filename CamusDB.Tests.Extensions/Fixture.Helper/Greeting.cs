/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Fixtures.Helper;

/// <summary>Test-only dependency of Fixture.UsesHelper. Its AssemblyRef is not on the SAFE
/// allow-list, so a SAFE load of Fixture.UsesHelper must fail, and an UNSAFE load must resolve it
/// from the extension's own folder.</summary>
public static class Greeting
{
    public static string Hello(string name) => "hello " + name;
}
