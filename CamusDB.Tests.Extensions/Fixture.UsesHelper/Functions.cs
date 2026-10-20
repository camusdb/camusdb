/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;
using CamusDB.Fixtures.Helper;

namespace CamusDB.Fixtures.UsesHelper;

/// <summary>Test-only: a function that calls a private dependency assembly.</summary>
public static class Functions
{
    [CamusFunction("fx_helper_hello")]
    public static string Hello(string name) => Greeting.Hello(name);
}
