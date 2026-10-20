/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

namespace CamusDB.Fixtures.Collision;

/// <summary>Test-only: a function named like a built-in, which must fail the boot.</summary>
public static class Functions
{
    [CamusFunction("upper")]
    public static string Upper(string value) => value;
}
