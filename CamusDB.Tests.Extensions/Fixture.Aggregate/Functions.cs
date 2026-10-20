/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

namespace CamusDB.Fixtures.Aggregate;

/// <summary>Test-only: a scalar function named like a built-in aggregate, which must fail the boot.</summary>
public static class Functions
{
    [CamusFunction("sum")]
    public static long Sum(long value) => 999;
}
