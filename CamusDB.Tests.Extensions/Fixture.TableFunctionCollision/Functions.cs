/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Extensions;

[assembly: CamusExtension("tf_collision", "1.0.0")]

namespace CamusDB.Fixtures.TableFunctionCollision;

/// <summary>Test-only: a table function named like the built-in generate_series, which must fail the boot.</summary>
public static class Functions
{
    [CamusTableFunction("generate_series")]
    public static IEnumerable<long> Series(long n)
    {
        yield return n;
    }
}
