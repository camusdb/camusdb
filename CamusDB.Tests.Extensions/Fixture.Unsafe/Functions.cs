/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

namespace CamusDB.Fixtures.Unsafe;

/// <summary>Test-only: code compiled with /unsafe, which the SAFE verifier must refuse.</summary>
public static class Functions
{
    [CamusFunction("fx_peek")]
    public static unsafe long Peek(long value)
    {
        long* pointer = &value;
        return *pointer;
    }
}
