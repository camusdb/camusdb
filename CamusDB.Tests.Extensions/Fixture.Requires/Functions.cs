/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

[assembly: CamusExtension("needs_good", "1.0.0", Requires = new[] { "good" })]

namespace CamusDB.Fixtures.Requires;

/// <summary>Test-only: an extension that requires "good", so it installs only after it.</summary>
public static class Functions
{
    [CamusFunction("fx_needs_good")]
    public static string NeedsGood(string value) => "needs:" + value;
}
