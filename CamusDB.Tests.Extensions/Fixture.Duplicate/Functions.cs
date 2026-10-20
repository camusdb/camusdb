/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

namespace CamusDB.Fixtures.Duplicate;

/// <summary>Test-only: a function named like one in Fixture.Good, which must fail the boot when
/// both load.</summary>
public static class Functions
{
    [CamusFunction("fx_slugify")]
    public static string Slugify(string value) => value;
}
