/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

namespace CamusDB.Fixtures.NoManifest;

/// <summary>Test-only: an assembly without a CamusExtension manifest, which must fail the boot.</summary>
public static class Functions
{
    [CamusFunction("fx_no_manifest")]
    public static long NoManifest() => 1;
}
