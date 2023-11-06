
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeConfig
{
    public const int Layout = 1;

    // max children per B-tree node = M-1 (must be even and greater than 2)
    public const int MaxChildren = 64;

    public const int MaxChildrenHalf = MaxChildren / 2;
}
