
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Serializer.Models;

public static class SerializatorTypeSizes
{
    public const int TypeNull = 1;
    public const int TypeInteger4 = 1;
    public const int TypeInteger8 = 1;
    public const int TypeInteger16 = 2;
    public const int TypeInteger32 = 4;
    public const int TypeUnsignedInteger8 = 1;
    public const int TypeUnsignedInteger16 = 2;
    public const int TypeUnsignedInteger32 = 4;
    public const int TypeEmptyString = 1;
    public const int TypeBool = 1;
    public const int TypeFloat = 2;
    public const int TypeDouble = 4;
}

