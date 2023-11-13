
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Serializer.Models;

/**
 * These constants represent value types. 
 * Keep these values without modification as these are written to disk
 */
public static class SerializatorTypes
{
    public const int TypeNull = 0;

    public const int TypeId = 1;

    public const int TypeInteger4 = 2;
    public const int TypeInteger8 = 3;
    public const int TypeInteger16 = 4;
    public const int TypeInteger32 = 5;
    public const int TypeInteger64 = 6;
   
    public const int TypeBool = 7;    

    public const int TypeString8 = 8;
    public const int TypeString16 = 9;
    public const int TypeString32 = 10;
    public const int TypeStringInterned = 11;
    public const int TypeStringDict = 12;

    public const int TypeFloat = 13;
    public const int TypeDouble = 14;

    public const int TypeExtended = 15; // Must be this fixed value 

    public const int TypeArray8 = 16;
    public const int TypeArray16 = 17;
    public const int TypeArray32 = 18;

    public const int TypeObject8 = 19;
    public const int TypeObject16 = 19;
    public const int TypeObject32 = 20;    

    public const int TypeWordsDict = 22;
}

