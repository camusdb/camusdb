
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
   
    public const int TypeBool = 6;    

    public const int TypeString8 = 7;
    public const int TypeString16 = 8;
    public const int TypeString32 = 9;
    public const int TypeStringInterned = 10;
    public const int TypeStringDict = 11;
    
    public const int TypeArray8 = 12;
    public const int TypeArray16 = 13;
    public const int TypeArray32 = 14;

    public const int TypeExtended = 15;

    public const int TypeObject8 = 16;
    public const int TypeObject16 = 17;
    public const int TypeObject32 = 18;

    public const int TypeFloat = 19;
    public const int TypeDouble = 20;

    public const int TypeWordsDict = 21;
}

