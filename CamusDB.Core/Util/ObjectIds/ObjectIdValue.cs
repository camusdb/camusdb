
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.ObjectIds;

public record struct ObjectIdValue
{
    public int a;

    public int b;

    public int c;

    public ObjectIdValue(int a, int b, int c)
    {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public override string ToString()
    {
        return ObjectId.ToString(a, b, c);
    }
}
