﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.ObjectIds;

public record struct ObjectIdValue : IComparable<ObjectIdValue>
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

    public byte[] ToBytes()
    {
        byte[] buffer = new byte[12];

        buffer[0] = (byte)((a >> 0) & 0xff);
        buffer[1] = (byte)((a >> 8) & 0xff);
        buffer[2] = (byte)((a >> 16) & 0xff);
        buffer[3] = (byte)((a >> 24) & 0xff);

        buffer[4] = (byte)((b >> 0) & 0xff);
        buffer[5] = (byte)((b >> 8) & 0xff);
        buffer[6] = (byte)((b >> 16) & 0xff);
        buffer[7] = (byte)((b >> 24) & 0xff);

        buffer[8] = (byte)((c >> 0) & 0xff);
        buffer[9] = (byte)((c >> 8) & 0xff);
        buffer[10] = (byte)((c >> 16) & 0xff);
        buffer[11] = (byte)((c >> 24) & 0xff);

        return buffer;
    }

    public bool IsNull()
    {
        return a == 0 && b == 0 && c == 0;
    }

    public int CompareTo(ObjectIdValue other)
    {
        int result = ((uint)a).CompareTo((uint)other.a);
        if (result != 0)
            return result;

        result = ((uint)b).CompareTo((uint)other.b);
        if (result != 0)
            return result;

        return ((uint)c).CompareTo((uint)other.c);
    }

    public bool Equals(ObjectIdValue other)
    {
        return a == other.a && b == other.b && c == other.c;
    }

    public override int GetHashCode()
    {
        int hash = 17;
        hash = 37 * hash + a.GetHashCode();
        hash = 37 * hash + b.GetHashCode();
        hash = 37 * hash + c.GetHashCode();
        return hash;
    }
}
