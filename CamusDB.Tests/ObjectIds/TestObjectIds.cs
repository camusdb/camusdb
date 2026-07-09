
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

using NUnit.Framework;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.ObjectIds;

public class TestObjectIds
{
    [Test]
    public void TestObjectIdConstructor()
    {
        ObjectIdValue objectId = new(1, 2, 3);

        Assert.AreEqual(1, objectId.a);
        Assert.AreEqual(2, objectId.b);
        Assert.AreEqual(3, objectId.c);
    }

    [Test]
    public void TestObjectIdConstructor2()
    {
        ObjectIdValue objectId = new(1639931684, -1154155741, -743207513);

        Assert.AreEqual(1639931684, objectId.a);
        Assert.AreEqual(-1154155741, objectId.b);
        Assert.AreEqual(-743207513, objectId.c);
    }

    [Test]
    public void TestObjectIdConstructor3()
    {
        ObjectIdValue objectId = new(1639931684, -1154155741, -743207513);
        string objectIdStr = objectId.ToString();

        Assert.AreEqual(24, objectIdStr.Length);
        Assert.AreEqual("61bf5f24bb34fb23d3b38da7", objectIdStr);
    }

    [Test]
    public void TestObjectIdGenerator()
    {
        ObjectIdValue objectId = ObjectIdGenerator.Generate();
        string objectIdStr = objectId.ToString();

        Assert.AreEqual(24, objectIdStr.Length);
    }

    [Test]
    public void TestObjectIdGenerator2()
    {
        ObjectIdValue objectId1 = ObjectIdGenerator.Generate();
        ObjectIdValue objectId2 = ObjectIdGenerator.Generate();

        string objectId1Str = objectId1.ToString();
        string objectId2Str = objectId2.ToString();

        Assert.AreEqual(24, objectId1Str.Length);
        Assert.AreEqual(24, objectId2Str.Length);
        Assert.AreNotEqual(objectId1Str, objectId2Str);
    }

    [Test]
    public void TestObjectIdToValue()
    {
        const int a = 1639931684;
        const int b = -1154155741;
        const int c = -743207513;

        const string val = "61bf5f24bb34fb23d3b38da7";

        ObjectIdValue objectId = new(a, b, c);
        string objectIdStr = objectId.ToString();

        Assert.AreEqual(24, objectIdStr.Length);
        Assert.AreEqual(val, objectIdStr);

        ObjectIdValue objectId2 = ObjectId.ToValue(val);
        Assert.AreEqual(objectId2.a, a);
        Assert.AreEqual(objectId2.b, b);
        Assert.AreEqual(objectId2.c, c);
    }

    [Test]
    public void TestObjectIdGeneratedToValue()
    {
        ObjectIdValue objectId = ObjectIdGenerator.Generate();
        string objectIdStr = objectId.ToString();

        Assert.AreEqual(24, objectIdStr.Length);

        ObjectIdValue objectId2 = ObjectId.ToValue(objectIdStr);
        Assert.AreEqual(objectId2.a, objectId.a);
        Assert.AreEqual(objectId2.b, objectId.b);
        Assert.AreEqual(objectId2.c, objectId.c);
    }

    [Test]
    public void TestObjectIdToValueSpanMatchesStringOverload()
    {
        // The span overload must be byte-identical to the string overload, including for segments
        // whose high bit is set (negative ints: b and c below are negative). 100 generated ids plus
        // a known value cover the hex-nibble assembly across the full 32-bit range per segment.
        const string known = "61bf5f24bb34fb23d3b38da7";
        ObjectIdValue fromString = ObjectId.ToValue(known);
        ObjectIdValue fromSpan   = ObjectId.ToValue(known.AsSpan());
        Assert.AreEqual(fromString.a, fromSpan.a);
        Assert.AreEqual(fromString.b, fromSpan.b);
        Assert.AreEqual(fromString.c, fromSpan.c);
        Assert.IsTrue(fromSpan.b < 0 && fromSpan.c < 0, "This fixture exercises the negative-segment path");

        for (int i = 0; i < 100; i++)
        {
            string s = ObjectIdGenerator.Generate().ToString();
            ObjectIdValue str  = ObjectId.ToValue(s);
            ObjectIdValue span = ObjectId.ToValue(s.AsSpan());
            Assert.AreEqual(str.a, span.a);
            Assert.AreEqual(str.b, span.b);
            Assert.AreEqual(str.c, span.c);
            // Parsing a sub-span (no allocation of a trimmed string) must also work.
            ObjectIdValue subSpan = ObjectId.ToValue(("xx" + s).AsSpan(2));
            Assert.AreEqual(str.a, subSpan.a);
            Assert.AreEqual(str.c, subSpan.c);
        }
    }

    [Test]
    public void TestObjectIdToValueSpanRejectsWrongLength()
    {
        Assert.Throws<System.FormatException>(() => ObjectId.ToValue("61bf5f24".AsSpan()));         // too short
        Assert.Throws<System.FormatException>(() => ObjectId.ToValue("61bf5f24bb34fb23d3b38da7z".AsSpan())); // too long
        Assert.Throws<System.FormatException>(() => ObjectId.ToValue("61bf5f24bb34fb23d3b38daZ".AsSpan()));  // non-hex
    }
}

