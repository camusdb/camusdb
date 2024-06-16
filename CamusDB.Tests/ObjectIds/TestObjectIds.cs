
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
}

