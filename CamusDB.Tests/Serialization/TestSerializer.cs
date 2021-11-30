
using System.IO;
using System.Text;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Tests.Serialization;

public class TestSerializer
{
    [Test]
    public void TestSerializeType()
    {
        byte[] buffer = new byte[1];

        int pointer = 0;
        Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
        Assert.AreEqual(pointer, 1);

        pointer = 0;
        int type = Serializator.ReadType(buffer, ref pointer);
        Assert.AreEqual(pointer, 1);
        Assert.AreEqual(type, SerializatorTypes.TypeNull);        
    }

    [Test]
    public void TestSerializeAllTypes()
    {
        byte[] buffer = new byte[1];

        for (int i = 0; i < 22; i++)
        {
            int pointer = 0;
            Serializator.WriteType(buffer, i, ref pointer);
            Assert.AreEqual(pointer, 1);

            pointer = 0;
            int type = Serializator.ReadType(buffer, ref pointer);
            Assert.AreEqual(type, i);
            Assert.AreEqual(pointer, 1);
        }
    }
}
