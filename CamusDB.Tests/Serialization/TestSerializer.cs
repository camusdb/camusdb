
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
        byte[] buffer = new byte[SerializatorTypeSizes.TypeInteger8];

        int pointer = 0;
        Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger8);

        pointer = 0;
        int type = Serializator.ReadType(buffer, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger8);
        Assert.AreEqual(type, SerializatorTypes.TypeNull);        
    }

    [Test]
    public void TestSerializeAllTypes()
    {
        byte[] buffer = new byte[SerializatorTypeSizes.TypeInteger8];

        for (int i = 0; i < 22; i++)
        {
            int pointer = 0;
            Serializator.WriteType(buffer, i, ref pointer);
            Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger8);

            pointer = 0;
            int type = Serializator.ReadType(buffer, ref pointer);
            Assert.AreEqual(type, i);
            Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger8);
        }
    }

    [Test]
    //[TestCase(-128)]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(128)]
    public void TestSerializeInt8(int writeValue)
    {
        byte[] buffer = new byte[SerializatorTypeSizes.TypeInteger8];

        int pointer = 0;
        Serializator.WriteInt8(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger8);

        pointer = 0;
        int readValue = Serializator.ReadInt8(buffer, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger8);
        Assert.AreEqual(readValue, writeValue);
    }

    [Test]
    [TestCase(-128)]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(256)]
    [TestCase(2048)]
    public void TestSerializeInt16(int writeValue)
    {
        byte[] buffer = new byte[SerializatorTypeSizes.TypeInteger16];

        int pointer = 0;
        Serializator.WriteInt16(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger16);

        pointer = 0;
        int readValue = Serializator.ReadInt16(buffer, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger16);
        Assert.AreEqual(readValue, writeValue);
    }

    [Test]
    [TestCase(-128)]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(256)]
    [TestCase(2048)]
    [TestCase(65512)]
    public void TestSerializeInt32(int writeValue)
    {
        byte[] buffer = new byte[SerializatorTypeSizes.TypeInteger32];

        int pointer = 0;
        Serializator.WriteInt32(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger32);

        pointer = 0;
        int readValue = Serializator.ReadInt32(buffer, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeInteger32);
        Assert.AreEqual(readValue, writeValue);
    }

    [Test]    
    [TestCase(0U)]
    [TestCase(1U)]
    [TestCase(256U)]
    [TestCase(2048U)]
    [TestCase(65512U)]    
    public void TestSerializeUInt32(uint writeValue)
    {
        byte[] buffer = new byte[SerializatorTypeSizes.TypeUnsignedInteger32];

        int pointer = 0;
        Serializator.WriteUInt32(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeUnsignedInteger32);

        pointer = 0;
        uint readValue = Serializator.ReadUInt32(buffer, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeUnsignedInteger32);
        Assert.AreEqual(readValue, writeValue);
    }

    [Test]
    public void TestSerializeBool()
    {
        bool writeValue = true;
        byte[] buffer = new byte[SerializatorTypeSizes.TypeBool];

        int pointer = 0;
        Serializator.WriteBool(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeBool);

        pointer = 1;
        bool readValue = Serializator.ReadBool(buffer, ref pointer);
        Assert.AreEqual(pointer, SerializatorTypeSizes.TypeBool);
        Assert.AreEqual(readValue, writeValue);
    }

    [Test]
    public void TestSerializeString()
    {
        string writeValue = "hello";
        byte[] buffer = Encoding.UTF8.GetBytes(writeValue);

        int pointer = 0;
        Serializator.WriteString(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, buffer.Length);

        pointer = 0;
        string readValue = Serializator.ReadString(buffer, buffer.Length, ref pointer);
        Assert.AreEqual(pointer, buffer.Length);
        Assert.AreEqual(readValue, writeValue);
    }

    [Test]
    public void TestSerializeLargeString()
    {
        string writeValue = new string('f', 4096);
        byte[] buffer = Encoding.UTF8.GetBytes(writeValue);

        int pointer = 0;
        Serializator.WriteString(buffer, writeValue, ref pointer);
        Assert.AreEqual(pointer, buffer.Length);

        pointer = 0;
        string readValue = Serializator.ReadString(buffer, buffer.Length, ref pointer);
        Assert.AreEqual(pointer, buffer.Length);
        Assert.AreEqual(readValue, writeValue);
    }
}
