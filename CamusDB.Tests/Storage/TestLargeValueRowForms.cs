/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Byte-level tests of compressed and out-of-line cells: the LZ4 codec and its payback rule, the
/// storage-form trailer the row codec writes, the four storage strategies, resolution of marked cells,
/// and the guarantees that make old rows and unmarked rows unchanged.
/// </summary>
public sealed class TestLargeValueRowForms
{
    private static TableColumnSchema Col(string name, ColumnType type, ColumnStorageStrategy? storage = null)
        => new(name, name, type, false, null, storage: storage);

    private static byte[] RandomBytes(int length, int seed)
    {
        byte[] bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    private static string RandomText(int length, int seed)
    {
        Random random = new(seed);
        const string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        return new string(Enumerable.Range(0, length).Select(_ => alphabet[random.Next(alphabet.Length)]).ToArray());
    }

    /// <summary>Packed little-endian float32, the vector encoding: close to incompressible.</summary>
    private static byte[] Embedding(int dimensions, int seed)
    {
        Random random = new(seed);
        byte[] bytes = new byte[dimensions * 4];
        for (int i = 0; i < dimensions; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4), (float)(random.NextDouble() * 2 - 1));
        return bytes;
    }

    /// <summary>Resolves every marked cell of an encoded row, feeding out-of-line values from the encode result.</summary>
    private static byte[] ResolveAll(EncodedRow encoded)
    {
        ReadOnlySpan<byte> payload = encoded.StorageValue.AsSpan(1);
        if (!RowStorageForms.HasTrailer(payload))
            return payload.ToArray();

        RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);
        byte[]?[] replacements = new byte[]?[layout.VariableCount];
        for (int v = 0; v < layout.VariableCount; v++)
        {
            if (!layout.IsMarked(payload, v))
                continue;

            ReadOnlySpan<byte> fetched = default;
            if (layout.IsOutOfLine(payload, v))
                fetched = encoded.OutOfLine!.Single(w => w.VariableOrdinal == v).StorageValue.AsSpan(1);

            replacements[v] = RowStorageForms.ResolveCell(payload, layout, v, fetched);
        }

        return RowStorageForms.Rebuild(payload, layout, replacements);
    }

    // ── LZ4 codec and the payback rule ──────────────────────────────────────

    [Test]
    public void Compression_RoundTripsCompressibleText()
    {
        byte[] source = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("repeated phrase ", 400)));

        Assert.IsTrue(LargeValueCompression.TryCompress(source, 12, out byte[]? block));
        Assert.Less(block!.Length, source.Length);
        CollectionAssert.AreEqual(source, LargeValueCompression.Decompress(block, source.Length));
    }

    [Test]
    public void Compression_RefusesIncompressibleData()
    {
        Assert.IsFalse(LargeValueCompression.TryCompress(RandomBytes(8000, 1), 12, out byte[]? block));
        Assert.IsNull(block);
    }

    [Test]
    public void Compression_RefusesFloat32Embedding()
    {
        // The case that protects vector search: an embedding must never be stored compressed.
        Assert.IsFalse(LargeValueCompression.TryCompress(Embedding(768, 7), 12, out _));
    }

    [Test]
    public void Compression_RefusesValuesBelowTheMinimumSize()
    {
        byte[] small = new byte[LargeValueCompression.MinCompressibleBytes - 1];
        Assert.IsFalse(LargeValueCompression.TryCompress(small, 0, out _));
    }

    [Test]
    public void Compression_HonoursTheMinimumSaving()
    {
        // Half compressible, half random: saves roughly a third, so it passes at 12% and fails at 90%.
        byte[] mixed = [.. new byte[3000], .. RandomBytes(3000, 3)];
        Assert.IsTrue(LargeValueCompression.TryCompress(mixed, 12, out _));
        Assert.IsFalse(LargeValueCompression.TryCompress(mixed, 90, out _));
    }

    [Test]
    public void Decompression_OfCorruptBlock_RaisesLargeValueCorrupt()
    {
        byte[] source = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("abcdefgh", 200)));
        Assert.IsTrue(LargeValueCompression.TryCompress(source, 0, out byte[]? block));

        byte[] damaged = block!.ToArray();
        damaged[^1] ^= 0xFF;
        damaged[0] = 0xFF;

        CamusDBException ex = Assert.Throws<CamusDBException>(() => LargeValueCompression.Decompress(damaged, source.Length))!;
        Assert.AreEqual(CamusDBErrorCodes.LargeValueCorrupt, ex.Code);

        CamusDBException wrongLength = Assert.Throws<CamusDBException>(() => LargeValueCompression.Decompress(block, source.Length + 1))!;
        Assert.AreEqual(CamusDBErrorCodes.LargeValueCorrupt, wrongLength.Code);
    }

    [Test]
    public void Decompression_OfImpossibleRecordedLength_IsRejectedBeforeAllocation()
    {
        // A damaged length prefix on a tiny block: no checksum covers the length, so only the expansion
        // bound stands between this read and a 2 GiB allocation.
        byte[] tiny = [0x1F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        long before = GC.GetAllocatedBytesForCurrentThread();
        CamusDBException ex = Assert.Throws<CamusDBException>(() => LargeValueCompression.Decompress(tiny, int.MaxValue - 64))!;
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.AreEqual(CamusDBErrorCodes.LargeValueCorrupt, ex.Code);
        Assert.Less(allocated, 1 << 20, "the recorded length must be rejected before the output buffer is allocated");
    }

    [Test]
    public void Decompression_OfDamagedInlineLengthPrefix_IsRejectedBeforeAllocation()
    {
        byte[] body = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("compressible ", 300)));
        TableColumnSchema[] columns = [Col("body", ColumnType.String)];
        CompiledRowCodec codec = CompiledRowCodec.Build(1, columns);
        LargeValuePolicy policy = LargeValuePolicy.Create(columns, thresholdBytes: 1_000_000, compressionEnabled: true, minSavingPercent: 12);

        EncodedRow encoded = codec.EncodeStorageValue([ValueSlot.FromString(Encoding.UTF8.GetString(body))], policy);
        byte[] payload = encoded.StorageValue.AsSpan(1).ToArray();
        RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);
        Assert.IsTrue(layout.IsCompressed(payload, 0));

        // Overwrite the cell's raw-length prefix with a length no block of this size can decode to. The
        // first variable cell starts at the beginning of the variable area.
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(layout.VariableAreaOffset), int.MaxValue - 64);

        long before = GC.GetAllocatedBytesForCurrentThread();
        CamusDBException ex = Assert.Throws<CamusDBException>(() => RowStorageForms.ResolveCell(payload, layout, 0, default))!;
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.AreEqual(CamusDBErrorCodes.LargeValueCorrupt, ex.Code);
        Assert.Less(allocated, 1 << 20);
    }

    [Test]
    public void Decompression_OfTheMostCompressibleValue_StaysWithinTheExpansionBound()
    {
        // All zeros is LZ4's best case. If the bound were wrong, this valid value would read as corrupt.
        byte[] zeros = new byte[8 * 1024 * 1024];
        Assert.IsTrue(LargeValueCompression.TryCompress(zeros, 12, out byte[]? block));
        Assert.LessOrEqual(zeros.Length, LargeValueCompression.MaxDecodedLength(block!.Length));
        CollectionAssert.AreEqual(zeros, LargeValueCompression.Decompress(block, zeros.Length));
    }

    // ── Row format ──────────────────────────────────────────────────────────

    [Test]
    public void RowWithOnlySmallValues_IsByteIdenticalToThePlainEncoding()
    {
        TableColumnSchema[] columns = [Col("id", ColumnType.Integer64), Col("name", ColumnType.String), Col("blob", ColumnType.Bytes)];
        CompiledRowCodec codec = CompiledRowCodec.Build(3, columns);
        ValueSlot[] values = [ValueSlot.FromLong(ColumnType.Integer64, 5), ValueSlot.FromString("short"), ValueSlot.FromBytes([1, 2, 3])];

        EncodedRow encoded = codec.EncodeStorageValue(values, LargeValuePolicy.Create(columns, 2048, true, 12));

        CollectionAssert.AreEqual(codec.EncodeStorageValue(values), encoded.StorageValue, "a row with nothing large must grow by zero bytes");
        Assert.IsNull(encoded.OutOfLine);
        Assert.IsFalse(RowStorageForms.HasTrailer(encoded.StorageValue.AsSpan(1)));
    }

    [Test]
    public void AllFourCellForms_CoexistInOneRow_AndResolveToTheOriginalValues()
    {
        TableColumnSchema[] columns =
        [
            Col("plain", ColumnType.String),
            Col("inlineCompressed", ColumnType.String),
            Col("outOfLineRaw", ColumnType.Bytes),
            Col("outOfLineCompressed", ColumnType.String),
            Col("n", ColumnType.Integer64),
        ];
        CompiledRowCodec codec = CompiledRowCodec.Build(9, columns);

        string compressible = string.Concat(Enumerable.Repeat("tick tock ", 150));      // 1.5 KB, compresses under 2 KB
        byte[] random = RandomBytes(5000, 11);                                              // 5 KB, does not compress
        string largeCompressible = string.Concat(Enumerable.Repeat("0123456789", 3000)) + RandomText(3000, 5); // compresses, stays above 2 KB

        ValueSlot[] values =
        [
            ValueSlot.FromString("small"),
            ValueSlot.FromString(compressible),
            ValueSlot.FromBytes(random),
            ValueSlot.FromString(largeCompressible),
            ValueSlot.FromLong(ColumnType.Integer64, 42),
        ];

        EncodedRow encoded = codec.EncodeStorageValue(values, LargeValuePolicy.Create(columns, 2048, true, 12));
        ReadOnlySpan<byte> payload = encoded.StorageValue.AsSpan(1);

        Assert.IsTrue(RowStorageForms.HasTrailer(payload));
        RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);
        Assert.IsFalse(layout.IsMarked(payload, 0), "plain");
        Assert.IsTrue(layout.IsCompressed(payload, 1) && !layout.IsOutOfLine(payload, 1), "inline compressed");
        Assert.IsTrue(!layout.IsCompressed(payload, 2) && layout.IsOutOfLine(payload, 2), "out of line, raw");
        Assert.IsTrue(layout.IsCompressed(payload, 3) && layout.IsOutOfLine(payload, 3), "out of line, compressed");
        Assert.AreEqual(2, encoded.OutOfLine!.Count);

        // The codec refuses to read a marked cell before resolution, but reads unmarked cells.
        codec.ValidateFrame(payload);
        Assert.AreEqual("small", codec.GetSlot(payload, 0).AsString);
        Assert.AreEqual(42, codec.GetSlot(payload, 4).AsLong);
        CamusDBException unresolved = Assert.Throws<CamusDBException>(() => codec.GetSlot(encoded.StorageValue.AsSpan(1), 1))!;
        Assert.AreEqual(CamusDBErrorCodes.LargeValueNotResolved, unresolved.Code);

        byte[] resolved = ResolveAll(encoded);
        Assert.IsFalse(RowStorageForms.HasTrailer(resolved), "a fully resolved row carries no trailer");
        CollectionAssert.AreEqual(codec.Encode(values), resolved, "a fully resolved row is byte-identical to the plain encoding");
    }

    [Test]
    public void StoredVersionIsUnchanged_AndOldRowsDecodeUnchanged()
    {
        TableColumnSchema[] columns = [Col("body", ColumnType.String)];
        CompiledRowCodec codec = CompiledRowCodec.Build(7, columns);

        byte[] plain = codec.Encode([ValueSlot.FromString(RandomText(5000, 2))]);
        EncodedRow marked = codec.EncodeStorageValue([ValueSlot.FromString(RandomText(5000, 2))], LargeValuePolicy.Create(columns, 2048, true, 12));

        Assert.AreEqual(7u, BinaryPrimitives.ReadUInt32LittleEndian(plain));
        Assert.AreEqual(7, RowStorageForms.StoredVersion(BinaryPrimitives.ReadUInt32LittleEndian(marked.StorageValue.AsSpan(1))));
        Assert.AreNotEqual(0u, BinaryPrimitives.ReadUInt32LittleEndian(marked.StorageValue.AsSpan(1)) & RowStorageForms.TrailerFlag);

        codec.ValidateFrame(plain);
        codec.ValidateFrame(marked.StorageValue.AsSpan(1));
    }

    [Test]
    public void Strategies_DecideTheFormOfEachCell()
    {
        string compressibleLarge = string.Concat(Enumerable.Repeat("abcdefghij", 1000));   // 10 KB, compresses very well
        byte[] randomLarge = RandomBytes(6000, 21);

        (bool compressed, bool outOfLine) FormOf(ColumnStorageStrategy strategy, ValueSlot value, ColumnType type)
        {
            TableColumnSchema[] columns = [Col("c", type, strategy)];
            CompiledRowCodec codec = CompiledRowCodec.Build(1, columns);
            EncodedRow encoded = codec.EncodeStorageValue([value], LargeValuePolicy.Create(columns, 2048, true, 12));
            ReadOnlySpan<byte> payload = encoded.StorageValue.AsSpan(1);
            if (!RowStorageForms.HasTrailer(payload))
                return (false, false);

            RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);
            return (layout.IsCompressed(payload, 0), layout.IsOutOfLine(payload, 0));
        }

        ValueSlot text = ValueSlot.FromString(compressibleLarge);
        ValueSlot bytes = ValueSlot.FromBytes(randomLarge);

        Assert.AreEqual((false, false), FormOf(ColumnStorageStrategy.Plain, text, ColumnType.String));
        Assert.AreEqual((false, false), FormOf(ColumnStorageStrategy.Plain, bytes, ColumnType.Bytes));

        Assert.AreEqual((true, false), FormOf(ColumnStorageStrategy.Main, text, ColumnType.String));
        Assert.AreEqual((false, false), FormOf(ColumnStorageStrategy.Main, bytes, ColumnType.Bytes), "MAIN never moves a value out of line");

        Assert.AreEqual((false, true), FormOf(ColumnStorageStrategy.External, text, ColumnType.String), "EXTERNAL never compresses");
        Assert.AreEqual((false, true), FormOf(ColumnStorageStrategy.External, bytes, ColumnType.Bytes));

        Assert.AreEqual((true, false), FormOf(ColumnStorageStrategy.Extended, text, ColumnType.String), "compressed below the threshold, so inline");
        Assert.AreEqual((false, true), FormOf(ColumnStorageStrategy.Extended, bytes, ColumnType.Bytes), "does not compress, so out of line raw");
    }

    [Test]
    public void VectorColumn_UnderEveryStrategy_IsNeverStoredCompressed()
    {
        byte[] embedding = Embedding(768, 99);   // 3072 bytes, above the 2048 threshold

        foreach (ColumnStorageStrategy strategy in Enum.GetValues<ColumnStorageStrategy>())
        {
            TableColumnSchema[] columns = [Col("embedding", ColumnType.Bytes, strategy)];
            CompiledRowCodec codec = CompiledRowCodec.Build(1, columns);
            EncodedRow encoded = codec.EncodeStorageValue([ValueSlot.FromBytes(embedding)], LargeValuePolicy.Create(columns, 2048, true, 12));
            ReadOnlySpan<byte> payload = encoded.StorageValue.AsSpan(1);

            if (RowStorageForms.HasTrailer(payload))
                Assert.IsFalse(RowStorageForms.ReadLayout(payload).IsCompressed(payload, 0), $"{strategy} stored an embedding compressed");

            bool expectOutOfLine = strategy is ColumnStorageStrategy.External or ColumnStorageStrategy.Extended;
            Assert.AreEqual(expectOutOfLine, encoded.OutOfLine is { Count: 1 }, strategy.ToString());
        }
    }

    [Test]
    public void ThresholdZero_AndCompressionOff_KeepEveryValueRaw()
    {
        TableColumnSchema[] columns = [Col("body", ColumnType.String)];
        CompiledRowCodec codec = CompiledRowCodec.Build(1, columns);
        ValueSlot[] values = [ValueSlot.FromString(string.Concat(Enumerable.Repeat("x", 50_000)))];

        EncodedRow encoded = codec.EncodeStorageValue(values, LargeValuePolicy.Create(columns, 0, false, 12));
        CollectionAssert.AreEqual(codec.EncodeStorageValue(values), encoded.StorageValue);
    }

    [Test]
    public void OutOfLinePointer_DetectsAChangedValue()
    {
        TableColumnSchema[] columns = [Col("body", ColumnType.Bytes)];
        CompiledRowCodec codec = CompiledRowCodec.Build(1, columns);
        EncodedRow encoded = codec.EncodeStorageValue([ValueSlot.FromBytes(RandomBytes(4000, 8))], LargeValuePolicy.Create(columns, 2048, false, 12));
        ReadOnlySpan<byte> payload = encoded.StorageValue.AsSpan(1);
        RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);

        byte[] other = RandomBytes(4000, 9);
        Assert.IsNull(RowStorageForms.ResolveCell(payload, layout, 0, other), "a value from another write must not pair with this row");
        Assert.IsNotNull(RowStorageForms.ResolveCell(payload, layout, 0, encoded.OutOfLine![0].StorageValue.AsSpan(1)));
    }

    [Test]
    public void CarriedCell_IsCopiedVerbatim_WithItsMarks()
    {
        TableColumnSchema[] columns = [Col("title", ColumnType.String), Col("body", ColumnType.Bytes)];
        CompiledRowCodec codec = CompiledRowCodec.Build(2, columns);
        LargeValuePolicy policy = LargeValuePolicy.Create(columns, 2048, true, 12);

        EncodedRow original = codec.EncodeStorageValue([ValueSlot.FromString("before"), ValueSlot.FromBytes(RandomBytes(6000, 4))], policy);

        // The update does not know body's value: its slot is null and the cell is carried.
        EncodedRow updated = codec.EncodeStorageValue(
            [ValueSlot.FromString("after"), ValueSlot.Null], policy, original.StorageValue.AsSpan(1), [false, true]);

        Assert.IsNull(updated.OutOfLine, "a carried pointer writes no value");

        ReadOnlySpan<byte> before = original.StorageValue.AsSpan(1);
        ReadOnlySpan<byte> after = updated.StorageValue.AsSpan(1);
        RowStorageForms.TrailerLayout beforeLayout = RowStorageForms.ReadLayout(before);
        RowStorageForms.TrailerLayout afterLayout = RowStorageForms.ReadLayout(after);

        Assert.IsTrue(afterLayout.IsOutOfLine(after, 1));
        Assert.IsTrue(beforeLayout.Cell(before, 1).SequenceEqual(afterLayout.Cell(after, 1)), "the pointer bytes are carried unchanged");
        Assert.IsFalse(codec.IsNull(after, 1));
        Assert.AreEqual("after", codec.GetSlot(after, 0).AsString);
    }

    [Test]
    public void KeyOfAnOutOfLineValue_HasOneSlash_AndFixedWidthSuffix()
    {
        KvKeyBuilder keys = new("db1", "db1", "A0", "docs");
        string key = keys.BuildLargeValueKey(new Core.Util.ObjectIds.ObjectIdValue(1, 2, 3), 10);

        Assert.AreEqual(1, key.Count(c => c == '/'), "one slash keeps every large value in the table's large-value key space");
        Assert.IsTrue(key.StartsWith("db1:A0|v/", StringComparison.Ordinal));
        Assert.IsTrue(key.EndsWith("000a", StringComparison.Ordinal));
        Assert.AreEqual("db1:A0|v/".Length + 24 + 4, key.Length);
        Assert.AreEqual("db1:A0|v", keys.LargeValueBucketPrefix);
    }
}
