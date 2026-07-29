
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Operational-safety guard for the intentional row-format cut: backward compatibility is waived, so a
/// store written by the old self-describing codec is unsupported and must be recreated. The one hard
/// requirement is that the new positional decoder never <b>silently misreads</b> old (or otherwise
/// foreign) bytes as a valid row — it must fail loudly with a <see cref="CamusDBException"/>. This test
/// reconstructs genuine old-format bytes (the legacy type-tagged header + cells, still expressible via
/// <see cref="Serializator"/> / <see cref="RowEncoder.WriteColumnValue"/>) and feeds them, plus truncated
/// and version-tampered positional bytes, through the decode path to prove the loud-rejection contract.
///
/// <para>This is the lightweight alternative to a store-format-epoch open gate: it verifies the actual
/// safety property (no silent corruption) without touching the boot path. A store-metadata epoch that
/// fails <i>earlier</i> (at open, with a clearer message) is deferred future work for when a second
/// on-disk format actually exists.</para>
/// </summary>
[TestFixture]
public sealed class TestFormatIncompatibilityRejection
{
    private static TableColumnSchema Col(string name, ColumnType type) => new(name, name, type, false, null);

    private static TableSchema MakeSchema(int version, params TableColumnSchema[] columns)
    {
        List<TableColumnSchema> cols = new(columns);
        List<TableSchemaHistory> history = new();
        for (int v = 0; v <= version; v++)
            history.Add(new TableSchemaHistory { Version = v, Columns = cols });
        return new TableSchema { Id = "t", Name = "t", Version = version, Columns = cols, SchemaHistory = history };
    }

    /// <summary>Reconstructs a row exactly as the pre-positional self-describing codec wrote it.</summary>
    private static byte[] BuildLegacyRow(int schemaVersion, ObjectIdValue rowId, TableSchema schema, Dictionary<string, ColumnValue> row)
    {
        byte[] buffer = new byte[4096];
        int pointer = 0;

        Serializator.WriteType(buffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(buffer, schemaVersion, ref pointer);
        Serializator.WriteType(buffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteObjectId(buffer, rowId, ref pointer);

        foreach (TableColumnSchema column in schema.Columns!)
        {
            if (row.TryGetValue(column.Name, out ColumnValue? value))
                RowEncoder.WriteColumnValue(buffer, value, ref pointer);
            else
                Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
        }

        return buffer[..pointer];
    }

    private static readonly ObjectIdValue RowId = new(1, 2, 3);

    [Test]
    public void LegacySelfDescribingRow_RejectedLoudly()
    {
        TableSchema schema = MakeSchema(0, Col("id", ColumnType.Id), Col("n", ColumnType.Integer64), Col("s", ColumnType.String));
        Dictionary<string, ColumnValue> row = new()
        {
            ["id"] = new ColumnValue(ColumnType.Id, new ObjectIdValue(4, 5, 6).ToString()),
            ["n"] = new ColumnValue(ColumnType.Integer64, 42L),
            ["s"] = new ColumnValue(ColumnType.String, "legacy"),
        };

        byte[] legacy = BuildLegacyRow(0, RowId, schema, row);

        // Must throw — never return a silently-misdecoded dictionary.
        Assert.Throws<CamusDBException>(() => RowEncoder.Decode(schema, RowId, legacy));
    }

    [Test]
    public void ShorterThanHeaderRow_RejectedWithSystemSpaceCorrupt([Values(0, 1, 2, 3)] int length)
    {
        TableSchema schema = MakeSchema(0, Col("n", ColumnType.Integer64));
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => RowEncoder.Decode(schema, RowId, new byte[length]));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex!.Code);
    }

    [Test]
    public void ForeignBytes_RejectedLoudly()
    {
        TableSchema schema = MakeSchema(0, Col("n", ColumnType.Integer64));

        // A buffer that claims a schema version that does not exist (0xFFFFFFF0) — the first thing decode
        // reads. It cannot resolve to a layout, so it must fail rather than guess.
        byte[] foreign = { 0xF0, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0 };
        Assert.Throws<CamusDBException>(() => RowEncoder.Decode(schema, RowId, foreign));
    }

    [Test]
    public void TruncatedPositionalRow_RejectedWithSystemSpaceCorrupt()
    {
        CompiledRowCodec codec = CompiledRowCodec.Build(0, new[] { Col("n", ColumnType.Integer64), Col("s", ColumnType.String) });
        byte[] payload = codec.Encode(new[] { ValueSlot.FromLong(ColumnType.Integer64, 7), ValueSlot.FromString("abc") });

        // Chop the payload — the one-shot frame check must reject it before any unchecked read.
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => codec.ValidateFrame(payload.AsSpan(0, payload.Length - 2)));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex!.Code);
    }

    [Test]
    public void VersionTamperedPositionalRow_RejectedBeforeDecode()
    {
        // Two layouts of different width: a row written under v0 whose stored version is flipped to v1
        // must be caught by the frame check (v1's fixed area is wider), not decoded as if it were v1.
        CompiledRowCodec v0 = CompiledRowCodec.Build(0, new[] { Col("a", ColumnType.Integer64) });
        CompiledRowCodec v1 = CompiledRowCodec.Build(1, new[] { Col("a", ColumnType.Integer64), Col("b", ColumnType.Integer64) });

        byte[] payload = v0.Encode(new[] { ValueSlot.FromLong(ColumnType.Integer64, 1) });
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(payload, 1); // claim version 1

        CamusDBException? ex = Assert.Throws<CamusDBException>(() => v1.ValidateFrame(payload));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex!.Code);
    }
}
