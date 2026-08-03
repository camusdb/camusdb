
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.App.Grpc;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using ProtoValue     = CamusDB.Grpc.Value;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Parity tests for the slot-direct gRPC response path: a projected cell of a slot-backed
/// <see cref="QueryRow"/> is serialized straight from its <see cref="ValueSlot"/>
/// (<see cref="GrpcValueCodec.ToProto(in ValueSlot)"/> via <see cref="CamusSqlService.BuildResultRow"/>)
/// and must produce a Protobuf message identical to the <see cref="ColumnValue"/> path for every
/// column type — the wire format must not depend on which backing a row happens to have.
/// </summary>
[Parallelizable(ParallelScope.All)]
public sealed class TestGrpcResultRowSlots
{
    /// <summary>One representative <see cref="ColumnValue"/> per wire-visible type, plus edge cases.</summary>
    private static IEnumerable<ColumnValue> AllTypedValues()
    {
        yield return ColumnValue.Null;
        yield return new ColumnValue(CoreColumnType.Id, new ObjectIdValue(1, 2, 3).ToString());
        yield return new ColumnValue(CoreColumnType.Integer64, long.MinValue);
        yield return new ColumnValue(CoreColumnType.String, "héllo \"quoted\"");
        yield return new ColumnValue(CoreColumnType.String, "");
        yield return ColumnValue.True;
        yield return ColumnValue.False;
        yield return new ColumnValue(CoreColumnType.Float64, Math.PI);
        yield return new ColumnValue(CoreColumnType.Float32, -1.5f);
        yield return new ColumnValue(new byte[] { 0, 1, 250, 255 });
        yield return new ColumnValue(Array.Empty<byte>());
        yield return new ColumnValue(CoreColumnType.Date, 638000000000000000L);
        yield return new ColumnValue(CoreColumnType.DateTime, 638123456789012345L);
        yield return ColumnValue.FromUuid(Guid.Parse("550e8400-e29b-41d4-a716-446655440000"));
        yield return ColumnValue.FromArray(CoreColumnType.Integer64,
        [
            new ColumnValue(CoreColumnType.Integer64, 1L),
            ColumnValue.Null,
            new ColumnValue(CoreColumnType.Integer64, 3L),
        ]);
        yield return ColumnValue.FromArray(CoreColumnType.String, []);
    }

    [Test]
    public void ToProtoSlot_MatchesColumnValuePath_ForEveryType()
    {
        foreach (ColumnValue cv in AllTypedValues())
        {
            ValueSlot slot = ValueSlot.FromColumnValue(cv);
            ProtoValue viaSlot = GrpcValueCodec.ToProto(in slot);
            ProtoValue viaColumnValue = GrpcValueCodec.ToProto(cv);

            Assert.AreEqual(viaColumnValue, viaSlot, $"slot path diverged for {cv.Type}");
        }
    }

    [Test]
    public void BuildResultRow_SlotBackedRow_MatchesDictRow()
    {
        ColumnValue[] values = AllTypedValues().ToArray();
        DerivedColumnSchema[] schema = values
            .Select((v, i) => new DerivedColumnSchema($"c{i}", v.Type))
            .ToArray();
        RowLayout layout = new(schema.Select(c => c.Name));

        ValueSlot[] slots = new ValueSlot[layout.Count];
        for (int i = 0; i < values.Length; i++)
            slots[i] = ValueSlot.FromColumnValue(values[i]);
        QueryRow slotRow = QueryRow.FromSlots(new ObjectIdValue(1, 2, 3), layout, slots);

        Dictionary<string, ColumnValue> dict = new(StringComparer.Ordinal);
        for (int i = 0; i < values.Length; i++)
            dict[schema[i].Name] = values[i];

        Assert.AreEqual(
            CamusSqlService.BuildResultRow(dict, schema),
            CamusSqlService.BuildResultRow(slotRow, schema, new ResultRowBinder()));
    }

    [Test]
    public void BuildResultRow_BinderReusedAcrossRows_AndAbsentColumnIsNull()
    {
        DerivedColumnSchema[] schema =
        [
            new("n", CoreColumnType.Integer64),
            new("missing", CoreColumnType.String),
        ];
        RowLayout layout = new(["n"]); // "missing" is not in the layout → ordinal -1 → NULL cell

        ResultRowBinder binder = new();
        for (long i = 0; i < 3; i++)
        {
            QueryRow row = QueryRow.FromSlots(new ObjectIdValue(1, 1, (int)i), layout,
                [ValueSlot.FromColumnValue(new ColumnValue(CoreColumnType.Integer64, i))]);

            CamusDB.Grpc.ResultRow rr = CamusSqlService.BuildResultRow(row, schema, binder);

            Assert.AreEqual(2, rr.Values.Count);
            Assert.AreEqual(i, rr.Values[0].Int64Value);
            Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, rr.Values[1].KindCase);
        }
    }
}
