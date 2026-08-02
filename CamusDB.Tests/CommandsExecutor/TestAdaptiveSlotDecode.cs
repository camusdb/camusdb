
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Locks the per-scan slot-decode selection plumbing: <see cref="RowEncoder.RowDecodeState.SlotBackedDecode"/>
/// overrides the configured <see cref="CamusDBOptions.SlotBackedDecode"/> so a scan can opt in (rejecting
/// filter present) or out (full-materialize) per query. The scanner's policy — enable slots when a
/// residual filter exists — is asserted end-to-end by <see cref="TestSlotBackedDecodeParity"/> for value
/// parity; here we pin that the override actually drives the decode backing.
/// </summary>
public sealed class TestAdaptiveSlotDecode
{
    private static readonly ObjectIdValue RowId = new(1, 2, 3);
    private static readonly HLCTimestamp TxId = default;

    private static TableColumnSchema Col(string name, ColumnType type)
        => new(name, name, type, false, null, SchemaElementState.Public);

    private static (TableSchema schema, ReadOnlyMemory<byte> payload) BuildRow()
    {
        TableSchema schema = new()
        {
            Id = "t", Name = "t", Version = 0,
            Columns =
            [
                Col("id", ColumnType.Id),
                Col("n",  ColumnType.Integer64),
                Col("s",  ColumnType.String),
            ],
        };
        schema.SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = schema.Columns }];

        Dictionary<string, ColumnValue> row = new()
        {
            ["id"] = new ColumnValue(ColumnType.Id, new ObjectIdValue(4, 5, 6).ToString()),
            ["n"]  = new ColumnValue(ColumnType.Integer64, 42L),
            ["s"]  = new ColumnValue(ColumnType.String, "hello"),
        };

        byte[] storage = RowEncoder.EncodeStorageValue(schema, row, RowId);
        BranchKvValue env = BranchKvCodec.Decode(storage);
        return (schema, env.Payload);
    }

    /// <summary>Baseline configuration — these tests drive the decoder directly, with no engine.</summary>
    private static CamusDBOptions DecodeOptions => CamusDBOptions.Default;

    private static async Task<QueryRow> Decode(
        TableSchema schema, ReadOnlyMemory<byte> payload, bool? slotOverride, CamusDBOptions? options = null)
    {
        RowEncoder.RowDecodeState state = new() { SlotBackedDecode = slotOverride };
        return await RowEncoder.DecodeToQueryRowAsync(schema, TxId, RowId, payload, options ?? DecodeOptions, decodeState: state);
    }

    [Test]
    public async Task Override_True_ProducesSlotBackedRow()
    {
        (TableSchema schema, ReadOnlyMemory<byte> payload) = BuildRow();
        QueryRow row = await Decode(schema, payload, slotOverride: true);
        Assert.IsTrue(row.IsSlotBacked);
        // Value is still correct regardless of backing.
        Assert.AreEqual(42L, row.GetColumnValue(row.Layout.IndexOf("n")).LongValue);
    }

    [Test]
    public async Task Override_False_ProducesEagerRow()
    {
        (TableSchema schema, ReadOnlyMemory<byte> payload) = BuildRow();
        QueryRow row = await Decode(schema, payload, slotOverride: false);
        Assert.IsFalse(row.IsSlotBacked);
        Assert.AreEqual(42L, row.GetColumnValue(row.Layout.IndexOf("n")).LongValue);
    }

    [Test]
    public async Task NoOverride_FollowsConfiguredFlag()
    {
        (TableSchema schema, ReadOnlyMemory<byte> payload) = BuildRow();

        // With no per-row override the configured flag decides.
        Task<QueryRow> DecodeUnder(bool slotBacked)
            => Decode(schema, payload, slotOverride: null,
                      options: DecodeOptions with { SlotBackedDecode = slotBacked });

        Assert.IsTrue((await DecodeUnder(true)).IsSlotBacked);
        Assert.IsFalse((await DecodeUnder(false)).IsSlotBacked);
    }
}
