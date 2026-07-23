
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.App.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Wire-format tests for the streaming query endpoint's newline-delimited JSON
/// (<see cref="QueryStreamNdjsonWriter"/>). They assert the framing (header line, one row array per
/// line, terminal trailer line), that row bytes match the buffered endpoint's compact-raw encoding
/// exactly, and that a mid-stream failure trailer is well-formed — so the buffered and streaming
/// endpoints stay decode-compatible for a client's row parser.
/// </summary>
public sealed class TestQueryStreamNdjsonWriter
{
    private static readonly JsonSerializerOptions RowOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private static QueryResultRow DictRow(IReadOnlyList<DerivedColumnSchema> schema, params ColumnValue[] values)
    {
        Dictionary<string, ColumnValue> dict = new(StringComparer.Ordinal);
        for (int i = 0; i < schema.Count; i++)
            dict[schema[i].Name] = values[i];
        return new QueryResultRow(new ObjectIdValue(1, 2, 3), dict);
    }

    /// <summary>Drives the writer over an ArrayBufferWriter and returns the decoded UTF-8 lines.</summary>
    private static List<string> WriteLines(
        IReadOnlyList<DerivedColumnSchema> schema,
        IReadOnlyList<QueryResultRow> rows,
        QueryStreamTrailer trailer)
    {
        ArrayBufferWriter<byte> buffer = new();
        using (Utf8JsonWriter jsonWriter = new(buffer, new JsonWriterOptions { SkipValidation = true }))
        {
            QueryStreamNdjsonWriter ndjson = new(jsonWriter, buffer);
            ndjson.WriteHeader(schema);
            foreach (QueryResultRow row in rows)
                ndjson.WriteRow(row, schema);
            ndjson.WriteTrailer(trailer);
        }

        string text = Encoding.UTF8.GetString(buffer.WrittenSpan);
        // Trailing newline after the final line yields an empty trailing element — drop it.
        return text.Split('\n').Where(l => l.Length > 0).ToList();
    }

    [Test]
    public void Framing_HeaderRowsTrailer()
    {
        DerivedColumnSchema[] schema = [new("n", ColumnType.Integer64), new("s", ColumnType.String)];
        List<QueryResultRow> rows =
        [
            DictRow(schema, new ColumnValue(ColumnType.Integer64, 1L), new ColumnValue(ColumnType.String, "a")),
            DictRow(schema, new ColumnValue(ColumnType.Integer64, 2L), new ColumnValue(ColumnType.String, "b")),
        ];

        List<string> lines = WriteLines(schema, rows, new QueryStreamTrailer { Status = "ok", Total = 2, ServerTimeMs = 1.5 });

        Assert.AreEqual(4, lines.Count, "header + 2 rows + trailer");

        // Header line: object with the column schema.
        using JsonDocument header = JsonDocument.Parse(lines[0]);
        Assert.AreEqual(JsonValueKind.Object, header.RootElement.ValueKind);
        Assert.AreEqual("ok", header.RootElement.GetProperty("status").GetString());
        JsonElement cols = header.RootElement.GetProperty("columns");
        Assert.AreEqual(2, cols.GetArrayLength());
        Assert.AreEqual("n", cols[0].GetProperty("name").GetString());
        Assert.AreEqual((int)ColumnType.Integer64, cols[0].GetProperty("type").GetInt32());

        // Row lines: positional arrays.
        using JsonDocument r0 = JsonDocument.Parse(lines[1]);
        Assert.AreEqual(JsonValueKind.Array, r0.RootElement.ValueKind);
        Assert.AreEqual(1, r0.RootElement[0].GetInt32());
        Assert.AreEqual("a", r0.RootElement[1].GetString());
        using JsonDocument r1 = JsonDocument.Parse(lines[2]);
        Assert.AreEqual(2, r1.RootElement[0].GetInt32());
        Assert.AreEqual("b", r1.RootElement[1].GetString());

        // Trailer line: object with the terminal metadata.
        using JsonDocument trailer = JsonDocument.Parse(lines[3]);
        Assert.AreEqual(JsonValueKind.Object, trailer.RootElement.ValueKind);
        Assert.AreEqual("ok", trailer.RootElement.GetProperty("status").GetString());
        Assert.AreEqual(2, trailer.RootElement.GetProperty("total").GetInt32());
    }

    [Test]
    public void RowBytes_MatchBufferedEncoding()
    {
        DerivedColumnSchema[] schema =
        [
            new("i", ColumnType.Integer64),
            new("s", ColumnType.String),
            new("b", ColumnType.Bool),
            new("by", ColumnType.Bytes),
            new("nul", ColumnType.String),
        ];
        QueryResultRow row = DictRow(schema,
            new ColumnValue(ColumnType.Integer64, -42L),
            new ColumnValue(ColumnType.String, "héllo \"q\"\n"),
            ColumnValue.True,
            new ColumnValue(new byte[] { 0, 1, 250, 255 }),
            ColumnValue.Null);

        List<string> lines = WriteLines(schema, [row], new QueryStreamTrailer { Status = "ok", Total = 1 });

        // The streamed row line must be byte-identical to the buffered positional encoding for one row.
        string bufferedOneRow = JsonSerializer.Serialize(new PositionalRowSet([row], schema), RowOptions);
        // PositionalRowSet wraps rows in an outer array: [[...]]. The single inner row equals our line.
        string expectedRow = bufferedOneRow.Substring(1, bufferedOneRow.Length - 2);

        Assert.AreEqual(expectedRow, lines[1]);
    }

    [Test]
    public void EmptyResult_HeaderThenTrailer_NoRows()
    {
        DerivedColumnSchema[] schema = [new("a", ColumnType.Integer64)];

        List<string> lines = WriteLines(schema, [], new QueryStreamTrailer { Status = "ok", Total = 0 });

        Assert.AreEqual(2, lines.Count, "header + trailer only");
        using JsonDocument header = JsonDocument.Parse(lines[0]);
        Assert.AreEqual(JsonValueKind.Object, header.RootElement.ValueKind);
        using JsonDocument trailer = JsonDocument.Parse(lines[1]);
        Assert.AreEqual(0, trailer.RootElement.GetProperty("total").GetInt32());
    }

    [Test]
    public void FailureTrailer_CarriesCodeAndMessage()
    {
        DerivedColumnSchema[] schema = [new("a", ColumnType.Integer64)];
        List<QueryResultRow> rows = [DictRow(schema, new ColumnValue(ColumnType.Integer64, 7L))];

        List<string> lines = WriteLines(schema, rows, new QueryStreamTrailer
        {
            Status  = "failed",
            Total   = 1,
            Code    = "CADB9999",
            Message = "conflict after streaming started",
        });

        using JsonDocument trailer = JsonDocument.Parse(lines[^1]);
        Assert.AreEqual("failed", trailer.RootElement.GetProperty("status").GetString());
        Assert.AreEqual("CADB9999", trailer.RootElement.GetProperty("code").GetString());
        Assert.AreEqual("conflict after streaming started", trailer.RootElement.GetProperty("message").GetString());
        Assert.AreEqual(1, trailer.RootElement.GetProperty("total").GetInt32());
    }

    [Test]
    public void SuccessTrailer_OmitsNullErrorFields_IncludesCausalToken()
    {
        DerivedColumnSchema[] schema = [new("a", ColumnType.Integer64)];

        List<string> lines = WriteLines(schema, [], new QueryStreamTrailer
        {
            Status      = "ok",
            Total       = 0,
            CausalToken = new HLCTimestamp(1, 100, 2),
        });

        using JsonDocument trailer = JsonDocument.Parse(lines[^1]);
        JsonElement root = trailer.RootElement;
        Assert.IsFalse(root.TryGetProperty("code", out _), "null code omitted");
        Assert.IsFalse(root.TryGetProperty("message", out _), "null message omitted");
        Assert.IsTrue(root.TryGetProperty("causalToken", out _), "causal token present");
    }
}
