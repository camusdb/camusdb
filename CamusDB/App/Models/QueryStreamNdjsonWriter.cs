
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using Kommander.Time;

namespace CamusDB.App.Models;

/// <summary>
/// Header line of the streaming query protocol (<c>application/x-ndjson</c>): the ordered output
/// column schema, emitted once as the first line so a client can decode the positional row arrays
/// that follow. Mirrors <see cref="ExecuteSQLQueryResponse.Columns"/> but carries no rows.
/// </summary>
public sealed class QueryStreamHeader
{
    public string Status { get; set; } = "ok";

    public List<ColumnSchemaDto> Columns { get; set; } = [];
}

/// <summary>
/// Trailer line of the streaming query protocol: the terminal line after the last row. On success it
/// carries the final row <see cref="Total"/>, the commit <see cref="CausalToken"/>, and
/// <see cref="ServerTimeMs"/>. On a failure that surfaces <b>after</b> the header/rows are already on
/// the wire, <see cref="Status"/> is <c>"failed"</c> and <see cref="Code"/>/<see cref="Message"/>
/// describe the error — the HTTP status is already 200 and cannot change, so the outcome is reported
/// in-band, exactly like the gRPC stream reports a mid-stream conflict as its terminal error.
/// </summary>
public sealed class QueryStreamTrailer
{
    public string Status { get; set; } = "ok";

    public int Total { get; set; }

    public HLCTimestamp? CausalToken { get; set; }

    public double ServerTimeMs { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }
}

/// <summary>
/// Writes a query result set as newline-delimited JSON (<c>application/x-ndjson</c>) — one
/// <see cref="QueryStreamHeader"/> object first, then one positional row array per result row, then one
/// terminal <see cref="QueryStreamTrailer"/> object. Unlike <see cref="PositionalRowSet"/> (which
/// buffers the whole result set and serializes it as a single response), this streams each row straight
/// to the transport as it is pulled from the cursor, so a multi-MB / multi-thousand-row result never
/// materializes in server memory and the client can begin consuming immediately.
///
/// <para>Line shapes (distinguished by their first JSON token — objects are header/trailer, arrays are
/// rows):</para>
/// <code>
/// {"status":"ok","columns":[{"name":"id","type":1}, ...]}
/// ["6849f3…", 42, true]
/// ["6849f4…", 43, false]
/// {"status":"ok","total":2,"causalToken":{…},"serverTimeMs":3.1}
/// </code>
///
/// <para>Row values use the identical compact-raw positional encoding as the buffered endpoint
/// (<see cref="CompactRowJsonWriter"/>), so a client's row decoder is shared between the two endpoints;
/// only the framing differs. This type only formats bytes into an <see cref="IBufferWriter{T}"/>; the
/// caller owns flushing those bytes to the network (and thus back-pressure).</para>
///
/// <para><b>Retry semantics:</b> because the schema line is emitted before the rows are pulled and a
/// row can be flushed to the network before the autocommit transaction commits, the streaming endpoint
/// cannot transparently replay a serializable conflict once output has started — a late conflict is
/// reported through the failure trailer instead. Callers needing automatic retry should use the
/// buffered <c>/execute-sql-query</c> endpoint or drive an explicit transaction and retry client-side.</para>
/// </summary>
public sealed class QueryStreamNdjsonWriter
{
    /// <summary>MIME type for the streaming response body.</summary>
    public const string ContentType = "application/x-ndjson";

    private static readonly JsonSerializerOptions MetaOptions = new()
    {
        PropertyNamingPolicy    = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition  = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly Utf8JsonWriter writer;
    private readonly IBufferWriter<byte> buffer;

    // Carried across rows so a run of rows sharing one RowLayout resolves the schema→ordinal map once
    // (see CompactRowJsonWriter.WriteRow) instead of per-row dictionary lookups.
    private RowLayout? boundLayout;
    private int[]? ordinals;

    /// <summary>
    /// True once the header line has been written. The controller uses this to decide, on error,
    /// between a clean HTTP error response (nothing streamed yet) and an in-band failure trailer
    /// (the 200 header and possibly rows are already on the wire and the status can no longer change).
    /// </summary>
    public bool HeaderWritten { get; private set; }

    public QueryStreamNdjsonWriter(Utf8JsonWriter writer, IBufferWriter<byte> buffer)
    {
        this.writer = writer;
        this.buffer = buffer;
    }

    /// <summary>Writes the schema header line. Must be called exactly once, before any row.</summary>
    public void WriteHeader(IReadOnlyList<DerivedColumnSchema> schema)
    {
        QueryStreamHeader header = new() { Columns = ToColumnDtos(schema) };
        JsonSerializer.Serialize(writer, header, MetaOptions);
        EndLine();
        HeaderWritten = true;
    }

    /// <summary>Writes one positional row array line, aligned to <paramref name="schema"/>.</summary>
    public void WriteRow(QueryResultRow row, IReadOnlyList<DerivedColumnSchema> schema)
    {
        CompactRowJsonWriter.WriteRow(writer, row, schema, ref boundLayout, ref ordinals);
        EndLine();
    }

    /// <summary>Writes the terminal trailer line (success or in-band failure).</summary>
    public void WriteTrailer(QueryStreamTrailer trailer)
    {
        JsonSerializer.Serialize(writer, trailer, MetaOptions);
        EndLine();
    }

    // Flush the JSON writer so its buffered bytes are committed to the IBufferWriter, append the record
    // separator, then Reset() the writer. The reset is essential: a Utf8JsonWriter otherwise treats
    // successive top-level values as array elements and prepends a ',' before each — which would corrupt
    // every line after the first. Reset() clears that pending-separator state while keeping the same
    // output buffer, so the next value starts clean.
    private void EndLine()
    {
        writer.Flush();
        buffer.Write("\n"u8);
        writer.Reset();
    }

    private static List<ColumnSchemaDto> ToColumnDtos(IReadOnlyList<DerivedColumnSchema> schema)
    {
        List<ColumnSchemaDto> dtos = new(schema.Count);
        foreach (DerivedColumnSchema col in schema)
            dtos.Add(new ColumnSchemaDto { Name = col.Name, Type = col.Type });
        return dtos;
    }
}
