
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.App.Models;

/// <summary>
/// Owning buffer of query result rows plus the response column schema, serialized to the compact-raw
/// positional JSON array (<c>[[…],[…]]</c>) directly from the rows' <see cref="ColumnValue"/> cells.
///
/// <para>
/// This exists to keep the response allocation to the query-result ownership cost plus JSON output
/// buffers: the rows are held as decoded <see cref="QueryResultRow"/>s (which already own their
/// values) and streamed straight to the <see cref="Utf8JsonWriter"/> at serialization time — there is
/// no per-row <c>object?[]</c> and no per-scalar box, unlike the earlier
/// <see cref="CompactRowEncoder.EncodeRow"/> graph.
/// </para>
///
/// <para>
/// <b>Retry ordering:</b> the controller must fully buffer the rows and <b>commit</b> before handing
/// them to this type. Serialization runs after the action returns (well after commit), so a
/// serializable autocommit retry can still restart without any bytes having been emitted. The buffer
/// therefore keeps decoded, transaction-independent rows — never a live cursor.
/// </para>
/// </summary>
[JsonConverter(typeof(PositionalRowSetConverter))]
public sealed class PositionalRowSet
{
    /// <summary>Buffered, fully-decoded rows. Owned; carries no reference into transaction state.</summary>
    public IReadOnlyList<QueryResultRow> Rows { get; }

    /// <summary>Ordered response column schema; column <c>c</c> is read from each row by this name/ordinal.</summary>
    public IReadOnlyList<DerivedColumnSchema> Schema { get; }

    /// <summary>Row count, mirrored into the response's <c>Total</c> field.</summary>
    public int Count => Rows.Count;

    /// <summary>An empty set, used for error responses that carry no rows.</summary>
    public static readonly PositionalRowSet Empty = new([], []);

    public PositionalRowSet(IReadOnlyList<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema> schema)
    {
        Rows = rows;
        Schema = schema;
    }
}

/// <summary>
/// Serialize-only converter: writes a <see cref="PositionalRowSet"/> as the positional row array using
/// <see cref="CompactRowJsonWriter"/>. Deserialization is unsupported — the server only ever writes this
/// shape; clients decode the raw wire JSON into their own models.
/// </summary>
internal sealed class PositionalRowSetConverter : JsonConverter<PositionalRowSet>
{
    public override PositionalRowSet Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => throw new NotSupportedException("PositionalRowSet is serialize-only (server response shape).");

    public override void Write(Utf8JsonWriter writer, PositionalRowSet value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();

        RowLayout? boundLayout = null;
        int[]? ordinals = null;
        IReadOnlyList<QueryResultRow> rows = value.Rows;
        for (int i = 0; i < rows.Count; i++)
            CompactRowJsonWriter.WriteRow(writer, rows[i], value.Schema, ref boundLayout, ref ordinals);

        writer.WriteEndArray();
    }
}
