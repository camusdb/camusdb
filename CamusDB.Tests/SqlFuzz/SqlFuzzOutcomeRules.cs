/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Tests.SqlFuzz;

/// <summary>
/// The outcome rules that the SQL fuzzer and the regression tests it prints share, so a printed test
/// fails for exactly the outcomes the campaign reported. The fuzzer (<c>CamusDB.SqlFuzz.Tests</c>) links
/// this file.
/// </summary>
internal static class SqlFuzzErrorRules
{
    /// <summary>
    /// Codes that report a defect of the engine, not a refusal of the statement: an internal operation
    /// that must not happen, and the codes for corrupt or unreadable storage. An outcome with one of them
    /// is a finding wherever it comes from: a statement, a COMMIT, or an integrity scan.
    /// </summary>
    public static IReadOnlySet<string> InternalCodes { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        CamusDBErrorCodes.InvalidInternalOperation,
        CamusDBErrorCodes.SystemSpaceCorrupt,
        CamusDBErrorCodes.TableCorrupt,
        CamusDBErrorCodes.LargeValueCorrupt,
        CamusDBErrorCodes.LargeValueNotResolved,
        CamusDBErrorCodes.JsonbValueCorrupt,
        CamusDBErrorCodes.InvalidIndexLayout,
    };

    /// <summary>
    /// The shape of an error message: quoted values become <c>'…'</c> and digit runs become <c>N</c>, so
    /// two failures with one cause have one shape, and failures with different causes under one code
    /// (<c>CADB0099</c> is used for many) have different shapes.
    /// </summary>
    public static string MessageShape(string? message, int maxLength = 80)
    {
        if (string.IsNullOrEmpty(message))
            return "";
        string text = Regex.Replace(message, "'[^']*'", "'…'");
        text = Regex.Replace(text, "\\d+", "N");
        return text.Length > maxLength ? text[..maxLength] : text;
    }
}

/// <summary>
/// Writes result rows the way the HTTP JSON endpoints do (<see cref="CompactRowJsonWriter"/>), in both
/// jsonb forms, into a buffer that is thrown away. It only finds rows that cannot be written; the bytes
/// are not compared. A statement that succeeds in the engine but cannot reach a client is a finding.
/// </summary>
internal sealed class SqlFuzzWire
{
    private readonly MemoryStream buffer = new();
    private readonly Utf8JsonWriter writer;
    private RowLayout? layout;
    private int[]? ordinals;
    private RowLayout? textLayout;
    private int[]? textOrdinals;

    public SqlFuzzWire() => writer = new Utf8JsonWriter(buffer);

    /// <summary>Null when the row writes in both forms, else the exception type and message.</summary>
    public string? TryWrite(QueryResultRow row, IReadOnlyList<DerivedColumnSchema> schema)
    {
        if (schema.Count == 0)
            return null;
        try
        {
            CompactRowJsonWriter.WriteRow(writer, row, schema, ref layout, ref ordinals);
            Restart();
            CompactRowJsonWriter.WriteRow(writer, row, schema, ref textLayout, ref textOrdinals, jsonbAsText: true);
            Restart();
            return null;
        }
        catch (Exception ex)
        {
            writer.Reset(buffer);
            buffer.SetLength(0);
            return ex.GetType().Name + ": " + ex.Message;
        }
    }

    // One row is one top-level JSON value; the writer refuses a second one, so it starts again.
    private void Restart()
    {
        writer.Flush();
        buffer.SetLength(0);
        writer.Reset(buffer);
    }
}
