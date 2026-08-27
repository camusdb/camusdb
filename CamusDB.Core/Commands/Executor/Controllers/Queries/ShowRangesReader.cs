/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// One span of a relation's key space, paired with the readable form of its bounds.
///
/// <para><see cref="DecodedStartKey"/> and <see cref="DecodedEndKey"/> are null when the bound is
/// unbounded on that side. They fall back to the raw key text when the bound does not decode, which
/// is a normal outcome rather than an error — see <see cref="ShowRangesReader.DecodeBound"/>.</para>
/// </summary>
internal sealed record ShowRangesSpan(
    long Ordinal,
    string? DecodedStartKey,
    string? DecodedEndKey,
    PlacementSpan Placement);

/// <summary>
/// The resolved answer to one <c>SHOW RANGES</c> / <c>SHOW RANGE</c> statement: which relation was
/// asked about, which Kahuna key space that resolved to, how this node routes it, and the spans.
///
/// <para><see cref="ProbeKey"/> is the exact KV key a <c>FOR ROW</c> clause located, and is null for
/// the all-spans forms. When it is set, <see cref="Spans"/> holds exactly one entry.</para>
/// </summary>
internal sealed record ShowRangesResult(
    string Relation,
    string KeySpace,
    bool IsKeyRange,
    string? ProbeKey,
    IReadOnlyList<ShowRangesSpan> Spans);

/// <summary>
/// Resolves a <c>SHOW RANGES</c> target to a Kahuna key space, reads that key space's placement
/// without disturbing the planner's cache, and renders each span's bounds in column terms.
///
/// <para><b>Key spaces come from the store, never rebuilt by hand.</b>
/// <c>KvTableStore</c> is constructed with <c>TableSchema.EffectiveStorageId</c>, which differs from
/// <c>TableSchema.Id</c> once a materialized view has been refreshed — a refresh builds new contents
/// under a fresh storage id and then adopts it. Composing a key space from the relation id would
/// therefore report the ranges of a key space that no longer holds the relation's rows.</para>
///
/// <para><b>Everything reported is node-local and advisory.</b> The range map is this node's applied
/// view, the routing mode is unreplicated per-node state, and leadership is a hint. None of it is a
/// correctness gate, so nothing here throws because the map is stale, a leader is unknown, or a
/// bound will not decode — those render as nulls or as raw text.</para>
/// </summary>
internal sealed class ShowRangesReader
{
    /// <summary>
    /// Names the primary index accepts besides its internal <c>~pk</c> spelling. Both are shapes a
    /// reader arrives with from other systems, where the primary index of <c>users</c> is
    /// <c>users_pkey</c>; <c>~pk</c> is not a name anyone guesses. Resolution-only — <c>SHOW
    /// INDEXES</c> still prints <c>~pk</c>, because that is what the index is called.
    /// </summary>
    private const string PrimaryKeyAlias = "primary";

    private static string PrimaryKeySuffixAlias(string tableName) => tableName + "_pkey";

    /// <summary>
    /// Resolves the statement's target and reads its placement.
    ///
    /// <para>Callers must reject a plain view before reaching this: a view has no key space, and the
    /// table-open path a caller would otherwise take answers a read with "cannot be written to".
    /// A materialized view needs no such guard — it is a real relation with a real store.</para>
    /// </summary>
    /// <param name="table">The already-opened relation.</param>
    /// <param name="indexName">The index to report, or null for the relation's row space.</param>
    /// <param name="kahuna">The embedded node, or null in configurations with no shared node.</param>
    internal static ShowRangesResult Read(TableDescriptor table, string? indexName, EmbeddedKahuna? kahuna)
    {
        (string keySpace, string relation, ColumnType[]? keyTypes, OrderType[]? directions) =
            ResolveTarget(table, indexName);

        return BuildResult(table, keySpace, relation, keyTypes, directions, kahuna, probeKey: null);
    }

    /// <summary>
    /// Resolves the statement's target, locates the single span covering the key the values name,
    /// and reports that span alone.
    ///
    /// <para>On an index this needs no read at all: the probe key is computed from the values, so it
    /// answers for a key that does not exist. That is the well-defined form, and it is why a
    /// key-space question should be asked of an index where possible.</para>
    /// </summary>
    /// <param name="rowId">
    /// The row id a table-form probe already resolved through the primary index, or null for an
    /// index-form probe. Resolved by the caller because it needs an await; see
    /// <see cref="ResolveRowIdForTableProbeAsync"/>.
    /// </param>
    internal static ShowRangesResult ReadForRow(
        TableDescriptor table,
        string? indexName,
        IReadOnlyList<ColumnValue> values,
        ObjectIdValue? rowId,
        EmbeddedKahuna? kahuna)
    {
        (string keySpace, string relation, ColumnType[]? keyTypes, OrderType[]? directions) =
            ResolveTarget(table, indexName);

        string probeKey;

        if (indexName is null)
        {
            // A table's row key is built from the stored row id, not from the primary key, so the
            // caller had to look the row up first. See ResolveRowIdForTableProbeAsync.
            probeKey = table.Store.RowPointKey(rowId!.Value);
        }
        else
        {
            CompositeColumnValue composite = CoerceProbeValues(table, indexName, values, keyTypes!);
            probeKey = keySpace + "/" + KeyEncoder.Encode(composite, directions);
        }

        return BuildResult(table, keySpace, relation, keyTypes, directions, kahuna, probeKey);
    }

    /// <summary>
    /// Resolves the row id behind a <c>FOR ROW</c> clause on a <b>table</b>, by point-reading the
    /// primary index entry the values name.
    ///
    /// <para>This read exists because a CamusDB row key is <c>{dbId}:{tableId}:r/{rowIdHex24}</c> —
    /// ordered by the stored row id, not by the primary key — so the span holding the row with
    /// primary key 1500 simply is not derivable from 1500. Other systems can compute a table row key
    /// from its primary key; here, pretending to would return a confidently wrong range.</para>
    ///
    /// <para>The probe is deliberately lock-free and non-tracking (see
    /// <c>KvTableStore.LookupUniqueUntracked</c>): an inspection statement must not be able to
    /// decide whether the transaction it runs inside commits.</para>
    /// </summary>
    /// <exception cref="CamusDBException">
    /// When the table has no primary key, or when no row carries the given primary key. A missing
    /// row raises rather than returning no rows, because an empty result is indistinguishable from a
    /// filter that matched nothing — and inventing a key would be worse than either.
    /// </exception>
    internal static async Task<ObjectIdValue> ResolveRowIdForTableProbeAsync(
        TableDescriptor table,
        IReadOnlyList<ColumnValue> values,
        CancellationToken cancellationToken)
    {
        if (!table.Indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? primary))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Table '{table.Name}' has no primary key, so FOR ROW cannot locate a row in it");

        ColumnType[] keyTypes = KeyColumnTypes(table, primary);
        CompositeColumnValue composite = CoerceProbeValues(
            table, CamusDBConstants.PrimaryKeyInternalName, values, keyTypes);

        ObjectIdValue? rowId = await table.Store
            .LookupUniqueUntracked(primary.KvId, composite, cancellationToken)
            .ConfigureAwait(false);

        if (rowId is null)
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                $"No row in '{table.Name}' has that primary key. A table's row key is derived from the "
                + "stored row id rather than from the primary key, so the row must exist before its "
                + "span can be named. Ask SHOW RANGE FROM INDEX instead for a key-space question "
                + "about a key that need not exist.");

        return rowId.Value;
    }

    /// <summary>True when the statement's target is a table's row space rather than an index.</summary>
    internal static bool TargetsRowSpace(string? indexName) => indexName is null;

    // -----------------------------------------------------------------------
    // Target resolution
    // -----------------------------------------------------------------------

    /// <summary>
    /// Maps the statement's target to one key space, plus the label to report it under and the key
    /// column types needed to decode its bounds.
    ///
    /// <para>A row space has no key column types: its keys are row ids, which decode by rendering.</para>
    /// </summary>
    private static (string keySpace, string relation, ColumnType[]? keyTypes, OrderType[]? directions)
        ResolveTarget(TableDescriptor table, string? indexName)
    {
        if (indexName is null)
            return (table.Store.RowKeySpace, table.Name, null, null);

        TableIndexSchema index = ResolveIndex(table, indexName);

        return (
            table.Store.IndexKeySpace(index.KvId),
            table.Name + "@" + index.Name,
            KeyColumnTypes(table, index),
            index.ColumnDirections);
    }

    /// <summary>
    /// Resolves an index name against the relation, accepting the primary index's aliases.
    ///
    /// <para>Exact match runs first so that a user index literally named <c>primary</c> or
    /// <c>&lt;table&gt;_pkey</c> keeps its own identity; the aliases only apply on a miss.</para>
    ///
    /// <para>An index that no query can read yet — still backfilling, or not yet public — is
    /// rejected rather than shown, exactly as <c>SHOW INDEXES</c> hides it. An element the planner
    /// refuses to use must not become visible through a side channel.</para>
    /// </summary>
    private static TableIndexSchema ResolveIndex(TableDescriptor table, string indexName)
    {
        if (!table.Indexes.TryGetValue(indexName, out TableIndexSchema? index)
            && (string.Equals(indexName, PrimaryKeyAlias, StringComparison.OrdinalIgnoreCase)
                || string.Equals(indexName, PrimaryKeySuffixAlias(table.Name), StringComparison.OrdinalIgnoreCase)))
        {
            table.Indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out index);
        }

        if (index is null)
            throw new CamusDBException(
                CamusDBErrorCodes.IndexDoesntExist,
                $"Index '{indexName}' does not exist on '{table.Name}'. Readable indexes: "
                + ReadableIndexNames(table));

        if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
            throw new CamusDBException(
                CamusDBErrorCodes.IndexDoesntExist,
                $"Index '{index.Name}' on '{table.Name}' is not readable yet, so it has no reportable "
                + "key space. Readable indexes: " + ReadableIndexNames(table));

        return index;
    }

    private static string ReadableIndexNames(TableDescriptor table)
    {
        List<string> names = [];

        foreach (KeyValuePair<string, TableIndexSchema> candidate in table.Indexes)
        {
            if (SchemaElementStateRules.IsReadableIndex(table.Schema, candidate.Value))
                names.Add(candidate.Key);
        }

        names.Sort(StringComparer.Ordinal);

        return names.Count == 0 ? "(none)" : string.Join(", ", names);
    }

    /// <summary>
    /// The declared types of an index's key columns, in key order — what
    /// <see cref="KeyEncoder.Decode"/> needs to read a bound back into values.
    ///
    /// <para>Resolved from the in-memory index entry's <c>Columns</c>, which the table open already
    /// populated from the immutable column ids. A column the schema no longer carries falls back to
    /// String, which decodes as text rather than throwing — consistent with the rule that this
    /// statement never fails because something will not decode.</para>
    /// </summary>
    private static ColumnType[] KeyColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
            types[i] = ColumnTypeOf(table, index.Columns[i]);

        return types;
    }

    private static ColumnType ColumnTypeOf(TableDescriptor table, string columnName)
    {
        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                return column.Type;
        }

        return ColumnType.String;
    }

    // -----------------------------------------------------------------------
    // FOR ROW value coercion
    // -----------------------------------------------------------------------

    /// <summary>
    /// Coerces the <c>FOR ROW</c> values to the target index's key column types, using the same
    /// implicit conversions an index seek applies — so a string literal reaches an <c>oid</c> or
    /// <c>uuid</c> key column the way it does in a <c>WHERE</c> clause.
    ///
    /// <para>Fewer values than key columns is allowed and useful: a prefix still lands in exactly one
    /// span, and "which range would this prefix start in?" is a real question. More values than key
    /// columns is not, and neither is a value that will not convert — both would silently locate
    /// some span, and a plausible wrong answer is the one failure mode this statement cannot
    /// afford.</para>
    /// </summary>
    private static CompositeColumnValue CoerceProbeValues(
        TableDescriptor table,
        string indexName,
        IReadOnlyList<ColumnValue> values,
        ColumnType[] keyTypes)
    {
        if (values.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "FOR ROW needs at least one value");

        if (values.Count > keyTypes.Length)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"FOR ROW carries {values.Count} values but '{indexName}' has {keyTypes.Length} key "
                + "column(s). Give at most that many; fewer is allowed and locates the span the "
                + "prefix falls in.");

        ColumnValue[] coerced = new ColumnValue[values.Count];

        for (int i = 0; i < values.Count; i++)
        {
            ColumnValue value = CastScalarFunctions.CoerceToColumnType(values[i], keyTypes[i]);

            // CoerceToColumnType passes an unconvertible pair through unchanged, so the mismatch has
            // to be caught here rather than assumed away.
            if (value.Type != keyTypes[i] && value.Type != ColumnType.Null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"FOR ROW value {i + 1} is {value.Type} but key column {i + 1} of '{indexName}' "
                    + $"is {keyTypes[i]}");

            coerced[i] = value;
        }

        _ = table;

        return new CompositeColumnValue(coerced);
    }

    // -----------------------------------------------------------------------
    // Placement read and span rendering
    // -----------------------------------------------------------------------

    private static ShowRangesResult BuildResult(
        TableDescriptor table,
        string keySpace,
        string relation,
        ColumnType[]? keyTypes,
        OrderType[]? directions,
        EmbeddedKahuna? kahuna,
        string? probeKey)
    {
        _ = table;

        TablePlacement placement;

        if (kahuna is null)
        {
            // No shared node: the process routes everything locally, so there is one span and
            // nothing to ask. Same answer standalone gives, reached without a node to ask it of.
            placement = TablePlacement.Local(keySpace);
        }
        else
        {
            placement = kahuna.ReadPlacementUncached(keySpace, out bool initialized);

            // "Not known yet" is not "one span". Reporting the hash fallback here would hand an
            // operator a shape that reads like a fact about the key space when this node has simply
            // not applied the cluster's meta partition.
            if (!initialized)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"This node has not applied the cluster's range map yet, so the placement of "
                    + $"'{keySpace}' is unknown rather than empty. Retry once the node has caught up.");
        }

        List<ShowRangesSpan> spans = new(probeKey is null ? placement.Spans.Count : 1);

        long ordinal = 0;

        foreach (PlacementSpan span in placement.Spans)
        {
            ordinal++;

            if (probeKey is not null && !Covers(span, probeKey))
                continue;

            spans.Add(new ShowRangesSpan(
                ordinal,
                DecodeBound(span.StartKey, keySpace, keyTypes, directions),
                DecodeBound(span.EndKey, keySpace, keyTypes, directions),
                span));

            if (probeKey is not null)
                break;
        }

        return new ShowRangesResult(relation, keySpace, placement.IsKeyRange, probeKey, spans);
    }

    /// <summary>
    /// Whether a span's half-open <c>[start, end)</c> bounds contain a key, compared the way the
    /// router compares them: <b>ordinally</b>. A culture-aware comparison reorders keys that differ
    /// only in punctuation or case, which would select a neighbouring span while looking right.
    /// </summary>
    private static bool Covers(PlacementSpan span, string key)
    {
        if (span.StartKey is not null && string.CompareOrdinal(key, span.StartKey) < 0)
            return false;

        if (span.EndKey is not null && string.CompareOrdinal(key, span.EndKey) >= 0)
            return false;

        return true;
    }

    /// <summary>
    /// Renders one descriptor bound as readable text, or null when the bound is unbounded.
    ///
    /// <para>A bound is a <b>full KV key</b> — the router compares descriptor bounds against whole
    /// keys — so the key space prefix is stripped before anything is decoded.</para>
    ///
    /// <para><b>This must never throw</b>, and two ordinary situations produce a bound that is not a
    /// whole encoded tuple: a non-unique index appends the row id to its key with no separator, and
    /// a split point is chosen from sampled keys, so it can land at a boundary the decoder does not
    /// accept. Either falls back to the raw text. An introspection statement that fails because a
    /// split landed awkwardly is worse than one that shows a raw key, which is also why the raw
    /// bounds are first-class columns rather than a debug detail.</para>
    /// </summary>
    internal static string? DecodeBound(
        string? bound,
        string keySpace,
        ColumnType[]? keyTypes,
        OrderType[]? directions)
    {
        if (bound is null)
            return null;

        string prefix = keySpace + "/";
        if (!bound.StartsWith(prefix, StringComparison.Ordinal))
            return bound;

        string suffix = bound[prefix.Length..];

        // A row space's keys are row ids. There is nothing to decode: the hex form is the value,
        // and it is what the operator will match against a row id they already have.
        if (keyTypes is null)
            return suffix;

        try
        {
            CompositeColumnValue decoded = KeyEncoder.Decode(suffix, keyTypes, directions);

            return RenderTuple(decoded);
        }
        catch (Exception)
        {
            return suffix;
        }
    }

    private static string RenderTuple(CompositeColumnValue composite)
    {
        string[] parts = new string[composite.Values.Length];

        for (int i = 0; i < composite.Values.Length; i++)
            parts[i] = RenderValue(composite.Values[i]);

        return string.Join(", ", parts);
    }

    /// <summary>
    /// Renders one decoded key value using the literal conventions the rest of the engine displays:
    /// ISO-8601 for dates, an <c>X'…'</c> literal for bytes, the canonical string form for uuids.
    /// A bound is meant to be readable next to the data it bounds.
    /// </summary>
    private static string RenderValue(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Null => "NULL",
            ColumnType.Id => value.StrValue ?? "",
            ColumnType.String => value.StrValue ?? "",
            ColumnType.Bool => value.BoolValue ? "true" : "false",
            ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float64 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float32 => ((float)value.FloatValue).ToString(CultureInfo.InvariantCulture),
            ColumnType.Date or ColumnType.DateTime => value.IsoValue ?? "",
            ColumnType.Bytes => SqlStringLiteral.QuoteBytes(value.BytesValue ?? []),
            ColumnType.Uuid => value.UuidValue ?? "",
            _ => value.ToString() ?? "",
        };
    }
}
