
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Row read/write facade over the positional <see cref="CompiledRowCodec"/>. It keeps the historical
/// name-keyed / <see cref="QueryRow"/> entry points (encode from a name→value dictionary, decode to a
/// dictionary or a layout-backed <see cref="QueryRow"/> with visibility, projection, and injected
/// defaults) while the actual byte layout is the schema-driven positional format the codec owns — no
/// per-cell type tags and no stored row id. Name-keyed encode goes through
/// <see cref="RowSlotAdapter"/>; decode selects the stored version's codec (per-scan cached in
/// <see cref="RowDecodeState"/>) and reads each column by its positional ordinal.
///
/// <para>
/// The self-describing column-value helpers (<see cref="WriteColumnValue"/>,
/// <see cref="ReadColumnValue"/>, <see cref="ColumnValueSize"/>, <see cref="SkipColumnValue"/>) remain
/// because the secondary-index INCLUDE-tuple codec still uses that tagged form; they are unrelated to
/// the positional row layout.
/// </para>
/// </summary>
public static class RowEncoder
{
    /// <summary>
    /// Per-<see cref="TableSchema"/> cache of compiled positional codecs, keyed by stored schema version.
    /// A <see cref="CompiledRowCodec"/> is a pure function of one version's (immutable) column layout, so
    /// building it once per (schema, version) — rather than on every dict-decode / encode call — avoids
    /// reallocating its plan arrays in row loops (analyze samples, DDL backfills, update/delete loads).
    /// Keyed weakly by the schema instance so a dropped/replaced schema's codecs are collected with it;
    /// keyed internally by version because a version's layout never changes (history is append-only).
    /// The per-scan <see cref="RowDecodeState"/> plan cache still covers <see cref="DecodeToQueryRowAsync"/>;
    /// this closes the remaining per-row rebuild on the dictionary decode/encode facades.
    /// </summary>
    private static readonly ConditionalWeakTable<TableSchema, ConcurrentDictionary<int, CompiledRowCodec>> CodecCache = new();

    private static CompiledRowCodec GetCodec(TableSchema schema, int version, IReadOnlyList<TableColumnSchema> columns)
    {
        ConcurrentDictionary<int, CompiledRowCodec> perSchema = CodecCache.GetValue(schema, static _ => new ConcurrentDictionary<int, CompiledRowCodec>());
        return perSchema.TryGetValue(version, out CompiledRowCodec? cached)
            ? cached
            : perSchema.GetOrAdd(version, CompiledRowCodec.Build(version, columns));
    }

    private enum ColumnVisibility
    {
        PublicOnly,
        Writable
    }

    /// <summary>
    /// One precomputed instruction for a single stored (history) column when decoding a row into a
    /// <see cref="QueryRow"/>. <see cref="OutputOrdinal"/> is the slot in the output value array to
    /// read this column into, or <c>-1</c> to skip the column's bytes (projected out or not visible).
    /// <see cref="Type"/> is the physical column type used to read/skip the value. Carrying just the
    /// type (not the whole <see cref="TableColumnSchema"/>) keeps the per-row loop branch-light.
    /// </summary>
    internal readonly struct ColumnDecodeStep
    {
        public readonly ColumnType Type;
        public readonly int OutputOrdinal;

        public ColumnDecodeStep(ColumnType type, int outputOrdinal)
        {
            Type = type;
            OutputOrdinal = outputOrdinal;
        }
    }

    /// <summary>
    /// The fully-resolved plan for decoding every row that shares one stored schema version within a
    /// scan into a <see cref="QueryRow"/>. Built once (a build may await lazy schema-history loading),
    /// then executed per row with no visibility checks, required-column set lookups, layout index
    /// lookups, or current-column searches — those decisions are all baked into <see cref="Steps"/>
    /// and <see cref="InjectedDefaults"/> at build time.
    /// </summary>
    internal sealed class RowDecodePlan
    {
        /// <summary>Shared output layout (column names + ordinals) for every row on this plan.</summary>
        public required RowLayout Layout { get; init; }

        /// <summary>
        /// Positional codec for the stored schema version these rows were written under. Reads a column
        /// by its stored ordinal directly (no byte-stream cursor), so a projected-out column costs
        /// nothing — there are no bytes to skip past.
        /// </summary>
        public required CompiledRowCodec Codec { get; init; }

        /// <summary>One step per stored column: <see cref="ColumnDecodeStep.OutputOrdinal"/> is the output
        /// slot to read this column's positional value into, or <c>-1</c> to leave it out of the row.</summary>
        public required ColumnDecodeStep[] Steps { get; init; }

        /// <summary>
        /// Inverse of <see cref="Steps"/> for borrowed decode: output ordinal → stored (codec) ordinal, or
        /// <c>-1</c> for an output column supplied by <see cref="InjectedDefaults"/> rather than the bytes.
        /// Lets a <see cref="RowView"/>-backed row read output ordinal <c>o</c> straight from the payload.
        /// </summary>
        public required int[] OutputToStored { get; init; }

        /// <summary>
        /// Output slots for columns that are visible in the current schema but were added after this
        /// row was written (absent from the byte stream): each gets its default value. Ordinals here
        /// never overlap a <see cref="Steps"/> read ordinal, so they are written unconditionally.
        /// </summary>
        public required (int Ordinal, ColumnValue Value)[] InjectedDefaults { get; init; }
    }

    /// <summary>
    /// Per-scan memoisation of <see cref="RowDecodePlan"/> keyed by the stored schema version embedded
    /// in each row. A scan fixes the required-column set, visibility version, and visibility mode, so
    /// every row at the same stored version shares one plan (usually a single entry). Create one
    /// instance per scan and pass it to <see cref="DecodeToQueryRowAsync"/>; never share it across
    /// scans that differ in those inputs, and never treat it as global transaction state.
    /// </summary>
    public sealed class RowDecodeState
    {
        internal readonly Dictionary<int, RowDecodePlan> Plans = new();

        /// <summary>
        /// Per-scan override for slot-backed decode. <see langword="null"/> defers to the global
        /// <see cref="CamusDBOptions.SlotBackedDecode"/>; a non-null value lets the scan opt in or out
        /// based on its own shape. Slot-backed decode only pays off when the scan rejects rows (a
        /// rejected row decodes to slots but never materializes its projection cells); a scan that
        /// materializes every row — no residual filter, or a full-materialize consumer — is faster on
        /// the eager path, so the scanner sets this to enable slots only when a rejecting filter exists.
        /// </summary>
        public bool? SlotBackedDecode { get; init; }

        /// <summary>
        /// Per-scan override for borrowed (zero-copy) decode. <see langword="null"/> defers to the global
        /// <see cref="CamusDBOptions.BorrowedDecode"/>. When set, a decoded row is backed by a
        /// <see cref="RowView"/> over the raw KV bytes instead of an eager <c>ValueSlot[]</c>: no per-row
        /// slot array is allocated and an unread cell decodes nothing, which wins hardest on selective
        /// scans over wide or string/bytes-heavy rows. Takes precedence over
        /// <see cref="SlotBackedDecode"/> when both would apply. The borrowed bytes must outlive the row
        /// (guaranteed on the scan path — Kahuna hands out a fresh, stable array per entry).
        /// </summary>
        public bool? BorrowedDecode { get; init; }
    }

    /// <summary>
    /// The fully-resolved plan for decoding every row that shares one stored schema version into a
    /// name-keyed dictionary (the <see cref="DecodeWritableAsync"/> output shape). Built once per
    /// (state, stored version) — the build may await lazy schema-history loading and performs all the
    /// per-column visibility resolution (<c>FindCurrentColumn</c> scans), required-column filtering,
    /// and injected-default selection. Executing the plan is fully synchronous and touches no schema
    /// metadata: it reads each retained stored ordinal and adds the injected defaults.
    /// Output is value-identical to the plan-less decode for the same inputs.
    /// </summary>
    internal sealed class DictionaryDecodePlan
    {
        /// <summary>Positional codec for the stored schema version these rows were written under.</summary>
        public required CompiledRowCodec Codec { get; init; }

        /// <summary>
        /// Per stored ordinal: the output dictionary key to decode this column under, or
        /// <see langword="null"/> to skip it (not visible, or projected out by the required-column set).
        /// </summary>
        public required string?[] DecodeNames { get; init; }

        /// <summary>
        /// Columns visible in the current schema but absent from this stored version's byte stream:
        /// each is added under its name with its default value (or a typed null). Names here never
        /// collide with a retained <see cref="DecodeNames"/> entry — that exclusion is baked in at
        /// build time, mirroring the plan-less decode's contains-check.
        /// </summary>
        public required (string Name, ColumnValue Value)[] InjectedDefaults { get; init; }

        /// <summary>Exact output entry count, so the dictionary never resizes.</summary>
        public required int OutputCapacity { get; init; }
    }

    /// <summary>
    /// Per-loop memoisation of <see cref="DictionaryDecodePlan"/> keyed by the stored schema version
    /// embedded in each row, for the mutation-side dictionary decode (<see cref="DecodeWritableAsync"/>).
    /// A mutation chunk fixes the required-column set and visibility version, so every row at the same
    /// stored version shares one plan (usually a single entry). Create one instance per chunk/scan loop
    /// and pass it to <see cref="DecodeWritableAsync"/>; never share it across loops that differ in
    /// those inputs, and never treat it as global transaction state.
    /// </summary>
    public sealed class DictionaryDecodeState
    {
        internal readonly Dictionary<int, DictionaryDecodePlan> Plans = new();
    }

    /// <summary>
    /// Serializes a row into a bare, un-enveloped byte buffer (no <see cref="BranchKvKind"/> marker).
    /// Prefer <see cref="EncodeStorageValue"/> for anything written to Kahuna; this bare form is for
    /// callers/tests that genuinely need the payload without the storage envelope.
    /// </summary>
    public static byte[] Encode(TableSchema schema, IReadOnlyDictionary<string, ColumnValue> row, ObjectIdValue rowId)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(row);

        List<TableColumnSchema> columns = schema.Columns!;
        CompiledRowCodec codec = GetCodec(schema, schema.Version, columns);
        return codec.Encode(RowSlotAdapter.FromRow(columns, row));
    }

    /// <summary>
    /// Serializes a row directly into its final Kahuna storage form: byte 0 is the
    /// <see cref="BranchKvKind.Value"/> marker and the row payload follows in the same array. This
    /// removes the second allocation-and-copy that <see cref="BranchKvCodec.EncodeValue"/> would incur
    /// on top of <see cref="Encode"/> — the value that Kahuna stores is produced with a single
    /// allocation. The output is byte-identical to <c>BranchKvCodec.EncodeValue(Encode(...))</c>.
    /// </summary>
    public static byte[] EncodeStorageValue(TableSchema schema, IReadOnlyDictionary<string, ColumnValue> row, ObjectIdValue rowId)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(row);

        List<TableColumnSchema> columns = schema.Columns!;
        CompiledRowCodec codec = GetCodec(schema, schema.Version, columns);
        return codec.EncodeStorageValue(RowSlotAdapter.FromRow(columns, row));
    }

    /// <summary>
    /// Writes a single column value in the self-describing wire form (a type marker followed by the
    /// payload; a <c>Null</c> value writes just the <see cref="SerializatorTypes.TypeNull"/> marker).
    /// Shared by the row encoder and the secondary-index include-tuple codec so both stay on exactly
    /// one serialization format. Pairs with <see cref="ReadColumnValue"/> and <see cref="ColumnValueSize"/>.
    /// </summary>
    internal static void WriteColumnValue(byte[] buffer, ColumnValue columnValue, ref int pointer)
    {
        switch (columnValue.Type)
        {
            case ColumnType.Id:
                Serializator.WriteType(buffer, SerializatorTypes.TypeId, ref pointer);
                Serializator.WriteObjectId(buffer, ObjectId.ToValue(columnValue.StrValue!), ref pointer);
                break;

            case ColumnType.Integer64:
                Serializator.WriteType(buffer, SerializatorTypes.TypeInteger64, ref pointer);
                Serializator.WriteInt64(buffer, columnValue.LongValue, ref pointer);
                break;

            case ColumnType.String:
                Serializator.WriteType(buffer, SerializatorTypes.TypeString32, ref pointer);
                Serializator.WriteString(buffer, columnValue.StrValue!, ref pointer);
                break;

            case ColumnType.Float64:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDouble, ref pointer);
                Serializator.WriteDouble(buffer, columnValue.FloatValue, ref pointer);
                break;

            case ColumnType.Bool:
                Serializator.WriteBool(buffer, columnValue.BoolValue, ref pointer);
                break;

            case ColumnType.Float32:
                Serializator.WriteType(buffer, SerializatorTypes.TypeFloat, ref pointer);
                Serializator.WriteFloat(buffer, (float)columnValue.FloatValue, ref pointer);
                break;

            case ColumnType.Date:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDate, ref pointer);
                Serializator.WriteInt64(buffer, columnValue.LongValue, ref pointer);
                break;

            case ColumnType.DateTime:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDateTime, ref pointer);
                Serializator.WriteInt64(buffer, columnValue.LongValue, ref pointer);
                break;

            case ColumnType.Bytes:
                Serializator.WriteType(buffer, SerializatorTypes.TypeBytes, ref pointer);
                Serializator.WriteBytesPayload(buffer, columnValue.BytesValue ?? [], ref pointer);
                break;

            case ColumnType.Uuid:
                Serializator.WriteType(buffer, SerializatorTypes.TypeUuid, ref pointer);
                Serializator.WriteUuid(buffer, columnValue.UuidHigh, columnValue.LongValue, ref pointer);
                break;

            case ColumnType.Array:
            {
                IReadOnlyList<ColumnValue> elements = columnValue.ArrayValues ?? [];
                Serializator.WriteType(buffer, SerializatorTypes.TypeArray32, ref pointer);
                Serializator.WriteInt32(buffer, elements.Count, ref pointer);
                buffer[pointer++] = (byte)columnValue.ArrayElementType;
                foreach (ColumnValue el in elements)
                    WriteArrayElement(buffer, el, ref pointer);
                break;
            }

            case ColumnType.Null:
                Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnValue.Type);
        }
    }

    /// <summary>
    /// Byte size of a single column value written by <see cref="WriteColumnValue"/> (a <c>Null</c>
    /// value costs one <see cref="SerializatorTypes.TypeNull"/> marker). Shared with the include-tuple
    /// codec so buffer sizing matches the writer exactly.
    /// </summary>
    internal static int ColumnValueSize(ColumnValue columnValue) => columnValue.Type switch
    {
        ColumnType.Null =>
            SerializatorTypeSizes.TypeNull,
        ColumnType.Id =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeObjectId,
        ColumnType.Integer64 =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger64,
        ColumnType.Float64 =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeDouble,
        ColumnType.Float32 =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeFloat32,
        ColumnType.String =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32
            + Encoding.Unicode.GetByteCount(columnValue.StrValue!),
        ColumnType.Bool =>
            SerializatorTypeSizes.TypeBool,
        ColumnType.Date or ColumnType.DateTime =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger64,
        ColumnType.Bytes =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32
            + (columnValue.BytesValue?.Length ?? 0),
        ColumnType.Uuid =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeUuid,
        ColumnType.Array =>
            SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32 + 1  // type + count + element-type byte
            + ArrayElementsSize(columnValue.ArrayValues ?? []),
        _ => throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnValue.Type)
    };

    /// <summary>
    /// Reads the stored schema version from the front of a positional row payload (a raw little-endian
    /// <c>u32</c> at offset 0). The row id is not stored — it already lives in the KV key — so there is
    /// no header past the version; every value is addressed positionally from the payload start. Kept
    /// synchronous and span-based so async decoders can read the version before awaiting lazy
    /// schema-history loads without holding a span across the await.
    /// </summary>
    private static int ReadRowHeader(ReadOnlySpan<byte> data)
    {
        // Guard the version read: a shorter-than-header payload must surface as a corruption error, not a
        // framework span/bounds exception, to honor the SystemSpaceCorrupt contract on malformed bytes.
        if (data.Length < 4)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Row payload is {data.Length} bytes, shorter than the 4-byte schema-version header");

        return (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data);
    }

    public static Dictionary<string, ColumnValue> Decode(
        TableSchema schema,
        ObjectIdValue rowId,
        ReadOnlySpan<byte> data,
        IReadOnlySet<string>? requiredColumns = null)
    {
        ArgumentNullException.ThrowIfNull(schema);

        int schemaVersion = ReadRowHeader(data);

        List<TableColumnSchema> columns = schema.GetSchemaHistory(schemaVersion).Columns!;
        CompiledRowCodec codec = GetCodec(schema, schemaVersion, columns);
        return DecodeColumns(
            codec,
            columns,
            schema.Columns,
            data,
            requiredColumns,
            ColumnVisibility.PublicOnly,
            injectMissingCurrentColumns: false);
    }

    public static async ValueTask<Dictionary<string, ColumnValue>> DecodeAsync(
        TableSchema schema,
        HLCTimestamp txId,
        ObjectIdValue rowId,
        ReadOnlyMemory<byte> data,
        IReadOnlySet<string>? requiredColumns = null,
        long? visibilitySchemaVersion = null)
    {
        ArgumentNullException.ThrowIfNull(schema);

        // Read the version synchronously (span released before the await); it selects which
        // lazily-loaded history layout to await.
        int schemaVersion = ReadRowHeader(data.Span);

        List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
        List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
            ? columns
            : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);
        CompiledRowCodec codec = GetCodec(schema, schemaVersion, columns);
        return DecodeColumns(
            codec,
            columns,
            visibilityColumns,
            data.Span,
            requiredColumns,
            ColumnVisibility.PublicOnly,
            injectMissingCurrentColumns: visibilitySchemaVersion is not null);
    }

    /// <summary>
    /// Decodes a stored row into a name-keyed dictionary using <b>writable</b> column visibility —
    /// the shape mutation paths (UPDATE/DELETE, TTL, DDL backfills) consume.
    ///
    /// <para>
    /// <paramref name="decodeState"/> is a per-chunk memoisation of the resolved decode plan keyed by
    /// the stored schema version embedded in each row's bytes. Because
    /// <paramref name="requiredColumns"/> and <paramref name="visibilitySchemaVersion"/> are constant
    /// for one mutation chunk/scan loop, every row written under the same stored version shares one
    /// plan; without it, each row pays the schema-history lookups and per-column visibility resolution
    /// again. Pass <see langword="null"/> for one-off decodes outside a loop. Output is value-identical
    /// with and without a state.
    /// </para>
    /// </summary>
    public static async ValueTask<Dictionary<string, ColumnValue>> DecodeWritableAsync(
        TableSchema schema,
        HLCTimestamp txId,
        ObjectIdValue rowId,
        ReadOnlyMemory<byte> data,
        IReadOnlySet<string>? requiredColumns = null,
        long? visibilitySchemaVersion = null,
        DictionaryDecodeState? decodeState = null)
    {
        ArgumentNullException.ThrowIfNull(schema);

        int schemaVersion = ReadRowHeader(data.Span);

        if (decodeState is not null)
        {
            // Plan BUILDING may await lazy schema-history loading; plan EXECUTION below is fully
            // synchronous and span-based, so no span crosses an await.
            if (!decodeState.Plans.TryGetValue(schemaVersion, out DictionaryDecodePlan? plan))
            {
                plan = await BuildDictionaryDecodePlanAsync(
                    schema, txId, schemaVersion, requiredColumns, visibilitySchemaVersion).ConfigureAwait(false);
                decodeState.Plans.Add(schemaVersion, plan);
            }

            return ExecuteDictionaryDecodePlan(plan, data.Span);
        }

        List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
        List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
            ? columns
            : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);
        CompiledRowCodec codec = GetCodec(schema, schemaVersion, columns);
        return DecodeColumns(
            codec,
            columns,
            visibilityColumns,
            data.Span,
            requiredColumns,
            ColumnVisibility.Writable,
            injectMissingCurrentColumns: visibilitySchemaVersion is not null);
    }

    /// <summary>
    /// Resolves the <see cref="DictionaryDecodePlan"/> for one stored schema version under writable
    /// visibility, baking in every decision <see cref="DecodeColumns"/> makes per row: which stored
    /// ordinals are retained (visibility × required-column filter), and which current-schema columns
    /// get an injected default (only when a visibility version is supplied — same rule as the
    /// <c>injectMissingCurrentColumns</c> flag on the plan-less path).
    /// </summary>
    private static async ValueTask<DictionaryDecodePlan> BuildDictionaryDecodePlanAsync(
        TableSchema schema,
        HLCTimestamp txId,
        int schemaVersion,
        IReadOnlySet<string>? requiredColumns,
        long? visibilitySchemaVersion)
    {
        List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
        List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
            ? columns
            : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);
        CompiledRowCodec codec = GetCodec(schema, schemaVersion, columns);

        bool decodeAll = requiredColumns is null;
        string?[] decodeNames = new string?[columns.Count];
        int outputCount = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!IsVisible(column, visibilityColumns, ColumnVisibility.Writable))
                continue;

            if (decodeAll || requiredColumns!.Contains(column.Name))
            {
                decodeNames[i] = column.Name;
                outputCount++;
            }
        }

        (string, ColumnValue)[] injectedDefaults = [];

        if (visibilitySchemaVersion is not null && visibilityColumns is not null)
        {
            List<(string, ColumnValue)>? injected = null;
            HashSet<string>? retainedNames = null;

            foreach (TableColumnSchema current in visibilityColumns)
            {
                if (retainedNames is null)
                {
                    // Same comparer as the output dictionary, mirroring the plan-less path's
                    // result.ContainsKey guard against a name already claimed by a decoded column.
                    retainedNames = new(outputCount, StringComparer.OrdinalIgnoreCase);
                    foreach (string? name in decodeNames)
                    {
                        if (name is not null)
                            retainedNames.Add(name);
                    }
                }

                if (retainedNames.Contains(current.Name))
                    continue;

                if (FindCurrentColumn(current, columns) is not null)
                    continue; // present in row schema — was filtered by visibility or requiredColumns, not a new column

                if (!SchemaElementStateRules.IsWritable(current))
                    continue;

                if (!decodeAll && !requiredColumns!.Contains(current.Name))
                    continue;

                (injected ??= new()).Add((current.Name, current.DefaultValue ?? ColumnValue.Null));
            }

            if (injected is not null)
                injectedDefaults = injected.ToArray();
        }

        return new DictionaryDecodePlan
        {
            Codec = codec,
            DecodeNames = decodeNames,
            InjectedDefaults = injectedDefaults,
            OutputCapacity = outputCount + injectedDefaults.Length,
        };
    }

    private static Dictionary<string, ColumnValue> ExecuteDictionaryDecodePlan(DictionaryDecodePlan plan, ReadOnlySpan<byte> data)
    {
        CompiledRowCodec codec = plan.Codec;
        codec.ValidateFrame(data);

        Dictionary<string, ColumnValue> result = new(plan.OutputCapacity, StringComparer.OrdinalIgnoreCase);

        string?[] decodeNames = plan.DecodeNames;
        for (int i = 0; i < decodeNames.Length; i++)
        {
            if (decodeNames[i] is string name)
                result.Add(name, codec.GetSlot(data, i).ToColumnValue());
        }

        foreach ((string name, ColumnValue value) in plan.InjectedDefaults)
            result[name] = value;

        return result;
    }

    /// <summary>
    /// Decodes a stored row byte buffer into a layout-backed <see cref="QueryRow"/> rather than
    /// a plain dictionary. Values are stored in an ordinal <c>ColumnValue[]</c>, allowing O(1)
    /// access by position.
    ///
    /// <para>
    /// <paramref name="decodeState"/> is a per-scan memoisation of the full <see cref="RowDecodePlan"/>
    /// (layout plus precomputed per-column read/skip steps and injected defaults) keyed by the stored
    /// schema version embedded in each row's bytes. Because <paramref name="requiredColumns"/> and
    /// <paramref name="visibilitySchemaVersion"/> are constant for the life of a single scan, every
    /// row written under the same stored version shares one plan. Passing a non-null state builds the
    /// plan once and executes it for all subsequent rows with that stored version — the common case is
    /// a single entry, built on the first decoded row. Only the build may await (lazy schema-history
    /// loading); executing the plan touches no schema metadata and does no per-column layout/visibility
    /// work. Pass <see langword="null"/> for one-off decodes outside a scan loop.
    /// </para>
    ///
    /// <para>
    /// The <see cref="QueryRow"/> implements <see cref="IReadOnlyDictionary{TKey,TValue}"/>, so
    /// unmigrated consumers that access values by string key continue to work correctly during the
    /// incremental pipeline migration.
    /// </para>
    ///
    /// <para>
    /// Output is value-identical to <see cref="DecodeAsync"/>: same column names, same values, for
    /// the same byte buffer and schema parameters.
    /// </para>
    /// </summary>
    public static async ValueTask<QueryRow> DecodeToQueryRowAsync(
        TableSchema schema,
        HLCTimestamp txId,
        ObjectIdValue rowId,
        ReadOnlyMemory<byte> data,
        CamusDBOptions options,
        IReadOnlySet<string>? requiredColumns = null,
        long? visibilitySchemaVersion = null,
        RowDecodeState? decodeState = null)
    {
        ArgumentNullException.ThrowIfNull(schema);

        int schemaVersion = ReadRowHeader(data.Span);

        // Resolve or build the decode plan for this stored schema version. Plan BUILDING may await lazy
        // schema-history loading; plan EXECUTION below is fully synchronous and span-based, so no span
        // crosses an await.
        RowDecodePlan plan;
        if (decodeState is null || !decodeState.Plans.TryGetValue(schemaVersion, out plan!))
        {
            List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
            List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
                ? columns
                : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);
            bool injectMissing = visibilitySchemaVersion is not null;

            plan = BuildRowDecodePlan(schema, schemaVersion, columns, visibilityColumns, requiredColumns, ColumnVisibility.PublicOnly, injectMissing);
            decodeState?.Plans.Add(schemaVersion, plan);
        }

        // Borrowed (zero-copy) decode is chosen per scan and takes precedence over slot-backed: the row
        // is backed by a RowView over the KV bytes with no per-row ValueSlot[]. The frame is validated
        // once here, up front, so later lazy per-cell reads are safe.
        // The per-scan override wins; without one, only the global ForceBorrowed policy turns borrowed on
        // at this level (the scanner sets the override for Adaptive-borrowed scans — this facade has no
        // plan/filter context of its own, so Adaptive/ForceEager both mean eager here).
        bool borrowed = decodeState?.BorrowedDecode ?? (options.BorrowedDecode == BorrowedDecodePolicy.ForceBorrowed);
        if (borrowed)
        {
            plan.Codec.ValidateFrame(data.Span);
            RowView view = new(rowId, data, plan.Codec);
            return QueryRow.FromRowView(rowId, plan.Layout, view, plan.OutputToStored, plan.InjectedDefaults);
        }

        // Slot-backed decode is chosen per scan: the caller's RowDecodeState may opt in/out based on
        // its shape (see RowDecodeState.SlotBackedDecode); absent an override, the global default applies.
        bool useSlots = decodeState?.SlotBackedDecode ?? options.SlotBackedDecode;
        return ExecuteDecodePlan(plan, data.Span, rowId, useSlots);
    }

    /// <summary>
    /// Builds the reusable <see cref="RowDecodePlan"/> for a combination of stored (history) columns,
    /// current visibility columns, required-column filter, and visibility mode. This performs all the
    /// per-column visibility, required-set, layout-index, and current-column-search work once so the
    /// per-row execution loop does none of it. Output is decode-equivalent to the previous inline
    /// two-pass decode.
    /// </summary>
    private static RowDecodePlan BuildRowDecodePlan(
        TableSchema schema,
        int schemaVersion,
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility,
        bool injectMissingCurrentColumns)
    {
        RowLayout layout = BuildRowLayout(columns, currentColumns, requiredColumns, visibility, injectMissingCurrentColumns);
        bool decodeAll = requiredColumns is null;

        // One step per stored column, in byte-stream order. A visible + required column whose name is
        // in the layout reads into its ordinal; everything else skips its bytes.
        ColumnDecodeStep[] steps = new ColumnDecodeStep[columns.Count];
        bool[] written = new bool[layout.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];
            int ord = -1;
            if (IsVisible(column, currentColumns, visibility) &&
                (decodeAll || requiredColumns!.Contains(column.Name)))
            {
                int candidate = layout.IndexOf(column.Name);
                if (candidate >= 0)
                {
                    ord = candidate;
                    written[candidate] = true;
                }
            }
            steps[i] = new ColumnDecodeStep(column.Type, ord);
        }

        // Columns added after this row was written are absent from the byte stream: precompute their
        // default value and output slot. Mirrors the inject-missing pass, including the "already
        // decoded from bytes" guard (an ordinal a step writes is excluded here).
        (int Ordinal, ColumnValue Value)[] injected;
        if (injectMissingCurrentColumns && currentColumns is not null)
        {
            List<(int, ColumnValue)> list = new();
            foreach (TableColumnSchema current in currentColumns)
            {
                int ord = layout.IndexOf(current.Name);
                if (ord < 0) continue;
                if (written[ord]) continue;                                  // decoded from bytes
                if (FindCurrentColumn(current, columns) is not null) continue; // present in history, not new

                bool visible = visibility switch
                {
                    ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
                    ColumnVisibility.Writable   => SchemaElementStateRules.IsWritable(current),
                    _ => false
                };
                if (!visible) continue;
                if (!decodeAll && !requiredColumns!.Contains(current.Name)) continue;

                list.Add((ord, current.DefaultValue ?? ColumnValue.Null));
            }
            injected = list.ToArray();
        }
        else
        {
            injected = [];
        }

        // Inverse map for borrowed decode: output ordinal → stored ordinal (-1 = injected default).
        int[] outputToStored = new int[layout.Count];
        Array.Fill(outputToStored, -1);
        for (int i = 0; i < steps.Length; i++)
        {
            int outputOrdinal = steps[i].OutputOrdinal;
            if (outputOrdinal >= 0)
                outputToStored[outputOrdinal] = i;
        }

        return new RowDecodePlan
        {
            Layout = layout,
            Codec = GetCodec(schema, schemaVersion, columns),
            Steps = steps,
            OutputToStored = outputToStored,
            InjectedDefaults = injected,
        };
    }

    /// <summary>
    /// Executes a prebuilt <see cref="RowDecodePlan"/> against one row's bytes. Fully synchronous — the
    /// per-column loop does only a read-or-skip on the precomputed step, with no visibility checks,
    /// required-set lookups, layout index lookups, or current-column searches.
    ///
    /// <para>
    /// Produces a slot-backed <see cref="QueryRow"/> when <paramref name="useSlots"/> is set: one
    /// <c>ValueSlot[]</c> allocation and zero per-cell <see cref="ColumnValue"/> objects, with cells
    /// materialized lazily on access — a win only when the scan rejects rows, so those cells are never
    /// materialized. Otherwise it takes the eager <see cref="ColumnValue"/><c>[]</c> path, which is
    /// faster when every row is fully materialized. The caller chooses per scan (see
    /// <see cref="RowDecodeState.SlotBackedDecode"/> / <see cref="CamusDBOptions.SlotBackedDecode"/>);
    /// both paths are value-identical.
    /// </para>
    /// </summary>
    private static QueryRow ExecuteDecodePlan(RowDecodePlan plan, ReadOnlySpan<byte> data, ObjectIdValue rowId, bool useSlots)
    {
        // One frame bounds-check before any positional read; a projected-out column costs nothing since
        // positional access needs no byte-stream skip.
        plan.Codec.ValidateFrame(data);

        if (!useSlots)
            return ExecuteDecodePlanEager(plan, data, rowId);

        ValueSlot[] values = new ValueSlot[plan.Layout.Count];

        ColumnDecodeStep[] steps = plan.Steps;
        for (int i = 0; i < steps.Length; i++)
        {
            int outputOrdinal = steps[i].OutputOrdinal;
            if (outputOrdinal >= 0)
                values[outputOrdinal] = plan.Codec.GetSlot(data, i);
        }

        (int Ordinal, ColumnValue Value)[] injected = plan.InjectedDefaults;
        for (int i = 0; i < injected.Length; i++)
            values[injected[i].Ordinal] = ValueSlot.FromColumnValue(injected[i].Value);

        return QueryRow.FromSlots(rowId, plan.Layout, values);
    }

    /// <summary>
    /// Eager fallback for <see cref="ExecuteDecodePlan"/>: decodes each cell straight into a
    /// <see cref="ColumnValue"/> as the pipeline did before slot-backed rows. Kept as the kill-switch
    /// path (<see cref="CamusDBOptions.SlotBackedDecode"/> == false) and the A/B baseline.
    /// </summary>
    private static QueryRow ExecuteDecodePlanEager(RowDecodePlan plan, ReadOnlySpan<byte> data, ObjectIdValue rowId)
    {
        ColumnValue[] values = new ColumnValue[plan.Layout.Count];

        ColumnDecodeStep[] steps = plan.Steps;
        for (int i = 0; i < steps.Length; i++)
        {
            int outputOrdinal = steps[i].OutputOrdinal;
            if (outputOrdinal >= 0)
                values[outputOrdinal] = plan.Codec.GetSlot(data, i).ToColumnValue();
        }

        (int Ordinal, ColumnValue Value)[] injected = plan.InjectedDefaults;
        for (int i = 0; i < injected.Length; i++)
            values[injected[i].Ordinal] = injected[i].Value;

        return new QueryRow(rowId, plan.Layout, values);
    }

    /// <summary>
    /// Builds the <see cref="RowLayout"/> for a given combination of history columns, visibility
    /// columns, required-column filter, and visibility mode. This is the first pass of the decode:
    /// it determines which column names will appear in the output and in what order, without
    /// touching the byte buffer. The result can be cached and reused for every row that shares the
    /// same stored schema version within a scan.
    /// </summary>
    private static RowLayout BuildRowLayout(
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility,
        bool injectMissingCurrentColumns)
    {
        bool decodeAll = requiredColumns is null;

        List<string> outputNames = new(columns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];
            if (IsVisible(column, currentColumns, visibility) &&
                (decodeAll || requiredColumns!.Contains(column.Name)))
                outputNames.Add(column.Name);
        }

        if (injectMissingCurrentColumns && currentColumns is not null)
        {
            // Use a set over outputNames to avoid O(n²) Contains checks.
            HashSet<string> included = new(outputNames, StringComparer.Ordinal);

            foreach (TableColumnSchema current in currentColumns)
            {
                if (included.Contains(current.Name)) continue;
                if (FindCurrentColumn(current, columns) is not null) continue;

                bool visible = visibility switch
                {
                    ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
                    ColumnVisibility.Writable   => SchemaElementStateRules.IsWritable(current),
                    _ => false
                };
                if (!visible) continue;
                if (!decodeAll && !requiredColumns!.Contains(current.Name)) continue;

                outputNames.Add(current.Name);
            }
        }

        return RowLayout.ForColumns(outputNames);
    }

    private static async ValueTask<List<TableColumnSchema>?> GetVisibilityColumnsAsync(
        TableSchema schema,
        HLCTimestamp txId,
        long visibilitySchemaVersion
    )
    {
        if (visibilitySchemaVersion > int.MaxValue || visibilitySchemaVersion < int.MinValue)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Invalid table schema visibility version {visibilitySchemaVersion}"
            );

        return (await schema.GetSchemaHistoryAsync(txId, (int)visibilitySchemaVersion).ConfigureAwait(false)).Columns;
    }

    private static Dictionary<string, ColumnValue> DecodeColumns(
        CompiledRowCodec codec,
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        ReadOnlySpan<byte> data,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility,
        bool injectMissingCurrentColumns)
    {
        codec.ValidateFrame(data);

        bool decodeAll = requiredColumns is null;

        Dictionary<string, ColumnValue> result = new(decodeAll ? columns.Count : requiredColumns!.Count, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            // Positional decode: a not-visible or projected-out column is simply not read — there is no
            // byte-stream cursor to advance past it.
            if (!IsVisible(column, currentColumns, visibility))
                continue;

            if (decodeAll || requiredColumns!.Contains(column.Name))
                result.Add(column.Name, codec.GetSlot(data, i).ToColumnValue());
        }

        // Columns added after this row was written are absent from the byte stream.
        // Inject their default value (or a typed null) so callers see consistent output.
        if (injectMissingCurrentColumns && currentColumns is not null)
        {
            foreach (TableColumnSchema current in currentColumns)
            {
                if (result.ContainsKey(current.Name))
                    continue;

                if (FindCurrentColumn(current, columns) is not null)
                    continue; // present in row schema — was filtered by visibility or requiredColumns, not a new column

                bool visible = visibility switch
                {
                    ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
                    ColumnVisibility.Writable => SchemaElementStateRules.IsWritable(current),
                    _ => false
                };

                if (!visible)
                    continue;

                if (!decodeAll && !requiredColumns!.Contains(current.Name))
                    continue;

                result[current.Name] = current.DefaultValue ?? ColumnValue.Null;
            }
        }

        return result;
    }

    private static bool IsVisible(
        TableColumnSchema column,
        List<TableColumnSchema>? currentColumns,
        ColumnVisibility visibility
    )
    {
        TableColumnSchema? current = FindCurrentColumn(column, currentColumns);
        if (current is null)
            return false;

        return visibility switch
        {
            ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
            ColumnVisibility.Writable => SchemaElementStateRules.IsWritable(current),
            _ => false
        };
    }

    private static TableColumnSchema? FindCurrentColumn(TableColumnSchema historyColumn, List<TableColumnSchema>? currentColumns)
    {
        if (currentColumns is null)
            return null;

        foreach (TableColumnSchema current in currentColumns)
        {
            if (current.Id == historyColumn.Id)
                return current;
        }

        if (string.IsNullOrWhiteSpace(historyColumn.Id))
        {
            foreach (TableColumnSchema current in currentColumns)
            {
                if (string.Equals(current.Name, historyColumn.Name, StringComparison.OrdinalIgnoreCase))
                    return current;
            }
        }

        return null;
    }

    internal static ColumnValue ReadColumnValue(ColumnType columnType, ReadOnlySpan<byte> data, ref int pointer)
    {
        switch (columnType)
        {
            case ColumnType.Id:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeId =>
                        new(ColumnType.Id, Serializator.ReadObjectId(data, ref pointer).ToString()),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Integer64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeInteger64 =>
                        new(ColumnType.Integer64, Serializator.ReadInt64(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.String:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeString8 or
                    SerializatorTypes.TypeString16 or
                    SerializatorTypes.TypeString32 =>
                        new(ColumnType.String, Serializator.ReadString(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Float64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeDouble =>
                        new(ColumnType.Float64, Serializator.ReadDouble(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Bool:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeBool =>
                        ColumnValue.FromBool(Serializator.ReadBool(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Float32:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeFloat =>
                        new(ColumnType.Float32, (double)Serializator.ReadFloat(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Date:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeDate =>
                        new(ColumnType.Date, Serializator.ReadInt64(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.DateTime:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeDateTime =>
                        new(ColumnType.DateTime, Serializator.ReadInt64(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Bytes:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeBytes =>
                        new(Serializator.ReadBytesPayload(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Uuid:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeUuid)
                {
                    (long high, long low) = Serializator.ReadUuid(data, ref pointer);
                    return new ColumnValue(ColumnType.Uuid, high, low);
                }
                if (t == SerializatorTypes.TypeNull)
                    return ColumnValue.Null;
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
            }

            case ColumnType.Array:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeNull)
                    return ColumnValue.Null;
                if (t != SerializatorTypes.TypeArray32)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());

                int count = Serializator.ReadInt32(data, ref pointer);
                ColumnType elementType = (ColumnType)Serializator.ReadInt8(data, ref pointer);
                List<ColumnValue> elements = new(count);
                for (int j = 0; j < count; j++)
                    elements.Add(ReadArrayElement(elementType, data, ref pointer));
                return ColumnValue.FromArray(elementType, elements);
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnType);
        }
    }

    internal static void SkipColumnValue(ColumnType columnType, ReadOnlySpan<byte> data, ref int pointer)
    {
        switch (columnType)
        {
            case ColumnType.Id:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeId)
                    Serializator.ReadObjectId(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Integer64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeInteger64)
                    Serializator.ReadInt64(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.String:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t is SerializatorTypes.TypeString8 or SerializatorTypes.TypeString16 or SerializatorTypes.TypeString32)
                    Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Float64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeDouble)
                    Serializator.ReadDouble(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Bool:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t is not (SerializatorTypes.TypeBool or SerializatorTypes.TypeNull))
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Float32:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeFloat)
                    pointer += SerializatorTypeSizes.TypeFloat32;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Date:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeDate)
                    pointer += SerializatorTypeSizes.TypeInteger64;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.DateTime:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeDateTime)
                    pointer += SerializatorTypeSizes.TypeInteger64;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Bytes:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeBytes)
                    Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Uuid:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeUuid)
                    pointer += SerializatorTypeSizes.TypeUuid;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Array:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeNull)
                    break;
                if (t != SerializatorTypes.TypeArray32)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                int count = Serializator.ReadInt32(data, ref pointer);
                Serializator.ReadInt8(data, ref pointer); // element type byte — discard
                for (int j = 0; j < count; j++)
                    SkipArrayElement(data, ref pointer);
                break;
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnType);
        }
    }

    // -------------------------------------------------------------------------
    // Array element helpers
    // -------------------------------------------------------------------------

    private static int ArrayElementsSize(IReadOnlyList<ColumnValue> elements)
    {
        int total = 0;
        foreach (ColumnValue el in elements)
        {
            total += el.Type switch
            {
                ColumnType.Null      => 1,
                ColumnType.Bool      => 1,
                ColumnType.Integer64 => 1 + SerializatorTypeSizes.TypeInteger64,
                ColumnType.Float32   => 1 + SerializatorTypeSizes.TypeFloat32,
                ColumnType.Float64   => 1 + SerializatorTypeSizes.TypeDouble,
                ColumnType.Date or ColumnType.DateTime => 1 + SerializatorTypeSizes.TypeInteger64,
                ColumnType.String    => 1 + SerializatorTypeSizes.TypeInteger32 + Encoding.Unicode.GetByteCount(el.StrValue!),
                ColumnType.Bytes     => 1 + SerializatorTypeSizes.TypeInteger32 + (el.BytesValue?.Length ?? 0),
                ColumnType.Uuid      => 1 + SerializatorTypeSizes.TypeUuid,
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported in size: " + el.Type)
            };
        }
        return total;
    }

    private static void WriteArrayElement(byte[] buffer, ColumnValue el, ref int pointer)
    {
        switch (el.Type)
        {
            case ColumnType.Null:
                Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
                break;
            case ColumnType.Integer64:
                Serializator.WriteType(buffer, SerializatorTypes.TypeInteger64, ref pointer);
                Serializator.WriteInt64(buffer, el.LongValue, ref pointer);
                break;
            case ColumnType.Float32:
                Serializator.WriteType(buffer, SerializatorTypes.TypeFloat, ref pointer);
                Serializator.WriteFloat(buffer, (float)el.FloatValue, ref pointer);
                break;
            case ColumnType.Float64:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDouble, ref pointer);
                Serializator.WriteDouble(buffer, el.FloatValue, ref pointer);
                break;
            case ColumnType.Bool:
                Serializator.WriteBool(buffer, el.BoolValue, ref pointer);
                break;
            case ColumnType.String:
                Serializator.WriteType(buffer, SerializatorTypes.TypeString32, ref pointer);
                Serializator.WriteString(buffer, el.StrValue!, ref pointer);
                break;
            case ColumnType.Bytes:
                Serializator.WriteType(buffer, SerializatorTypes.TypeBytes, ref pointer);
                Serializator.WriteBytesPayload(buffer, el.BytesValue ?? [], ref pointer);
                break;
            case ColumnType.Uuid:
                Serializator.WriteType(buffer, SerializatorTypes.TypeUuid, ref pointer);
                Serializator.WriteUuid(buffer, el.UuidHigh, el.LongValue, ref pointer);
                break;
            case ColumnType.Date:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDate, ref pointer);
                Serializator.WriteInt64(buffer, el.LongValue, ref pointer);
                break;
            case ColumnType.DateTime:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDateTime, ref pointer);
                Serializator.WriteInt64(buffer, el.LongValue, ref pointer);
                break;
            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported in write: " + el.Type);
        }
    }

    private static ColumnValue ReadArrayElement(ColumnType elementType, ReadOnlySpan<byte> data, ref int pointer)
    {
        int t = Serializator.ReadType(data, ref pointer);
        if (t == SerializatorTypes.TypeNull)
            return ColumnValue.Null;

        return elementType switch
        {
            ColumnType.Integer64 => new(ColumnType.Integer64, Serializator.ReadInt64(data, ref pointer)),
            ColumnType.Float32   => new(ColumnType.Float32,   (double)Serializator.ReadFloat(data, ref pointer)),
            ColumnType.Float64   => new(ColumnType.Float64,   Serializator.ReadDouble(data, ref pointer)),
            ColumnType.Bool      => ColumnValue.FromBool(Serializator.ReadBool(data, ref pointer)),
            ColumnType.String    => new(ColumnType.String,    Serializator.ReadString(data, ref pointer)),
            ColumnType.Bytes     => new(Serializator.ReadBytesPayload(data, ref pointer)),
            ColumnType.Uuid      => ReadUuidElement(data, ref pointer),
            ColumnType.Date      => new(ColumnType.Date,      Serializator.ReadInt64(data, ref pointer)),
            ColumnType.DateTime  => new(ColumnType.DateTime,  Serializator.ReadInt64(data, ref pointer)),
            _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Unknown array element type: " + elementType)
        };
    }

    private static ColumnValue ReadUuidElement(ReadOnlySpan<byte> data, ref int pointer)
    {
        (long high, long low) = Serializator.ReadUuid(data, ref pointer);
        return new ColumnValue(ColumnType.Uuid, high, low);
    }

    private static void SkipArrayElement(ReadOnlySpan<byte> data, ref int pointer)
    {
        int t = Serializator.ReadType(data, ref pointer);
        switch (t)
        {
            case SerializatorTypes.TypeNull:
                break;
            case SerializatorTypes.TypeBool:
                break; // value is in the same byte as the type tag — already consumed
            case SerializatorTypes.TypeInteger64:
                pointer += SerializatorTypeSizes.TypeInteger64;
                break;
            case SerializatorTypes.TypeFloat:
                pointer += SerializatorTypeSizes.TypeFloat32;
                break;
            case SerializatorTypes.TypeDouble:
                pointer += SerializatorTypeSizes.TypeDouble;
                break;
            case SerializatorTypes.TypeDate:
            case SerializatorTypes.TypeDateTime:
                pointer += SerializatorTypeSizes.TypeInteger64;
                break;
            case SerializatorTypes.TypeString8:
            case SerializatorTypes.TypeString16:
            case SerializatorTypes.TypeString32:
                Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                break;
            case SerializatorTypes.TypeBytes:
                Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                break;
            case SerializatorTypes.TypeUuid:
                pointer += SerializatorTypeSizes.TypeUuid;
                break;
            default:
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Unknown array element disk type: " + t);
        }
    }

}
