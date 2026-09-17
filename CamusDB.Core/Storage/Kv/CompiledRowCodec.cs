
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// An immutable, schema-version-specific plan for encoding and decoding one physical row layout, plus
/// the encode/decode primitives that operate against it. This is the positional, schema-driven codec
/// that replaces the self-describing <see cref="RowEncoder"/> format: types and column count come from
/// the compiled plan (built from a single schema-history version), never from per-cell type tags, and
/// the row id is never stored in the payload (it already lives in the KV key).
///
/// <para><b>Wire format</b> (payload, i.e. after the one-byte <see cref="BranchKvKind"/> envelope):</para>
/// <code>
///   schemaVersion : u32 little-endian
///   nullBitmap    : ceil(columnCount / 8) bytes   — one bit per column, set = NULL
///   boolBitmap    : ceil(boolColumnCount / 8) bytes — one bit per bool column, set = true
///   fixedArea     : dense, schema-compiled offsets (a NULL fixed cell still reserves its slot)
///   varEndOffsets : u32 little-endian × variableColumnCount — end offset of each variable column,
///                   relative to the start of variableArea (start = previous end, or 0)
///   variableArea  : concatenated UTF-8 / raw-bytes / array payloads
/// </code>
///
/// <para>
/// Bit 31 of <c>schemaVersion</c> marks a row whose variable cells may be compressed or moved out of
/// line; such a row ends with a storage-form trailer. <see cref="RowStorageForms"/> owns that extension.
/// The codec only masks the bit when it compares versions, and refuses to read a marked cell: the store
/// resolves every cell a caller needs before the bytes reach a decoder (see
/// <see cref="KvTableStore.ResolveLargeValuesAsync"/>).
/// </para>
///
/// <para>
/// A NULL cell of a fixed-width column still occupies its fixed slot so that fixed offsets stay
/// compile-time constant and projected fixed reads are O(1). A NULL variable cell writes no payload
/// but keeps its (zero-length) offset-directory entry, so a NULL string and an empty string are
/// distinguished only by the null bitmap. All integers/floats are little-endian and unaligned;
/// CamusDB owns both writer and reader.
/// </para>
///
/// <para>
/// The plan is a pure function of one schema version's column list; build it once per encountered
/// stored version and cache it (see the per-table codec cache). It carries no mutable state and is
/// safe to share across threads and rows.
/// </para>
/// </summary>
internal sealed class CompiledRowCodec
{
    /// <summary>How a column's value is physically stored.</summary>
    private enum StorageClass : byte
    {
        /// <summary>Dense fixed-width slot in <c>fixedArea</c> (Id, Integer64, Float64, Float32, Date, DateTime, Uuid).</summary>
        Fixed,

        /// <summary>One bit in <c>boolBitmap</c>; no <c>fixedArea</c> or <c>variableArea</c> footprint.</summary>
        Bool,

        /// <summary>Payload in <c>variableArea</c> located via the offset directory (String, Bytes, Array).</summary>
        Variable,
    }

    /// <summary>
    /// One compiled column instruction. Only the fields relevant to <see cref="Class"/> are meaningful:
    /// <see cref="FixedOffset"/>/<see cref="FixedWidth"/> for <see cref="StorageClass.Fixed"/>,
    /// <see cref="BoolBitIndex"/> for <see cref="StorageClass.Bool"/>, and
    /// <see cref="VariableOrdinal"/> for <see cref="StorageClass.Variable"/>. <see cref="NullBitIndex"/>
    /// is the column's own ordinal and is always valid.
    /// </summary>
    private readonly struct ColumnPlan
    {
        public required ColumnType Type { get; init; }
        public required StorageClass Class { get; init; }
        public required int NullBitIndex { get; init; }
        public int FixedOffset { get; init; }
        public int FixedWidth { get; init; }
        public int BoolBitIndex { get; init; }
        public int VariableOrdinal { get; init; }
        public ColumnType ArrayElementType { get; init; }
    }

    /// <summary>Stored schema version this plan decodes/encodes; the first four payload bytes must equal it.</summary>
    public int SchemaVersion { get; }

    /// <summary>Number of stored columns (= length of the <see cref="ValueSlot"/> span an encode expects).</summary>
    public int ColumnCount => columns.Length;

    /// <summary>True when the row has no variable columns, so its encoded size is a compile-time constant.</summary>
    public bool IsFixedOnly => variableColumnCount == 0;

    private readonly ColumnPlan[] columns;
    private readonly int nullBitmapBytes;
    private readonly int boolBitmapBytes;
    private readonly int fixedAreaSize;
    private readonly int variableColumnCount;

    // Precomputed absolute payload offsets of each region (the fixed prefix that precedes variableArea).
    private readonly int nullBitmapOffset;
    private readonly int boolBitmapOffset;
    private readonly int fixedAreaOffset;
    private readonly int variableOffsetsOffset;

    /// <summary>Byte length of the fixed prefix (everything before the first variable payload byte).</summary>
    private readonly int headerSize;

    private const int SchemaVersionSize = 4;

    private CompiledRowCodec(int schemaVersion, ColumnPlan[] columns, int nullBitmapBytes, int boolBitmapBytes, int fixedAreaSize, int variableColumnCount)
    {
        SchemaVersion = schemaVersion;
        this.columns = columns;
        this.nullBitmapBytes = nullBitmapBytes;
        this.boolBitmapBytes = boolBitmapBytes;
        this.fixedAreaSize = fixedAreaSize;
        this.variableColumnCount = variableColumnCount;

        nullBitmapOffset = SchemaVersionSize;
        boolBitmapOffset = nullBitmapOffset + nullBitmapBytes;
        fixedAreaOffset = boolBitmapOffset + boolBitmapBytes;
        variableOffsetsOffset = fixedAreaOffset + fixedAreaSize;
        headerSize = variableOffsetsOffset + variableColumnCount * 4;
    }

    /// <summary>
    /// Compiles a plan from one schema version's column list (in schema/ordinal order). Each column
    /// gets a null-bitmap bit; bool columns additionally get a bool-bitmap bit; fixed-width columns get
    /// a dense <c>fixedArea</c> slot; variable columns (String/Bytes/Array) get an offset-directory
    /// ordinal. The column list must match the layout the rows of <paramref name="schemaVersion"/> were
    /// written under — pass the historical layout from <c>TableSchema.GetSchemaHistory</c> for old rows.
    /// </summary>
    public static CompiledRowCodec Build(int schemaVersion, IReadOnlyList<TableColumnSchema> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        ColumnPlan[] plans = new ColumnPlan[columns.Count];
        int fixedCursor = 0;
        int boolCursor = 0;
        int variableCursor = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];
            StorageClass storageClass = ClassOf(column.Type);

            switch (storageClass)
            {
                case StorageClass.Fixed:
                {
                    int width = FixedWidthOf(column.Type);
                    plans[i] = new ColumnPlan
                    {
                        Type = column.Type,
                        Class = StorageClass.Fixed,
                        NullBitIndex = i,
                        FixedWidth = width,
                        FixedOffset = fixedCursor,   // relative to fixedArea; absolute offset added lazily below
                    };
                    fixedCursor += width;
                    break;
                }

                case StorageClass.Bool:
                    plans[i] = new ColumnPlan
                    {
                        Type = column.Type,
                        Class = StorageClass.Bool,
                        NullBitIndex = i,
                        BoolBitIndex = boolCursor++,
                    };
                    break;

                default: // Variable
                    plans[i] = new ColumnPlan
                    {
                        Type = column.Type,
                        Class = StorageClass.Variable,
                        NullBitIndex = i,
                        VariableOrdinal = variableCursor++,
                        ArrayElementType = column.Type == ColumnType.Array
                            ? column.ArrayElementType ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Array column '{column.Name}' has no element type")
                            : ColumnType.Null,
                    };
                    break;
            }
        }

        int nullBitmapBytes = CeilDiv(columns.Count, 8);
        int boolBitmapBytes = CeilDiv(boolCursor, 8);
        int fixedAreaSize = fixedCursor;

        // Rebase fixed offsets to absolute payload offsets now that the header geometry is known.
        int fixedAreaOffset = SchemaVersionSize + nullBitmapBytes + boolBitmapBytes;
        for (int i = 0; i < plans.Length; i++)
        {
            if (plans[i].Class == StorageClass.Fixed)
                plans[i] = plans[i] with { FixedOffset = fixedAreaOffset + plans[i].FixedOffset };
        }

        return new CompiledRowCodec(schemaVersion, plans, nullBitmapBytes, boolBitmapBytes, fixedAreaSize, variableCursor);
    }

    // ─────────────────────────────── Encode ───────────────────────────────

    /// <summary>
    /// Encodes <paramref name="values"/> (one slot per stored column, in schema ordinal order) directly
    /// into the final Kahuna storage form: byte 0 is the <see cref="BranchKvKind.Value"/> envelope
    /// marker and the row payload follows in the same array — a single allocation, no envelope copy.
    /// Preconditions the caller must satisfy: <paramref name="values"/> length equals
    /// <see cref="ColumnCount"/>, and each non-NULL slot's type matches its column's physical type
    /// (validation resolves defaults/visibility and rejects type mismatches before encode).
    /// </summary>
    public byte[] EncodeStorageValue(ReadOnlySpan<ValueSlot> values)
    {
        int payloadSize = ComputePayloadSize(values);
        byte[] buffer = new byte[1 + payloadSize];
        buffer[0] = (byte)BranchKvKind.Value;
        WritePayload(buffer.AsSpan(1), values);
        return buffer;
    }

    /// <summary>
    /// Encodes one row into its Kahuna storage value under a <see cref="LargeValuePolicy"/>: each
    /// variable cell is stored raw, compressed inline, or as a pointer to a separately written value,
    /// following the order PostgreSQL uses — compress first when the column strategy allows it, then
    /// compare the <em>resulting</em> size with the threshold. So a value that compresses well stays
    /// inline and costs no second key.
    ///
    /// <para>A row whose cells all stay raw is byte-identical to <see cref="EncodeStorageValue(ReadOnlySpan{ValueSlot})"/>
    /// and allocates nothing more: the policy is checked against each cell's size before any value bytes
    /// are produced.</para>
    ///
    /// <para><paramref name="carrySource"/> and <paramref name="carryColumns"/> serve an update that
    /// leaves a large column untouched. A column marked in <paramref name="carryColumns"/> copies its
    /// stored cell, marks included, from <paramref name="carrySource"/> — the old row payload, which must
    /// have been written under this same schema version — and its slot in <paramref name="values"/> is
    /// ignored. A carried out-of-line pointer keeps naming the same key, so the large value itself is not
    /// rewritten; that is the write-amplification win for an update of a small column.</para>
    /// </summary>
    public EncodedRow EncodeStorageValue(
        ReadOnlySpan<ValueSlot> values,
        LargeValuePolicy policy,
        ReadOnlySpan<byte> carrySource = default,
        ReadOnlySpan<bool> carryColumns = default)
    {
        if (values.Length != ColumnCount)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Row has {values.Length} values but schema version {SchemaVersion} has {ColumnCount} columns");

        bool anyCarry = carryColumns.IndexOf(true) >= 0;
        if (variableColumnCount == 0 || (!anyCarry && !AnyCellMayChange(values, policy)))
            return new EncodedRow(EncodeStorageValue(values), null);

        RowStorageForms.TrailerLayout carryLayout = default;
        if (anyCarry)
        {
            if (carryColumns.Length != ColumnCount)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Carried-column mask does not match the row layout");

            uint carryWord = BinaryPrimitives.ReadUInt32LittleEndian(carrySource);
            if ((carryWord & RowStorageForms.VersionMask) != (uint)SchemaVersion)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"A carried cell must come from a row of schema version {SchemaVersion}");

            if (RowStorageForms.HasTrailer(carrySource))
                carryLayout = RowStorageForms.ReadLayout(carrySource);
        }

        CellPlan[] cells = new CellPlan[variableColumnCount];
        List<LargeValueWrite>? outOfLine = null;
        bool anyMark = false;
        int variableAreaSize = 0;

        for (int i = 0; i < columns.Length; i++)
        {
            ColumnPlan plan = columns[i];
            if (plan.Class != StorageClass.Variable)
                continue;

            int v = plan.VariableOrdinal;

            if (anyCarry && carryColumns[i])
            {
                ReadOnlySpan<byte> carried = GetCarriedCell(carrySource, carryLayout, v, out bool carriedOutOfLine, out bool carriedCompressed, out bool carriedNull);
                cells[v] = new CellPlan { Carry = true, CarryNull = carriedNull, OutOfLine = carriedOutOfLine, Compressed = carriedCompressed, Size = carried.Length };
                variableAreaSize += carried.Length;
                anyMark |= carriedOutOfLine | carriedCompressed;
                continue;
            }

            ref readonly ValueSlot slot = ref values[i];
            if (slot.IsNull)
                continue;

            int rawSize = VariablePayloadSize(in slot, plan.Type, plan.ArrayElementType);
            ColumnStorageStrategy strategy = policy.StrategyOf(i);

            bool mayCompress = policy.CompressionEnabled
                && strategy is ColumnStorageStrategy.Extended or ColumnStorageStrategy.Main
                && rawSize >= LargeValueCompression.MinCompressibleBytes;

            bool mayMoveOut = policy.ThresholdBytes > 0
                && strategy is ColumnStorageStrategy.Extended or ColumnStorageStrategy.External
                && v <= RowStorageForms.MaxOutOfLineOrdinal
                && rawSize >= policy.ThresholdBytes;

            if (!mayCompress && !mayMoveOut)
            {
                variableAreaSize += rawSize;
                continue;
            }

            byte[] raw = MaterializeVariable(in slot, plan.Type, plan.ArrayElementType, rawSize);
            byte[] stored = raw;
            bool compressed = false;

            if (mayCompress && LargeValueCompression.TryCompress(raw, policy.MinSavingPercent, out byte[]? block))
            {
                stored = block!;
                compressed = true;
            }

            if (mayMoveOut && stored.Length >= policy.ThresholdBytes)
            {
                byte[] storageValue = new byte[1 + stored.Length];
                storageValue[0] = (byte)BranchKvKind.Value;
                stored.CopyTo(storageValue, 1);

                (outOfLine ??= []).Add(new LargeValueWrite(v, storageValue));
                cells[v] = new CellPlan { OutOfLine = true, Compressed = compressed, RawLength = raw.Length, Bytes = stored, Size = RowStorageForms.PointerSize };
                variableAreaSize += RowStorageForms.PointerSize;
                anyMark = true;
                continue;
            }

            if (compressed)
            {
                cells[v] = new CellPlan { Compressed = true, RawLength = raw.Length, Bytes = stored, Size = RowStorageForms.CompressedPrefixSize + stored.Length };
                variableAreaSize += RowStorageForms.CompressedPrefixSize + stored.Length;
                anyMark = true;
                continue;
            }

            // Compression did not pay and the value stays inline: keep the materialized bytes so the
            // write pass does not encode the value a second time.
            cells[v] = new CellPlan { Bytes = raw, RawLength = raw.Length, Size = raw.Length };
            variableAreaSize += raw.Length;
        }

        int payloadSize = headerSize + variableAreaSize + (anyMark ? RowStorageForms.TrailerSize(variableColumnCount) : 0);
        byte[] buffer = new byte[1 + payloadSize];
        buffer[0] = (byte)BranchKvKind.Value;
        WritePayloadWithForms(buffer.AsSpan(1), values, cells, anyMark, carrySource, carryLayout);

        return new EncodedRow(buffer, outOfLine);
    }

    /// <summary>
    /// Returns the stored cell of variable ordinal <paramref name="variableOrdinal"/> in a carried
    /// row, with its marks. A carried cell is copied verbatim, so its marks travel with it.
    /// </summary>
    private ReadOnlySpan<byte> GetCarriedCell(
        ReadOnlySpan<byte> carrySource,
        in RowStorageForms.TrailerLayout carryLayout,
        int variableOrdinal,
        out bool outOfLine,
        out bool compressed,
        out bool isNull)
    {
        int storedOrdinal = StoredOrdinalOfVariable(variableOrdinal);
        isNull = GetBit(carrySource.Slice(nullBitmapOffset, nullBitmapBytes), columns[storedOrdinal].NullBitIndex);

        int start = variableOrdinal == 0 ? 0 : (int)BinaryPrimitives.ReadUInt32LittleEndian(carrySource.Slice(variableOffsetsOffset + (variableOrdinal - 1) * 4, 4));
        int end = (int)BinaryPrimitives.ReadUInt32LittleEndian(carrySource.Slice(variableOffsetsOffset + variableOrdinal * 4, 4));

        if (carryLayout.VariableCount > 0)
        {
            outOfLine = carryLayout.IsOutOfLine(carrySource, variableOrdinal);
            compressed = carryLayout.IsCompressed(carrySource, variableOrdinal);
        }
        else
        {
            outOfLine = false;
            compressed = false;
        }

        return carrySource.Slice(headerSize + start, end - start);
    }

    private int StoredOrdinalOfVariable(int variableOrdinal)
    {
        for (int i = 0; i < columns.Length; i++)
        {
            if (columns[i].Class == StorageClass.Variable && columns[i].VariableOrdinal == variableOrdinal)
                return i;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"No variable column at ordinal {variableOrdinal}");
    }

    /// <summary>The per-cell decision of <see cref="EncodeStorageValue(ReadOnlySpan{ValueSlot}, LargeValuePolicy, ReadOnlySpan{byte}, ReadOnlySpan{bool})"/>.</summary>
    private struct CellPlan
    {
        /// <summary>Stored bytes to write: raw bytes, the LZ4 block of a compressed cell, or the out-of-line stored bytes the pointer names.</summary>
        public byte[]? Bytes;
        public int RawLength;
        public int Size;
        public bool OutOfLine;
        public bool Compressed;
        public bool Carry;
        public bool CarryNull;
    }

    private static byte[] MaterializeVariable(in ValueSlot slot, ColumnType type, ColumnType arrayElementType, int size)
    {
        if (type == ColumnType.Bytes)
            return slot.AsBytes ?? [];

        byte[] raw = new byte[size];
        WriteVariablePayload(raw, type, arrayElementType, in slot);
        return raw;
    }

    private void WritePayloadWithForms(
        Span<byte> payload,
        ReadOnlySpan<ValueSlot> values,
        CellPlan[] cells,
        bool anyMark,
        ReadOnlySpan<byte> carrySource,
        in RowStorageForms.TrailerLayout carryLayout)
    {
        uint versionWord = (uint)SchemaVersion;
        if (anyMark)
            versionWord |= RowStorageForms.TrailerFlag;
        BinaryPrimitives.WriteUInt32LittleEndian(payload, versionWord);

        int variableCursor = 0;
        int variableAreaOffset = headerSize;

        for (int i = 0; i < columns.Length; i++)
        {
            ColumnPlan plan = columns[i];
            ref readonly ValueSlot slot = ref values[i];

            if (plan.Class == StorageClass.Variable)
            {
                ref CellPlan cell = ref cells[plan.VariableOrdinal];
                Span<byte> dest = payload[(variableAreaOffset + variableCursor)..];

                if (cell.Carry)
                {
                    if (cell.CarryNull)
                        SetBit(payload.Slice(nullBitmapOffset, nullBitmapBytes), plan.NullBitIndex);

                    ReadOnlySpan<byte> carried = GetCarriedCell(carrySource, carryLayout, plan.VariableOrdinal, out _, out _, out _);
                    carried.CopyTo(dest);
                    variableCursor += carried.Length;
                }
                else if (slot.IsNull)
                {
                    SetBit(payload.Slice(nullBitmapOffset, nullBitmapBytes), plan.NullBitIndex);
                }
                else if (cell.OutOfLine)
                {
                    RowStorageForms.WritePointer(dest, cell.RawLength, cell.Bytes!);
                    variableCursor += RowStorageForms.PointerSize;
                }
                else if (cell.Compressed)
                {
                    byte[] block = cell.Bytes!;
                    BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)cell.RawLength);
                    block.CopyTo(dest[RowStorageForms.CompressedPrefixSize..]);
                    variableCursor += RowStorageForms.CompressedPrefixSize + block.Length;
                }
                else if (cell.Bytes is not null)
                {
                    cell.Bytes.CopyTo(dest);
                    variableCursor += cell.Bytes.Length;
                }
                else
                {
                    variableCursor += WriteVariablePayload(dest, plan.Type, plan.ArrayElementType, in slot);
                }

                BinaryPrimitives.WriteUInt32LittleEndian(payload.Slice(variableOffsetsOffset + plan.VariableOrdinal * 4, 4), (uint)variableCursor);
                continue;
            }

            if (slot.IsNull)
            {
                SetBit(payload.Slice(nullBitmapOffset, nullBitmapBytes), plan.NullBitIndex);
                continue;
            }

            if (plan.Class == StorageClass.Fixed)
                WriteFixed(payload.Slice(plan.FixedOffset, plan.FixedWidth), plan.Type, in slot);
            else if (slot.AsBool)
                SetBit(payload.Slice(boolBitmapOffset, boolBitmapBytes), plan.BoolBitIndex);
        }

        if (!anyMark)
            return;

        int trailer = variableAreaOffset + variableCursor;
        int bitmapBytes = CeilDiv(variableColumnCount, 8);
        for (int v = 0; v < cells.Length; v++)
        {
            if (cells[v].OutOfLine)
                SetBit(payload.Slice(trailer, bitmapBytes), v);
            if (cells[v].Compressed)
                SetBit(payload.Slice(trailer + bitmapBytes, bitmapBytes), v);
        }

        int tail = trailer + 2 * bitmapBytes;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.Slice(tail, 4), (uint)variableColumnCount);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.Slice(tail + 4, 4), (uint)variableOffsetsOffset);
    }

    /// <summary>
    /// True when some non-null variable value of <paramref name="values"/> is large enough for
    /// <paramref name="policy"/> to compress it or move it out of line. A row of short values answers
    /// false after a length check per cell, so it stays on the plain single-allocation encode path.
    /// </summary>
    internal bool AnyCellMayChange(ReadOnlySpan<ValueSlot> values, LargeValuePolicy policy)
    {
        for (int i = 0; i < columns.Length; i++)
        {
            ColumnPlan plan = columns[i];
            if (plan.Class != StorageClass.Variable || values[i].IsNull)
                continue;

            int minBytes = policy.MinCandidateBytes(i);
            if (minBytes > 0 && VariablePayloadSizeAtLeast(in values[i], plan.Type, plan.ArrayElementType, minBytes))
                return true;
        }

        return false;
    }

    /// <summary>
    /// The stored-row counterpart of <see cref="AnyCellMayChange"/>: true when an unmarked stored row has
    /// a non-null variable cell large enough for <paramref name="policy"/> to store it in another form.
    /// A row that answers false is already in the form <paramref name="policy"/> would write, so a
    /// storage rewrite can skip it without decoding a single value.
    /// </summary>
    internal bool AnyStoredCellMayChange(ReadOnlySpan<byte> payload, LargeValuePolicy policy)
    {
        for (int i = 0; i < columns.Length; i++)
        {
            ColumnPlan plan = columns[i];
            if (plan.Class != StorageClass.Variable || IsNull(payload, i))
                continue;

            int minBytes = policy.MinCandidateBytes(i);
            if (minBytes > 0 && GetVariableSlice(payload, i).Length >= minBytes)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Cheap lower-bound size test: a UTF-16 string of N chars is at least N UTF-8 bytes, so most
    /// strings are decided without counting their encoded bytes.
    /// </summary>
    private static bool VariablePayloadSizeAtLeast(in ValueSlot slot, ColumnType type, ColumnType arrayElementType, int minBytes)
    {
        if (type == ColumnType.String)
        {
            string text = slot.AsString ?? "";
            if (text.Length >= minBytes)
                return true;
            if (text.Length * 3 < minBytes)
                return false;
        }

        return VariablePayloadSize(in slot, type, arrayElementType) >= minBytes;
    }

    /// <summary>
    /// Encodes <paramref name="values"/> into a bare payload buffer with no <see cref="BranchKvKind"/>
    /// envelope. Prefer <see cref="EncodeStorageValue"/> for anything written to Kahuna; this bare form
    /// is for callers/tests that need the payload without the storage marker.
    /// </summary>
    public byte[] Encode(ReadOnlySpan<ValueSlot> values)
    {
        int payloadSize = ComputePayloadSize(values);
        byte[] buffer = new byte[payloadSize];
        WritePayload(buffer, values);
        return buffer;
    }

    /// <summary>
    /// Sizing pass: total payload byte length for <paramref name="values"/>. For a fixed-only schema the
    /// size is the compile-time <see cref="headerSize"/> and no per-cell work is done. Otherwise it sums
    /// the variable payloads (UTF-8 / raw bytes / array blobs) on top of the fixed prefix.
    /// </summary>
    private int ComputePayloadSize(ReadOnlySpan<ValueSlot> values)
    {
        if (values.Length != ColumnCount)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Row has {values.Length} values but schema version {SchemaVersion} has {ColumnCount} columns");

        if (variableColumnCount == 0)
            return headerSize;

        int variableAreaSize = 0;
        for (int i = 0; i < columns.Length; i++)
        {
            ColumnPlan plan = columns[i];
            if (plan.Class != StorageClass.Variable)
                continue;

            ref readonly ValueSlot slot = ref values[i];
            if (slot.IsNull)
                continue;

            variableAreaSize += VariablePayloadSize(in slot, plan.Type, plan.ArrayElementType);
        }

        return headerSize + variableAreaSize;
    }

    /// <summary>Writes the full payload into <paramref name="payload"/> (exactly <c>ComputePayloadSize</c> bytes).</summary>
    private void WritePayload(Span<byte> payload, ReadOnlySpan<ValueSlot> values)
    {
        // payload is freshly zeroed by the byte[] allocation: null bits/bool bits default to 0 and
        // unused fixed slots default to 0, so we only ever set bits and write present values.
        BinaryPrimitives.WriteUInt32LittleEndian(payload, (uint)SchemaVersion);

        int variableCursor = 0; // running end offset within variableArea
        int variableAreaOffset = headerSize;

        for (int i = 0; i < columns.Length; i++)
        {
            ColumnPlan plan = columns[i];
            ref readonly ValueSlot slot = ref values[i];

            if (slot.IsNull)
            {
                SetBit(payload.Slice(nullBitmapOffset, nullBitmapBytes), plan.NullBitIndex);
                if (plan.Class == StorageClass.Variable)
                    BinaryPrimitives.WriteUInt32LittleEndian(payload.Slice(variableOffsetsOffset + plan.VariableOrdinal * 4, 4), (uint)variableCursor);
                continue;
            }

            switch (plan.Class)
            {
                case StorageClass.Fixed:
                    WriteFixed(payload.Slice(plan.FixedOffset, plan.FixedWidth), plan.Type, in slot);
                    break;

                case StorageClass.Bool:
                    if (slot.AsBool)
                        SetBit(payload.Slice(boolBitmapOffset, boolBitmapBytes), plan.BoolBitIndex);
                    break;

                default: // Variable
                {
                    int written = WriteVariablePayload(payload.Slice(variableAreaOffset + variableCursor), plan.Type, plan.ArrayElementType, in slot);
                    variableCursor += written;
                    BinaryPrimitives.WriteUInt32LittleEndian(payload.Slice(variableOffsetsOffset + plan.VariableOrdinal * 4, 4), (uint)variableCursor);
                    break;
                }
            }
        }
    }

    private static void WriteFixed(Span<byte> dest, ColumnType type, in ValueSlot slot)
    {
        switch (type)
        {
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(dest, slot.AsLong);
                break;
            case ColumnType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(slot.AsDouble));
                break;
            case ColumnType.Float32:
                BinaryPrimitives.WriteInt32LittleEndian(dest, BitConverter.SingleToInt32Bits((float)slot.AsDouble));
                break;
            case ColumnType.Id:
                WriteObjectId(dest, ObjectId.ToValue(slot.AsString!));
                break;
            case ColumnType.Uuid:
                BinaryPrimitives.WriteInt64LittleEndian(dest, slot.UuidHigh);
                BinaryPrimitives.WriteInt64LittleEndian(dest[8..], slot.UuidLow);
                break;
            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Not a fixed-width type: " + type);
        }
    }

    /// <summary>Bytes a non-NULL variable value occupies in <c>variableArea</c>.</summary>
    private static int VariablePayloadSize(in ValueSlot slot, ColumnType type, ColumnType arrayElementType) => type switch
    {
        ColumnType.String => Encoding.UTF8.GetByteCount(slot.AsString ?? ""),
        ColumnType.Bytes => slot.AsBytes?.Length ?? 0,
        ColumnType.Array => ArrayBlobSize(arrayElementType, slot.ArrayElements),
        _ => throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Not a variable-width type: " + type),
    };

    /// <summary>Writes a non-NULL variable value into <paramref name="dest"/>; returns bytes written (== <see cref="VariablePayloadSize"/>).</summary>
    private static int WriteVariablePayload(Span<byte> dest, ColumnType type, ColumnType arrayElementType, in ValueSlot slot)
    {
        switch (type)
        {
            case ColumnType.String:
                return Encoding.UTF8.GetBytes(slot.AsString ?? "", dest);
            case ColumnType.Bytes:
            {
                byte[] bytes = slot.AsBytes ?? [];
                bytes.CopyTo(dest);
                return bytes.Length;
            }
            case ColumnType.Array:
                return WriteArrayBlob(dest, arrayElementType, slot.ArrayElements);
            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Not a variable-width type: " + type);
        }
    }

    // ─────────────────────────────── Decode primitives ───────────────────────────────
    //
    // All accessors take the row payload (envelope already stripped) and a column ordinal. Callers
    // MUST have validated the frame once via ValidateFrame before using the unchecked typed reads.

    /// <summary>True when the stored column at <paramref name="ordinal"/> is a UTF-8 <see cref="ColumnType.String"/>
    /// (so its variable slice is directly byte-comparable). False for every other type, including
    /// <see cref="ColumnType.Id"/>, whose bytes are not UTF-8 text.</summary>
    public bool IsStringColumn(int ordinal) => columns[ordinal].Type == ColumnType.String;

    /// <summary>True when the column at <paramref name="ordinal"/> is NULL in <paramref name="payload"/>.</summary>
    public bool IsNull(ReadOnlySpan<byte> payload, int ordinal) =>
        GetBit(payload.Slice(nullBitmapOffset, nullBitmapBytes), columns[ordinal].NullBitIndex);

    public long GetInt64(ReadOnlySpan<byte> payload, int ordinal) =>
        BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(columns[ordinal].FixedOffset, 8));

    public double GetDouble(ReadOnlySpan<byte> payload, int ordinal) =>
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(columns[ordinal].FixedOffset, 8)));

    public float GetFloat(ReadOnlySpan<byte> payload, int ordinal) =>
        BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(columns[ordinal].FixedOffset, 4)));

    public bool GetBool(ReadOnlySpan<byte> payload, int ordinal) =>
        GetBit(payload.Slice(boolBitmapOffset, boolBitmapBytes), columns[ordinal].BoolBitIndex);

    public ObjectIdValue GetId(ReadOnlySpan<byte> payload, int ordinal) =>
        ReadObjectId(payload.Slice(columns[ordinal].FixedOffset, 12));

    public (long High, long Low) GetUuid(ReadOnlySpan<byte> payload, int ordinal)
    {
        int off = columns[ordinal].FixedOffset;
        long high = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(off, 8));
        long low = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(off + 8, 8));
        return (high, low);
    }

    /// <summary>
    /// Returns the raw UTF-8 bytes (String) or raw bytes (Bytes/Array blob) of the variable column at
    /// <paramref name="ordinal"/> as a zero-copy slice of <paramref name="payload"/>. Empty for a NULL
    /// or empty value — check <see cref="IsNull"/> to tell them apart.
    /// </summary>
    public ReadOnlySpan<byte> GetVariableSlice(ReadOnlySpan<byte> payload, int ordinal)
    {
        int varOrdinal = columns[ordinal].VariableOrdinal;

        // One byte test on the common path. A marked row must have had the cells a caller reads
        // resolved by the store; reading a pointer or an LZ4 block as a value would return garbage.
        if ((payload[3] & 0x80) != 0)
            ThrowIfMarked(payload, varOrdinal);

        int start = varOrdinal == 0 ? 0 : (int)BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(variableOffsetsOffset + (varOrdinal - 1) * 4, 4));
        int end = (int)BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(variableOffsetsOffset + varOrdinal * 4, 4));
        return payload.Slice(headerSize + start, end - start);
    }

    /// <summary>
    /// Materializes the column at <paramref name="ordinal"/> as a <see cref="ValueSlot"/>. Fixed scalars
    /// are read in place; strings/bytes/arrays allocate their managed payload here — call the typed
    /// accessors (or <see cref="GetVariableSlice"/>) instead when a borrowed view suffices.
    /// </summary>
    public ValueSlot GetSlot(ReadOnlySpan<byte> payload, int ordinal)
    {
        ColumnPlan plan = columns[ordinal];
        if (IsNull(payload, ordinal))
            return ValueSlot.Null;

        switch (plan.Type)
        {
            case ColumnType.Integer64: return ValueSlot.FromLong(ColumnType.Integer64, GetInt64(payload, ordinal));
            case ColumnType.Date: return ValueSlot.FromLong(ColumnType.Date, GetInt64(payload, ordinal));
            case ColumnType.DateTime: return ValueSlot.FromLong(ColumnType.DateTime, GetInt64(payload, ordinal));
            case ColumnType.Float64: return ValueSlot.FromDouble(ColumnType.Float64, GetDouble(payload, ordinal));
            case ColumnType.Float32: return ValueSlot.FromDouble(ColumnType.Float32, GetFloat(payload, ordinal));
            case ColumnType.Bool: return ValueSlot.FromBool(GetBool(payload, ordinal));
            case ColumnType.Id: return ValueSlot.FromId(GetId(payload, ordinal).ToString());
            case ColumnType.Uuid:
            {
                (long high, long low) = GetUuid(payload, ordinal);
                return ValueSlot.FromUuid(high, low);
            }
            case ColumnType.String: return ValueSlot.FromString(Encoding.UTF8.GetString(GetVariableSlice(payload, ordinal)));
            case ColumnType.Bytes: return ValueSlot.FromBytes(GetVariableSlice(payload, ordinal).ToArray());
            case ColumnType.Array: return DecodeArrayBlob(GetVariableSlice(payload, ordinal), plan.ArrayElementType);
            default: throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown column type " + plan.Type);
        }
    }

    /// <summary>Decodes every column of <paramref name="payload"/> into a fresh <see cref="ValueSlot"/> array.</summary>
    public ValueSlot[] DecodeToSlots(ReadOnlySpan<byte> payload)
    {
        ValueSlot[] slots = new ValueSlot[columns.Length];
        for (int i = 0; i < columns.Length; i++)
            slots[i] = GetSlot(payload, i);
        return slots;
    }

    // ─────────────────────────────── Frame validation ───────────────────────────────

    /// <summary>
    /// Validates the top-level row frame in a single pass before any unchecked typed read: the payload
    /// must be at least the fixed prefix, its schema version must equal this plan's, and the variable
    /// offset directory must be non-decreasing and terminate exactly at the payload end. Throws
    /// <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/> otherwise.
    /// Nested array frames are bounds-checked as they are decoded in <see cref="DecodeArrayBlob"/>.
    /// </summary>
    public void ValidateFrame(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < headerSize)
            throw Corrupt($"row payload {payload.Length} bytes is shorter than the {headerSize}-byte header for schema version {SchemaVersion}");

        uint versionWord = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        uint storedVersion = versionWord & RowStorageForms.VersionMask;
        if (storedVersion != (uint)SchemaVersion)
            throw Corrupt($"row schema version {storedVersion} does not match codec version {SchemaVersion}");

        int variableAreaEnd = payload.Length;
        if ((versionWord & RowStorageForms.TrailerFlag) != 0)
        {
            if (variableColumnCount == 0)
                throw Corrupt($"row of schema version {SchemaVersion} has no variable columns but carries a storage-form trailer");

            RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);
            if (layout.VariableCount != variableColumnCount || layout.OffsetsOffset != variableOffsetsOffset)
                throw Corrupt($"storage-form trailer does not match the layout of schema version {SchemaVersion}");

            variableAreaEnd = layout.OutOfLineBitmapOffset;
        }

        if (variableColumnCount == 0)
        {
            if (payload.Length != headerSize)
                throw Corrupt($"fixed-only row payload {payload.Length} bytes does not equal the exact {headerSize}-byte layout");
            return;
        }

        long prev = 0;
        for (int v = 0; v < variableColumnCount; v++)
        {
            uint end = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(variableOffsetsOffset + v * 4, 4));
            if (end < prev)
                throw Corrupt($"variable offset directory is not monotonic at ordinal {v} ({end} < {prev})");
            prev = end;
        }

        if (headerSize + prev != variableAreaEnd)
            throw Corrupt($"variable area end {headerSize + prev} does not match payload length {variableAreaEnd}");
    }

    // ─────────────────────────────── Arrays ───────────────────────────────
    //
    // Array blob: elementCount:u32 | elementNullBitmap | (dense fixed elements) | (u32 endOffsets + payloads).
    // The element type is compiled from the column schema and never written per element. Nullable
    // elements are tracked by the element null bitmap. Supported element types match the legacy codec:
    // scalar fixed types, Bool (one byte per element in arrays), String and Bytes. No nested arrays.

    private static int ArrayBlobSize(ColumnType elementType, ValueSlot[] elements)
    {
        int count = elements.Length;
        int size = 4 + CeilDiv(count, 8);
        int width = ArrayElementFixedWidth(elementType);

        if (width >= 0)
            return size + count * width;

        // Variable element type (String/Bytes): offset directory + payloads.
        size += count * 4;
        for (int i = 0; i < count; i++)
        {
            ValueSlot el = elements[i];
            if (el.IsNull)
                continue;
            size += elementType == ColumnType.String
                ? Encoding.UTF8.GetByteCount(el.AsString ?? "")
                : (el.AsBytes?.Length ?? 0);
        }
        return size;
    }

    private static int WriteArrayBlob(Span<byte> dest, ColumnType elementType, ValueSlot[] elements)
    {
        int count = elements.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)count);

        int bitmapBytes = CeilDiv(count, 8);
        Span<byte> nullBitmap = dest.Slice(4, bitmapBytes);
        int width = ArrayElementFixedWidth(elementType);

        if (width >= 0)
        {
            int body = 4 + bitmapBytes;
            for (int i = 0; i < count; i++)
            {
                ValueSlot el = elements[i];
                if (el.IsNull)
                {
                    SetBit(nullBitmap, i);
                    continue;
                }
                WriteArrayFixedElement(dest.Slice(body + i * width, width), elementType, el);
            }
            return body + count * width;
        }

        int dirStart = 4 + bitmapBytes;
        int areaStart = dirStart + count * 4;
        int cursor = 0;
        for (int i = 0; i < count; i++)
        {
            ValueSlot el = elements[i];
            if (el.IsNull)
            {
                SetBit(nullBitmap, i);
            }
            else if (elementType == ColumnType.String)
            {
                cursor += Encoding.UTF8.GetBytes(el.AsString ?? "", dest.Slice(areaStart + cursor));
            }
            else // Bytes
            {
                byte[] bytes = el.AsBytes ?? [];
                bytes.CopyTo(dest.Slice(areaStart + cursor));
                cursor += bytes.Length;
            }
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(dirStart + i * 4, 4), (uint)cursor);
        }
        return areaStart + cursor;
    }

    /// <summary>Internal (not private) so corruption tests can drive a hand-crafted array blob directly.</summary>
    internal static ValueSlot DecodeArrayBlob(ReadOnlySpan<byte> blob, ColumnType elementType)
    {
        if (blob.Length < 4)
            throw Corrupt("array blob shorter than its 4-byte element count");

        // Read the count as unsigned and compute all geometry in long: a corrupt count near uint.MaxValue
        // would overflow int arithmetic (count * width, ceilDiv) and could produce negative geometry that
        // slips past a naive check, then throw a framework exception. Reject in long space before any
        // narrowing, slice, or allocation.
        uint rawCount = BinaryPrimitives.ReadUInt32LittleEndian(blob);
        int width = ArrayElementFixedWidth(elementType);

        long bitmapBytesLong = ((long)rawCount + 7) / 8;
        long bodyBytesLong = width >= 0 ? (long)rawCount * width : (long)rawCount * 4;
        long minHeaderLong = 4L + bitmapBytesLong + bodyBytesLong;
        if (rawCount > (uint)Array.MaxLength || blob.Length < minHeaderLong)
            throw Corrupt($"array blob {blob.Length} bytes too short for {rawCount} elements of {elementType}");

        // Past the guard: minHeaderLong <= blob.Length (an int), so every value below fits int.
        int count = (int)rawCount;
        int bitmapBytes = (int)bitmapBytesLong;
        ReadOnlySpan<byte> nullBitmap = blob.Slice(4, bitmapBytes);
        ValueSlot[] elements = new ValueSlot[count];

        if (width >= 0)
        {
            int body = 4 + bitmapBytes;
            for (int i = 0; i < count; i++)
                elements[i] = GetBit(nullBitmap, i)
                    ? ValueSlot.Null
                    : ReadArrayFixedElement(blob.Slice(body + i * width, width), elementType);
            return ValueSlot.FromArray(elementType, elements);
        }

        int dirStart = 4 + bitmapBytes;
        int areaStart = dirStart + count * 4;
        int prev = 0;
        for (int i = 0; i < count; i++)
        {
            int end = (int)BinaryPrimitives.ReadUInt32LittleEndian(blob.Slice(dirStart + i * 4, 4));
            if (end < prev || areaStart + end > blob.Length)
                throw Corrupt($"array element offset {end} at index {i} is out of bounds");

            if (GetBit(nullBitmap, i))
                elements[i] = ValueSlot.Null;
            else
            {
                ReadOnlySpan<byte> slice = blob.Slice(areaStart + prev, end - prev);
                elements[i] = elementType == ColumnType.String
                    ? ValueSlot.FromString(Encoding.UTF8.GetString(slice))
                    : ValueSlot.FromBytes(slice.ToArray());
            }
            prev = end;
        }
        return ValueSlot.FromArray(elementType, elements);
    }

    private static void WriteArrayFixedElement(Span<byte> dest, ColumnType type, in ValueSlot slot)
    {
        switch (type)
        {
            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(dest, slot.AsLong);
                break;
            case ColumnType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(slot.AsDouble));
                break;
            case ColumnType.Float32:
                BinaryPrimitives.WriteInt32LittleEndian(dest, BitConverter.SingleToInt32Bits((float)slot.AsDouble));
                break;
            case ColumnType.Uuid:
                BinaryPrimitives.WriteInt64LittleEndian(dest, slot.UuidHigh);
                BinaryPrimitives.WriteInt64LittleEndian(dest[8..], slot.UuidLow);
                break;
            case ColumnType.Bool:
                dest[0] = slot.AsBool ? (byte)1 : (byte)0;
                break;
            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported: " + type);
        }
    }

    private static ValueSlot ReadArrayFixedElement(ReadOnlySpan<byte> src, ColumnType type) => type switch
    {
        ColumnType.Integer64 => ValueSlot.FromLong(ColumnType.Integer64, BinaryPrimitives.ReadInt64LittleEndian(src)),
        ColumnType.Date => ValueSlot.FromLong(ColumnType.Date, BinaryPrimitives.ReadInt64LittleEndian(src)),
        ColumnType.DateTime => ValueSlot.FromLong(ColumnType.DateTime, BinaryPrimitives.ReadInt64LittleEndian(src)),
        ColumnType.Float64 => ValueSlot.FromDouble(ColumnType.Float64, BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(src))),
        ColumnType.Float32 => ValueSlot.FromDouble(ColumnType.Float32, BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(src))),
        ColumnType.Uuid => ValueSlot.FromUuid(BinaryPrimitives.ReadInt64LittleEndian(src), BinaryPrimitives.ReadInt64LittleEndian(src[8..])),
        ColumnType.Bool => ValueSlot.FromBool(src[0] != 0),
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported: " + type),
    };

    // ─────────────────────────────── Small helpers ───────────────────────────────

    private static StorageClass ClassOf(ColumnType type) => type switch
    {
        ColumnType.Bool => StorageClass.Bool,
        ColumnType.String or ColumnType.Bytes or ColumnType.Array => StorageClass.Variable,
        _ => StorageClass.Fixed,
    };

    private static int FixedWidthOf(ColumnType type) => type switch
    {
        ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Date or ColumnType.DateTime => 8,
        ColumnType.Float32 => 4,
        ColumnType.Id => 12,
        ColumnType.Uuid => 16,
        _ => throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Not a fixed-width type: " + type),
    };

    /// <summary>Fixed element width inside an array, or -1 for variable element types (String/Bytes).</summary>
    private static int ArrayElementFixedWidth(ColumnType type) => type switch
    {
        ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Date or ColumnType.DateTime => 8,
        ColumnType.Float32 => 4,
        ColumnType.Uuid => 16,
        ColumnType.Bool => 1,
        ColumnType.String or ColumnType.Bytes => -1,
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported: " + type),
    };

    private static int CeilDiv(int value, int divisor) => (value + divisor - 1) / divisor;

    private static void SetBit(Span<byte> bitmap, int bitIndex) => bitmap[bitIndex >> 3] |= (byte)(1 << (bitIndex & 7));

    private static bool GetBit(ReadOnlySpan<byte> bitmap, int bitIndex) => (bitmap[bitIndex >> 3] & (1 << (bitIndex & 7))) != 0;

    private static void WriteObjectId(Span<byte> dest, ObjectIdValue id)
    {
        BinaryPrimitives.WriteInt32LittleEndian(dest, id.a);
        BinaryPrimitives.WriteInt32LittleEndian(dest[4..], id.b);
        BinaryPrimitives.WriteInt32LittleEndian(dest[8..], id.c);
    }

    private static ObjectIdValue ReadObjectId(ReadOnlySpan<byte> src) => new(
        BinaryPrimitives.ReadInt32LittleEndian(src),
        BinaryPrimitives.ReadInt32LittleEndian(src[4..]),
        BinaryPrimitives.ReadInt32LittleEndian(src[8..]));

    private void ThrowIfMarked(ReadOnlySpan<byte> payload, int variableOrdinal)
    {
        int bitmapBytes = CeilDiv(variableColumnCount, 8);
        int outOfLineOffset = payload.Length - RowStorageForms.TrailerTailSize - 2 * bitmapBytes;
        int bit = 1 << (variableOrdinal & 7);

        if ((payload[outOfLineOffset + (variableOrdinal >> 3)] & bit) != 0 ||
            (payload[outOfLineOffset + bitmapBytes + (variableOrdinal >> 3)] & bit) != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.LargeValueNotResolved,
                $"variable column {variableOrdinal} of a schema version {SchemaVersion} row was read before its stored value was resolved");
    }

    private static CamusDBException Corrupt(string message) => new(CamusDBErrorCodes.SystemSpaceCorrupt, message);
}
