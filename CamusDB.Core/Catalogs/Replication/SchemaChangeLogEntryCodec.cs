/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Catalogs.Replication;

/// <summary>
/// The three fields a schema-log subscriber needs before it can decide whether an entry concerns
/// it at all. Read straight out of the entry's byte frame, so the decision costs no allocation.
/// <see cref="DatabaseId"/> points into the caller's buffer and must not outlive it.
/// </summary>
internal readonly ref struct SchemaEntryHeader
{
    /// <summary>The owning database's opaque id, UTF-8, as it appears in the frame.</summary>
    public ReadOnlySpan<byte> DatabaseId { get; }

    /// <summary>Schema version the delta expects to apply onto.</summary>
    public long FromVersion { get; }

    /// <summary>Schema version the delta produces. Always <see cref="FromVersion"/> + 1.</summary>
    public long ToVersion { get; }

    public SchemaEntryHeader(ReadOnlySpan<byte> databaseId, long fromVersion, long toVersion)
    {
        DatabaseId = databaseId;
        FromVersion = fromVersion;
        ToVersion = toVersion;
    }
}

/// <summary>
/// The single place that turns a <see cref="SchemaChangeLogEntry"/> into replicated bytes and back.
///
/// <para><b>Why there is a header at all.</b> Every open database subscribes to its schema-log
/// partition, and several databases can hash to the same partition, so an entry is delivered to
/// subscribers that must drop it. The proposer is delivered its own entry twice — once through the
/// replication callback and once through the local apply that lets it observe its change before it
/// returns. Each of those deliveries used to pay a full entry decode plus a base64 payload decode
/// only to discard the result. The fixed header in front of the body carries the three fields a
/// subscriber needs to decide "not my database" or "already applied", so a skip reads a few bytes
/// and allocates nothing.</para>
///
/// <para><b>The header duplicates body fields on purpose.</b> The body stays a complete entry, so
/// every consumer of <see cref="SchemaChangeLogEntry"/> keeps working from the decoded object and
/// nothing has to be reassembled from the frame.</para>
///
/// <para><b>Why the magic byte cannot collide.</b> Entries written before this format are UTF-16
/// JSON produced without a byte-order mark, so they always begin <c>0x7B 0x00</c> — <c>{</c> in
/// UTF-16 LE. A first byte of <see cref="FramedMagic"/> is therefore impossible for one of them,
/// and <see cref="Decode"/> can branch on that byte alone. Both forms decode until log compaction
/// retires the old entries.</para>
///
/// <para><b>Upgrade constraint.</b> A build without this codec cannot read a framed entry: it
/// fails with a JSON error and Kommander raises a replication error. The log type string is
/// deliberately unchanged, so that failure is loud rather than a silent drop by a type filter.
/// Every node in a cluster must therefore run the same build.</para>
/// </summary>
internal static class SchemaChangeLogEntryCodec
{
    /// <summary>First byte of a framed entry. Version 1 of the format.</summary>
    internal const byte FramedMagic = 0x01;

    /// <summary>First byte of a pre-framing entry: <c>{</c>, the start of its UTF-16 JSON.</summary>
    private const byte LegacyJsonFirstByte = 0x7B;

    /// <summary>Magic byte plus the one-byte database-id length.</summary>
    private const int FramePrefixLength = 2;

    /// <summary>The two little-endian int64 versions that follow the database id.</summary>
    private const int FrameVersionsLength = 16;

    /// <summary>
    /// Writes the framed form: magic byte, database-id length, UTF-8 database id, from-version and
    /// to-version as little-endian int64, then the entry as UTF-8 JSON. The returned array is fresh
    /// and exactly sized, because the Raft log retains it.
    /// </summary>
    internal static byte[] Encode(SchemaChangeLogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        int databaseIdLength = Encoding.UTF8.GetByteCount(entry.Database);

        // The length is one byte, so the frame can only describe ids up to 255 bytes. Database ids
        // are short base-62 or 24-hex strings, so this never fires in practice; the check keeps the
        // one-byte length honest instead of truncating an id into a frame that decodes to garbage.
        if (databaseIdLength > byte.MaxValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Database id for a schema log entry must be at most {byte.MaxValue} UTF-8 bytes, got {databaseIdLength}"
            );

        byte[] body = JsonSerializer.SerializeToUtf8Bytes(entry, MetaJsonContext.Default.SchemaChangeLogEntry);

        int headerLength = FramePrefixLength + databaseIdLength + FrameVersionsLength;
        byte[] framed = new byte[headerLength + body.Length];

        framed[0] = FramedMagic;
        framed[1] = (byte)databaseIdLength;

        Encoding.UTF8.GetBytes(entry.Database, framed.AsSpan(FramePrefixLength, databaseIdLength));
        BinaryPrimitives.WriteInt64LittleEndian(framed.AsSpan(FramePrefixLength + databaseIdLength), entry.FromVersion);
        BinaryPrimitives.WriteInt64LittleEndian(framed.AsSpan(FramePrefixLength + databaseIdLength + 8), entry.ToVersion);

        body.CopyTo(framed.AsSpan(headerLength));

        return framed;
    }

    /// <summary>
    /// Reads the fixed header of a framed entry without allocating. Returns false for a pre-framing
    /// entry and for a buffer too short to hold the header the frame claims, in which case the
    /// caller must fall back to <see cref="Decode"/>, which reports the corrupt frame.
    /// </summary>
    internal static bool TryReadHeader(ReadOnlySpan<byte> bytes, out SchemaEntryHeader header)
    {
        header = default;

        if (bytes.Length < FramePrefixLength || bytes[0] != FramedMagic)
            return false;

        int databaseIdLength = bytes[1];
        int headerLength = FramePrefixLength + databaseIdLength + FrameVersionsLength;
        if (bytes.Length < headerLength)
            return false;

        header = new(
            bytes.Slice(FramePrefixLength, databaseIdLength),
            BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(FramePrefixLength + databaseIdLength)),
            BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(FramePrefixLength + databaseIdLength + 8))
        );
        return true;
    }

    /// <summary>
    /// A cheap, allocation-free 64-bit digest of a whole entry, used to recognize the very entry a
    /// node already applied when the same bytes are delivered again.
    ///
    /// <para>Version numbers cannot answer that question. A re-delivery of the entry that produced
    /// the current schema version and a <i>different</i> entry claiming that same target version
    /// carry identical from/to versions; the first must be skipped and the second must fail loudly
    /// as an out-of-order change. Comparing digests separates them without reading the body.</para>
    ///
    /// <para>FNV-1a, because the value is only ever compared against another digest this node
    /// computed the same way. It is not a checksum and it protects against nothing; a collision
    /// would silence a divergent delta, which is why the digest covers the entire entry rather
    /// than a few fields.</para>
    /// </summary>
    internal static ulong Fingerprint(ReadOnlySpan<byte> bytes)
    {
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        ulong hash = offsetBasis;
        for (int i = 0; i < bytes.Length; i++)
        {
            hash ^= bytes[i];
            hash *= prime;
        }

        return hash;
    }

    /// <summary>
    /// Decodes either form. A framed entry parses its UTF-8 body through the source-generated
    /// context; a pre-framing entry parses its UTF-16 JSON. The resulting entry carries the payload
    /// format its bytes were written in, so <see cref="SchemaChangeLogEntry.GetPayload{T}"/> reads
    /// the payload with the matching reader. Anything else is a corrupt frame and throws.
    /// </summary>
    internal static SchemaChangeLogEntry Decode(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);

        if (bytes.Length == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Empty schema replication entry");

        if (bytes[0] == FramedMagic)
            return DecodeFramed(bytes);

        if (bytes[0] == LegacyJsonFirstByte)
            return DecodeLegacy(bytes);

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Unrecognized schema replication entry: first byte 0x{bytes[0]:X2} is neither the framed marker nor JSON"
        );
    }

    private static SchemaChangeLogEntry DecodeFramed(byte[] bytes)
    {
        if (!TryReadHeader(bytes, out SchemaEntryHeader header))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Truncated schema replication entry: {bytes.Length} bytes do not hold the framed header"
            );

        int headerLength = FramePrefixLength + header.DatabaseId.Length + FrameVersionsLength;

        SchemaChangeLogEntry? entry;
        try
        {
            entry = JsonSerializer.Deserialize(bytes.AsSpan(headerLength), MetaJsonContext.Default.SchemaChangeLogEntry);
        }
        catch (JsonException ex)
        {
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Corrupt schema replication entry body: {ex.Message}");
        }

        if (entry is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid schema replication entry");

        entry.PayloadFormat = SchemaPayloadFormat.Utf8;
        return entry;
    }

    private static SchemaChangeLogEntry DecodeLegacy(byte[] bytes)
    {
        SchemaChangeLogEntry? entry;
        try
        {
            entry = JsonSerializer.Deserialize(Encoding.Unicode.GetString(bytes), MetaJsonContext.Default.SchemaChangeLogEntry);
        }
        catch (JsonException ex)
        {
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Corrupt schema replication entry: {ex.Message}");
        }

        if (entry is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid schema replication entry");

        entry.PayloadFormat = SchemaPayloadFormat.Utf16Legacy;
        return entry;
    }

    /// <summary>
    /// Serializes one op-specific payload as UTF-8 JSON. Every builder of a
    /// <see cref="SchemaChangeLogEntry"/> goes through here, so the bytes in
    /// <see cref="SchemaChangeLogEntry.Payload"/> always match the entry's default
    /// <see cref="SchemaPayloadFormat.Utf8"/>. A payload type that is not registered in the
    /// source-generated context throws rather than falling back to reflection, which would work
    /// on this build and break under trimming.
    /// </summary>
    internal static byte[] EncodePayload<T>(T payload)
    {
        return JsonSerializer.SerializeToUtf8Bytes(payload, PayloadTypeInfo(typeof(T)));
    }

    /// <summary>
    /// Reads one op-specific payload with the reader that matches how its bytes were written.
    /// <paramref name="op"/> only enriches the error message, so a failure names the delta.
    /// </summary>
    internal static T DecodePayload<T>(byte[] payload, SchemaPayloadFormat format, SchemaOp op) where T : new()
    {
        JsonTypeInfo typeInfo = PayloadTypeInfo(typeof(T));

        object? decoded = format == SchemaPayloadFormat.Utf16Legacy
            ? JsonSerializer.Deserialize(Encoding.Unicode.GetString(payload), typeInfo)
            : JsonSerializer.Deserialize(payload.AsSpan(), typeInfo);

        if (decoded is not T typed)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid payload for schema operation '{op}'");

        return typed;
    }

    private static JsonTypeInfo PayloadTypeInfo(Type type)
    {
        return MetaJsonContext.Default.GetTypeInfo(type)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Schema payload type '{type.Name}' is not registered for serialization"
            );
    }
}
