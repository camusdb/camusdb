# Serializer

Binary serialization and deserialization for all CamusDB value types.

`Serializator` provides `[MethodImpl(AggressiveInlining)]` read/write helpers for every supported column type (integers, floats, booleans, strings, ObjectIds, dates, …) into and out of `byte[]` / `Span<byte>` buffers. It uses `System.Buffers.Binary.BinaryPrimitives` for endian-safe integer encoding and avoids unnecessary allocations on the hot path.

Schema metadata (table definitions, index definitions) is serialized separately as JSON via `MetaJsonSerializer` using a source-generated `MetaJsonContext` (System.Text.Json, AOT-friendly).

`SerializatorTypes` and `SerializatorTypeSizes` define the one-byte type tags and fixed widths embedded in every encoded value, allowing the deserializer to decode heterogeneous rows without a schema header.
