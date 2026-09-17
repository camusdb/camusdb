/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using K4os.Compression.LZ4;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The one place that compresses and decompresses a stored column value. No other engine code calls
/// an LZ4 API, so the wire contract below has exactly one owner.
///
/// <para><b>Wire contract.</b> A compressed value is a raw LZ4 <em>block</em> (not the frame format).
/// The caller already stores the uncompressed length next to the block, so a frame header and a
/// frame checksum would add bytes and CPU for nothing. Decompression allocates exactly the recorded
/// length once, with no growth loop, and rejects a block that does not decode to exactly that
/// length. A recorded length that no block of that size can reach is rejected before the allocation
/// (<see cref="MaxExpansion"/>).</para>
///
/// <para><b>Why the payback rule exists.</b> <see cref="TryCompress"/> keeps the compressed form only
/// when it is strictly smaller and saves at least the configured percentage. Otherwise the caller
/// stores the raw bytes with the compressed mark clear. Data that does not compress — float32
/// embeddings, JPEGs, archives — would otherwise pay a decompression on every read for the life of
/// the row and save nothing. Do not simplify the rule away.</para>
///
/// <para>Compression never changes a result. A reader decompresses every cell that carries the
/// compressed mark, whatever the current configuration says, so turning compression off never makes
/// an existing value unreadable.</para>
/// </summary>
internal static class LargeValueCompression
{
    /// <summary>
    /// Smallest raw value the writer tries to compress. Below this size LZ4 rarely saves a useful
    /// amount, and the attempt would cost time on the insert path of every row with a short string.
    /// </summary>
    internal const int MinCompressibleBytes = 256;

    /// <summary>
    /// Compresses <paramref name="source"/> and returns true only when the result pays back: it is
    /// strictly smaller than the source and saves at least <paramref name="minSavingPercent"/> percent.
    /// On false, <paramref name="compressed"/> is null and the caller stores the raw bytes.
    /// </summary>
    internal static bool TryCompress(ReadOnlySpan<byte> source, int minSavingPercent, out byte[]? compressed)
    {
        compressed = null;

        if (source.Length < MinCompressibleBytes)
            return false;

        // The largest size that still pays back. Any block at or above it is discarded, so the
        // scratch buffer never needs to be larger than this bound plus LZ4's small slack.
        long saving = Math.Clamp(minSavingPercent, 0, 99);
        int limit = (int)Math.Min(source.Length - 1L, source.Length - (source.Length * saving + 99) / 100);
        if (limit <= 0)
            return false;

        byte[] scratch = ArrayPool<byte>.Shared.Rent(LZ4Codec.MaximumOutputSize(source.Length));
        try
        {
            int written = LZ4Codec.Encode(source, scratch.AsSpan(), LZ4Level.L00_FAST);

            // Encode returns a non-positive value when the target is too small; the rented buffer is
            // sized for the worst case, so that only happens on a library fault. Store raw then.
            if (written <= 0 || written > limit)
                return false;

            compressed = scratch.AsSpan(0, written).ToArray();
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <summary>
    /// Decompresses an LZ4 block into a new array of exactly <paramref name="uncompressedLength"/>
    /// bytes. A block that fails to decode, or decodes to any other length, raises
    /// <see cref="CamusDBErrorCodes.LargeValueCorrupt"/> rather than a library exception.
    /// </summary>
    internal static byte[] Decompress(ReadOnlySpan<byte> block, int uncompressedLength)
    {
        if (uncompressedLength < 0)
            throw Corrupt($"compressed value records a negative length {uncompressedLength}");

        // The recorded length is not covered by any checksum, so a damaged cell of a few bytes could
        // otherwise make this read allocate gigabytes before the block fails to decode. No valid block
        // can decode to more than MaxExpansion times its own size, so the check rejects damage only.
        if (uncompressedLength > MaxDecodedLength(block.Length))
            throw Corrupt($"compressed value records {uncompressedLength} bytes, more than a {block.Length}-byte block can decode to");

        byte[] output = new byte[uncompressedLength];
        if (uncompressedLength == 0)
            return output;

        int decoded;
        try
        {
            decoded = LZ4Codec.Decode(block, output.AsSpan());
        }
        catch (Exception ex) when (ex is not CamusDBException)
        {
            throw Corrupt($"compressed value failed to decode: {ex.Message}");
        }

        if (decoded != uncompressedLength)
            throw Corrupt($"compressed value decoded to {decoded} bytes, but the row records {uncompressedLength}");

        return output;
    }

    /// <summary>
    /// The largest ratio of decoded bytes to block bytes that the LZ4 block format allows. Output bytes
    /// come from literals, which cost one block byte each, and from matches. A match length grows by at
    /// most 255 for each extension byte, and the token, offset and first 19 match bytes cost 3 block
    /// bytes. No block byte therefore yields more than 255 output bytes.
    /// </summary>
    internal const int MaxExpansion = 255;

    /// <summary>The largest decoded length a block of <paramref name="blockLength"/> bytes can have.</summary>
    internal static long MaxDecodedLength(int blockLength) => Math.Min((long)blockLength * MaxExpansion, Array.MaxLength);

    private static CamusDBException Corrupt(string message) => new(CamusDBErrorCodes.LargeValueCorrupt, message);
}
