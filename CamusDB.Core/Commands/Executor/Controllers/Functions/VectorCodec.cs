
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The one definition of how a vector is laid out in a Bytes value, and the only way vector code
/// reads one.
///
/// <para><b>Wire contract.</b> A vector is a Bytes value holding tightly packed IEEE-754 float32
/// elements in <b>little-endian</b> byte order, with no header and no padding. Its dimension is the
/// byte count divided by four. Nothing in the schema records the layout, so this contract is the
/// only thing that makes a stored vector readable — it crosses the wire between nodes and survives
/// on disk, and changing it would silently reinterpret every vector already written.</para>
///
/// <para>The endianness is explicit for that reason. <c>MemoryMarshal.Cast&lt;byte, float&gt;</c>
/// reads in the <em>host's</em> order, which happens to match on x64 and ARM64 and would silently
/// produce garbage anywhere else. <see cref="BinaryPrimitives.ReadSingleLittleEndian"/> states the
/// order, and compiles to a plain load on a little-endian target, so the correctness costs nothing.
/// </para>
///
/// <para>Bytes columns are not vector columns: the schema cannot tell an embedding from a file, so
/// no write path may reject a Bytes value for failing this contract. Validation belongs here, at the
/// point a caller asks to read the value <em>as a vector</em>.</para>
/// </summary>
internal static class VectorCodec
{
    /// <summary>Bytes occupied by one element. Part of the wire contract described on this class.</summary>
    internal const int ElementByteWidth = 4;

    /// <summary>
    /// Number of elements in <paramref name="vector"/>, or a raise when the byte count is not a whole
    /// number of elements.
    ///
    /// <para>It never rounds. A floored dimension is worse than an error: a 3070-byte value would
    /// report 767 elements and satisfy a constraint written for 767, so the corrupt payload would
    /// pass exactly the check meant to catch it.</para>
    /// </summary>
    internal static int Dimensions(string calledName, ReadOnlySpan<byte> vector)
    {
        if (vector.Length % ElementByteWidth != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.MalformedVector,
                $"Function '{calledName}' received a {vector.Length}-byte value, which is not a whole number of " +
                $"{ElementByteWidth}-byte float32 elements");

        return vector.Length / ElementByteWidth;
    }

    /// <summary>
    /// Shared dimension of two operands that must line up element for element, or a raise naming both.
    ///
    /// <para>A zero-dimension operand is rejected rather than treated as an empty sum. Every metric
    /// over no elements is either meaningless or degenerate — L2 would report the two values as
    /// identical and cosine has no defined answer — so an empty payload here is a corrupt row, not a
    /// distance of zero.</para>
    /// </summary>
    internal static int RequireMatchingDimensions(string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int leftDimensions = Dimensions(calledName, left);
        int rightDimensions = Dimensions(calledName, right);

        if (leftDimensions != rightDimensions)
            throw new CamusDBException(
                CamusDBErrorCodes.VectorDimensionMismatch,
                $"Function '{calledName}' requires operands of equal dimension, but argument 1 has " +
                $"{leftDimensions} and argument 2 has {rightDimensions}");

        if (leftDimensions == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.MalformedVector,
                $"Function '{calledName}' received an empty vector; a distance over zero elements is undefined");

        return leftDimensions;
    }

    /// <summary>
    /// Element <paramref name="index"/> of <paramref name="vector"/>, widened to double.
    ///
    /// <para>It returns double rather than float so a caller cannot accumulate in float by accident.
    /// At 768 dimensions a float accumulator loses the ordering between near neighbors, which is the
    /// one thing a similarity search must get right, and it can overflow on values a double handles.
    /// </para>
    ///
    /// <para>A non-finite element is rejected here, at the read. NaN poisons every comparison it
    /// reaches — a sort would place the row arbitrarily and report no error — so the value must not
    /// escape the decoder. <paramref name="operandOrdinal"/> is 1-based and names the argument in the
    /// message, because "some operand has a NaN" is not something a caller can act on.</para>
    /// </summary>
    internal static double ReadElement(string calledName, ReadOnlySpan<byte> vector, int index, int operandOrdinal)
    {
        float element = BinaryPrimitives.ReadSingleLittleEndian(vector.Slice(index * ElementByteWidth, ElementByteWidth));

        if (!float.IsFinite(element))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidVectorValue,
                $"Function '{calledName}' received a non-finite element ({element}) at index {index} of " +
                $"argument {operandOrdinal}");

        return element;
    }

    /// <summary>
    /// Guards a metric that divides by a magnitude. Cosine similarity has no answer for a
    /// zero-magnitude vector, and every direction is equally (un)related to it.
    ///
    /// <para>The failure is reported rather than returned as NaN, so the caller cannot rank rows by
    /// a value that compares false against everything including itself.</para>
    /// </summary>
    internal static void RequireNonZeroMagnitude(string calledName, double magnitude, int operandOrdinal)
    {
        if (magnitude > 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidVectorValue,
            $"Function '{calledName}' is undefined for the zero-magnitude vector in argument {operandOrdinal}");
    }
}
