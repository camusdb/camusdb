
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;

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
/// The vectorized kernels below do use the reinterpreting read, but only behind an explicit
/// <see cref="BitConverter.IsLittleEndian"/> guard; a big-endian host takes the scalar form.
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

    // ── Accumulation kernels ──────────────────────────────────────────────────
    //
    // The kernels below are the inner loops of the distance metrics. Each has a vectorized main
    // loop and a scalar form. The vectorized loop widens every float lane to double before any
    // arithmetic, so the accumulation semantics match the scalar form: no subtraction, product or
    // sum ever runs in float. Lane sums are combined at the end, so the summation order differs
    // from the scalar form; callers accept a small relative tolerance for that reason.
    //
    // Validation is identical on both forms. The vectorized loop records non-finite lanes with a
    // mask and, when the mask trips, re-reads both operands through the scalar form. The scalar
    // form always raises on the first non-finite element in read order, so both forms report the
    // same element in the same message.

    /// <summary>Exponent bits of a float32. A value is non-finite exactly when all are set.</summary>
    private const int FloatExponentMask = 0x7F80_0000;

    /// <summary>
    /// True when the vectorized loop is usable: the host is little-endian, so a reinterpreted
    /// float read matches the wire contract; the hardware accelerates <see cref="Vector{T}"/>; and
    /// the input has at least one full lane of elements.
    /// </summary>
    private static bool CanVectorize(int dimensions) =>
        BitConverter.IsLittleEndian && Vector.IsHardwareAccelerated && dimensions >= Vector<float>.Count;

    /// <summary>Lanes whose element is NaN or infinity, as an all-ones mask per matching lane.</summary>
    private static Vector<int> NonFiniteLanes(Vector<float> values)
    {
        Vector<int> mask = new(FloatExponentMask);

        return Vector.Equals(Vector.AsVectorInt32(values) & mask, mask);
    }

    /// <summary>
    /// Sum of squared element differences — the L2 distance before its square root. Rejects a
    /// non-finite element in either operand, naming the first one in read order.
    /// </summary>
    internal static double SumSquaredDifferences(
        string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int dimensions)
    {
        if (!CanVectorize(dimensions))
            return SumSquaredDifferencesScalar(calledName, left, right, dimensions);

        ReadOnlySpan<float> leftElements = MemoryMarshal.Cast<byte, float>(left);
        ReadOnlySpan<float> rightElements = MemoryMarshal.Cast<byte, float>(right);

        int lanes = Vector<float>.Count;
        int vectorized = dimensions - dimensions % lanes;

        Vector<double> sumLow = Vector<double>.Zero;
        Vector<double> sumHigh = Vector<double>.Zero;
        Vector<int> nonFinite = Vector<int>.Zero;

        for (int index = 0; index < vectorized; index += lanes)
        {
            Vector<float> leftChunk = new(leftElements.Slice(index, lanes));
            Vector<float> rightChunk = new(rightElements.Slice(index, lanes));

            nonFinite |= NonFiniteLanes(leftChunk) | NonFiniteLanes(rightChunk);

            Vector.Widen(leftChunk, out Vector<double> leftLow, out Vector<double> leftHigh);
            Vector.Widen(rightChunk, out Vector<double> rightLow, out Vector<double> rightHigh);

            Vector<double> differenceLow = leftLow - rightLow;
            Vector<double> differenceHigh = leftHigh - rightHigh;

            sumLow += differenceLow * differenceLow;
            sumHigh += differenceHigh * differenceHigh;
        }

        // The scalar re-read always raises, because the mask only trips on a non-finite element.
        if (nonFinite != Vector<int>.Zero)
            return SumSquaredDifferencesScalar(calledName, left, right, dimensions);

        double sum = Vector.Sum(sumLow) + Vector.Sum(sumHigh);

        for (int index = vectorized; index < dimensions; index++)
        {
            double difference = ReadElement(calledName, left, index, 1) - ReadElement(calledName, right, index, 2);

            sum += difference * difference;
        }

        return sum;
    }

    /// <summary>
    /// Scalar form of <see cref="SumSquaredDifferences"/>. It is the correctness baseline, the
    /// big-endian and short-input fallback, and the error path that names the first non-finite
    /// element. Tests compare the vectorized form against it.
    /// </summary>
    internal static double SumSquaredDifferencesScalar(
        string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int dimensions)
    {
        double sum = 0d;

        for (int index = 0; index < dimensions; index++)
        {
            double difference = ReadElement(calledName, left, index, 1) - ReadElement(calledName, right, index, 2);

            sum += difference * difference;
        }

        return sum;
    }

    /// <summary>
    /// Plain dot product of two vectors. Rejects a non-finite element in either operand, naming
    /// the first one in read order.
    /// </summary>
    internal static double DotProduct(
        string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int dimensions)
    {
        if (!CanVectorize(dimensions))
            return DotProductScalar(calledName, left, right, dimensions);

        ReadOnlySpan<float> leftElements = MemoryMarshal.Cast<byte, float>(left);
        ReadOnlySpan<float> rightElements = MemoryMarshal.Cast<byte, float>(right);

        int lanes = Vector<float>.Count;
        int vectorized = dimensions - dimensions % lanes;

        Vector<double> sumLow = Vector<double>.Zero;
        Vector<double> sumHigh = Vector<double>.Zero;
        Vector<int> nonFinite = Vector<int>.Zero;

        for (int index = 0; index < vectorized; index += lanes)
        {
            Vector<float> leftChunk = new(leftElements.Slice(index, lanes));
            Vector<float> rightChunk = new(rightElements.Slice(index, lanes));

            nonFinite |= NonFiniteLanes(leftChunk) | NonFiniteLanes(rightChunk);

            Vector.Widen(leftChunk, out Vector<double> leftLow, out Vector<double> leftHigh);
            Vector.Widen(rightChunk, out Vector<double> rightLow, out Vector<double> rightHigh);

            sumLow += leftLow * rightLow;
            sumHigh += leftHigh * rightHigh;
        }

        if (nonFinite != Vector<int>.Zero)
            return DotProductScalar(calledName, left, right, dimensions);

        double sum = Vector.Sum(sumLow) + Vector.Sum(sumHigh);

        for (int index = vectorized; index < dimensions; index++)
            sum += ReadElement(calledName, left, index, 1) * ReadElement(calledName, right, index, 2);

        return sum;
    }

    /// <summary>
    /// Scalar form of <see cref="DotProduct"/>; see <see cref="SumSquaredDifferencesScalar"/> for
    /// the roles the scalar forms keep.
    /// </summary>
    internal static double DotProductScalar(
        string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int dimensions)
    {
        double sum = 0d;

        for (int index = 0; index < dimensions; index++)
            sum += ReadElement(calledName, left, index, 1) * ReadElement(calledName, right, index, 2);

        return sum;
    }

    /// <summary>
    /// The three sums cosine distance needs — dot product plus each operand's sum of squares — in
    /// one pass over the elements, so the operands are read once, not three times. Rejects a
    /// non-finite element in either operand, naming the first one in read order.
    /// </summary>
    internal static (double product, double leftSquares, double rightSquares) CosineTerms(
        string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int dimensions)
    {
        if (!CanVectorize(dimensions))
            return CosineTermsScalar(calledName, left, right, dimensions);

        ReadOnlySpan<float> leftElements = MemoryMarshal.Cast<byte, float>(left);
        ReadOnlySpan<float> rightElements = MemoryMarshal.Cast<byte, float>(right);

        int lanes = Vector<float>.Count;
        int vectorized = dimensions - dimensions % lanes;

        Vector<double> productLow = Vector<double>.Zero;
        Vector<double> productHigh = Vector<double>.Zero;
        Vector<double> leftSquaresLow = Vector<double>.Zero;
        Vector<double> leftSquaresHigh = Vector<double>.Zero;
        Vector<double> rightSquaresLow = Vector<double>.Zero;
        Vector<double> rightSquaresHigh = Vector<double>.Zero;
        Vector<int> nonFinite = Vector<int>.Zero;

        for (int index = 0; index < vectorized; index += lanes)
        {
            Vector<float> leftChunk = new(leftElements.Slice(index, lanes));
            Vector<float> rightChunk = new(rightElements.Slice(index, lanes));

            nonFinite |= NonFiniteLanes(leftChunk) | NonFiniteLanes(rightChunk);

            Vector.Widen(leftChunk, out Vector<double> leftLow, out Vector<double> leftHigh);
            Vector.Widen(rightChunk, out Vector<double> rightLow, out Vector<double> rightHigh);

            productLow += leftLow * rightLow;
            productHigh += leftHigh * rightHigh;
            leftSquaresLow += leftLow * leftLow;
            leftSquaresHigh += leftHigh * leftHigh;
            rightSquaresLow += rightLow * rightLow;
            rightSquaresHigh += rightHigh * rightHigh;
        }

        if (nonFinite != Vector<int>.Zero)
            return CosineTermsScalar(calledName, left, right, dimensions);

        double product = Vector.Sum(productLow) + Vector.Sum(productHigh);
        double leftSquares = Vector.Sum(leftSquaresLow) + Vector.Sum(leftSquaresHigh);
        double rightSquares = Vector.Sum(rightSquaresLow) + Vector.Sum(rightSquaresHigh);

        for (int index = vectorized; index < dimensions; index++)
        {
            double leftElement = ReadElement(calledName, left, index, 1);
            double rightElement = ReadElement(calledName, right, index, 2);

            product += leftElement * rightElement;
            leftSquares += leftElement * leftElement;
            rightSquares += rightElement * rightElement;
        }

        return (product, leftSquares, rightSquares);
    }

    /// <summary>
    /// Scalar form of <see cref="CosineTerms"/>; see <see cref="SumSquaredDifferencesScalar"/> for
    /// the roles the scalar forms keep.
    /// </summary>
    internal static (double product, double leftSquares, double rightSquares) CosineTermsScalar(
        string calledName, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int dimensions)
    {
        double product = 0d;
        double leftSquares = 0d;
        double rightSquares = 0d;

        for (int index = 0; index < dimensions; index++)
        {
            double leftElement = ReadElement(calledName, left, index, 1);
            double rightElement = ReadElement(calledName, right, index, 2);

            product += leftElement * rightElement;
            leftSquares += leftElement * leftElement;
            rightSquares += rightElement * rightElement;
        }

        return (product, leftSquares, rightSquares);
    }
}
