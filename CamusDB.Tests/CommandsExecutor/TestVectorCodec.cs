
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for the vector wire contract. The byte patterns here are written out literally rather
/// than produced with <c>BitConverter</c>, so the tests pin the on-disk layout instead of restating
/// whatever the implementation happens to do.
/// </summary>
internal sealed class TestVectorCodec
{
    private const string Fn = "l2_distance";

    /// <summary>1.0f is 0x3F800000, so little-endian bytes run 00 00 80 3F.</summary>
    private static readonly byte[] OneLittleEndian = [0x00, 0x00, 0x80, 0x3F];

    // ── Byte order ────────────────────────────────────────────────────────────

    [Test]
    public void ReadElement_DecodesLittleEndian()
    {
        Assert.AreEqual(1.0d, VectorCodec.ReadElement(Fn, OneLittleEndian, 0, 1));
    }

    [Test]
    public void ReadElement_IsNotBigEndian()
    {
        // The same four bytes read big-endian are 0x0000803F, a denormal near 4.6e-41. If this ever
        // starts passing as ~1.0, the codec has silently switched byte order and every stored vector
        // written before the change decodes to noise.
        byte[] reversed = [0x3F, 0x80, 0x00, 0x00];
        double value = VectorCodec.ReadElement(Fn, reversed, 0, 1);

        Assert.AreNotEqual(1.0d, value);
        Assert.Less(value, 1e-40d);
        Assert.Greater(value, 0d);
    }

    [Test]
    public void ReadElement_ReadsTheRequestedIndex()
    {
        // [1.0f, -2.0f, 0.5f]
        byte[] vector =
        [
            0x00, 0x00, 0x80, 0x3F,
            0x00, 0x00, 0x00, 0xC0,
            0x00, 0x00, 0x00, 0x3F,
        ];

        Assert.AreEqual(1.0d,  VectorCodec.ReadElement(Fn, vector, 0, 1));
        Assert.AreEqual(-2.0d, VectorCodec.ReadElement(Fn, vector, 1, 1));
        Assert.AreEqual(0.5d,  VectorCodec.ReadElement(Fn, vector, 2, 1));
    }

    [Test]
    public void ReadElement_WidensToDoubleWithoutLosingTheFloatValue()
    {
        byte[] maxFinite = [0xFF, 0xFF, 0x7F, 0x7F];   // float.MaxValue
        Assert.AreEqual((double)float.MaxValue, VectorCodec.ReadElement(Fn, maxFinite, 0, 1));
    }

    // ── Non-finite elements ───────────────────────────────────────────────────

    [Test]
    public void ReadElement_RejectsNaN()
    {
        byte[] nan = [0x00, 0x00, 0xC0, 0x7F];

        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => VectorCodec.ReadElement(Fn, nan, 0, 2));

        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue, ex!.Code);
        StringAssert.Contains("argument 2", ex.Message);
        StringAssert.Contains("index 0", ex.Message);
    }

    [Test]
    public void ReadElement_RejectsBothInfinities()
    {
        byte[] positiveInfinity = [0x00, 0x00, 0x80, 0x7F];
        byte[] negativeInfinity = [0x00, 0x00, 0x80, 0xFF];

        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue,
            Assert.Throws<CamusDBException>(() => VectorCodec.ReadElement(Fn, positiveInfinity, 0, 1))!.Code);
        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue,
            Assert.Throws<CamusDBException>(() => VectorCodec.ReadElement(Fn, negativeInfinity, 0, 1))!.Code);
    }

    // ── Dimensions ────────────────────────────────────────────────────────────

    [Test]
    public void Dimensions_CountsElements()
    {
        Assert.AreEqual(768, VectorCodec.Dimensions(Fn, new byte[3072]));
    }

    [Test]
    public void Dimensions_OfAnEmptyValueIsZero()
    {
        // Measuring an empty value is legitimate; only a *pair* operation refuses it.
        Assert.AreEqual(0, VectorCodec.Dimensions(Fn, []));
    }

    [Test]
    public void Dimensions_RejectsAPartialElementRatherThanRounding()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => VectorCodec.Dimensions(Fn, new byte[3070]));

        Assert.AreEqual(CamusDBErrorCodes.MalformedVector, ex!.Code);
        StringAssert.Contains("3070", ex.Message);
    }

    // ── Operand pairing ───────────────────────────────────────────────────────

    [Test]
    public void RequireMatchingDimensions_ReturnsTheSharedDimension()
    {
        Assert.AreEqual(768, VectorCodec.RequireMatchingDimensions(Fn, new byte[3072], new byte[3072]));
    }

    [Test]
    public void RequireMatchingDimensions_NamesBothDimensions()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => VectorCodec.RequireMatchingDimensions(Fn, new byte[3072], new byte[3068]));

        Assert.AreEqual(CamusDBErrorCodes.VectorDimensionMismatch, ex!.Code);
        StringAssert.Contains("768", ex.Message);
        StringAssert.Contains("767", ex.Message);
    }

    [Test]
    public void RequireMatchingDimensions_RejectsTwoEmptyOperands()
    {
        // Equal dimensions, so the mismatch check passes — but a distance over zero elements would
        // report the two values as identical, which is a wrong answer rather than an empty one.
        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => VectorCodec.RequireMatchingDimensions(Fn, [], []));

        Assert.AreEqual(CamusDBErrorCodes.MalformedVector, ex!.Code);
        StringAssert.Contains("empty", ex.Message);
    }

    [Test]
    public void RequireMatchingDimensions_ReportsAMalformedOperandBeforeComparing()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => VectorCodec.RequireMatchingDimensions(Fn, new byte[3070], new byte[3072]));

        Assert.AreEqual(CamusDBErrorCodes.MalformedVector, ex!.Code);
    }

    // ── Magnitude guard ───────────────────────────────────────────────────────

    [Test]
    public void RequireNonZeroMagnitude_AcceptsAPositiveMagnitude()
    {
        Assert.DoesNotThrow(() => VectorCodec.RequireNonZeroMagnitude("cosine_distance", 0.5d, 1));
    }

    [Test]
    public void RequireNonZeroMagnitude_RejectsZero()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => VectorCodec.RequireNonZeroMagnitude("cosine_distance", 0d, 2));

        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue, ex!.Code);
        StringAssert.Contains("cosine_distance", ex.Message);
        StringAssert.Contains("argument 2", ex.Message);
    }

    // ── Accumulation kernels: vectorized against scalar ───────────────────────

    /// <summary>
    /// Relative tolerance between the vectorized and scalar kernels. Both accumulate in double;
    /// only the summation order differs, so the allowed drift is tight.
    /// </summary>
    private const double KernelTolerance = 1e-9;

    /// <summary>
    /// Dimensions chosen to hit every loop shape: below one SIMD lane (scalar fallback), exact lane
    /// multiples, and lane multiples plus a remainder (the scalar tail), for both 128-bit and
    /// 256-bit lane widths.
    /// </summary>
    private static readonly int[] KernelDimensions = [1, 3, 4, 5, 7, 8, 9, 16, 33, 128, 768, 771];

    private static byte[] PackRandom(System.Random random, int dimensions)
    {
        byte[] bytes = new byte[dimensions * 4];

        for (int i = 0; i < dimensions; i++)
        {
            float element = (float)(random.NextDouble() * 200d - 100d);
            System.Buffers.Binary.BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), element);
        }

        return bytes;
    }

    private static void AssertClose(double expected, double actual)
    {
        double scale = System.Math.Max(1d, System.Math.Abs(expected));
        Assert.That(actual, Is.EqualTo(expected).Within(KernelTolerance * scale));
    }

    [Test]
    public void Kernels_VectorizedAndScalarFormsAgree_AcrossLaneShapes()
    {
        System.Random random = new(20260820);

        foreach (int dimensions in KernelDimensions)
        {
            byte[] left = PackRandom(random, dimensions);
            byte[] right = PackRandom(random, dimensions);

            AssertClose(
                VectorCodec.SumSquaredDifferencesScalar(Fn, left, right, dimensions),
                VectorCodec.SumSquaredDifferences(Fn, left, right, dimensions));

            AssertClose(
                VectorCodec.DotProductScalar(Fn, left, right, dimensions),
                VectorCodec.DotProduct(Fn, left, right, dimensions));

            (double product, double leftSquares, double rightSquares) =
                VectorCodec.CosineTerms(Fn, left, right, dimensions);
            (double productScalar, double leftSquaresScalar, double rightSquaresScalar) =
                VectorCodec.CosineTermsScalar(Fn, left, right, dimensions);

            AssertClose(productScalar, product);
            AssertClose(leftSquaresScalar, leftSquares);
            AssertClose(rightSquaresScalar, rightSquares);
        }
    }

    [Test]
    public void Kernels_SubtractInDouble_SoAFiniteDifferenceCannotOverflowFloat()
    {
        // +float.MaxValue minus -float.MaxValue overflows a float subtraction to infinity. The
        // scalar form widens each element before subtracting, so the vectorized form must too —
        // a float-lane subtraction here would square infinity and return it.
        const int dimensions = 8;

        byte[] left = new byte[dimensions * 4];
        byte[] right = new byte[dimensions * 4];

        for (int i = 0; i < dimensions; i++)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteSingleLittleEndian(left.AsSpan(i * 4, 4), float.MaxValue);
            System.Buffers.Binary.BinaryPrimitives.WriteSingleLittleEndian(right.AsSpan(i * 4, 4), float.MinValue);
        }

        double result = VectorCodec.SumSquaredDifferences(Fn, left, right, dimensions);

        Assert.IsTrue(double.IsFinite(result));
        AssertClose(VectorCodec.SumSquaredDifferencesScalar(Fn, left, right, dimensions), result);
    }

    /// <summary>
    /// A non-finite element must be rejected with the same error the scalar form raises — same
    /// error code, same element index, same operand — wherever it sits: in a full SIMD chunk, in
    /// the scalar tail, or in either operand.
    /// </summary>
    [Test]
    public void Kernels_RejectNonFiniteElements_NamingTheSameElementAsTheScalarForm()
    {
        System.Random random = new(20260821);

        // (dimensions, poisoned index) pairs covering a chunk element and a tail element.
        (int dimensions, int poisonedIndex)[] cases = [(768, 5), (771, 770), (9, 8), (16, 0)];

        byte[] nan = [0x00, 0x00, 0xC0, 0x7F];

        foreach ((int dimensions, int poisonedIndex) in cases)
        {
            foreach (int poisonedOperand in (int[])[1, 2])
            {
                byte[] left = PackRandom(random, dimensions);
                byte[] right = PackRandom(random, dimensions);

                byte[] poisoned = poisonedOperand == 1 ? left : right;
                nan.CopyTo(poisoned.AsSpan(poisonedIndex * 4, 4));

                CamusDBException vectorizedError = Assert.Throws<CamusDBException>(
                    () => VectorCodec.SumSquaredDifferences(Fn, left, right, dimensions))!;
                CamusDBException scalarError = Assert.Throws<CamusDBException>(
                    () => VectorCodec.SumSquaredDifferencesScalar(Fn, left, right, dimensions))!;

                Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue, vectorizedError.Code);
                Assert.AreEqual(scalarError.Message, vectorizedError.Message);

                Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue,
                    Assert.Throws<CamusDBException>(() => VectorCodec.DotProduct(Fn, left, right, dimensions))!.Code);
                Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue,
                    Assert.Throws<CamusDBException>(() => VectorCodec.CosineTerms(Fn, left, right, dimensions))!.Code);
            }
        }
    }

    [Test]
    public void Kernels_RejectInfinityInsideAChunk()
    {
        // Infinity has the same all-ones exponent as NaN but a zero mantissa; the lane mask must
        // catch both.
        byte[] left = PackRandom(new System.Random(20260822), 16);
        byte[] right = PackRandom(new System.Random(20260823), 16);

        byte[] positiveInfinity = [0x00, 0x00, 0x80, 0x7F];
        positiveInfinity.CopyTo(left.AsSpan(3 * 4, 4));

        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue,
            Assert.Throws<CamusDBException>(() => VectorCodec.SumSquaredDifferences(Fn, left, right, 16))!.Code);
    }
}
