
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using static CamusDB.Core.CommandsExecutor.Controllers.Functions.ScalarFunctionArguments;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Measurement functions over Bytes values. The vector layout they assume is stated once, in
/// <see cref="VectorCodec"/>, which also owns every read of a vector's contents.
///
/// <para>These exist so a fixed dimension can be stated in SQL. A <c>bytes(N)</c> column declares a
/// <em>maximum</em> length, never an exact one, so nothing stops a short value from reaching a
/// column meant to hold 768 floats. A CHECK over <c>vector_dims</c> is what turns that maximum into
/// an exact width, and it is the only enforcement available until a native vector type carries the
/// dimension in the catalog.</para>
///
/// <para>Both functions are deterministic and non-aggregate, which is what makes them legal inside a
/// CHECK condition — see the CHECK validation in <c>SQLExecutorCreateTableCreator</c>.</para>
/// </summary>
internal static class VectorScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "octet_length",
            MinArity = 1,
            MaxArity = 1,
            Evaluator = EvaluateOctetLength,
            InferReturnType = _ => ColumnType.Integer64,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "vector_dims",
            MinArity = 1,
            MaxArity = 1,
            Evaluator = EvaluateVectorDims,
            InferReturnType = _ => ColumnType.Integer64,
        });

        RegisterMetric(registry, "l2_distance", EvaluateL2Distance);
        RegisterMetric(registry, "inner_product", EvaluateInnerProduct);
        RegisterMetric(registry, "cosine_distance", EvaluateCosineDistance);
    }

    private static void RegisterMetric(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 2,
            MaxArity = 2,
            Evaluator = evaluator,
            InferReturnType = _ => ColumnType.Float64,
        });
    }

    /// <summary>
    /// Length of a value in bytes. Accepts Bytes and String, because that is what the name means
    /// elsewhere in SQL and a String-rejecting <c>octet_length</c> would read as a defect. For a
    /// String this is the UTF-8 byte count, which differs from <c>length</c>: <c>length</c> counts
    /// characters, and the two disagree for every non-ASCII value.
    /// </summary>
    private static ColumnValue EvaluateOctetLength(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ColumnValue argument = arguments[0];
        RequireType(calledName, 0, argument, ColumnType.Bytes, ColumnType.String);

        int octets = argument.Type == ColumnType.Bytes
            ? argument.BytesValue?.Length ?? 0
            : Encoding.UTF8.GetByteCount(argument.StrValue ?? "");

        return new ColumnValue(ColumnType.Integer64, octets);
    }

    /// <summary>
    /// Number of float32 elements in a packed vector. It exists so a constraint states its intent —
    /// <c>vector_dims(v) = 768</c> rather than <c>octet_length(v) = 3072</c> — and so a payload that
    /// cannot be a vector is rejected rather than measured. <see cref="VectorCodec.Dimensions"/>
    /// decides both, so the SQL surface and the distance functions agree on what a vector is.
    /// </summary>
    private static ColumnValue EvaluateVectorDims(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ColumnValue argument = arguments[0];
        RequireType(calledName, 0, argument, ColumnType.Bytes);

        return new ColumnValue(
            ColumnType.Integer64,
            VectorCodec.Dimensions(calledName, argument.BytesValue ?? []));
    }

    // ── Distance metrics ──────────────────────────────────────────────────────

    /// <summary>
    /// Euclidean distance. Smaller is nearer, so a nearest-neighbour query orders ascending.
    ///
    /// <para>Returns the true distance rather than the squared form. The squared form ranks
    /// identically and skips a square root, but it is not a distance, and a projected column showing
    /// 9.4 where the answer is 3.07 reads as a defect. The square root runs once per row, not once
    /// per element, so it is not what this costs.</para>
    /// </summary>
    private static ColumnValue EvaluateL2Distance(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        (byte[] left, byte[] right, int dimensions) = RequireVectorPair(calledName, arguments);

        double sumOfSquares = 0d;

        for (int index = 0; index < dimensions; index++)
        {
            double difference = VectorCodec.ReadElement(calledName, left, index, 1)
                              - VectorCodec.ReadElement(calledName, right, index, 2);

            sumOfSquares += difference * difference;
        }

        return new ColumnValue(ColumnType.Float64, Math.Sqrt(sumOfSquares));
    }

    /// <summary>
    /// Plain dot product. <b>Larger is more similar</b>, which is the opposite direction from the
    /// other two metrics, so a nearest-neighbour query over this one orders <c>DESC</c>. Ordering it
    /// ascending returns the least similar rows and still returns a plausible-looking answer.
    /// </summary>
    private static ColumnValue EvaluateInnerProduct(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        (byte[] left, byte[] right, int dimensions) = RequireVectorPair(calledName, arguments);

        double product = 0d;

        for (int index = 0; index < dimensions; index++)
        {
            product += VectorCodec.ReadElement(calledName, left, index, 1)
                     * VectorCodec.ReadElement(calledName, right, index, 2);
        }

        return new ColumnValue(ColumnType.Float64, product);
    }

    /// <summary>
    /// Cosine distance, <c>1 - cosine_similarity</c>. Smaller is nearer, so it sorts in the same
    /// direction as <c>l2_distance</c> — that is the reason for the subtraction, rather than
    /// returning the similarity directly.
    ///
    /// <para>The similarity is clamped to <c>[-1, 1]</c> before the subtraction. Rounding can carry
    /// it a few ulps past 1 for two identical vectors, which would yield a small <em>negative</em>
    /// distance that sorts ahead of a true zero and reads as a defect.</para>
    ///
    /// <para>A zero-magnitude operand has no direction, so the metric is undefined and
    /// <see cref="VectorCodec.RequireNonZeroMagnitude"/> raises instead of letting a NaN escape into
    /// the ordering.</para>
    /// </summary>
    private static ColumnValue EvaluateCosineDistance(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        (byte[] left, byte[] right, int dimensions) = RequireVectorPair(calledName, arguments);

        double product = 0d;
        double leftSquares = 0d;
        double rightSquares = 0d;

        for (int index = 0; index < dimensions; index++)
        {
            double leftElement = VectorCodec.ReadElement(calledName, left, index, 1);
            double rightElement = VectorCodec.ReadElement(calledName, right, index, 2);

            product += leftElement * rightElement;
            leftSquares += leftElement * leftElement;
            rightSquares += rightElement * rightElement;
        }

        double leftMagnitude = Math.Sqrt(leftSquares);
        double rightMagnitude = Math.Sqrt(rightSquares);

        VectorCodec.RequireNonZeroMagnitude(calledName, leftMagnitude, 1);
        VectorCodec.RequireNonZeroMagnitude(calledName, rightMagnitude, 2);

        double similarity = Math.Clamp(product / (leftMagnitude * rightMagnitude), -1d, 1d);

        return new ColumnValue(ColumnType.Float64, 1d - similarity);
    }

    /// <summary>
    /// Validates the two operands of a metric and returns their bytes plus the shared dimension.
    /// The payloads are handed back as arrays rather than spans so the caller can index both inside
    /// one loop without materializing a <c>float[]</c> per row.
    /// </summary>
    private static (byte[] left, byte[] right, int dimensions) RequireVectorPair(
        string calledName,
        IReadOnlyList<ColumnValue> arguments)
    {
        RequireType(calledName, 0, arguments[0], ColumnType.Bytes);
        RequireType(calledName, 1, arguments[1], ColumnType.Bytes);

        byte[] left = arguments[0].BytesValue ?? [];
        byte[] right = arguments[1].BytesValue ?? [];

        return (left, right, VectorCodec.RequireMatchingDimensions(calledName, left, right));
    }
}
