
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
/// Measurement functions over Bytes values, and the first place the vector wire contract is written
/// down: a vector is a Bytes value holding tightly packed little-endian IEEE-754 float32 elements
/// with no header, so its dimension is the byte count divided by four.
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
    /// <summary>
    /// Bytes occupied by one vector element. Part of the wire contract described on this class:
    /// changing it would silently reinterpret every stored vector.
    /// </summary>
    private const int Float32ByteWidth = 4;

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
    /// cannot be a vector is rejected rather than measured.
    ///
    /// <para>A byte count that is not a multiple of four is malformed, and it raises rather than
    /// flooring the division. Flooring would hide exactly the corruption this function exists to
    /// catch: a 3070-byte value would report 767 dimensions and pass a check written for 767.</para>
    /// </summary>
    private static ColumnValue EvaluateVectorDims(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ColumnValue argument = arguments[0];
        RequireType(calledName, 0, argument, ColumnType.Bytes);

        int octets = argument.BytesValue?.Length ?? 0;

        if (octets % Float32ByteWidth != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.MalformedVector,
                $"Function '{calledName}' received a {octets}-byte value, which is not a whole number of " +
                $"{Float32ByteWidth}-byte float32 elements");

        return new ColumnValue(ColumnType.Integer64, octets / Float32ByteWidth);
    }
}
