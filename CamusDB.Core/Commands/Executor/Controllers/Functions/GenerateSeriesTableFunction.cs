
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// <c>generate_series(start, stop [, step])</c> over <see cref="ColumnType.Integer64"/>, with
/// PostgreSQL's results:
/// <list type="bullet">
/// <item>The default step is 1. The series includes <c>stop</c> when a step lands on it exactly.</item>
/// <item>A negative step counts down. A series whose direction never reaches <c>stop</c> is empty.</item>
/// <item>A NULL argument gives zero rows (the function is strict), and that wins over a step of 0.</item>
/// <item>A step of 0 is an error.</item>
/// </list>
///
/// <para><b>Overflow ends the series.</b> When <c>current + step</c> does not fit in a 64-bit integer,
/// no further value can be in range, so the series stops after <c>current</c> instead of wrapping to the
/// other end of the range. <c>generate_series(9223372036854775806, 9223372036854775807)</c> therefore
/// returns two rows and stops.</para>
/// </summary>
internal sealed class GenerateSeriesTableFunction : TableFunction
{
    public static readonly GenerateSeriesTableFunction Instance = new();

    public override string Name => "generate_series";

    public override int MinArguments => 2;

    public override int MaxArguments => 3;

    public override ColumnType OutputType => ColumnType.Integer64;

    public override IEnumerable<ColumnValue> Generate(IReadOnlyList<ColumnValue> arguments)
    {
        // Validate eagerly so a bad argument fails the statement even when nothing pulls a row.
        if (!TryReadArguments(arguments, out long start, out long stop, out long step))
            return [];

        return Iterate(start, stop, step);
    }

    public override long? CountRows(IReadOnlyList<ColumnValue> arguments)
    {
        try
        {
            if (!TryReadArguments(arguments, out long start, out long stop, out long step))
                return 0;

            return Count(start, stop, step);
        }
        catch (CamusDBException)
        {
            // The error is raised again, to the caller, when the rows are generated.
            return null;
        }
    }

    /// <summary>
    /// The number of values in the series. Computed in 128 bits, because <c>stop - start</c> overflows
    /// a 64-bit integer for a series that spans most of the range.
    /// </summary>
    internal static long Count(long start, long stop, long step)
    {
        if (step > 0 ? start > stop : start < stop)
            return 0;

        Int128 count = ((Int128)stop - start) / step + 1;
        return count > long.MaxValue ? long.MaxValue : (long)count;
    }

    private static IEnumerable<ColumnValue> Iterate(long start, long stop, long step)
    {
        long current = start;

        while (step > 0 ? current <= stop : current >= stop)
        {
            yield return new ColumnValue(ColumnType.Integer64, current);

            // Stop when the next value would not fit. Neither bound subtraction can overflow, because
            // it moves away from the side the step points to.
            if (step > 0 ? current > long.MaxValue - step : current < long.MinValue - step)
                yield break;

            current += step;
        }
    }

    /// <summary>
    /// Reads the three operands. Returns false when an argument is NULL, which gives zero rows.
    /// </summary>
    private bool TryReadArguments(IReadOnlyList<ColumnValue> arguments, out long start, out long stop, out long step)
    {
        start = stop = 0;
        step = 1;

        if (arguments.Count < MinArguments || arguments.Count > MaxArguments)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"generate_series expects 2 or 3 arguments, got {arguments.Count}");

        for (int i = 0; i < arguments.Count; i++)
        {
            ColumnValue argument = arguments[i];

            if (argument.Type == ColumnType.Null)
                return false;

            if (argument.Type != ColumnType.Integer64)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"generate_series argument {i + 1} must be an integer, got {argument.Type}");
        }

        start = arguments[0].LongValue;
        stop = arguments[1].LongValue;

        if (arguments.Count == 3)
            step = arguments[2].LongValue;

        if (step == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "generate_series step size cannot equal zero");

        return true;
    }
}
