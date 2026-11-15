
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
/// The trigonometric functions of PostgreSQL: <c>sin</c>, <c>cos</c>, <c>tan</c>, <c>cot</c>,
/// <c>asin</c>, <c>acos</c>, <c>atan</c>, <c>atan2</c>, the degree forms <c>sind</c>, <c>cosd</c>,
/// <c>tand</c>, <c>cotd</c>, <c>asind</c>, <c>acosd</c>, <c>atand</c>, <c>atan2d</c>, the angle
/// conversions <c>radians</c> and <c>degrees</c>, and the constant <c>pi()</c>. Every one returns
/// <c>float64</c>, for an <c>int64</c>, <c>float64</c> or <c>numeric</c> argument.
/// <para>
/// The rules for the special values are those of PostgreSQL 16:
/// <list type="bullet">
///   <item>A NaN argument gives NaN, in every function.</item>
///   <item>An infinite argument to a sine, cosine, tangent or cotangent, in radians or in degrees,
///   is <see cref="CamusDBErrorCodes.InvalidInput"/>: the function has no value at infinity.</item>
///   <item>An argument of an inverse sine or cosine outside [-1, 1] is
///   <see cref="CamusDBErrorCodes.InvalidInput"/>.</item>
///   <item><c>tan</c> and <c>cot</c> return an infinity where the true value is one (<c>cot(0)</c>,
///   <c>tand(90)</c>), and do not fail.</item>
///   <item><c>radians</c> and <c>degrees</c> check the result as a multiplication and a division do:
///   an overflow or an underflow from a finite argument is
///   <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>.</item>
/// </list>
/// </para>
/// <para>
/// The degree forms are a port of the PostgreSQL algorithm, not <c>sin(radians(x))</c>. They reduce
/// the angle to the first quadrant and scale by constants computed with the same library calls, so
/// that the common angles give exact results: <c>sind(30)</c> is 0.5, <c>cosd(90)</c> is 0 and
/// <c>tand(45)</c> is 1. The radian forms call the platform math library, as PostgreSQL does, so a
/// last-digit difference from PostgreSQL on another platform comes from the library, not from this code.
/// </para>
/// </summary>
internal static class TrigonometricScalarFunctions
{
    /// <summary>The PostgreSQL constant <c>RADIANS_PER_DEGREE</c>, π / 180.</summary>
    private const double RadiansPerDegree = 0.0174532925199432957692;

    // The scale constants of the degree forms. Each one is computed with the same library call that
    // the function uses, so that the function gives the exact value at its end point. The order of
    // these fields is significant: the tangent and cotangent constants call SineFirstQuadrant and
    // CosineFirstQuadrant, which read the sine and cosine constants above them.
    private static readonly double SineOf30 = Math.Sin(30.0 * RadiansPerDegree);
    private static readonly double OneMinusCosineOf60 = 1.0 - Math.Cos(60.0 * RadiansPerDegree);
    private static readonly double ArcsineOfHalf = Math.Asin(0.5);
    private static readonly double ArccosineOfHalf = Math.Acos(0.5);
    private static readonly double ArctangentOfOne = Math.Atan(1.0);
    private static readonly double TangentOf45 = SineFirstQuadrant(45.0) / CosineFirstQuadrant(45.0);
    private static readonly double CotangentOf45 = CosineFirstQuadrant(45.0) / SineFirstQuadrant(45.0);

    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterUnary(registry, "sin", EvaluateSin);
        RegisterUnary(registry, "cos", EvaluateCos);
        RegisterUnary(registry, "tan", EvaluateTan);
        RegisterUnary(registry, "cot", EvaluateCot);
        RegisterUnary(registry, "asin", EvaluateAsin);
        RegisterUnary(registry, "acos", EvaluateAcos);
        RegisterUnary(registry, "atan", EvaluateAtan);
        RegisterBinary(registry, "atan2", EvaluateAtan2);

        RegisterUnary(registry, "sind", EvaluateSind);
        RegisterUnary(registry, "cosd", EvaluateCosd);
        RegisterUnary(registry, "tand", EvaluateTand);
        RegisterUnary(registry, "cotd", EvaluateCotd);
        RegisterUnary(registry, "asind", EvaluateAsind);
        RegisterUnary(registry, "acosd", EvaluateAcosd);
        RegisterUnary(registry, "atand", EvaluateAtand);
        RegisterBinary(registry, "atan2d", EvaluateAtan2d);

        RegisterUnary(registry, "radians", EvaluateRadians);
        RegisterUnary(registry, "degrees", EvaluateDegrees);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "pi",
            MinArity = 0,
            MaxArity = 0,
            Evaluator = static (_, _) => new ColumnValue(ColumnType.Float64, Math.PI),
            InferReturnType = static _ => ColumnType.Float64,
        });
    }

    private static void RegisterUnary(ScalarFunctionRegistry registry, string name, Func<string, double, double> function)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = (calledName, arguments) =>
            {
                if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
                    return nullResult;

                ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);

                return new ColumnValue(ColumnType.Float64, function(calledName, ScalarFunctionArguments.ToDouble(arguments[0])));
            },
            InferReturnType = static _ => ColumnType.Float64,
        });
    }

    private static void RegisterBinary(ScalarFunctionRegistry registry, string name, Func<double, double, double> function)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 2,
            MaxArity = 2,
            Evaluator = (calledName, arguments) =>
            {
                if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
                    return nullResult;

                ScalarFunctionArguments.RequireNumeric(calledName, 0, arguments[0]);
                ScalarFunctionArguments.RequireNumeric(calledName, 1, arguments[1]);

                return new ColumnValue(ColumnType.Float64, function(
                    ScalarFunctionArguments.ToDouble(arguments[0]),
                    ScalarFunctionArguments.ToDouble(arguments[1])));
            },
            InferReturnType = static _ => ColumnType.Float64,
        });
    }

    // ── Radian forms ──

    private static double EvaluateSin(string calledName, double x)
        => double.IsNaN(x) ? double.NaN : Math.Sin(RequireFinite(calledName, x));

    private static double EvaluateCos(string calledName, double x)
        => double.IsNaN(x) ? double.NaN : Math.Cos(RequireFinite(calledName, x));

    /// <summary><c>tan(x)</c>. The result is not checked for overflow, because the tangent is infinite at π/2.</summary>
    private static double EvaluateTan(string calledName, double x)
        => double.IsNaN(x) ? double.NaN : Math.Tan(RequireFinite(calledName, x));

    /// <summary>
    /// <c>cot(x)</c>, computed as <c>1 / tan(x)</c>, as in PostgreSQL. <c>cot(0)</c> is infinity and
    /// <c>cot(-0)</c> is negative infinity; neither is an overflow.
    /// </summary>
    private static double EvaluateCot(string calledName, double x)
        => double.IsNaN(x) ? double.NaN : 1.0 / Math.Tan(RequireFinite(calledName, x));

    private static double EvaluateAsin(string calledName, double x)
        => double.IsNaN(x) ? double.NaN : Math.Asin(RequireUnitRange(calledName, x));

    private static double EvaluateAcos(string calledName, double x)
        => double.IsNaN(x) ? double.NaN : Math.Acos(RequireUnitRange(calledName, x));

    /// <summary><c>atan(x)</c>. An infinite argument is valid: <c>atan('Infinity')</c> is π/2.</summary>
    private static double EvaluateAtan(string calledName, double x)
        => Math.Atan(x);

    /// <summary>
    /// <c>atan2(y, x)</c>: the angle of the point (x, y), in (-π, π]. The first argument is the y
    /// coordinate, as in PostgreSQL and the C library. <c>atan2(0, 0)</c> is 0, and the sign of a zero
    /// y selects the side: <c>atan2(-0, -1)</c> is -π.
    /// </summary>
    private static double EvaluateAtan2(double y, double x)
        => Math.Atan2(y, x);

    // ── Degree forms ──

    /// <summary>
    /// <c>sind(x)</c>: the sine of an angle in degrees. The angle is reduced to [0, 90] with the
    /// symmetries of the sine, and the sign is applied last.
    /// </summary>
    private static double EvaluateSind(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        double angle = RequireFinite(calledName, x) % 360.0;
        double sign = 1.0;

        // sind(-x) = -sind(x)
        if (angle < 0.0)
        {
            angle = -angle;
            sign = -sign;
        }

        // sind(360 - x) = -sind(x)
        if (angle > 180.0)
        {
            angle = 360.0 - angle;
            sign = -sign;
        }

        // sind(180 - x) = sind(x)
        if (angle > 90.0)
            angle = 180.0 - angle;

        return sign * SineFirstQuadrant(angle);
    }

    /// <summary><c>cosd(x)</c>: the cosine of an angle in degrees, reduced to [0, 90] as <see cref="EvaluateSind"/> does.</summary>
    private static double EvaluateCosd(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        double angle = RequireFinite(calledName, x) % 360.0;
        double sign = 1.0;

        // cosd(-x) = cosd(x)
        if (angle < 0.0)
            angle = -angle;

        // cosd(360 - x) = cosd(x)
        if (angle > 180.0)
            angle = 360.0 - angle;

        // cosd(180 - x) = -cosd(x)
        if (angle > 90.0)
        {
            angle = 180.0 - angle;
            sign = -sign;
        }

        return sign * CosineFirstQuadrant(angle);
    }

    /// <summary>
    /// <c>tand(x)</c>: the tangent of an angle in degrees. <c>tand(90)</c> is infinity, not an
    /// overflow. A zero result is always a positive zero, as in PostgreSQL, because some libraries
    /// give -0 for <c>tand(180)</c>.
    /// </summary>
    private static double EvaluateTand(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        double angle = RequireFinite(calledName, x) % 360.0;
        double sign = 1.0;

        // tand(-x) = -tand(x)
        if (angle < 0.0)
        {
            angle = -angle;
            sign = -sign;
        }

        // tand(360 - x) = -tand(x)
        if (angle > 180.0)
        {
            angle = 360.0 - angle;
            sign = -sign;
        }

        // tand(180 - x) = -tand(x)
        if (angle > 90.0)
        {
            angle = 180.0 - angle;
            sign = -sign;
        }

        double result = sign * (SineFirstQuadrant(angle) / CosineFirstQuadrant(angle) / TangentOf45);
        return result == 0.0 ? 0.0 : result;
    }

    /// <summary>
    /// <c>cotd(x)</c>: the cotangent of an angle in degrees. <c>cotd(0)</c> is infinity, not an
    /// overflow. A zero result is always a positive zero, as in <see cref="EvaluateTand"/>.
    /// </summary>
    private static double EvaluateCotd(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        double angle = RequireFinite(calledName, x) % 360.0;
        double sign = 1.0;

        // cotd(-x) = -cotd(x)
        if (angle < 0.0)
        {
            angle = -angle;
            sign = -sign;
        }

        // cotd(360 - x) = -cotd(x)
        if (angle > 180.0)
        {
            angle = 360.0 - angle;
            sign = -sign;
        }

        // cotd(180 - x) = -cotd(x)
        if (angle > 90.0)
        {
            angle = 180.0 - angle;
            sign = -sign;
        }

        double result = sign * (CosineFirstQuadrant(angle) / SineFirstQuadrant(angle) / CotangentOf45);
        return result == 0.0 ? 0.0 : result;
    }

    /// <summary><c>asind(x)</c>, in degrees. <c>asind(0.5)</c> is exactly 30 and <c>asind(1)</c> is exactly 90.</summary>
    private static double EvaluateAsind(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        RequireUnitRange(calledName, x);

        return x >= 0.0 ? ArcsineFirstQuadrant(x) : -ArcsineFirstQuadrant(-x);
    }

    /// <summary><c>acosd(x)</c>, in degrees. <c>acosd(0.5)</c> is exactly 60 and <c>acosd(-1)</c> is exactly 180.</summary>
    private static double EvaluateAcosd(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        RequireUnitRange(calledName, x);

        return x >= 0.0 ? ArccosineFirstQuadrant(x) : 90.0 + ArcsineFirstQuadrant(-x);
    }

    /// <summary><c>atand(x)</c>, in degrees, in [-90, 90]. An infinite argument is valid, and <c>atand(1)</c> is exactly 45.</summary>
    private static double EvaluateAtand(string calledName, double x)
    {
        if (double.IsNaN(x))
            return double.NaN;

        return Math.Atan(x) / ArctangentOfOne * 45.0;
    }

    /// <summary><c>atan2d(y, x)</c>, in degrees, in [-180, 180]. Infinite arguments are valid.</summary>
    private static double EvaluateAtan2d(double y, double x)
    {
        if (double.IsNaN(y) || double.IsNaN(x))
            return double.NaN;

        return Math.Atan2(y, x) / ArctangentOfOne * 45.0;
    }

    /// <summary>
    /// The sine of an angle in [0, 90] degrees. It joins the sine on [0, 30] and the cosine of the
    /// complement on (30, 90]. Each piece is exact at its end points, so the result is exact at 0,
    /// 30 and 90 and has no step where the pieces meet.
    /// </summary>
    private static double SineFirstQuadrant(double x)
        => x <= 30.0 ? SineUpTo30(x) : CosineUpTo60(90.0 - x);

    /// <summary>The cosine of an angle in [0, 90] degrees, joined from the same two pieces as <see cref="SineFirstQuadrant"/>.</summary>
    private static double CosineFirstQuadrant(double x)
        => x <= 60.0 ? CosineUpTo60(x) : SineUpTo30(90.0 - x);

    /// <summary>The sine of an angle in [0, 30] degrees, scaled so that 30 gives exactly 0.5.</summary>
    private static double SineUpTo30(double x)
        => Math.Sin(x * RadiansPerDegree) / SineOf30 / 2.0;

    /// <summary>The cosine of an angle in [0, 60] degrees, scaled so that 60 gives exactly 0.5.</summary>
    private static double CosineUpTo60(double x)
        => 1.0 - (1.0 - Math.Cos(x * RadiansPerDegree)) / OneMinusCosineOf60 / 2.0;

    /// <summary>
    /// The inverse sine in degrees of a value in [0, 1]. It joins the inverse sine on [0, 0.5] and the
    /// inverse cosine on (0.5, 1]. Both pieces give exactly 30 at 0.5, so the result is continuous.
    /// </summary>
    private static double ArcsineFirstQuadrant(double x)
        => x <= 0.5 ? Math.Asin(x) / ArcsineOfHalf * 30.0 : 90.0 - Math.Acos(x) / ArccosineOfHalf * 60.0;

    /// <summary>The inverse cosine in degrees of a value in [0, 1], from the same two pieces as <see cref="ArcsineFirstQuadrant"/>.</summary>
    private static double ArccosineFirstQuadrant(double x)
        => x <= 0.5 ? 90.0 - Math.Asin(x) / ArcsineOfHalf * 30.0 : Math.Acos(x) / ArccosineOfHalf * 60.0;

    // ── Angle conversion ──

    /// <summary>
    /// <c>radians(x)</c>: <c>x * (π / 180)</c>, checked as a multiplication. A nonzero argument too small
    /// to give a nonzero result (<c>radians(5e-324)</c>) is an underflow.
    /// </summary>
    private static double EvaluateRadians(string calledName, double x)
        => FloatMath.CheckFunctionResult(calledName, x * RadiansPerDegree, finiteInputs: double.IsFinite(x), nonzeroInput: x != 0);

    /// <summary>
    /// <c>degrees(x)</c>: <c>x / (π / 180)</c>, checked as a division. A finite argument too large to
    /// give a finite result (<c>degrees(1e308)</c>) is an overflow.
    /// </summary>
    private static double EvaluateDegrees(string calledName, double x)
        => FloatMath.CheckFunctionResult(calledName, x / RadiansPerDegree, finiteInputs: double.IsFinite(x), nonzeroInput: x != 0);

    // ── Domain checks ──

    private static double RequireFinite(string calledName, double x)
    {
        if (double.IsInfinity(x))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' domain error: input is out of range, the argument must be finite");
        }

        return x;
    }

    private static double RequireUnitRange(string calledName, double x)
    {
        if (x < -1.0 || x > 1.0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' domain error: input is out of range, the argument must be between -1 and 1");
        }

        return x;
    }
}
