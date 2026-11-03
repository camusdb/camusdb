/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>
/// Makes literal values. Values come from small pools, so that rows share values (duplicates in
/// non-unique columns, join matches, equal group keys), plus boundary values: INT64 min and max, 0,
/// -0.0, NaN, the infinities, very large and very small exponents, the empty string, Unicode, the
/// NUMERIC limits, the first and last dates.
///
/// <para>A boundary value is a literal the engine stores as written. An expression on it can still
/// overflow, and that error is on the expected-error list (<c>CADB0420</c>, <c>CADB0421</c>,
/// <c>CADB0417</c>).</para>
/// </summary>
internal sealed class ValueGenerator
{
    private static readonly long[] SmallInts = [-3, -2, -1, 0, 1, 2, 3, 4, 5, 7, 10, 42, 100];
    private static readonly long[] BoundaryInts = [long.MinValue, long.MaxValue, long.MinValue + 1, long.MaxValue - 1, int.MaxValue, int.MinValue];

    private static readonly double[] SmallFloats = [0.0, 1.0, -1.0, 1.5, -2.5, 0.1, 3.0, 2.0, 100.25, 0.5];
    private static readonly double[] BoundaryFloats = [-0.0, double.NaN, double.PositiveInfinity, double.NegativeInfinity, 1e300, -1e300, 5e-324, 2.2250738585072014e-308, 1e-300];

    private static readonly string[] SmallNumerics = ["0", "1", "-1", "12.5", "0.5", "3", "-7.25", "100", "0.000000001"];
    private static readonly string[] BoundaryNumerics = ["99999999999999999999999999999.999999999", "-99999999999999999999999999999.999999999", "0.000000001", "-0.000000001"];

    private static readonly string[] SmallStrings = ["a", "b", "ab", "A", "abc", "x", "%", "_", "a_b", "zz", "1", "10"];
    private static readonly string[] BoundaryStrings = ["", " ", "é", "日本", "Ω≈ç", "a b", "🙂", "'", "éé"];

    private static readonly string[] SmallDates = ["2024-01-01", "2024-02-29", "2000-01-01", "2023-12-31", "1999-06-15", "2024-01-02"];
    private static readonly string[] BoundaryDates = ["0001-01-01", "9999-12-31", "1970-01-01"];

    private static readonly string[] Jsons = ["{}", "{\"a\":1}", "{\"a\":2,\"b\":\"x\"}", "[1,2]", "null", "\"x\"", "{\"a\":{\"b\":null}}", "{\"b\":[1,{\"c\":true}]}", "1.5", "[]"];

    private readonly FuzzRandom random;
    private readonly GeneratorProfile profile;

    public ValueGenerator(FuzzRandom random, GeneratorProfile profile)
    {
        this.random = random;
        this.profile = profile;
    }

    /// <summary>The JSON object keys that the values use, for <c>-&gt;</c> reads that find something.</summary>
    public static IReadOnlyList<string> JsonKeys { get; } = ["a", "b", "c"];

    /// <summary>
    /// A value of <paramref name="type"/>. With <paramref name="nullable"/>, about one value in seven
    /// is NULL. <paramref name="boundaries"/> allows the boundary pool for this value.
    /// </summary>
    public FuzzValue Next(FuzzType type, bool nullable, bool boundaries)
    {
        if (nullable && random.Chance(0.15))
            return FuzzValue.Null(type);

        bool boundary = boundaries && profile.Allows(ExprFeature.BoundaryLiterals) && random.Chance(0.06);

        return type switch
        {
            FuzzType.Int64 => FuzzValue.Of(boundary ? random.Pick(BoundaryInts) : random.Pick(SmallInts)),
            FuzzType.Float64 => FuzzValue.Of(boundary ? random.Pick(BoundaryFloats) : random.Pick(SmallFloats)),
            FuzzType.Numeric => FuzzValue.Numeric(boundary ? random.Pick(BoundaryNumerics) : random.Pick(SmallNumerics)),
            FuzzType.String => FuzzValue.String(boundary ? random.Pick(BoundaryStrings) : random.Pick(SmallStrings)),
            FuzzType.Bool => FuzzValue.Of(random.NextBool()),
            FuzzType.Date => FuzzValue.Date(boundary ? random.Pick(BoundaryDates) : random.Pick(SmallDates)),
            FuzzType.Jsonb => FuzzValue.Json(random.Pick(Jsons)),
            _ => throw new ArgumentOutOfRangeException(nameof(type)),
        };
    }

    /// <summary>
    /// A STRING long enough to go out of line (a <c>|v</c> cell): random letters, so compression does
    /// not bring it under the 2,048-byte threshold. The first characters come from the small pool, so a
    /// LIKE prefix or a comparison still selects some of these rows.
    /// </summary>
    public FuzzValue LargeString()
    {
        int length = random.Between(3000, 6000);
        StringBuilder sb = new(length);
        sb.Append(random.Pick(SmallStrings));
        while (sb.Length < length)
            sb.Append((char)('a' + random.Next(26)));
        return FuzzValue.String(sb.ToString());
    }

    /// <summary>A LIKE pattern built from the small string pool with <c>%</c> and <c>_</c> wildcards.</summary>
    public FuzzValue LikePattern()
    {
        string core = random.Pick(SmallStrings);
        return random.Next(5) switch
        {
            0 => FuzzValue.String(core + "%"),
            1 => FuzzValue.String("%" + core),
            2 => FuzzValue.String("%" + core + "%"),
            3 => FuzzValue.String("_" + (core.Length > 1 ? core[1..] : "")),
            _ => FuzzValue.String(core),
        };
    }

    /// <summary>A non-zero INT64 divisor, so most divisions do not fail with a division by zero.</summary>
    public FuzzValue NonZeroInt() => FuzzValue.Of(random.Pick(new long[] { -3, -2, -1, 1, 2, 3, 7 }));
}
