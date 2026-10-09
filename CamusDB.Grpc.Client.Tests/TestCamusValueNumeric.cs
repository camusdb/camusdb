/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// The client side of NUMERIC: a <see cref="decimal"/> parameter binds as <c>numeric_value</c> with
/// every digit, and a NUMERIC cell reads back as a <see cref="decimal"/> only when the decimal holds it
/// exactly. A rounded decimal would be a silent change of a money value, so it is refused.
/// </summary>
[TestFixture]
public sealed class TestCamusValueNumeric
{
    [Test]
    public void Decimal_BindsAsNumericText_WithEveryDigit()
    {
        Value value = CamusValue.From(1234567890123456789.123456789m);

        Assert.AreEqual(Value.KindOneofCase.NumericValue, value.KindCase, "never through a double");
        Assert.AreEqual("1234567890123456789.123456789", value.NumericValue);
        Assert.AreEqual("-0.5", CamusValue.From(-0.5m).NumericValue);
    }

    [Test]
    public void Decimal_BindsWithTheInvariantCulture()
    {
        System.Globalization.CultureInfo previous = System.Globalization.CultureInfo.CurrentCulture;
        try
        {
            System.Globalization.CultureInfo.CurrentCulture = new System.Globalization.CultureInfo("fr-FR");
            Assert.AreEqual("2.75", CamusValue.From(2.75m).NumericValue, "the point, not a comma");
        }
        finally
        {
            System.Globalization.CultureInfo.CurrentCulture = previous;
        }
    }

    [TestCase("1.5", 1.5)]
    [TestCase("-0.000000001", -0.000000001)]
    [TestCase("0", 0)]
    [TestCase("79228162514264337593543950335", null)]
    [TestCase("12345678901234567890.123456789", null)]
    public void TryGetDecimal_ReadsAValueTheDecimalHolds(string text, double? approx)
    {
        Assert.IsTrue(CamusValue.TryGetDecimal(new Value { NumericValue = text }, out decimal result));
        Assert.AreEqual(text, result.ToString(System.Globalization.CultureInfo.InvariantCulture));

        if (approx is { } expected)
            Assert.AreEqual(expected, (double)result, 1e-18);
    }

    [TestCase("123456789012345678901.123456789", Description = "30 significant digits: past the 96-bit mantissa")]
    [TestCase("99999999999999999999999999999.999999999", Description = "past the decimal magnitude")]
    [TestCase("79228162514264337593543950336", Description = "decimal.MaxValue + 1")]
    public void TryGetDecimal_RefusesAValueTheDecimalWouldRound(string text)
    {
        Assert.IsFalse(CamusValue.TryGetDecimal(new Value { NumericValue = text }, out decimal result));
        Assert.AreEqual(0m, result);
    }

    [Test]
    public void TryGetDecimal_RefusesOtherKinds()
    {
        Assert.IsFalse(CamusValue.TryGetDecimal(new Value { NullValue = NullValue.Unset }, out _));
        Assert.IsFalse(CamusValue.TryGetDecimal(new Value { StringValue = "1.5" }, out _), "a STRING is not a NUMERIC");
        Assert.IsFalse(CamusValue.TryGetDecimal(new Value { Float64Value = 1.5 }, out _));
    }
}
