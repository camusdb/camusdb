/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Drives the numeric bound normalization table directly, with no database. Every rewrite must be
/// exact: the rewritten integer bound must select the same integer rows the evaluator's widened
/// double comparison would, and anything with no exact rewrite must be handed to the evaluator.
/// </summary>
public class TestNumericBoundNormalizer
{
    private static ColumnValue F64(double v) => new(ColumnType.Float64, v);

    private static ColumnValue I64(long v) => new(ColumnType.Integer64, v);

    // ── Integer64 column, Float64 literal ─────────────────────────────────────

    [TestCase("=",  1.0,  "=",  1L)]
    [TestCase("=",  -0.0, "=",  0L)]
    [TestCase("!=", 2.0,  "!=", 2L)]
    [TestCase("<",  1.5,  "<=", 1L)]
    [TestCase("<",  2.0,  "<",  2L)]
    [TestCase("<",  -1.5, "<=", -2L)]
    [TestCase("<=", 1.5,  "<=", 1L)]
    [TestCase("<=", 2.0,  "<=", 2L)]
    [TestCase("<=", -1.5, "<=", -2L)]
    [TestCase(">",  1.5,  ">=", 2L)]
    [TestCase(">",  2.0,  ">",  2L)]
    [TestCase(">",  -1.5, ">=", -1L)]
    [TestCase(">=", 1.5,  ">=", 2L)]
    [TestCase(">=", 2.0,  ">=", 2L)]
    [TestCase(">=", -1.5, ">=", -1L)]
    public void IntegerColumn_FloatLiteral_RewritesExactly(string op, double literal, string expectedOp, long expectedValue)
    {
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            op, F64(literal), ColumnType.Integer64, out string newOp, out ColumnValue newConstant);

        Assert.AreEqual(NumericBoundNormalizer.Outcome.Converted, outcome);
        Assert.AreEqual(expectedOp, newOp);
        Assert.AreEqual(ColumnType.Integer64, newConstant.Type);
        Assert.AreEqual(expectedValue, newConstant.LongValue);
    }

    [TestCase("=",  1.5)]
    [TestCase("!=", 1.5)]
    [TestCase("=",  double.NaN)]
    [TestCase("<",  double.NaN)]
    [TestCase(">=", double.NaN)]
    [TestCase("=",  double.PositiveInfinity)]
    [TestCase("<",  double.PositiveInfinity)]
    [TestCase(">",  double.NegativeInfinity)]
    [TestCase("=",  9223372036854775808.0)]   // 2^63: one past long.MaxValue
    [TestCase("<",  9223372036854775808.0)]
    [TestCase(">=", -9223372036854777856.0)]  // below long.MinValue
    public void IntegerColumn_NoExactRewrite_LeavesToEvaluator(string op, double literal)
    {
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            op, F64(literal), ColumnType.Integer64, out _, out _);

        Assert.AreEqual(NumericBoundNormalizer.Outcome.LeaveToEvaluator, outcome);
    }

    [Test]
    public void IntegerColumn_LongMinValueLiteral_Converts()
    {
        // -2^63 is exactly representable and inside the long range.
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            "=", F64(-9223372036854775808.0), ColumnType.Integer64, out _, out ColumnValue c);

        Assert.AreEqual(NumericBoundNormalizer.Outcome.Converted, outcome);
        Assert.AreEqual(long.MinValue, c.LongValue);
    }

    // ── Float64 column, Integer64 literal ─────────────────────────────────────

    [TestCase(1L)]
    [TestCase(-7L)]
    [TestCase(9007199254740993L)] // 2^53 + 1: widening rounds, exactly as the evaluator does
    public void FloatColumn_IntegerLiteral_WidensLikeTheEvaluator(long literal)
    {
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            ">", I64(literal), ColumnType.Float64, out string newOp, out ColumnValue newConstant);

        Assert.AreEqual(NumericBoundNormalizer.Outcome.Converted, outcome);
        Assert.AreEqual(">", newOp);
        Assert.AreEqual(ColumnType.Float64, newConstant.Type);
        Assert.AreEqual((double)literal, newConstant.FloatValue);
    }

    // ── Float32 column ────────────────────────────────────────────────────────

    [Test]
    public void Float32Column_RepresentableLiteral_Converts()
    {
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            "=", F64(1.5), ColumnType.Float32, out _, out ColumnValue c);

        Assert.AreEqual(NumericBoundNormalizer.Outcome.Converted, outcome);
        Assert.AreEqual(ColumnType.Float32, c.Type);
        Assert.AreEqual(1.5, c.FloatValue);
    }

    [Test]
    public void Float32Column_IntegerLiteral_Converts()
    {
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            "<", I64(16777216L), ColumnType.Float32, out _, out ColumnValue c); // 2^24, exact in float

        Assert.AreEqual(NumericBoundNormalizer.Outcome.Converted, outcome);
        Assert.AreEqual(16777216.0, c.FloatValue);
    }

    [Test]
    public void Float32Column_UnrepresentableLiteral_LeavesToEvaluator()
    {
        // 1.1 does not survive a round trip through float; no stored Float32 can equal it.
        NumericBoundNormalizer.Outcome outcome = NumericBoundNormalizer.Normalize(
            "=", F64(1.1), ColumnType.Float32, out _, out _);

        Assert.AreEqual(NumericBoundNormalizer.Outcome.LeaveToEvaluator, outcome);
    }

    // ── Same type or non-numeric: untouched ───────────────────────────────────

    [Test]
    public void SameType_IsUnchanged()
    {
        Assert.AreEqual(NumericBoundNormalizer.Outcome.Unchanged,
            NumericBoundNormalizer.Normalize("=", I64(1), ColumnType.Integer64, out _, out _));
        Assert.AreEqual(NumericBoundNormalizer.Outcome.Unchanged,
            NumericBoundNormalizer.Normalize("=", F64(1), ColumnType.Float64, out _, out _));
    }

    [Test]
    public void NonNumericPair_IsUnchanged()
    {
        Assert.AreEqual(NumericBoundNormalizer.Outcome.Unchanged,
            NumericBoundNormalizer.Normalize("=", new ColumnValue(ColumnType.String, "1"), ColumnType.Integer64, out _, out _));
        Assert.AreEqual(NumericBoundNormalizer.Outcome.Unchanged,
            NumericBoundNormalizer.Normalize("=", I64(1), ColumnType.String, out _, out _));
        Assert.AreEqual(NumericBoundNormalizer.Outcome.Unchanged,
            NumericBoundNormalizer.Normalize("=", I64(1), ColumnType.Bool, out _, out _));
    }

    // ── IN-list items ─────────────────────────────────────────────────────────

    [Test]
    public void InListItem_IntegralFloatOnIntegerColumn_Converts()
    {
        NumericBoundNormalizer.ItemOutcome outcome = NumericBoundNormalizer.NormalizeInListItem(
            F64(2.0), ColumnType.Integer64, out ColumnValue item);

        Assert.AreEqual(NumericBoundNormalizer.ItemOutcome.Converted, outcome);
        Assert.AreEqual(ColumnType.Integer64, item.Type);
        Assert.AreEqual(2L, item.LongValue);
    }

    [TestCase(1.5)]
    [TestCase(double.NaN)]
    [TestCase(double.PositiveInfinity)]
    [TestCase(9223372036854775808.0)]
    public void InListItem_NoIntegerEquivalent_IsDropped(double literal)
    {
        NumericBoundNormalizer.ItemOutcome outcome = NumericBoundNormalizer.NormalizeInListItem(
            F64(literal), ColumnType.Integer64, out _);

        Assert.AreEqual(NumericBoundNormalizer.ItemOutcome.Drop, outcome);
    }

    [Test]
    public void InListItem_IntegerOnFloatColumn_Converts()
    {
        NumericBoundNormalizer.ItemOutcome outcome = NumericBoundNormalizer.NormalizeInListItem(
            I64(3), ColumnType.Float64, out ColumnValue item);

        Assert.AreEqual(NumericBoundNormalizer.ItemOutcome.Converted, outcome);
        Assert.AreEqual(3.0, item.FloatValue);
    }

    [Test]
    public void InListItem_UnrepresentableOnFloat32Column_IsDropped()
    {
        Assert.AreEqual(NumericBoundNormalizer.ItemOutcome.Drop,
            NumericBoundNormalizer.NormalizeInListItem(F64(1.1), ColumnType.Float32, out _));
    }
}
