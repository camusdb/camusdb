/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Tpch;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

[TestFixture]
public class TpchGeneratorTests
{
    [Test]
    public void RowCountsFollowTheScale()
    {
        TpchGenerator g = new(1847, 0.01);

        Assert.That(g.Suppliers, Is.EqualTo(100));
        Assert.That(g.Parts, Is.EqualTo(2_000));
        Assert.That(g.PartSupps, Is.EqualTo(8_000));
        Assert.That(g.Customers, Is.EqualTo(1_500));
        Assert.That(g.Orders, Is.EqualTo(15_000));
        Assert.That(g.RegionRows().Count(), Is.EqualTo(5));
        Assert.That(g.NationRows().Count(), Is.EqualTo(25));
    }

    [Test]
    public void TheSameSeedGivesTheSameRowsAndAnotherSeedDoesNot()
    {
        TpchGenerator a = new(1847, 0.01);
        TpchGenerator b = new(1847, 0.01);
        TpchGenerator other = new(7, 0.01);

        Assert.That(b.Supplier(42), Is.EqualTo(a.Supplier(42)));
        Assert.That(b.Part(1999), Is.EqualTo(a.Part(1999)));
        Assert.That(b.Customer(3), Is.EqualTo(a.Customer(3)));
        Assert.That(b.PartSupp(17, 2), Is.EqualTo(a.PartSupp(17, 2)));

        TpchOrder first = a.Order(500);
        TpchOrder second = b.Order(500);
        Assert.That(second.Key, Is.EqualTo(first.Key));
        Assert.That(second.TotalCents, Is.EqualTo(first.TotalCents));
        Assert.That(second.Lines, Is.EqualTo(first.Lines));

        Assert.That(other.Customer(3), Is.Not.EqualTo(a.Customer(3)));
    }

    [Test]
    public void OrderKeysAreSparseAndUnique()
    {
        Assert.That(TpchGenerator.OrderKey(1), Is.EqualTo(1));
        Assert.That(TpchGenerator.OrderKey(8), Is.EqualTo(8));
        Assert.That(TpchGenerator.OrderKey(9), Is.EqualTo(33));

        HashSet<long> keys = new();
        for (long i = 1; i <= 15_000; i++)
            Assert.That(keys.Add(TpchGenerator.OrderKey(i)), Is.True);
    }

    [TestCase(0.01)]
    [TestCase(0.1)]
    public void EveryPartHasFourDistinctSuppliers(double scale)
    {
        TpchGenerator g = new(1847, scale);
        for (long part = 1; part <= g.Parts; part++)
        {
            HashSet<long> suppliers = new();
            for (int position = 0; position < 4; position++)
            {
                long supplier = g.SupplierOf(part, position);
                Assert.That(supplier, Is.InRange(1, g.Suppliers));
                Assert.That(suppliers.Add(supplier), Is.True, $"part {part} repeats supplier {supplier}");
            }
        }
    }

    [Test]
    public void OrdersObeyTheSpecificationRules()
    {
        TpchGenerator g = new(1847, 0.01);
        int minDate = new DateOnly(1992, 1, 1).DayNumber;
        int maxDate = new DateOnly(1998, 12, 31).DayNumber;
        HashSet<char> statuses = new();
        long lines = 0;

        for (long i = 1; i <= g.Orders; i++)
        {
            TpchOrder o = g.Order(i);
            statuses.Add(o.Status);
            lines += o.Lines.Length;

            Assert.That(o.CustKey % 3, Is.Not.EqualTo(0));
            Assert.That(o.CustKey, Is.InRange(1, g.Customers));
            Assert.That(o.Lines.Length, Is.InRange(1, 7));
            Assert.That(o.OrderDate, Is.GreaterThanOrEqualTo(minDate));

            long total = 0;
            for (int n = 0; n < o.Lines.Length; n++)
            {
                TpchLineItem l = o.Lines[n];
                Assert.That(l.OrderKey, Is.EqualTo(o.Key));
                Assert.That(l.LineNumber, Is.EqualTo(n + 1));
                Assert.That(l.PartKey, Is.InRange(1, g.Parts));
                Assert.That(Enumerable.Range(0, 4).Select(p => g.SupplierOf(l.PartKey, p)), Does.Contain(l.SuppKey));
                Assert.That(l.ExtendedCents, Is.EqualTo(l.Quantity * TpchGenerator.RetailCents(l.PartKey)));
                Assert.That(l.ShipDate, Is.GreaterThan(o.OrderDate));
                Assert.That(l.ReceiptDate, Is.GreaterThan(l.ShipDate).And.LessThanOrEqualTo(maxDate));
                Assert.That(l.LineStatus, Is.EqualTo(l.ShipDate > TpchGenerator.CurrentDate ? 'O' : 'F'));
                Assert.That(l.ReturnFlag == 'N', Is.EqualTo(l.ReceiptDate > TpchGenerator.CurrentDate));
                total += (l.ExtendedCents * (100 + l.TaxPct) * (100 - l.DiscountPct) + 5_000) / 10_000;
            }
            Assert.That(o.TotalCents, Is.EqualTo(total));
        }

        Assert.That(statuses, Is.EquivalentTo(new[] { 'F', 'O', 'P' }));
        Assert.That(lines, Is.InRange(g.Orders * 3, g.Orders * 5));
    }

    [Test]
    public void TheDataFeedsTheQueriesThatSelectOnText()
    {
        TpchGenerator g = new(1847, 0.01);

        List<TpchPart> parts = Enumerable.Range(1, (int)g.Parts).Select(k => g.Part(k)).ToList();
        Assert.That(parts.Count(p => p.Name.Contains("green")), Is.GreaterThan(0));
        Assert.That(parts.Count(p => p.Name.StartsWith("forest")), Is.GreaterThan(0));
        Assert.That(parts.Count(p => p.Type.EndsWith("BRASS") && p.Size == 15), Is.GreaterThan(0));
        Assert.That(parts.Count(p => p.Type.StartsWith("PROMO")), Is.GreaterThan(0));
        Assert.That(parts.All(p => p.Name.Split(' ').Distinct().Count() == 5), Is.True);

        List<TpchSupplier> suppliers = Enumerable.Range(1, (int)g.Suppliers).Select(k => g.Supplier(k)).ToList();
        Assert.That(suppliers.Count(s => s.Comment.StartsWith("Customer") && s.Comment.EndsWith("Complaints")), Is.GreaterThan(0));
        Assert.That(suppliers.All(s => s.Phone.StartsWith($"{s.NationKey + 10:D2}-")), Is.True);
    }

    [TestCase(0.001)]
    [TestCase(0.0)]
    [TestCase(double.NaN)]
    public void AScaleBelowTheMinimumIsRefused(double scale)
    {
        Assert.That(TpchGenerator.ScaleError(scale), Is.Not.Null);
        Assert.Throws<ArgumentException>(() => _ = new TpchGenerator(1, scale));
    }

    [TestCase(0.01)]
    [TestCase(0.1)]
    [TestCase(1.0)]
    [TestCase(10.0)]
    public void TheUsualScalesAreSound(double scale)
        => Assert.That(TpchGenerator.ScaleError(scale), Is.Null);
}

[TestFixture]
public class TpchFactsTests
{
    private static TpchLineItem Line(int quantity, long cents, int discount, int tax, char flag, char status, DateOnly ship)
        => new(1, 1, 1, 1, quantity, cents, discount, tax, flag, status, ship.DayNumber, ship.DayNumber, ship.DayNumber + 1, "NONE", "AIR", "");

    [Test]
    public void Q1AndQ6AreAccumulatedExactly()
    {
        TpchFacts facts = new();
        facts.Add(Line(10, 100_000, 6, 8, 'R', 'F', new DateOnly(1994, 3, 1)));   // in Q1 and Q6
        facts.Add(Line(30, 200_000, 6, 0, 'R', 'F', new DateOnly(1994, 3, 1)));   // quantity too high for Q6
        facts.Add(Line(5, 50_000, 10, 0, 'N', 'O', new DateOnly(1998, 9, 3)));    // after the Q1 cutoff

        Assert.That(facts.Q1.Keys, Is.EqualTo(new[] { "R|F" }));
        TpchQ1Group group = facts.Q1["R|F"];
        Assert.That(group.Count, Is.EqualTo(2));
        Assert.That(group.SumQty, Is.EqualTo(40m));
        Assert.That(group.SumBasePrice, Is.EqualTo(3000m));
        Assert.That(group.SumDiscPrice, Is.EqualTo(940m + 1880m));
        Assert.That(group.SumCharge, Is.EqualTo(940m * 1.08m + 1880m));
        Assert.That(facts.Q6Rows, Is.EqualTo(1));
        Assert.That(facts.Q6Revenue, Is.EqualTo(60m));
    }

    [Test]
    public void MergeAndJsonKeepEveryTotal()
    {
        TpchFacts a = new();
        a.AddRows("lineitem", 2);
        a.Add(Line(10, 100_000, 6, 8, 'R', 'F', new DateOnly(1994, 3, 1)));
        TpchFacts b = new();
        b.AddRows("lineitem", 3);
        b.AddRows("orders", 1);
        b.Add(Line(20, 300_000, 5, 1, 'R', 'F', new DateOnly(1994, 5, 1)));
        b.Add(Line(1, 1_000, 0, 0, 'A', 'F', new DateOnly(1993, 5, 1)));

        a.Merge(b);
        TpchFacts back = TpchFacts.FromJson(a.ToJson());

        Assert.That(back.Counts["lineitem"], Is.EqualTo(5));
        Assert.That(back.Counts["orders"], Is.EqualTo(1));
        Assert.That(back.Q1.Keys, Is.EqualTo(new[] { "A|F", "R|F" }));
        Assert.That(back.Q1["R|F"].Count, Is.EqualTo(2));
        Assert.That(back.Q1["R|F"].SumCharge, Is.EqualTo(a.Q1["R|F"].SumCharge));
        Assert.That(back.Q6Rows, Is.EqualTo(2));
        Assert.That(back.Q6Revenue, Is.EqualTo(60m + 150m));
    }

    [Test]
    public void TheLoadChecksAcceptTheRightAnswerAndNameAWrongOne()
    {
        TpchFacts facts = new();
        facts.Add(Line(10, 100_000, 6, 8, 'R', 'F', new DateOnly(1994, 3, 1)));
        facts.Add(Line(30, 200_000, 6, 0, 'R', 'F', new DateOnly(1994, 3, 1)));

        TpchCell[] row =
        [
            TpchCell.String("R"), TpchCell.String("F"), TpchCell.Exact("40.000000000"), TpchCell.Exact("3000.00"),
            TpchCell.Exact("2820"), TpchCell.Exact("2895.2"), TpchCell.Exact("20"), TpchCell.Float(1500.0),
            TpchCell.Exact("0.06"), TpchCell.Integer(2),
        ];
        Assert.That(TpchOperation.CheckQ1(facts, TpchResultComparer.Canonical([row])), Is.Empty);
        Assert.That(TpchOperation.CheckQ6(facts, TpchResultComparer.Canonical([[TpchCell.Exact("60.0000")]])), Is.Empty);

        TpchCell[] wrong = (TpchCell[])row.Clone();
        wrong[9] = TpchCell.Integer(1);
        List<string> failures = TpchOperation.CheckQ1(facts, TpchResultComparer.Canonical([wrong]));
        Assert.That(failures, Has.Count.EqualTo(1));
        Assert.That(failures[0], Does.Contain("count_order"));

        Assert.That(TpchOperation.CheckQ6(facts, TpchResultComparer.Canonical([[TpchCell.Exact("59.99")]])), Has.Count.EqualTo(1));
        Assert.That(TpchOperation.CheckQ6(new TpchFacts(), TpchResultComparer.Canonical([[TpchCell.Null]])), Is.Empty);
    }
}

[TestFixture]
public class TpchResultComparerTests
{
    private static TpchResultSet Set(params TpchCell[][] rows) => TpchResultComparer.Canonical(rows.ToList());

    [Test]
    public void RowOrderDoesNotMatter()
    {
        TpchResultSet a = Set([TpchCell.String("x"), TpchCell.Integer(1)], [TpchCell.String("y"), TpchCell.Integer(2)]);
        TpchResultSet b = Set([TpchCell.String("y"), TpchCell.Integer(2)], [TpchCell.String("x"), TpchCell.Integer(1)]);

        Assert.That(TpchResultComparer.Difference(a, b), Is.Null);
        Assert.That(b.Digest, Is.EqualTo(a.Digest));
    }

    [Test]
    public void NumericTextIsNormalized()
    {
        Assert.That(TpchResultComparer.NormalizeNumeric("12.500000000"), Is.EqualTo("12.5"));
        Assert.That(TpchResultComparer.NormalizeNumeric("100"), Is.EqualTo("100"));
        Assert.That(TpchResultComparer.NormalizeNumeric("0.000"), Is.EqualTo("0"));
        Assert.That(TpchResultComparer.NormalizeNumeric("-0.00"), Is.EqualTo("0"));
        Assert.That(TpchResultComparer.Difference(Set([TpchCell.Exact("7.10")]), Set([TpchCell.Exact("7.100000000")])), Is.Null);
    }

    [Test]
    public void FloatsAreComparedWithATolerance()
    {
        TpchResultSet a = Set([TpchCell.String("k"), TpchCell.Float(0.1 + 0.2)]);
        TpchResultSet close = Set([TpchCell.String("k"), TpchCell.Float(0.3)]);
        TpchResultSet far = Set([TpchCell.String("k"), TpchCell.Float(0.3001)]);

        Assert.That(TpchResultComparer.Difference(a, close), Is.Null);
        Assert.That(close.Digest, Is.EqualTo(a.Digest));
        Assert.That(TpchResultComparer.Difference(a, far), Does.Contain("column 2"));
    }

    [Test]
    public void AFloatThatDiffersInItsLastBitsDoesNotMoveARow()
    {
        TpchResultSet a = Set(
            [TpchCell.Float(1.0000000001), TpchCell.String("b")],
            [TpchCell.Float(1.0), TpchCell.String("a")]);
        TpchResultSet b = Set(
            [TpchCell.Float(1.0), TpchCell.String("b")],
            [TpchCell.Float(1.0000000001), TpchCell.String("a")]);

        Assert.That(TpchResultComparer.Difference(a, b), Is.Null);
    }

    [Test]
    public void ExactDifferencesAreReported()
    {
        TpchResultSet a = Set([TpchCell.Integer(1), TpchCell.Exact("10.00")]);

        Assert.That(TpchResultComparer.Difference(a, Set([TpchCell.Integer(1), TpchCell.Exact("10.01")])), Does.Contain("10.01"));
        Assert.That(TpchResultComparer.Difference(a, Set([TpchCell.Integer(1), TpchCell.Null])), Does.Contain("NULL"));
        Assert.That(TpchResultComparer.Difference(a, Set()), Does.Contain("0 row(s), expected 1"));
        Assert.That(TpchResultComparer.Difference(a, Set([TpchCell.Integer(1), TpchCell.Float(10.0)])), Is.Not.Null);
    }
}

[TestFixture]
public class TpchQueriesTests
{
    [Test]
    public void AllTwentyTwoQueriesArePresentInOrder()
    {
        Assert.That(TpchQueries.All.Select(q => q.Number), Is.EqualTo(Enumerable.Range(1, 22)));
        Assert.That(TpchQueries.All.All(q => q.Sql.Contains("select", StringComparison.Ordinal)), Is.True);
        Assert.That(TpchQueries.All[0].Name, Is.EqualTo("Q01"));
    }

    [Test]
    public void TheTextsAvoidWhatTheServerRefuses()
    {
        foreach (TpchQuery q in TpchQueries.All)
        {
            string sql = q.Sql.ToLowerInvariant();
            Assert.That(sql, Does.Not.Contain("interval"), q.Name);
            Assert.That(sql, Does.Not.Contain("count(distinct"), q.Name);
            Assert.That(sql, Does.Not.Contain("create view"), q.Name);
            Assert.That(sql, Does.Not.Contain(" from 1 for "), q.Name);
            Assert.That(sql, Does.Not.Contain("@"), q.Name);

            // A bare decimal literal is a float64; every decimal constant must be a NUMERIC literal.
            Assert.That(System.Text.RegularExpressions.Regex.IsMatch(sql, @"(?<!')\b\d+\.\d+\b(?!')"), Is.False, q.Name);
        }
    }

    [Test]
    public void ASelectionIsParsed()
    {
        Assert.That(TpchQueries.TryParseSelection("", out IReadOnlyList<TpchQuery> all, out _), Is.True);
        Assert.That(all, Has.Count.EqualTo(22));

        Assert.That(TpchQueries.TryParseSelection("6, 1-3,Q14", out IReadOnlyList<TpchQuery> some, out _), Is.True);
        Assert.That(some.Select(q => q.Number), Is.EqualTo(new[] { 1, 2, 3, 6, 14 }));

        foreach (string bad in new[] { "0", "23", "3-1", "x", "1-2-3", "," })
        {
            Assert.That(TpchQueries.TryParseSelection(bad, out _, out string? error), Is.False, bad);
            Assert.That(error, Is.Not.Null);
        }
    }
}

[TestFixture]
public class TpchReportTests
{
    private static readonly DateTime Start = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);

    private static TpchReference Reference(int number)
        => new(TpchQueries.All[number - 1], TpchResultComparer.Canonical([[TpchCell.Integer(number)]]), 12.5);

    [Test]
    public void OnlyExecutionsThatFinishedInTheWindowAreCounted()
    {
        TpchExecution[] executions =
        [
            new(1, Start.AddSeconds(-5), 100, TpchOutcome.Verified, 1, null),      // warm-up
            new(1, Start.AddSeconds(1), 100, TpchOutcome.Verified, 1, null),
            new(1, Start.AddSeconds(2), 300, TpchOutcome.Verified, 1, null),
            new(1, Start.AddSeconds(3), 50, TpchOutcome.Error, 0, "CADB0000"),
            new(1, Start.AddSeconds(59.95), 100, TpchOutcome.Verified, 1, null),   // finished after the window
        ];

        (TpchRunReport report, TpchResult result) = TpchReport.Build("0.01", "1847", [Reference(1)], executions, 0, Start, 60);

        TpchQueryStats q1 = report.Queries.Single();
        Assert.That(q1.Executions, Is.EqualTo(3));
        Assert.That(q1.Verified, Is.EqualTo(2));
        Assert.That(q1.Errors, Is.EqualTo(1));
        Assert.That(q1.MeanMs, Is.EqualTo(200));
        Assert.That(q1.MaxMs, Is.EqualTo(300));
        Assert.That(report.QueriesPerHour, Is.EqualTo(120));
        Assert.That(result.Passed, Is.True);
        Assert.That(result.QueriesChecked, Is.EqualTo(1));
    }

    [Test]
    public void AQueryWithNoVerifiedExecutionFailsTheRun()
    {
        TpchExecution[] executions = [new(1, Start.AddSeconds(1), 100, TpchOutcome.Verified, 1, null)];

        (_, TpchResult result) = TpchReport.Build("0.01", "1847", [Reference(1), Reference(17)], executions, 0, Start, 60);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.QueriesEnabled, Is.EqualTo(2));
        Assert.That(result.QueriesChecked, Is.EqualTo(1));
        Assert.That(result.Failures.Single(), Does.StartWith("Q17 has no verified execution"));
    }

    [Test]
    public void AMismatchOutsideTheWindowStillFailsTheRun()
    {
        TpchExecution[] executions =
        [
            new(1, Start.AddSeconds(-5), 100, TpchOutcome.Mismatch, 1, "TPCH_MISMATCH_Q01"),
            new(1, Start.AddSeconds(1), 100, TpchOutcome.Verified, 1, null),
        ];

        (TpchRunReport report, TpchResult result) = TpchReport.Build("0.01", "1847", [Reference(1)], executions, 1, Start, 60);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.Mismatches, Is.EqualTo(1));
        Assert.That(report.Queries.Single().Mismatches, Is.EqualTo(0));
        Assert.That(result.Failures[0], Does.Contain("differ from the reference"));
    }
}
