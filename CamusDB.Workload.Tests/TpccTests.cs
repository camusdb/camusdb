/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Tpcc;
using CamusDB.Workload.Util;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

[TestFixture]
public class TpccScaleTests
{
    [Test]
    public void TheFullScaleHasTheSpecifiedCardinalities()
    {
        TpccScale scale = new(3, 1);

        Assert.That(scale.Items, Is.EqualTo(100_000));
        Assert.That(scale.Customers, Is.EqualTo(3_000));
        Assert.That(scale.DeliveredOrders, Is.EqualTo(2_100));
        Assert.That(scale.FirstNewOrderId, Is.EqualTo(3_001));
        Assert.That(scale.DistrictYtdCents, Is.EqualTo(3_000_000));
        Assert.That(scale.WarehouseYtdCents, Is.EqualTo(30_000_000));
        Assert.That(scale.FractionText, Is.EqualTo("1"));
    }

    [Test]
    public void AFractionShrinksEveryCardinality()
    {
        TpccScale scale = new(1, 0.1);

        Assert.That(scale.Items, Is.EqualTo(10_000));
        Assert.That(scale.Customers, Is.EqualTo(300));
        Assert.That(scale.DeliveredOrders, Is.EqualTo(210));
        Assert.That(scale.DistrictYtdCents, Is.EqualTo(300_000));
        Assert.That(scale.FractionText, Is.EqualTo("0.1"));
    }

    [Test]
    public void SizesOutOfRangeAreRefused()
    {
        Assert.That(TpccScale.Error(1, 1), Is.Null);
        Assert.That(TpccScale.Error(1, 0.01), Is.Null);
        Assert.That(TpccScale.Error(0, 1), Does.Contain("warehouses"));
        Assert.That(TpccScale.Error(1001, 1), Does.Contain("warehouses"));
        Assert.That(TpccScale.Error(1, 0.001), Does.Contain("scale"));
        Assert.That(TpccScale.Error(1, 1.5), Does.Contain("scale"));
        Assert.That(TpccScale.Error(1, double.NaN), Does.Contain("scale"));
    }

    [Test]
    public void NuRandUsesTheSpecifiedConstantsAtTheSpecifiedRanges()
    {
        Assert.That(TpccScale.NuRandA(1_000), Is.EqualTo(255));
        Assert.That(TpccScale.NuRandA(3_000), Is.EqualTo(1023));
        Assert.That(TpccScale.NuRandA(100_000), Is.EqualTo(8191));
        Assert.That(TpccScale.NuRandA(300), Is.EqualTo(63));
        Assert.That(TpccScale.NuRandA(30), Is.EqualTo(7));
    }
}

[TestFixture]
public class TpccGeneratorTests
{
    private static readonly TpccScale Small = new(2, 0.01);

    [Test]
    public void TheSameSeedGivesTheSameRowsAndAnotherSeedDoesNot()
    {
        TpccGenerator a = new(1847, Small);
        TpccGenerator b = new(1847, Small);
        TpccGenerator other = new(7, Small);

        Assert.That(b.Item(42), Is.EqualTo(a.Item(42)));
        Assert.That(b.Warehouse(2), Is.EqualTo(a.Warehouse(2)));
        Assert.That(b.District(2, 7), Is.EqualTo(a.District(2, 7)));
        Assert.That(b.Customer(1, 3, 17).Data, Is.EqualTo(a.Customer(1, 3, 17).Data));
        Assert.That(b.Stock(2, 99).Dist, Is.EqualTo(a.Stock(2, 99).Dist));
        Assert.That(b.Order(1, 4, 12).Lines, Is.EqualTo(a.Order(1, 4, 12).Lines));
        Assert.That(other.Item(42), Is.Not.EqualTo(a.Item(42)));
    }

    [Test]
    public void RowsAreInTheSpecifiedRanges()
    {
        TpccGenerator g = new(1847, Small);

        for (long id = 1; id <= Small.Items; id++)
        {
            TpccItem item = g.Item(id);
            Assert.That(item.PriceCents, Is.InRange(100, 10_000));
            Assert.That(item.ImId, Is.InRange(1, 10_000));
            Assert.That(item.Name.Length, Is.InRange(14, 24));
            Assert.That(item.Data.Length, Is.InRange(26, 50));

            TpccStock stock = g.Stock(1, id);
            Assert.That(stock.Quantity, Is.InRange(10, 100));
            Assert.That(stock.Dist, Has.Length.EqualTo(10));
            Assert.That(stock.Dist, Has.All.Length.EqualTo(24));
        }

        Assert.That(Enumerable.Range(1, Small.Items).Count(i => g.Item(i).Data.Contains("ORIGINAL")), Is.InRange(50, 160),
            "about one item in ten carries ORIGINAL");
        Assert.That(g.Warehouse(1).TaxBasisPoints, Is.InRange(0, 2_000));
        Assert.That(g.Warehouse(1).Zip, Does.EndWith("11111").And.Length.EqualTo(9));
        Assert.That(g.District(1, 1).NextOrderId, Is.EqualTo(Small.Customers + 1));
    }

    [Test]
    public void EveryCustomerOfADistrictGetsExactlyOneInitialOrder()
    {
        TpccGenerator g = new(1847, new TpccScale(1, 0.1));

        for (long district = 1; district <= TpccScale.Districts; district++)
        {
            HashSet<long> customers = new();
            for (long order = 1; order <= g.Scale.Customers; order++)
                customers.Add(g.CustomerOfOrder(1, district, order));

            Assert.That(customers, Has.Count.EqualTo(g.Scale.Customers));
            Assert.That(customers.Min(), Is.EqualTo(1));
            Assert.That(customers.Max(), Is.EqualTo(g.Scale.Customers));
        }

        Assert.That(
            Enumerable.Range(1, 20).Select(o => g.CustomerOfOrder(1, 1, o)),
            Is.Not.EqualTo(Enumerable.Range(1, 20).Select(o => g.CustomerOfOrder(1, 2, o))),
            "the permutation depends on the district");
    }

    [Test]
    public void TheLastThirtyPercentOfTheOrdersAreUndelivered()
    {
        TpccGenerator g = new(1847, Small);

        TpccOrder delivered = g.Order(1, 1, Small.DeliveredOrders);
        TpccOrder open = g.Order(1, 1, Small.DeliveredOrders + 1);

        Assert.That(delivered.Delivered, Is.True);
        Assert.That(delivered.CarrierId, Is.InRange(1, 10));
        Assert.That(delivered.Lines, Has.All.Matches<TpccOrderLine>(l => l.AmountCents == 0));
        Assert.That(open.Delivered, Is.False);
        Assert.That(open.Lines, Has.All.Matches<TpccOrderLine>(l => l.AmountCents is >= 1 and <= 999_999));
        Assert.That(open.Lines.Length, Is.InRange(5, 15));
        Assert.That(open.Lines.Select(l => l.Number), Is.EqualTo(Enumerable.Range(1, open.Lines.Length)));
        Assert.That(open.Lines, Has.All.Matches<TpccOrderLine>(l => l.ItemId >= 1 && l.ItemId <= Small.Items));
    }

    [Test]
    public void TheFirstThousandCustomersTakeTheLastNamesInOrder()
    {
        TpccGenerator g = new(1847, new TpccScale(1, 1));

        Assert.That(TpccGenerator.LastName(0), Is.EqualTo("BARBARBAR"));
        Assert.That(TpccGenerator.LastName(371), Is.EqualTo("PRICALLYOUGHT"));
        Assert.That(TpccGenerator.LastName(999), Is.EqualTo("EINGEINGEING"));
        Assert.That(g.LastNames, Is.EqualTo(1_000));
        Assert.That(g.Customer(1, 1, 1).Last, Is.EqualTo("BARBARBAR"));
        Assert.That(g.Customer(1, 1, 372).Last, Is.EqualTo("PRICALLYOUGHT"));
        Assert.That(g.Customer(1, 1, 1000).Last, Is.EqualTo("EINGEINGEING"));

        TpccGenerator small = new(1847, Small);
        Assert.That(small.LastNames, Is.EqualTo(30));
        Assert.That(small.Customer(1, 1, 30).Last, Is.EqualTo(TpccGenerator.LastName(29)));
    }

    [Test]
    public void NuRandStaysInItsRangeAndIsNotUniform()
    {
        DeterministicRandom rng = new(11);
        int[] hits = new int[3_000];
        for (int i = 0; i < 200_000; i++)
        {
            long value = TpccGenerator.NuRand(rng, 1023, 259, 1, 3_000);
            Assert.That(value, Is.InRange(1, 3_000));
            hits[value - 1]++;
        }

        Assert.That(hits.Max(), Is.GreaterThan(3 * hits.Where(h => h > 0).Min()), "some values are drawn far more often than others");
    }

    [Test]
    public void TheMixIsTheStandardOne()
    {
        Dictionary<TpccTransaction, int> counts = new();
        for (int p = 0; p < 100; p++)
        {
            TpccTransaction type = TpccOperation.Pick(p);
            counts[type] = counts.GetValueOrDefault(type) + 1;
        }

        Assert.That(counts[TpccTransaction.NewOrder], Is.EqualTo(45));
        Assert.That(counts[TpccTransaction.Payment], Is.EqualTo(43));
        Assert.That(counts[TpccTransaction.OrderStatus], Is.EqualTo(4));
        Assert.That(counts[TpccTransaction.Delivery], Is.EqualTo(4));
        Assert.That(counts[TpccTransaction.StockLevel], Is.EqualTo(4));
    }
}

[TestFixture]
public class TpccConsistencyTests
{
    private static readonly TpccScale Scale = new(1, 0.01);

    /// <summary>The state of warehouse 1 right after the load, as the checks would read it.</summary>
    private static TpccWarehouseState Loaded()
    {
        TpccGenerator g = new(1847, Scale);
        TpccWarehouseState s = new() { Warehouse = 1, Found = true, Ytd = Scale.WarehouseYtdCents / 100m };

        for (long d = 1; d <= TpccScale.Districts; d++)
        {
            s.Districts[d] = new TpccDistrictState(Scale.DistrictYtdCents / 100m, Scale.FirstNewOrderId);
            s.HistoryByDistrict[d] = Scale.DistrictYtdCents / 100m;

            for (long id = 1; id <= Scale.Customers; id++)
            {
                s.Customers[(d, id)] = new TpccCustomerState(-10m, 10m, 1, 0);
                s.HistoryByCustomer[(d, id)] = (10m, 1);

                TpccOrder o = g.Order(1, d, id);
                s.Orders.Add(new TpccOrderState(d, id, o.CustomerId, o.Delivered, o.Lines.Length));
                s.Lines[(d, id)] = new TpccLineSummary(
                    o.Lines.Length, o.Delivered ? o.Lines.Length : 0, o.Lines.Sum(l => l.AmountCents) / 100m);
                if (!o.Delivered)
                    s.NewOrders.Add((d, id));
            }
        }
        return s;
    }

    private static TpccConsistency Check(TpccWarehouseState s)
    {
        TpccConsistency c = new(Scale);
        c.CheckWarehouse(s);
        return c;
    }

    private static IEnumerable<string> Broken(TpccConsistency c) => c.Conditions.Where(x => x.Violations > 0).Select(x => x.Name);

    [Test]
    public void TheLoadedStateHoldsEveryCondition()
    {
        TpccConsistency c = Check(Loaded());

        Assert.That(c.Violations, Is.Zero, string.Join("; ", c.Conditions.SelectMany(x => x.Examples)));
        Assert.That(c.Conditions.Select(x => x.Name),
            Is.EqualTo(new[] { "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C12", "X1", "X2", "X3" }));
        Assert.That(c.C1.Checked, Is.EqualTo(1));
        Assert.That(c.C2.Checked, Is.EqualTo(10));
        Assert.That(c.C6.Checked, Is.EqualTo(300));
        Assert.That(c.C10.Checked, Is.EqualTo(300));
    }

    /// <summary>A Payment of 12.50 by customer 5 of district 3, applied whole.</summary>
    private static void Pay(TpccWarehouseState s)
    {
        s.Ytd += 12.5m;
        s.Districts[3] = s.Districts[3] with { Ytd = s.Districts[3].Ytd + 12.5m };
        s.HistoryByDistrict[3] += 12.5m;
        TpccCustomerState c = s.Customers[(3, 5)];
        s.Customers[(3, 5)] = c with { Balance = c.Balance - 12.5m, YtdPayment = c.YtdPayment + 12.5m, PaymentCount = c.PaymentCount + 1 };
        (decimal amount, long count) = s.HistoryByCustomer[(3, 5)];
        s.HistoryByCustomer[(3, 5)] = (amount + 12.5m, count + 1);
    }

    /// <summary>A New-Order of two lines for customer 5 of district 3, applied whole.</summary>
    private static long Order(TpccWarehouseState s)
    {
        long id = s.Districts[3].NextOrderId;
        s.Districts[3] = s.Districts[3] with { NextOrderId = id + 1 };
        s.Orders.Add(new TpccOrderState(3, id, 5, false, 2));
        s.Lines[(3, id)] = new TpccLineSummary(2, 0, 40m);
        s.NewOrders.Add((3, id));
        s.StockYtd += 7;
        s.StockOrders += 2;
        s.SuppliedQuantity += 7;
        s.SuppliedLines += 2;
        return id;
    }

    /// <summary>The Delivery of the oldest undelivered order of district 3, applied whole.</summary>
    private static void Deliver(TpccWarehouseState s)
    {
        long id = s.NewOrders.Where(n => n.District == 3).Min(n => n.Order);
        s.NewOrders.Remove((3, id));
        int at = s.Orders.FindIndex(o => o.District == 3 && o.Id == id);
        TpccOrderState order = s.Orders[at];
        s.Orders[at] = order with { Delivered = true };
        TpccLineSummary lines = s.Lines[(3, id)];
        s.Lines[(3, id)] = lines with { Delivered = lines.Count };
        TpccCustomerState c = s.Customers[(3, order.Customer)];
        s.Customers[(3, order.Customer)] = c with { Balance = c.Balance + lines.Amount, DeliveryCount = c.DeliveryCount + 1 };
    }

    [Test]
    public void WholeTransactionsKeepEveryCondition()
    {
        TpccWarehouseState s = Loaded();
        Pay(s);
        Order(s);
        Deliver(s);
        Pay(s);

        TpccConsistency c = Check(s);
        Assert.That(c.Violations, Is.Zero, string.Join("; ", c.Conditions.SelectMany(x => x.Examples)));
    }

    [Test]
    public void APaymentThatMissedTheDistrictBreaksC1AndC9()
    {
        TpccWarehouseState s = Loaded();
        Pay(s);
        s.Districts[3] = s.Districts[3] with { Ytd = s.Districts[3].Ytd - 12.5m };

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C1", "C9" }));
    }

    [Test]
    public void APaymentWithoutItsHistoryRowBreaksC8C9C10AndThePaymentCount()
    {
        TpccWarehouseState s = Loaded();
        Pay(s);
        s.HistoryByDistrict[3] -= 12.5m;
        s.HistoryByCustomer[(3, 5)] = (10m, 1);

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C8", "C9", "C10", "X1" }));
    }

    [Test]
    public void ACustomerDebitedTwiceBreaksC10AndC12()
    {
        TpccWarehouseState s = Loaded();
        TpccCustomerState c = s.Customers[(1, 1)];
        s.Customers[(1, 1)] = c with { Balance = c.Balance - 5m };

        TpccConsistency check = Check(s);
        Assert.That(Broken(check), Is.EquivalentTo(new[] { "C10", "C12" }));
        Assert.That(check.C10.Examples.Single(), Does.Contain("customer 1/1/1"));
    }

    [Test]
    public void AnOrderIdGivenTwiceBreaksC2()
    {
        TpccWarehouseState s = Loaded();
        Order(s);
        s.Districts[3] = s.Districts[3] with { NextOrderId = s.Districts[3].NextOrderId - 1 };

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C2" }));
    }

    [Test]
    public void AnOrderWithoutItsNewOrderRowBreaksC2AndC5()
    {
        TpccWarehouseState s = Loaded();
        long id = Order(s);
        s.NewOrders.Remove((3, id));

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C2", "C5" }));
    }

    [Test]
    public void AHoleInTheNewOrdersBreaksC3AndC5()
    {
        TpccWarehouseState s = Loaded();
        long oldest = s.NewOrders.Where(n => n.District == 2).Min(n => n.Order);
        s.NewOrders.Remove((2, oldest + 1));

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C3", "C5" }));
    }

    [Test]
    public void AMissingOrderLineBreaksC4AndC6()
    {
        TpccWarehouseState s = Loaded();
        long id = Order(s);
        s.Lines[(3, id)] = new TpccLineSummary(1, 0, 20m);
        s.StockYtd -= 3;
        s.StockOrders -= 1;
        s.SuppliedQuantity -= 3;
        s.SuppliedLines -= 1;

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C4", "C6" }));
    }

    [Test]
    public void OrderLinesWithoutTheirOrderBreakC4AndC6()
    {
        TpccWarehouseState s = Loaded();
        s.Lines[(4, 9_999)] = new TpccLineSummary(3, 0, 1m);

        TpccConsistency c = Check(s);
        Assert.That(Broken(c), Is.EquivalentTo(new[] { "C4", "C6" }));
        Assert.That(c.C6.Examples.Single(), Does.Contain("have no orders row"));
    }

    [Test]
    public void ADeliveryThatMissedSomeLinesBreaksC7()
    {
        TpccWarehouseState s = Loaded();
        Deliver(s);
        (long, long) delivered = (3, Scale.DeliveredOrders + 1);
        TpccLineSummary lines = s.Lines[delivered];
        s.Lines[delivered] = lines with { Delivered = lines.Count - 1 };

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C7" }));
    }

    [Test]
    public void ADeliveryThatDidNotCreditTheCustomerBreaksC10C12AndTheDeliveryCount()
    {
        TpccWarehouseState s = Loaded();
        Dictionary<(long, long), TpccCustomerState> before = new(s.Customers);
        Deliver(s);
        foreach (KeyValuePair<(long, long), TpccCustomerState> c in before)
            s.Customers[c.Key] = c.Value;

        Assert.That(Broken(Check(s)), Is.EquivalentTo(new[] { "C10", "C12", "X2" }));
    }

    [Test]
    public void AStockUpdateThatWasLostBreaksX3()
    {
        TpccWarehouseState s = Loaded();
        Order(s);
        s.StockYtd -= 7;

        TpccConsistency c = Check(s);
        Assert.That(Broken(c), Is.EquivalentTo(new[] { "X3" }));
        Assert.That(c.X3.Examples.Single(), Does.Contain("stock says quantity 0").And.Contain("order_line says quantity 7"));
    }

    [Test]
    public void AMissingWarehouseRowIsAViolation()
    {
        TpccConsistency c = Check(new TpccWarehouseState { Warehouse = 4 });

        Assert.That(c.C1.Violations, Is.EqualTo(1));
        Assert.That(c.C1.Examples.Single(), Is.EqualTo("warehouse 4 has no row"));
    }

    [Test]
    public void OnlyTheFirstExamplesOfAConditionAreKept()
    {
        TpccWarehouseState s = Loaded();
        foreach ((long, long) key in s.Customers.Keys.ToList())
            s.Customers[key] = s.Customers[key] with { Balance = 0m };

        TpccConsistency c = Check(s);
        Assert.That(c.C10.Violations, Is.EqualTo(300));
        Assert.That(c.C10.Examples, Has.Count.EqualTo(5));
    }

    [Test]
    public void TheLedgerAcceptsWhatWasAcknowledgedAndWhatIsUndecided()
    {
        TpccWarehouseState s = Loaded();
        TpccBaseline baseline = TpccConsistency.BaselineOf(s);
        TpccLedger ledger = new(1);

        Order(s);
        ledger.NewOrder(1, 3, undecided: false);
        Pay(s);
        ledger.Payment(1, 1_250, undecided: false);
        Deliver(s);
        ledger.Delivery(1, 1);

        // Undecided commits: one of each applied, one of each did not.
        Order(s);
        ledger.NewOrder(1, 3, undecided: true);
        ledger.NewOrder(1, 3, undecided: true);
        Pay(s);
        ledger.Payment(1, 1_250, undecided: true);
        ledger.Payment(1, 9_900, undecided: true);
        Deliver(s);
        ledger.DeliveryUndecided(1);
        ledger.DeliveryUndecided(1);

        TpccConsistency c = new(Scale);
        c.CheckWarehouse(s);
        c.CheckLedger(s, baseline, ledger);

        Assert.That(c.Violations, Is.Zero, string.Join("; ", c.LedgerConditions.SelectMany(x => x.Examples)));
        Assert.That(c.L1.Checked, Is.EqualTo(10));
        Assert.That(c.L2.Checked, Is.EqualTo(1));
        Assert.That(c.L3.Checked, Is.EqualTo(1));
        Assert.That(ledger.Undecided, Is.EqualTo(4));
    }

    [Test]
    public void AnAcknowledgedCommitThatIsGoneBreaksItsLedgerBand()
    {
        TpccWarehouseState s = Loaded();
        TpccBaseline baseline = TpccConsistency.BaselineOf(s);
        TpccLedger ledger = new(1);

        // The client was told of one New-Order, one Payment and one Delivery; the database has none.
        ledger.NewOrder(1, 3, undecided: false);
        ledger.Payment(1, 1_250, undecided: false);
        ledger.Delivery(1, 1);

        TpccConsistency c = new(Scale);
        c.CheckWarehouse(s);
        c.CheckLedger(s, baseline, ledger);

        Assert.That(Broken(c), Is.Empty, "a commit lost whole leaves the database consistent");
        Assert.That(c.LedgerConditions.Where(x => x.Violations > 0).Select(x => x.Name), Is.EqualTo(new[] { "L1", "L2", "L3" }));
        Assert.That(c.L1.Examples.Single(), Does.Contain("district 1/3").And.Contain("advanced by 0, 1 New-Order commit(s) acknowledged"));
    }

    [Test]
    public void ACommitNobodyAskedForBreaksItsLedgerBand()
    {
        TpccWarehouseState s = Loaded();
        TpccBaseline baseline = TpccConsistency.BaselineOf(s);
        Order(s);
        Pay(s);
        Deliver(s);

        TpccConsistency c = new(Scale);
        c.CheckLedger(s, baseline, new TpccLedger(1));

        Assert.That(c.LedgerConditions.Where(x => x.Violations > 0).Select(x => x.Name), Is.EqualTo(new[] { "L1", "L2", "L3" }));
    }
}

[TestFixture]
public class TpccReportTests
{
    private static readonly TpccScale Scale = new(2, 0.1);
    private static readonly DateTime Start = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);

    private static TpccExecution Run(TpccTransaction type, double atSecond, double ms, TpccOutcome outcome = TpccOutcome.Committed, int attempts = 1)
        => new(type, Start.AddSeconds(atSecond), ms, outcome, attempts, outcome == TpccOutcome.Committed ? null : "CADB0502");

    private static List<TpccExecution> OneOfEach() =>
    [
        Run(TpccTransaction.NewOrder, 1, 100),
        Run(TpccTransaction.Payment, 2, 20),
        Run(TpccTransaction.OrderStatus, 3, 5),
        Run(TpccTransaction.Delivery, 4, 300),
        Run(TpccTransaction.StockLevel, 5, 40),
    ];

    private static TpccCheck CleanCheck() => new(new TpccConsistency(Scale), new Dictionary<long, TpccBaseline>());

    [Test]
    public void FiguresAreCutToTheMeasuredWindowAndCountRetries()
    {
        List<TpccExecution> executions = OneOfEach();
        executions.Add(Run(TpccTransaction.NewOrder, 10, 300, attempts: 3));
        executions.Add(Run(TpccTransaction.NewOrder, 11, 50, TpccOutcome.RolledBack));
        executions.Add(Run(TpccTransaction.NewOrder, 12, 80, TpccOutcome.Conflict, attempts: 30));
        executions.Add(Run(TpccTransaction.NewOrder, 13, 80, TpccOutcome.Undecided));
        executions.Add(Run(TpccTransaction.NewOrder, -5, 100));
        executions.Add(Run(TpccTransaction.NewOrder, 59.95, 100));

        (TpccRunReport report, TpccResult result) = TpccReport.Build(Scale, "1847", executions, 0, CleanCheck(), null, Start, 60);

        TpccTransactionStats newOrder = report.Transactions[0];
        Assert.That(newOrder.Transaction, Is.EqualTo("New-Order"));
        Assert.That(newOrder.MixPercent, Is.EqualTo(45));
        Assert.That(newOrder.Executions, Is.EqualTo(5));
        Assert.That(newOrder.Committed, Is.EqualTo(2));
        Assert.That(newOrder.RolledBack, Is.EqualTo(1));
        Assert.That(newOrder.Conflicts, Is.EqualTo(1));
        Assert.That(newOrder.Undecided, Is.EqualTo(1));
        Assert.That(newOrder.Retries, Is.EqualTo(31));
        Assert.That(newOrder.MeanMs, Is.EqualTo(150));
        Assert.That(newOrder.MaxMs, Is.EqualTo(300));
        Assert.That(report.Transactions.Select(t => t.Transaction),
            Is.EqualTo(new[] { "New-Order", "Payment", "Order-Status", "Delivery", "Stock-Level" }));
        Assert.That(report.NewOrdersPerMinute, Is.EqualTo(2));
        Assert.That(report.TransactionsPerSecond, Is.EqualTo(0.12));
        Assert.That(report.Warehouses, Is.EqualTo(2));
        Assert.That(report.Scale, Is.EqualTo("0.1"));

        Assert.That(result.Passed, Is.True);
        Assert.That(result.Status, Is.EqualTo("verified"));
        Assert.That(result.Committed, Is.EqualTo(6));
        Assert.That(result.NewOrders, Is.EqualTo(2));
        Assert.That(result.RolledBack, Is.EqualTo(1));
        Assert.That(result.Conflicts, Is.EqualTo(1));
        Assert.That(result.Undecided, Is.EqualTo(1));
    }

    [Test]
    public void ATransactionTypeWithNoCommitInTheWindowFailsTheRun()
    {
        List<TpccExecution> executions = OneOfEach();
        executions.RemoveAll(e => e.Type == TpccTransaction.Delivery);
        executions.Add(Run(TpccTransaction.Delivery, 4, 900, TpccOutcome.Conflict, attempts: 30));

        (_, TpccResult result) = TpccReport.Build(Scale, "1847", executions, 0, CleanCheck(), null, Start, 60);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.Failures.Single(), Does.StartWith("Delivery has no commit in the measured window (1 attempt(s), 1 conflict(s)"));
    }

    [Test]
    public void AnAnomalyOutsideTheWindowStillFailsTheRun()
    {
        (TpccRunReport report, TpccResult result) = TpccReport.Build(Scale, "1847", OneOfEach(), 1, CleanCheck(), null, Start, 60);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.Anomalies, Is.EqualTo(1));
        Assert.That(report.Transactions.Sum(t => t.Anomalies), Is.Zero);
        Assert.That(result.Failures[0], Does.Contain("no interleaving of whole transactions"));
    }

    [Test]
    public void ABrokenConditionFailsTheRunAndNamesItsFirstCase()
    {
        TpccConsistency consistency = new(Scale);
        consistency.C1.Pass();
        consistency.C1.Fail("warehouse 2: w_ytd 30010.00, SUM(d_ytd) 30000.00");
        TpccCheck check = new(consistency, new Dictionary<long, TpccBaseline>());

        (TpccRunReport report, TpccResult result) = TpccReport.Build(Scale, "1847", OneOfEach(), 0, check, null, Start, 60);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.ConditionsChecked, Is.EqualTo(2));
        Assert.That(result.ConditionViolations, Is.EqualTo(1));
        Assert.That(result.Failures.Single(), Does.StartWith("consistency condition C1").And.Contain("warehouse 2: w_ytd 30010.00"));
        Assert.That(report.Conditions.Select(c => c.Name), Does.Contain("C12").And.Contain("X3").And.Contain("L3"));
    }

    [Test]
    public void AFinalStateThatCouldNotBeReadFailsTheRun()
    {
        (TpccRunReport report, TpccResult result) = TpccReport.Build(
            Scale, "1847", OneOfEach(), 0, null, "TimeoutException: deadline exceeded", Start, 60);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.Status, Is.EqualTo("unavailable"));
        Assert.That(result.Failures.Single(), Does.Contain("could not be read").And.Contain("deadline exceeded"));
        Assert.That(report.Conditions, Is.Empty);
    }
}
