/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using CamusDB.Client;
using CamusDB.Workload.Operations;

namespace CamusDB.Workload.Tpcc;

public readonly record struct TpccDistrictState(decimal Ytd, long NextOrderId);

public readonly record struct TpccOrderState(long District, long Id, long Customer, bool Delivered, long LineCount);

public readonly record struct TpccLineSummary(long Count, long Delivered, decimal Amount);

public readonly record struct TpccCustomerState(decimal Balance, decimal YtdPayment, long PaymentCount, long DeliveryCount);

/// <summary>Everything the consistency conditions read about one warehouse.</summary>
public sealed class TpccWarehouseState
{
    public long Warehouse { get; init; }
    public decimal Ytd { get; set; }
    public bool Found { get; set; }
    public Dictionary<long, TpccDistrictState> Districts { get; } = new();
    public List<TpccOrderState> Orders { get; } = new();
    public Dictionary<(long District, long Order), TpccLineSummary> Lines { get; } = new();
    public HashSet<(long District, long Order)> NewOrders { get; } = new();

    /// <summary>SUM(h_amount) of the payments made at this warehouse, by district of payment.</summary>
    public Dictionary<long, decimal> HistoryByDistrict { get; } = new();

    /// <summary>The payments of this warehouse's customers, wherever they were made.</summary>
    public Dictionary<(long District, long Customer), (decimal Amount, long Count)> HistoryByCustomer { get; } = new();

    public Dictionary<(long District, long Customer), TpccCustomerState> Customers { get; } = new();
    public long StockYtd { get; set; }
    public long StockOrders { get; set; }
    public long StockRemote { get; set; }

    /// <summary>The order lines the run added that this warehouse supplied: quantity, count, and how many
    /// were for another warehouse's order.</summary>
    public long SuppliedQuantity { get; set; }
    public long SuppliedLines { get; set; }
    public long SuppliedRemoteLines { get; set; }

    public long DeliveredOrders => Orders.LongCount(o => o.Delivered);
}

/// <summary>The figures of a warehouse that the ledger bands start from, taken before the run.</summary>
public sealed record TpccBaseline(decimal WarehouseYtd, IReadOnlyDictionary<long, long> NextOrderIds, long DeliveredOrders);

/// <summary>One consistency condition over the whole database.</summary>
public sealed class TpccCondition
{
    private const int MaxExamples = 5;

    public string Name { get; }
    public string Rule { get; }
    public long Checked { get; private set; }
    public long Violations { get; private set; }
    public List<string> Examples { get; } = new();

    public TpccCondition(string name, string rule)
    {
        Name = name;
        Rule = rule;
    }

    public void Pass() => Checked++;

    public void Fail(string what)
    {
        Checked++;
        Violations++;
        if (Examples.Count < MaxExamples)
            Examples.Add(what);
    }

    public void Expect(bool holds, Func<string> what)
    {
        if (holds)
            Pass();
        else
            Fail(what());
    }
}

/// <summary>
/// The TPC-C consistency conditions (clause 3.3.2 of the specification) and three more of the same kind,
/// evaluated on the client from plain reads of each table.
///
/// <para>Every transaction of the workload keeps these conditions true when it commits whole, and a
/// transaction that did not commit changes nothing. So they hold at any quiet moment whatever was
/// committed, lost in flight or left undecided: a violation means a commit applied in part, applied
/// twice, or was read while half visible. They need no allowance for indeterminate commits.</para>
///
/// <para>The conditions cannot see a commit that was acknowledged and then lost whole, because the
/// database is consistent without it. <see cref="CheckLedger"/> covers that: it compares three counters
/// with what the client was told had committed.</para>
/// </summary>
public sealed class TpccConsistency
{
    private readonly TpccScale _scale;

    public TpccCondition C1 { get; } = new("C1", "w_ytd = SUM(d_ytd) of the warehouse");
    public TpccCondition C2 { get; } = new("C2", "d_next_o_id - 1 = MAX(o_id) = MAX(no_o_id) of the district");
    public TpccCondition C3 { get; } = new("C3", "MAX(no_o_id) - MIN(no_o_id) + 1 = COUNT(new_order) of the district");
    public TpccCondition C4 { get; } = new("C4", "SUM(o_ol_cnt) = COUNT(order_line) of the district");
    public TpccCondition C5 { get; } = new("C5", "o_carrier_id is null exactly when the order has a new_order row");
    public TpccCondition C6 { get; } = new("C6", "o_ol_cnt = COUNT(order_line) of the order, and no line without its order");
    public TpccCondition C7 { get; } = new("C7", "ol_delivery_d is null exactly when o_carrier_id is null");
    public TpccCondition C8 { get; } = new("C8", "w_ytd = SUM(h_amount) of the warehouse");
    public TpccCondition C9 { get; } = new("C9", "d_ytd = SUM(h_amount) of the district");
    public TpccCondition C10 { get; } = new("C10", "c_balance = SUM(ol_amount of delivered lines) - SUM(h_amount) of the customer");
    public TpccCondition C12 { get; } = new("C12", "c_balance + c_ytd_payment = SUM(ol_amount of delivered lines) of the customer");
    public TpccCondition X1 { get; } = new("X1", "c_payment_cnt = COUNT(history) of the customer");
    public TpccCondition X2 { get; } = new("X2", "c_delivery_cnt = the customer's orders delivered since the load");
    public TpccCondition X3 { get; } = new(
        "X3", "SUM(s_ytd), SUM(s_order_cnt) and SUM(s_remote_cnt) of the warehouse = the order lines added since the load that it supplied");
    public TpccCondition L1 { get; } = new("L1", "d_next_o_id advanced by the New-Order commits the client was told of");
    public TpccCondition L2 { get; } = new("L2", "w_ytd grew by the Payment amounts the client was told had committed");
    public TpccCondition L3 { get; } = new("L3", "the delivered orders grew by the orders the Delivery commits reported");

    public TpccConsistency(TpccScale scale)
    {
        _scale = scale;
    }

    public IReadOnlyList<TpccCondition> Conditions => [C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C12, X1, X2, X3];

    public IReadOnlyList<TpccCondition> LedgerConditions => [L1, L2, L3];

    public long Violations => Conditions.Sum(c => c.Violations) + LedgerConditions.Sum(c => c.Violations);

    private static string M(decimal value) => value.ToString(CultureInfo.InvariantCulture);

    public void CheckWarehouse(TpccWarehouseState s)
    {
        long w = s.Warehouse;
        if (!s.Found)
        {
            C1.Fail($"warehouse {w} has no row");
            return;
        }

        C1.Expect(s.Ytd == s.Districts.Values.Sum(d => d.Ytd),
            () => $"warehouse {w}: w_ytd {M(s.Ytd)}, SUM(d_ytd) {M(s.Districts.Values.Sum(d => d.Ytd))}");
        C8.Expect(s.Ytd == s.HistoryByDistrict.Values.Sum(),
            () => $"warehouse {w}: w_ytd {M(s.Ytd)}, SUM(h_amount) {M(s.HistoryByDistrict.Values.Sum())}");

        Dictionary<long, (long Max, long Count, long LineCountSum)> ordersOf = new();
        Dictionary<(long, long), bool> orderDelivered = new(s.Orders.Count);
        Dictionary<(long District, long Customer), (decimal DeliveredAmount, long DeliveredSinceLoad)> byCustomer = new();

        foreach (TpccOrderState o in s.Orders)
        {
            (long max, long count, long lineSum) = ordersOf.GetValueOrDefault(o.District);
            ordersOf[o.District] = (Math.Max(max, o.Id), count + 1, lineSum + o.LineCount);
            orderDelivered[(o.District, o.Id)] = o.Delivered;

            bool hasNewOrder = s.NewOrders.Contains((o.District, o.Id));
            C5.Expect(o.Delivered != hasNewOrder,
                () => $"order {w}/{o.District}/{o.Id}: o_carrier_id is {(o.Delivered ? "set" : "null")} and it " +
                      $"{(hasNewOrder ? "has" : "has no")} new_order row");

            TpccLineSummary lines = s.Lines.GetValueOrDefault((o.District, o.Id));
            C6.Expect(lines.Count == o.LineCount,
                () => $"order {w}/{o.District}/{o.Id}: o_ol_cnt {o.LineCount}, {lines.Count} order_line row(s)");
            C7.Expect(lines.Delivered == (o.Delivered ? lines.Count : 0),
                () => $"order {w}/{o.District}/{o.Id}: o_carrier_id is {(o.Delivered ? "set" : "null")}, " +
                      $"{lines.Delivered} of {lines.Count} line(s) have ol_delivery_d");

            if (o.Delivered)
            {
                (decimal amount, long sinceLoad) = byCustomer.GetValueOrDefault((o.District, o.Customer));
                byCustomer[(o.District, o.Customer)] = (amount + lines.Amount, sinceLoad + (o.Id > _scale.DeliveredOrders ? 1 : 0));
            }
        }

        foreach ((long district, long order) in s.Lines.Keys)
        {
            if (!orderDelivered.ContainsKey((district, order)))
                C6.Fail($"order_line rows of {w}/{district}/{order} have no orders row");
        }
        foreach ((long district, long order) in s.NewOrders)
        {
            if (!orderDelivered.ContainsKey((district, order)))
                C5.Fail($"new_order row {w}/{district}/{order} has no orders row");
        }

        Dictionary<long, (long Min, long Max, long Count)> newOrdersOf = new();
        foreach ((long district, long order) in s.NewOrders)
        {
            newOrdersOf[district] = newOrdersOf.TryGetValue(district, out (long Min, long Max, long Count) n)
                ? (Math.Min(n.Min, order), Math.Max(n.Max, order), n.Count + 1)
                : (order, order, 1);
        }
        Dictionary<long, long> linesOf = new();
        foreach (((long district, _), TpccLineSummary lines) in s.Lines)
            linesOf[district] = linesOf.GetValueOrDefault(district) + lines.Count;

        for (long d = 1; d <= TpccScale.Districts; d++)
        {
            if (!s.Districts.TryGetValue(d, out TpccDistrictState district))
            {
                C2.Fail($"district {w}/{d} has no row");
                continue;
            }

            (long maxOrder, _, long lineCountSum) = ordersOf.GetValueOrDefault(d);
            bool hasNew = newOrdersOf.TryGetValue(d, out (long Min, long Max, long Count) no);
            long district_ = d;
            C2.Expect(district.NextOrderId - 1 == maxOrder && (!hasNew || no.Max == maxOrder),
                () => $"district {w}/{district_}: d_next_o_id {district.NextOrderId}, MAX(o_id) {maxOrder}, " +
                      $"MAX(no_o_id) {(hasNew ? no.Max.ToString(CultureInfo.InvariantCulture) : "none")}");
            if (hasNew)
                C3.Expect(no.Max - no.Min + 1 == no.Count,
                    () => $"district {w}/{district_}: new_order ids {no.Min}..{no.Max}, {no.Count} row(s)");
            else
                C3.Pass();
            C4.Expect(lineCountSum == linesOf.GetValueOrDefault(d),
                () => $"district {w}/{district_}: SUM(o_ol_cnt) {lineCountSum}, {linesOf.GetValueOrDefault(district_)} order_line row(s)");
            C9.Expect(district.Ytd == s.HistoryByDistrict.GetValueOrDefault(d),
                () => $"district {w}/{district_}: d_ytd {M(district.Ytd)}, SUM(h_amount) {M(s.HistoryByDistrict.GetValueOrDefault(district_))}");
        }

        foreach (((long district, long customer), TpccCustomerState c) in s.Customers)
        {
            (decimal delivered, long deliveredSinceLoad) = byCustomer.GetValueOrDefault((district, customer));
            (decimal paid, long payments) = s.HistoryByCustomer.GetValueOrDefault((district, customer));

            C10.Expect(c.Balance == delivered - paid,
                () => $"customer {w}/{district}/{customer}: c_balance {M(c.Balance)}, delivered {M(delivered)}, paid {M(paid)}");
            C12.Expect(c.Balance + c.YtdPayment == delivered,
                () => $"customer {w}/{district}/{customer}: c_balance {M(c.Balance)} + c_ytd_payment {M(c.YtdPayment)}, delivered {M(delivered)}");
            X1.Expect(c.PaymentCount == payments,
                () => $"customer {w}/{district}/{customer}: c_payment_cnt {c.PaymentCount}, {payments} history row(s)");
            X2.Expect(c.DeliveryCount == deliveredSinceLoad,
                () => $"customer {w}/{district}/{customer}: c_delivery_cnt {c.DeliveryCount}, {deliveredSinceLoad} order(s) delivered since the load");
        }
        foreach ((long district, long customer) in s.HistoryByCustomer.Keys)
        {
            if (!s.Customers.ContainsKey((district, customer)))
                X1.Fail($"history rows name customer {w}/{district}/{customer}, which has no row");
        }

        X3.Expect(
            s.StockYtd == s.SuppliedQuantity && s.StockOrders == s.SuppliedLines && s.StockRemote == s.SuppliedRemoteLines,
            () => $"warehouse {w}: stock says quantity {s.StockYtd}, {s.StockOrders} line(s), {s.StockRemote} remote; " +
                  $"order_line says quantity {s.SuppliedQuantity}, {s.SuppliedLines} line(s), {s.SuppliedRemoteLines} remote");
    }

    public static TpccBaseline BaselineOf(TpccWarehouseState s)
        => new(s.Ytd, s.Districts.ToDictionary(d => d.Key, d => d.Value.NextOrderId), s.DeliveredOrders);

    /// <summary>
    /// Compares what moved since the baseline with what the client was told. Each counter must have
    /// grown by at least the acknowledged commits and by at most those plus the undecided ones.
    /// </summary>
    public void CheckLedger(TpccWarehouseState s, TpccBaseline baseline, TpccLedger ledger)
    {
        long w = s.Warehouse;

        for (long d = 1; d <= TpccScale.Districts; d++)
        {
            if (!s.Districts.TryGetValue(d, out TpccDistrictState district) || !baseline.NextOrderIds.TryGetValue(d, out long before))
                continue;
            long moved = district.NextOrderId - before;
            long committed = ledger.NewOrdersCommitted(w, d);
            long undecided = ledger.NewOrdersUndecided(w, d);
            long district_ = d;
            L1.Expect(moved >= committed && moved <= committed + undecided,
                () => $"district {w}/{district_}: d_next_o_id advanced by {moved}, {committed} New-Order commit(s) acknowledged, " +
                      $"{undecided} undecided");
        }

        decimal paid = (s.Ytd - baseline.WarehouseYtd) * 100m;
        long paidCommitted = ledger.PaidCentsCommitted(w);
        long paidUndecided = ledger.PaidCentsUndecided(w);
        L2.Expect(paid >= paidCommitted && paid <= paidCommitted + paidUndecided,
            () => $"warehouse {w}: w_ytd grew by {M(paid / 100m)}, {M(paidCommitted / 100m)} acknowledged, " +
                  $"{M(paidUndecided / 100m)} undecided");

        long delivered = s.DeliveredOrders - baseline.DeliveredOrders;
        long deliveredCommitted = ledger.DeliveredCommitted(w);
        long deliveredUndecided = ledger.DeliveriesUndecided(w);
        L3.Expect(delivered >= deliveredCommitted && delivered <= deliveredCommitted + deliveredUndecided,
            () => $"warehouse {w}: {delivered} more order(s) delivered, {deliveredCommitted} acknowledged, " +
                  $"up to {deliveredUndecided} undecided");
    }

    /// <summary>Reads the state of one warehouse with plain statements on a quiet database.</summary>
    public static async Task<TpccWarehouseState> ReadWarehouseAsync(
        CamusConnection conn, long warehouse, TpccScale scale, TimeSpan retryBudget, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await ReadOnceAsync(conn, warehouse, scale, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                bool retryable = ErrorClassifier.IsRetryableForIdempotentRead(ex)
                    || ErrorClassifier.Classify(ex).Status is OperationStatus.Conflict or OperationStatus.Transient;
                if (!retryable || waited.Elapsed > retryBudget)
                    throw;
                await Task.Delay(Math.Min(500 * attempt, 5000), ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task<TpccWarehouseState> ReadOnceAsync(CamusConnection conn, long w, TpccScale scale, CancellationToken ct)
    {
        TpccWarehouseState s = new() { Warehouse = w };

        await QueryAsync(conn, "SELECT w_ytd FROM warehouse WHERE w_id = @w", w, null, r =>
        {
            s.Found = true;
            s.Ytd = r.GetDecimal(0);
        }, ct).ConfigureAwait(false);

        await QueryAsync(conn, "SELECT d_id, d_ytd, d_next_o_id FROM district WHERE d_w_id = @w", w, null,
            r => s.Districts[r.GetInt64(0)] = new TpccDistrictState(r.GetDecimal(1), r.GetInt64(2)), ct).ConfigureAwait(false);

        await QueryAsync(conn, "SELECT o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt FROM orders WHERE o_w_id = @w", w, null,
            r => s.Orders.Add(new TpccOrderState(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2), !r.IsDBNull(3), r.GetInt64(4))), ct)
            .ConfigureAwait(false);

        await QueryAsync(conn,
            "SELECT ol_d_id, ol_o_id, COUNT(*), COUNT(ol_delivery_d), SUM(ol_amount) FROM order_line WHERE ol_w_id = @w " +
            "GROUP BY ol_d_id, ol_o_id", w, null,
            r => s.Lines[(r.GetInt64(0), r.GetInt64(1))] = new TpccLineSummary(r.GetInt64(2), r.GetInt64(3), r.GetDecimal(4)), ct)
            .ConfigureAwait(false);

        await QueryAsync(conn, "SELECT no_d_id, no_o_id FROM new_order WHERE no_w_id = @w", w, null,
            r => s.NewOrders.Add((r.GetInt64(0), r.GetInt64(1))), ct).ConfigureAwait(false);

        await QueryAsync(conn, "SELECT h_d_id, SUM(h_amount) FROM history WHERE h_w_id = @w GROUP BY h_d_id", w, null,
            r => s.HistoryByDistrict[r.GetInt64(0)] = r.GetDecimal(1), ct).ConfigureAwait(false);

        await QueryAsync(conn,
            "SELECT h_c_d_id, h_c_id, SUM(h_amount), COUNT(*) FROM history WHERE h_c_w_id = @w GROUP BY h_c_d_id, h_c_id", w, null,
            r => s.HistoryByCustomer[(r.GetInt64(0), r.GetInt64(1))] = (r.GetDecimal(2), r.GetInt64(3)), ct).ConfigureAwait(false);

        await QueryAsync(conn,
            "SELECT c_d_id, c_id, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt FROM customer WHERE c_w_id = @w", w, null,
            r => s.Customers[(r.GetInt64(0), r.GetInt64(1))] =
                new TpccCustomerState(r.GetDecimal(2), r.GetDecimal(3), r.GetInt64(4), r.GetInt64(5)), ct).ConfigureAwait(false);

        await QueryAsync(conn, "SELECT SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt) FROM stock WHERE s_w_id = @w", w, null, r =>
        {
            s.StockYtd = r.IsDBNull(0) ? 0 : r.GetInt64(0);
            s.StockOrders = r.IsDBNull(1) ? 0 : r.GetInt64(1);
            s.StockRemote = r.IsDBNull(2) ? 0 : r.GetInt64(2);
        }, ct).ConfigureAwait(false);

        // The lines this warehouse supplied since the load, for its own orders and for other warehouses'.
        await QueryAsync(conn,
            "SELECT ol_w_id, SUM(ol_quantity), COUNT(*) FROM order_line WHERE ol_supply_w_id = @w AND ol_o_id > @first " +
            "GROUP BY ol_w_id", w, scale.Customers, r =>
        {
            long count = r.GetInt64(2);
            s.SuppliedQuantity += r.GetInt64(1);
            s.SuppliedLines += count;
            if (r.GetInt64(0) != w)
                s.SuppliedRemoteLines += count;
        }, ct).ConfigureAwait(false);

        return s;
    }

    private static async Task QueryAsync(
        CamusConnection conn, string sql, long warehouse, long? first, Action<CamusDataReader> row, CancellationToken ct)
    {
        using CamusCommand cmd = conn.CreateSelectCommand(sql);
        cmd.Parameters.Add("@w", ColumnType.Integer64, warehouse);
        if (first is long firstValue)
            cmd.Parameters.Add("@first", ColumnType.Integer64, firstValue);
        using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            row(reader);
    }
}
