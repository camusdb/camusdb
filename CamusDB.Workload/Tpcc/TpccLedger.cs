/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Tpcc;

/// <summary>
/// What the client was told about the commits that move a counter the database keeps: the New-Order
/// commits of each district (they advance <c>d_next_o_id</c>), the Payment amounts of each warehouse
/// (they grow <c>w_ytd</c>) and the orders the Delivery commits delivered, one district at a time. A commit whose answer never
/// arrived is undecided: it may have applied or not, so it widens the band the final check accepts.
/// </summary>
public sealed class TpccLedger
{
    private readonly int _warehouses;
    private readonly long[] _newOrdersCommitted;
    private readonly long[] _newOrdersUndecided;
    private readonly long[] _paidCommitted;
    private readonly long[] _paidUndecided;
    private readonly long[] _deliveredCommitted;
    private readonly long[] _deliveriesUndecided;

    public TpccLedger(int warehouses)
    {
        _warehouses = warehouses;
        _newOrdersCommitted = new long[warehouses * TpccScale.Districts];
        _newOrdersUndecided = new long[warehouses * TpccScale.Districts];
        _paidCommitted = new long[warehouses];
        _paidUndecided = new long[warehouses];
        _deliveredCommitted = new long[warehouses];
        _deliveriesUndecided = new long[warehouses];
    }

    private static int Slot(long warehouse, long district) => (int)((warehouse - 1) * TpccScale.Districts + district - 1);

    public void NewOrder(long warehouse, long district, bool undecided)
        => Interlocked.Increment(ref (undecided ? _newOrdersUndecided : _newOrdersCommitted)[Slot(warehouse, district)]);

    public void Payment(long warehouse, long cents, bool undecided)
        => Interlocked.Add(ref (undecided ? _paidUndecided : _paidCommitted)[warehouse - 1], cents);

    public void Delivery(long warehouse, int ordersDelivered)
        => Interlocked.Add(ref _deliveredCommitted[warehouse - 1], ordersDelivered);

    public void DeliveryUndecided(long warehouse) => Interlocked.Increment(ref _deliveriesUndecided[warehouse - 1]);

    public long NewOrdersCommitted(long warehouse, long district) => Interlocked.Read(ref _newOrdersCommitted[Slot(warehouse, district)]);

    public long NewOrdersUndecided(long warehouse, long district) => Interlocked.Read(ref _newOrdersUndecided[Slot(warehouse, district)]);

    public long PaidCentsCommitted(long warehouse) => Interlocked.Read(ref _paidCommitted[warehouse - 1]);

    public long PaidCentsUndecided(long warehouse) => Interlocked.Read(ref _paidUndecided[warehouse - 1]);

    public long DeliveredCommitted(long warehouse) => Interlocked.Read(ref _deliveredCommitted[warehouse - 1]);

    public long DeliveriesUndecided(long warehouse) => Interlocked.Read(ref _deliveriesUndecided[warehouse - 1]);

    public long Undecided
    {
        get
        {
            long total = 0;
            for (int i = 0; i < _newOrdersUndecided.Length; i++)
                total += Interlocked.Read(ref _newOrdersUndecided[i]);
            for (int w = 0; w < _warehouses; w++)
                total += Interlocked.Read(ref _deliveriesUndecided[w]);
            return total;
        }
    }
}
