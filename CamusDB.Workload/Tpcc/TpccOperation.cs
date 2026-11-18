/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Util;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Tpcc;

public enum TpccTransaction
{
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

public enum TpccOutcome
{
    Committed,

    /// <summary>The New-Order that names an unused item and rolls back, as one in a hundred must.</summary>
    RolledBack,

    /// <summary>Still conflicting after the last retry.</summary>
    Conflict,

    /// <summary>The commit was sent and no answer came back.</summary>
    Undecided,

    Error,

    /// <summary>The database answered with something no interleaving of whole transactions can produce.</summary>
    Anomaly,
}

/// <summary>One transaction of the mix, kept so the figures can be cut to the measured window.</summary>
public readonly record struct TpccExecution(
    TpccTransaction Type, DateTime StartedUtc, double Milliseconds, TpccOutcome Outcome, int Attempts, string? Code);

public sealed record TpccAnomaly(string Transaction, string StartedUtc, string Code, string What);

/// <summary>
/// The write operation of the TPC-C shape: each operation is one of the five transactions of the
/// specification, drawn in the standard mix (New-Order 45, Payment 43, Order-Status 4, Delivery 4,
/// Stock-Level 4 percent), against the warehouse the worker is bound to.
///
/// <para>The three writing transactions run at the isolation and locking the run asked for and are
/// retried from a fresh BEGIN on a conflict. Every counter is moved with a relative update
/// (<c>SET x = x + n</c>) and the order id is read back after the increment, so the transactions keep
/// the consistency conditions at read committed as well as at serializable. The two read-only
/// transactions run on CamusDB's serializable snapshot. A Delivery is one transaction for each
/// district.</para>
///
/// <para>There are no keying or think times: a worker starts its next transaction as soon as one ends.
/// The throughput is therefore not a tpmC and is not comparable with a published TPC-C result.</para>
///
/// <para>While it runs, a transaction checks what it can see for itself (see <see cref="TpccAnomaly"/>):
/// an update of a row it just read that matches no row, an order whose lines are not all there, a
/// snapshot in which <c>w_ytd</c> differs from <c>SUM(d_ytd)</c>. Any of these fails the run.</para>
/// </summary>
public sealed class TpccOperation : IWriteOperation
{
    private const long NameConstant = 223;
    private const long CustomerConstant = 259;
    private const long ItemConstant = 7911;
    private const int MaxAnomaliesKept = 200;

    private readonly ConnectionSet _connections;
    private readonly TpccScale _scale;
    private readonly TpccLedger _ledger;
    private readonly ulong _seed;
    private readonly int _maxRetries;
    private readonly int _lastNames;
    private readonly CamusTransactionOptions _readWrite;
    private readonly CamusTransactionOptions _snapshot;
    private readonly ConcurrentQueue<TpccExecution> _executions = new();
    private readonly ConcurrentQueue<TpccAnomaly> _anomalies = new();
    private long _sequence;
    private long _indeterminateTxns;
    private long _retryAttempts;
    private long _retriedTxns;
    private long _maxAttemptsUsed;
    private int _anomalyCount;

    public TpccOperation(
        ConnectionSet connections, TpccScale scale, TpccLedger ledger, ulong seed,
        CamusLocking locking = CamusLocking.Optimistic,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted,
        int maxRetries = 30)
    {
        _connections = connections;
        _scale = scale;
        _ledger = ledger;
        _seed = seed;
        _maxRetries = Math.Max(1, maxRetries);
        _lastNames = Math.Min(1_000, scale.Customers);
        _readWrite = new CamusTransactionOptions
        {
            IsolationLevel = isolationLevel,
            Mode = CamusTransactionMode.ReadWrite,
            Locking = locking,
        };
        _snapshot = TokenOperation.SnapshotRead;
    }

    public IReadOnlyCollection<TpccExecution> Executions => _executions;

    public IReadOnlyCollection<TpccAnomaly> Anomalies => _anomalies;

    public int AnomalyCount => Volatile.Read(ref _anomalyCount);

    /// <summary>No transaction of this shape writes the seeded accounts rows.</summary>
    public long CommittedRows => 0;

    public long IndeterminateTxns => Interlocked.Read(ref _indeterminateTxns);

    public int RowsPerTransaction => 0;

    public long RetryAttempts => Interlocked.Read(ref _retryAttempts);

    public long RetriedTxns => Interlocked.Read(ref _retriedTxns);

    public long MaxAttemptsUsed => Interlocked.Read(ref _maxAttemptsUsed);

    /// <summary>The transaction for a number in 0..99: the standard mix.</summary>
    public static TpccTransaction Pick(long percentile) => percentile switch
    {
        < 45 => TpccTransaction.NewOrder,
        < 88 => TpccTransaction.Payment,
        < 92 => TpccTransaction.OrderStatus,
        < 96 => TpccTransaction.Delivery,
        _ => TpccTransaction.StockLevel,
    };

    private sealed record OrderLineInput(int Number, long Item, long SupplyWarehouse, int Quantity);

    private sealed record Input(
        TpccTransaction Type, long Warehouse, long District, long CustomerWarehouse, long CustomerDistrict, long Customer,
        string? LastName, long AmountCents, bool Rollback, OrderLineInput[] Lines, long Carrier, int Threshold, string HistoryId);

    private enum Step
    {
        Committed,
        RolledBack,
        Retry,
        Failed,
    }

    /// <summary>What an attempt ended with, before the commit: commit, the intended rollback, a lost race
    /// to retry, or an anomaly.</summary>
    private readonly record struct Body(Step Step, string? Code = null, string? What = null, int Delivered = 0)
    {
        public static Body Commit(int delivered = 0) => new(Step.Committed, Delivered: delivered);
        public static Body Anomaly(string code, string what) => new(Step.Failed, code, what);
    }

    private sealed class Session
    {
        public required CamusConnection Conn { get; init; }
        public required CamusTransaction Tx { get; init; }
        public required CancellationToken Ct { get; init; }
        public int Exchanges;

        public CamusCommand Write(string sql)
        {
            CamusCommand cmd = Conn.CreateCamusCommand(sql);
            cmd.Transaction = Tx;
            Exchanges++;
            return cmd;
        }

        public CamusCommand Read(string sql)
        {
            CamusCommand cmd = Conn.CreateSelectCommand(sql);
            cmd.Transaction = Tx;
            Exchanges++;
            return cmd;
        }
    }

    public async Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct)
    {
        ulong sequence = (ulong)Interlocked.Increment(ref _sequence);
        DeterministicRandom rng = DeterministicRandom.ForWorker(_seed ^ (sequence * 0xD1B54A32D192ED03UL), shard.WorkerIndex);
        long warehouse = shard.WorkerIndex % _scale.Warehouses + 1;
        Input input = Generate(rng, Pick(rng.NextLong(100)), warehouse);

        DateTime startedUtc = DateTime.UtcNow;
        long startedAt = Stopwatch.GetTimestamp();

        if (input.Type != TpccTransaction.Delivery)
        {
            (OperationResult result, TpccOutcome outcome, int attempts) = await RunAsync(input, ct).ConfigureAwait(false);
            Journal(input.Type, startedUtc, startedAt, outcome, attempts, result.ErrorCode);
            return result;
        }

        // A Delivery is one database transaction for each district, which clause 2.7.4.1 allows. One
        // transaction over the ten districts reads the oldest new_order of each, and under serializable
        // isolation every New-Order of the warehouse that commits meanwhile invalidates one of those
        // reads, so it almost never commits. The operation ends at the first district that fails; the
        // districts before it stay delivered, and each is whole.
        int totalAttempts = 0, exchanges = 0;
        double beginMs = 0, workMs = 0, commitMs = 0;
        for (long district = 1; district <= TpccScale.Districts; district++)
        {
            (OperationResult result, TpccOutcome outcome, int attempts) =
                await RunAsync(input with { District = district }, ct).ConfigureAwait(false);
            totalAttempts += attempts;
            if (outcome != TpccOutcome.Committed)
            {
                Journal(input.Type, startedUtc, startedAt, outcome, totalAttempts - (int)district + 1, result.ErrorCode);
                return result;
            }
            beginMs += result.BeginMs;
            workMs += result.UpdateMs;
            commitMs += result.CommitMs;
            exchanges += result.Exchanges;
        }

        Journal(input.Type, startedUtc, startedAt, TpccOutcome.Committed, totalAttempts - TpccScale.Districts + 1, null);
        return new OperationResult(OperationKind.Write, OperationStatus.Ok, null, beginMs, 0, workMs, commitMs, Exchanges: exchanges);
    }

    /// <summary>Runs one database transaction, from a fresh BEGIN again after each conflict.</summary>
    private async Task<(OperationResult Result, TpccOutcome Outcome, int Attempts)> RunAsync(Input input, CancellationToken ct)
    {
        OperationResult last = OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, "CADB0502");

        for (int attempt = 1; attempt <= _maxRetries; attempt++)
        {
            (Step step, OperationResult result, TpccOutcome outcome) = await TryOnceAsync(input, ct).ConfigureAwait(false);
            if (step != Step.Retry)
            {
                RecordAttempts(attempt);
                return (result, outcome, attempt);
            }

            // Every Payment of a warehouse writes the one warehouse row, so the losers of a conflict must
            // not all come back together: the wait is random and its ceiling doubles with each attempt.
            last = result;
            int ceiling = Math.Min(2 << Math.Min(attempt, 7), 250);
            await Task.Delay(1 + Random.Shared.Next(ceiling), ct).ConfigureAwait(false);
        }

        RecordAttempts(_maxRetries);
        return (last, TpccOutcome.Conflict, _maxRetries);
    }

    private void Journal(TpccTransaction type, DateTime startedUtc, long startedAt, TpccOutcome outcome, int attempts, string? code)
        => _executions.Enqueue(new TpccExecution(
            type, startedUtc, Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds, outcome, attempts, code));

    private Input Generate(DeterministicRandom rng, TpccTransaction type, long warehouse)
    {
        long district = 1 + rng.NextLong(TpccScale.Districts);
        long customerWarehouse = warehouse, customerDistrict = district, customer = 0, amount = 0, carrier = 0;
        string? lastName = null;
        bool rollback = false;
        int threshold = 0;
        OrderLineInput[] lines = [];
        string historyId = "";

        switch (type)
        {
            case TpccTransaction.NewOrder:
                customer = CustomerId(rng);
                rollback = rng.NextLong(100) == 0;
                lines = new OrderLineInput[5 + (int)rng.NextLong(11)];
                for (int i = 0; i < lines.Length; i++)
                {
                    long item = TpccGenerator.NuRand(rng, TpccScale.NuRandA(_scale.Items), ItemConstant, 1, _scale.Items);
                    long supply = _scale.Warehouses > 1 && rng.NextLong(100) == 0 ? OtherWarehouse(rng, warehouse) : warehouse;
                    lines[i] = new OrderLineInput(i + 1, item, supply, 1 + (int)rng.NextLong(10));
                }
                // The unused item is the last line, so the rollback comes after the work, as specified.
                if (rollback)
                    lines[^1] = lines[^1] with { Item = _scale.Items + 1 };
                break;

            case TpccTransaction.Payment:
                if (_scale.Warehouses > 1 && rng.NextLong(100) < 15)
                {
                    customerWarehouse = OtherWarehouse(rng, warehouse);
                    customerDistrict = 1 + rng.NextLong(TpccScale.Districts);
                }
                if (rng.NextLong(100) < 60)
                    lastName = Name(rng);
                else
                    customer = CustomerId(rng);
                amount = 100 + rng.NextLong(499_901);
                historyId = Guid.NewGuid().ToString("N");
                break;

            case TpccTransaction.OrderStatus:
                if (rng.NextLong(100) < 60)
                    lastName = Name(rng);
                else
                    customer = CustomerId(rng);
                break;

            case TpccTransaction.Delivery:
                carrier = 1 + rng.NextLong(10);
                break;

            case TpccTransaction.StockLevel:
                threshold = 10 + (int)rng.NextLong(11);
                break;
        }

        return new Input(
            type, warehouse, district, customerWarehouse, customerDistrict, customer, lastName, amount, rollback, lines, carrier,
            threshold, historyId);
    }

    private long CustomerId(DeterministicRandom rng)
        => TpccGenerator.NuRand(rng, TpccScale.NuRandA(_scale.Customers), CustomerConstant, 1, _scale.Customers);

    private string Name(DeterministicRandom rng)
        => TpccGenerator.LastName((int)TpccGenerator.NuRand(rng, TpccScale.NuRandA(_lastNames), NameConstant, 0, _lastNames - 1));

    private long OtherWarehouse(DeterministicRandom rng, long warehouse)
    {
        long other = 1 + rng.NextLong(_scale.Warehouses - 1);
        return other >= warehouse ? other + 1 : other;
    }

    private async Task<(Step Step, OperationResult Result, TpccOutcome Outcome)> TryOnceAsync(Input input, CancellationToken ct)
    {
        bool writes = input.Type is TpccTransaction.NewOrder or TpccTransaction.Payment or TpccTransaction.Delivery;
        CamusConnection conn = writes ? _connections.NextWrite() : _connections.NextRead();
        long ts = Stopwatch.GetTimestamp();

        CamusTransaction tx;
        try
        {
            tx = await conn.BeginTransactionAsync(writes ? _readWrite : _snapshot, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // A begin that failed touched nothing.
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex);
            OperationResult failure = OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex));
            return status == OperationStatus.Conflict
                ? (Step.Retry, failure, TpccOutcome.Conflict)
                : (Step.Failed, failure, TpccOutcome.Error);
        }
        double beginMs = Lap(ref ts);

        Session session = new() { Conn = conn, Tx = tx, Ct = ct, Exchanges = 1 };
        bool commitSubmitted = false;
        try
        {
            Body body = input.Type switch
            {
                TpccTransaction.NewOrder => await NewOrderAsync(session, input).ConfigureAwait(false),
                TpccTransaction.Payment => await PaymentAsync(session, input).ConfigureAwait(false),
                TpccTransaction.OrderStatus => await OrderStatusAsync(session, input).ConfigureAwait(false),
                TpccTransaction.Delivery => await DeliveryAsync(session, input).ConfigureAwait(false),
                _ => await StockLevelAsync(session, input).ConfigureAwait(false),
            };
            double workMs = Lap(ref ts);

            if (body.Step != Step.Committed)
            {
                await RollbackAsync(tx, ct).ConfigureAwait(false);
                switch (body.Step)
                {
                    case Step.RolledBack:
                        return (Step.RolledBack,
                            new OperationResult(OperationKind.Write, OperationStatus.Ok, null, beginMs, 0, workMs, 0, Exchanges: session.Exchanges + 1),
                            TpccOutcome.RolledBack);
                    case Step.Retry:
                        return (Step.Retry,
                            OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, body.Code, body.What),
                            TpccOutcome.Conflict);
                    default:
                        return (Step.Failed, Anomaly(input.Type, body.Code!, body.What!), TpccOutcome.Anomaly);
                }
            }

            // Only a writing commit can be lost in flight; a snapshot that fails to close changed nothing.
            commitSubmitted = writes;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            double commitMs = Lap(ref ts);

            switch (input.Type)
            {
                case TpccTransaction.NewOrder:
                    _ledger.NewOrder(input.Warehouse, input.District, undecided: false);
                    break;
                case TpccTransaction.Payment:
                    _ledger.Payment(input.Warehouse, input.AmountCents, undecided: false);
                    break;
                case TpccTransaction.Delivery:
                    _ledger.Delivery(input.Warehouse, body.Delivered);
                    break;
            }

            return (Step.Committed,
                new OperationResult(OperationKind.Write, OperationStatus.Ok, null, beginMs, 0, workMs, commitMs, Exchanges: session.Exchanges + 1),
                TpccOutcome.Committed);
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex, commitSubmitted);

            if (status == OperationStatus.Indeterminate)
            {
                Interlocked.Increment(ref _indeterminateTxns);
                switch (input.Type)
                {
                    case TpccTransaction.NewOrder:
                        _ledger.NewOrder(input.Warehouse, input.District, undecided: true);
                        break;
                    case TpccTransaction.Payment:
                        _ledger.Payment(input.Warehouse, input.AmountCents, undecided: true);
                        break;
                    case TpccTransaction.Delivery:
                        _ledger.DeliveryUndecided(input.Warehouse);
                        break;
                }
                return (Step.Failed,
                    OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex)), TpccOutcome.Undecided);
            }

            await RollbackAsync(tx, ct).ConfigureAwait(false);

            // The order id comes from a counter this transaction incremented itself, so an order that
            // already exists means two transactions were given the same value of d_next_o_id.
            if (input.Type == TpccTransaction.NewOrder && ErrorClassifier.IsDuplicateKey(ex))
                return (Step.Failed,
                    Anomaly(input.Type, "TPCC_ANOMALY_DUPLICATE_ORDER",
                        $"New-Order for district {input.Warehouse}/{input.District} met an existing key: {ErrorClassifier.MessageOf(ex)}"),
                    TpccOutcome.Anomaly);

            OperationResult failure = OperationResult.Failure(OperationKind.Write, status, code, ErrorClassifier.MessageOf(ex));
            return status == OperationStatus.Conflict
                ? (Step.Retry, failure, TpccOutcome.Conflict)
                : (Step.Failed, failure, TpccOutcome.Error);
        }
    }

    private static async Task RollbackAsync(CamusTransaction tx, CancellationToken ct)
    {
        try { await tx.RollbackAsync(ct).ConfigureAwait(false); }
        catch { /* abandoned; the server reaper resolves it */ }
    }

    private OperationResult Anomaly(TpccTransaction type, string code, string what)
    {
        if (Interlocked.Increment(ref _anomalyCount) <= MaxAnomaliesKept)
            _anomalies.Enqueue(new TpccAnomaly(type.ToString(), DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture), code, what));
        return OperationResult.Failure(OperationKind.Write, OperationStatus.InternalError, code, what);
    }

    // ---- New-Order (clause 2.4) ----

    private async Task<Body> NewOrderAsync(Session s, Input input)
    {
        long w = input.Warehouse, d = input.District;

        // The warehouse is read before the district is written, the order Payment takes them in.
        using (CamusCommand warehouse = s.Read("SELECT w_tax FROM warehouse WHERE w_id = @w"))
        {
            I(warehouse, "@w", w);
            if (await ScalarAsync(warehouse, s.Ct).ConfigureAwait(false) is null)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"warehouse {w} has no row");
        }

        using (CamusCommand next = s.Write("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = @w AND d_id = @d"))
        {
            I(next, "@w", w);
            I(next, "@d", d);
            int modified = await next.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
            if (modified != 1)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"the update of district {w}/{d} matched {modified} row(s)");
        }

        long orderId;
        using (CamusCommand district = s.Read("SELECT d_next_o_id, d_tax FROM district WHERE d_w_id = @w AND d_id = @d"))
        {
            I(district, "@w", w);
            I(district, "@d", d);
            using CamusDataReader reader = await district.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"district {w}/{d} has no row after its update");
            orderId = reader.GetInt64(0) - 1;
        }

        using (CamusCommand customer = s.Read(
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = @w AND c_d_id = @d AND c_id = @c"))
        {
            I(customer, "@w", w);
            I(customer, "@d", d);
            I(customer, "@c", input.Customer);
            if (await ScalarAsync(customer, s.Ct).ConfigureAwait(false) is null)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"customer {w}/{d}/{input.Customer} has no row");
        }

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bool allLocal = Array.TrueForAll(input.Lines, l => l.SupplyWarehouse == w);

        using (CamusCommand order = s.Write(
            $"INSERT INTO orders ({TpccSchema.OrdersColumns}) VALUES (@w, @d, @o, @c, @now, NULL, @cnt, @local)"))
        {
            I(order, "@w", w);
            I(order, "@d", d);
            I(order, "@o", orderId);
            I(order, "@c", input.Customer);
            I(order, "@now", now);
            I(order, "@cnt", input.Lines.Length);
            I(order, "@local", allLocal ? 1 : 0);
            await order.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
        }

        using (CamusCommand newOrder = s.Write($"INSERT INTO new_order ({TpccSchema.NewOrderColumns}) VALUES (@w, @d, @o)"))
        {
            I(newOrder, "@w", w);
            I(newOrder, "@d", d);
            I(newOrder, "@o", orderId);
            await newOrder.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
        }

        // Stock rows are taken in key order so two orders that share items cannot wait on each other in a
        // cycle. The unused item sorts last, after every real one.
        OrderLineInput[] byStock = [.. input.Lines.OrderBy(l => l.Item).ThenBy(l => l.SupplyWarehouse).ThenBy(l => l.Number)];
        decimal[] amounts = new decimal[input.Lines.Length];
        string[] distInfo = new string[input.Lines.Length];
        string distColumn = string.Create(CultureInfo.InvariantCulture, $"s_dist_{d:D2}");

        foreach (OrderLineInput line in byStock)
        {
            decimal price;
            using (CamusCommand item = s.Read("SELECT i_price, i_name, i_data FROM item WHERE i_id = @i"))
            {
                I(item, "@i", line.Item);
                using CamusDataReader reader = await item.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                {
                    return input.Rollback && line.Item > _scale.Items
                        ? new Body(Step.RolledBack)
                        : Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"item {line.Item} has no row");
                }
                price = reader.GetDecimal(0);
            }

            using (CamusCommand stock = s.Read(
                $"SELECT s_quantity, s_data, {distColumn} FROM stock WHERE s_w_id = @w AND s_i_id = @i"))
            {
                I(stock, "@w", line.SupplyWarehouse);
                I(stock, "@i", line.Item);
                using CamusDataReader reader = await stock.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                    return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"stock {line.SupplyWarehouse}/{line.Item} has no row");
                distInfo[line.Number - 1] = reader.GetString(2);
            }

            using (CamusCommand update = s.Write(
                "UPDATE stock SET s_quantity = CASE WHEN s_quantity >= @q + 10 THEN s_quantity - @q ELSE s_quantity - @q + 91 END, " +
                "s_ytd = s_ytd + @q, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + @r WHERE s_w_id = @w AND s_i_id = @i"))
            {
                I(update, "@q", line.Quantity);
                I(update, "@r", line.SupplyWarehouse == w ? 0 : 1);
                I(update, "@w", line.SupplyWarehouse);
                I(update, "@i", line.Item);
                int modified = await update.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
                if (modified != 1)
                    return Body.Anomaly("TPCC_ANOMALY_LOST_ROW",
                        $"the update of stock {line.SupplyWarehouse}/{line.Item} matched {modified} row(s) right after the row was read");
            }

            amounts[line.Number - 1] = line.Quantity * price;
        }

        StringBuilder sql = new($"INSERT INTO order_line ({TpccSchema.OrderLineColumns}) VALUES ");
        for (int i = 0; i < input.Lines.Length; i++)
        {
            if (i > 0)
                sql.Append(", ");
            sql.Append(CultureInfo.InvariantCulture, $"(@w, @d, @o, @n{i}, @i{i}, @s{i}, NULL, @q{i}, @a{i}, @x{i})");
        }
        using (CamusCommand lines = s.Write(sql.ToString()))
        {
            I(lines, "@w", w);
            I(lines, "@d", d);
            I(lines, "@o", orderId);
            for (int i = 0; i < input.Lines.Length; i++)
            {
                OrderLineInput line = input.Lines[i];
                string n = i.ToString(CultureInfo.InvariantCulture);
                I(lines, "@n" + n, line.Number);
                I(lines, "@i" + n, line.Item);
                I(lines, "@s" + n, line.SupplyWarehouse);
                I(lines, "@q" + n, line.Quantity);
                N(lines, "@a" + n, amounts[i]);
                lines.Parameters.Add("@x" + n, ColumnType.String, distInfo[i]);
            }
            int inserted = await lines.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
            if (inserted != input.Lines.Length)
                return Body.Anomaly("TPCC_ANOMALY_LOST_ROW",
                    $"the insert of {input.Lines.Length} order line(s) of {w}/{d}/{orderId} reported {inserted} row(s)");
        }

        return Body.Commit();
    }

    // ---- Payment (clause 2.5) ----

    private async Task<Body> PaymentAsync(Session s, Input input)
    {
        long w = input.Warehouse, d = input.District;
        decimal amount = input.AmountCents / 100m;

        using (CamusCommand warehouse = s.Write("UPDATE warehouse SET w_ytd = w_ytd + @a WHERE w_id = @w"))
        {
            N(warehouse, "@a", amount);
            I(warehouse, "@w", w);
            int modified = await warehouse.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
            if (modified != 1)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"the update of warehouse {w} matched {modified} row(s)");
        }

        string warehouseName;
        using (CamusCommand name = s.Read("SELECT w_name FROM warehouse WHERE w_id = @w"))
        {
            I(name, "@w", w);
            warehouseName = await ScalarAsync(name, s.Ct).ConfigureAwait(false) as string ?? "";
        }

        using (CamusCommand district = s.Write("UPDATE district SET d_ytd = d_ytd + @a WHERE d_w_id = @w AND d_id = @d"))
        {
            N(district, "@a", amount);
            I(district, "@w", w);
            I(district, "@d", d);
            int modified = await district.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
            if (modified != 1)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"the update of district {w}/{d} matched {modified} row(s)");
        }

        string districtName;
        using (CamusCommand name = s.Read("SELECT d_name FROM district WHERE d_w_id = @w AND d_id = @d"))
        {
            I(name, "@w", w);
            I(name, "@d", d);
            districtName = await ScalarAsync(name, s.Ct).ConfigureAwait(false) as string ?? "";
        }

        long cw = input.CustomerWarehouse, cd = input.CustomerDistrict;
        long customer = input.LastName is null
            ? input.Customer
            : await CustomerByNameAsync(s, cw, cd, input.LastName).ConfigureAwait(false);
        if (customer == 0)
            return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"no customer of {cw}/{cd} has the last name {input.LastName}");

        using (CamusCommand pay = s.Write(
            "UPDATE customer SET c_balance = c_balance - @a, c_ytd_payment = c_ytd_payment + @a, c_payment_cnt = c_payment_cnt + 1 " +
            "WHERE c_w_id = @w AND c_d_id = @d AND c_id = @c"))
        {
            N(pay, "@a", amount);
            I(pay, "@w", cw);
            I(pay, "@d", cd);
            I(pay, "@c", customer);
            int modified = await pay.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
            if (modified != 1)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"the update of customer {cw}/{cd}/{customer} matched {modified} row(s)");
        }

        string credit, data;
        using (CamusCommand read = s.Read("SELECT c_credit, c_data FROM customer WHERE c_w_id = @w AND c_d_id = @d AND c_id = @c"))
        {
            I(read, "@w", cw);
            I(read, "@d", cd);
            I(read, "@c", customer);
            using CamusDataReader reader = await read.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                return Body.Anomaly("TPCC_ANOMALY_LOST_ROW", $"customer {cw}/{cd}/{customer} has no row right after its update");
            credit = reader.GetString(0);
            data = reader.GetString(1);
        }

        if (credit == "BC")
        {
            string entry = string.Create(CultureInfo.InvariantCulture, $"{customer} {cd} {cw} {d} {w} {amount:0.00}|");
            string updated = entry + data;
            using CamusCommand note = s.Write("UPDATE customer SET c_data = @data WHERE c_w_id = @w AND c_d_id = @d AND c_id = @c");
            note.Parameters.Add("@data", ColumnType.String, updated.Length > 500 ? updated[..500] : updated);
            I(note, "@w", cw);
            I(note, "@d", cd);
            I(note, "@c", customer);
            await note.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
        }

        using (CamusCommand history = s.Write(
            $"INSERT INTO history ({TpccSchema.HistoryColumns}) VALUES (@h, @c, @cd, @cw, @d, @w, @now, @a, @data)"))
        {
            history.Parameters.Add("@h", ColumnType.String, input.HistoryId);
            I(history, "@c", customer);
            I(history, "@cd", cd);
            I(history, "@cw", cw);
            I(history, "@d", d);
            I(history, "@w", w);
            I(history, "@now", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            N(history, "@a", amount);
            history.Parameters.Add("@data", ColumnType.String, warehouseName + "    " + districtName);
            await history.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
        }

        return Body.Commit();
    }

    /// <summary>The customer in the middle of those with this last name, ordered by first name (clause
    /// 2.5.2.2); 0 when there is none.</summary>
    private static async Task<long> CustomerByNameAsync(Session s, long warehouse, long district, string lastName)
    {
        using CamusCommand byName = s.Read(
            "SELECT c_id FROM customer WHERE c_w_id = @w AND c_d_id = @d AND c_last = @l ORDER BY c_first");
        I(byName, "@w", warehouse);
        I(byName, "@d", district);
        byName.Parameters.Add("@l", ColumnType.String, lastName);

        List<long> ids = [];
        using CamusDataReader reader = await byName.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
        while (await reader.ReadAsync(s.Ct).ConfigureAwait(false))
            ids.Add(reader.GetInt64(0));
        return ids.Count == 0 ? 0 : ids[(ids.Count + 1) / 2 - 1];
    }

    // ---- Order-Status (clause 2.6) ----

    private async Task<Body> OrderStatusAsync(Session s, Input input)
    {
        long w = input.Warehouse, d = input.District;
        long customer = input.LastName is null
            ? input.Customer
            : await CustomerByNameAsync(s, w, d, input.LastName).ConfigureAwait(false);
        if (customer == 0)
            return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"no customer of {w}/{d} has the last name {input.LastName}");

        using (CamusCommand read = s.Read(
            "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = @w AND c_d_id = @d AND c_id = @c"))
        {
            I(read, "@w", w);
            I(read, "@d", d);
            I(read, "@c", customer);
            if (await ScalarAsync(read, s.Ct).ConfigureAwait(false) is null)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"customer {w}/{d}/{customer} has no row");
        }

        long orderId, lineCount;
        bool delivered;
        using (CamusCommand last = s.Read(
            "SELECT o_id, o_entry_d, o_carrier_id, o_ol_cnt FROM orders WHERE o_w_id = @w AND o_d_id = @d AND o_c_id = @c " +
            "ORDER BY o_id DESC LIMIT 1"))
        {
            I(last, "@w", w);
            I(last, "@d", d);
            I(last, "@c", customer);
            using CamusDataReader reader = await last.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"customer {w}/{d}/{customer} has no order; the load gives every customer one");
            orderId = reader.GetInt64(0);
            delivered = !reader.IsDBNull(2);
            lineCount = reader.GetInt64(3);
        }

        long lines = 0, deliveredLines = 0;
        using (CamusCommand read = s.Read(
            "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line " +
            "WHERE ol_w_id = @w AND ol_d_id = @d AND ol_o_id = @o"))
        {
            I(read, "@w", w);
            I(read, "@d", d);
            I(read, "@o", orderId);
            using CamusDataReader reader = await read.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
            while (await reader.ReadAsync(s.Ct).ConfigureAwait(false))
            {
                lines++;
                if (!reader.IsDBNull(4))
                    deliveredLines++;
            }
        }

        // An order and its lines are inserted by one transaction, and a delivery marks the order and all
        // its lines in one transaction, so one snapshot sees each of them whole.
        if (lines != lineCount)
            return Body.Anomaly("TPCC_ANOMALY_FRACTURED_ORDER",
                $"a snapshot shows order {w}/{d}/{orderId} with o_ol_cnt {lineCount} and {lines} order_line row(s)");
        if (deliveredLines != (delivered ? lines : 0))
            return Body.Anomaly("TPCC_ANOMALY_FRACTURED_DELIVERY",
                $"a snapshot shows order {w}/{d}/{orderId} with o_carrier_id {(delivered ? "set" : "null")} and " +
                $"{deliveredLines} of {lines} line(s) delivered");

        return Body.Commit();
    }

    // ---- Delivery (clause 2.7): the oldest undelivered order of one district ----

    private static async Task<Body> DeliveryAsync(Session s, Input input)
    {
        long w = input.Warehouse, d = input.District;
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        {
            long orderId;
            using (CamusCommand oldest = s.Read("SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = @w AND no_d_id = @d"))
            {
                I(oldest, "@w", w);
                I(oldest, "@d", d);
                // No undelivered order: the district is skipped, as specified.
                if (await ScalarAsync(oldest, s.Ct).ConfigureAwait(false) is not long found)
                    return Body.Commit(0);
                orderId = found;
            }

            using (CamusCommand take = s.Write("DELETE FROM new_order WHERE no_w_id = @w AND no_d_id = @d AND no_o_id = @o"))
            {
                I(take, "@w", w);
                I(take, "@d", d);
                I(take, "@o", orderId);
                // Another delivery took this order between the read and the delete. Start again.
                if (await take.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false) != 1)
                    return new Body(Step.Retry, "TPCC_DELIVERY_RACE", $"new_order {w}/{d}/{orderId} was taken by another delivery");
            }

            long customer, lineCount;
            using (CamusCommand order = s.Read("SELECT o_c_id, o_ol_cnt FROM orders WHERE o_w_id = @w AND o_d_id = @d AND o_id = @o"))
            {
                I(order, "@w", w);
                I(order, "@d", d);
                I(order, "@o", orderId);
                using CamusDataReader reader = await order.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                    return Body.Anomaly("TPCC_ANOMALY_FRACTURED_ORDER", $"new_order {w}/{d}/{orderId} exists and its orders row does not");
                customer = reader.GetInt64(0);
                lineCount = reader.GetInt64(1);
            }

            using (CamusCommand carrier = s.Write("UPDATE orders SET o_carrier_id = @carrier WHERE o_w_id = @w AND o_d_id = @d AND o_id = @o"))
            {
                I(carrier, "@carrier", input.Carrier);
                I(carrier, "@w", w);
                I(carrier, "@d", d);
                I(carrier, "@o", orderId);
                int modified = await carrier.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
                if (modified != 1)
                    return Body.Anomaly("TPCC_ANOMALY_LOST_ROW",
                        $"the update of order {w}/{d}/{orderId} matched {modified} row(s) right after the row was read");
            }

            using (CamusCommand lines = s.Write("UPDATE order_line SET ol_delivery_d = @now WHERE ol_w_id = @w AND ol_d_id = @d AND ol_o_id = @o"))
            {
                I(lines, "@now", now);
                I(lines, "@w", w);
                I(lines, "@d", d);
                I(lines, "@o", orderId);
                int modified = await lines.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
                if (modified != lineCount)
                    return Body.Anomaly("TPCC_ANOMALY_FRACTURED_ORDER",
                        $"order {w}/{d}/{orderId} has o_ol_cnt {lineCount} and the delivery updated {modified} order_line row(s)");
            }

            decimal total;
            using (CamusCommand sum = s.Read("SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = @w AND ol_d_id = @d AND ol_o_id = @o"))
            {
                I(sum, "@w", w);
                I(sum, "@d", d);
                I(sum, "@o", orderId);
                using CamusDataReader reader = await sum.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
                total = await reader.ReadAsync(s.Ct).ConfigureAwait(false) && !reader.IsDBNull(0) ? reader.GetDecimal(0) : 0m;
            }

            using (CamusCommand credit = s.Write(
                "UPDATE customer SET c_balance = c_balance + @a, c_delivery_cnt = c_delivery_cnt + 1 " +
                "WHERE c_w_id = @w AND c_d_id = @d AND c_id = @c"))
            {
                N(credit, "@a", total);
                I(credit, "@w", w);
                I(credit, "@d", d);
                I(credit, "@c", customer);
                int modified = await credit.ExecuteNonQueryAsync(s.Ct).ConfigureAwait(false);
                if (modified != 1)
                    return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"the update of customer {w}/{d}/{customer} matched {modified} row(s)");
            }

        }

        return Body.Commit(1);
    }

    // ---- Stock-Level (clause 2.8) ----

    private static async Task<Body> StockLevelAsync(Session s, Input input)
    {
        long w = input.Warehouse, d = input.District;

        long nextOrder;
        using (CamusCommand district = s.Read("SELECT d_next_o_id FROM district WHERE d_w_id = @w AND d_id = @d"))
        {
            I(district, "@w", w);
            I(district, "@d", d);
            if (await ScalarAsync(district, s.Ct).ConfigureAwait(false) is not long found)
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"district {w}/{d} has no row");
            nextOrder = found;
        }

        // COUNT(DISTINCT) is not supported, so the distinct items are counted through a derived table. The
        // columns are qualified: a join equality on bare names is not taken as a join key.
        using (CamusCommand low = s.Read(
            "SELECT COUNT(*) FROM (SELECT DISTINCT ol.ol_i_id FROM order_line ol JOIN stock st ON st.s_i_id = ol.ol_i_id " +
            "WHERE ol.ol_w_id = @w AND ol.ol_d_id = @d AND ol.ol_o_id >= @lo AND ol.ol_o_id < @hi " +
            "AND st.s_w_id = @w AND st.s_quantity < @t) low_stock"))
        {
            I(low, "@w", w);
            I(low, "@d", d);
            I(low, "@lo", nextOrder - 20);
            I(low, "@hi", nextOrder);
            I(low, "@t", input.Threshold);
            await ScalarAsync(low, s.Ct).ConfigureAwait(false);
        }

        // Not part of the specified transaction: the snapshot is also asked for the two sides of
        // consistency condition 1. A Payment moves both in one transaction, so they must agree here.
        decimal warehouseYtd, districtsYtd;
        using (CamusCommand ytd = s.Read("SELECT w_ytd FROM warehouse WHERE w_id = @w"))
        {
            I(ytd, "@w", w);
            using CamusDataReader reader = await ytd.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(s.Ct).ConfigureAwait(false))
                return Body.Anomaly("TPCC_ANOMALY_MISSING_ROW", $"warehouse {w} has no row");
            warehouseYtd = reader.GetDecimal(0);
        }
        using (CamusCommand sum = s.Read("SELECT SUM(d_ytd) FROM district WHERE d_w_id = @w"))
        {
            I(sum, "@w", w);
            using CamusDataReader reader = await sum.ExecuteReaderAsync(s.Ct).ConfigureAwait(false);
            districtsYtd = await reader.ReadAsync(s.Ct).ConfigureAwait(false) && !reader.IsDBNull(0) ? reader.GetDecimal(0) : 0m;
        }
        if (warehouseYtd != districtsYtd)
            return Body.Anomaly("TPCC_ANOMALY_FRACTURED_PAYMENT",
                string.Create(CultureInfo.InvariantCulture,
                    $"a snapshot shows warehouse {w} with w_ytd {warehouseYtd} and SUM(d_ytd) {districtsYtd}"));

        return Body.Commit();
    }

    // ---- helpers ----

    private static void I(CamusCommand cmd, string name, long value) => cmd.Parameters.Add(name, ColumnType.Integer64, value);

    private static void N(CamusCommand cmd, string name, decimal value) => cmd.Parameters.Add(name, ColumnType.Numeric, value);

    /// <summary>The first column of the first row; null when there is no row or the cell is NULL.</summary>
    private static async Task<object?> ScalarAsync(CamusCommand cmd, CancellationToken ct)
    {
        using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false) || reader.IsDBNull(0))
            return null;
        return reader.GetValue(0);
    }

    private static double Lap(ref long ts)
    {
        long now = Stopwatch.GetTimestamp();
        double ms = Stopwatch.GetElapsedTime(ts, now).TotalMilliseconds;
        ts = now;
        return ms;
    }

    private void RecordAttempts(int attempts)
    {
        if (attempts > 1)
        {
            Interlocked.Add(ref _retryAttempts, attempts - 1);
            Interlocked.Increment(ref _retriedTxns);
        }

        long observed = Interlocked.Read(ref _maxAttemptsUsed);
        while (attempts > observed)
        {
            long previous = Interlocked.CompareExchange(ref _maxAttemptsUsed, attempts, observed);
            if (previous == observed)
                break;
            observed = previous;
        }
    }
}
