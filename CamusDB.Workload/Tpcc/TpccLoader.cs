/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using System.Text;
using CamusDB.Client;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Tpch;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Tpcc;

/// <summary>What <c>tpcc_meta</c> says about the load in a database.</summary>
public sealed record TpccMeta(string Seed, int Warehouses, string Scale);

/// <summary>
/// Creates the TPC-C tables and loads the initial population.
///
/// <para>Rows go in as multi-row <c>INSERT ... VALUES</c> statements with literal values, each its own
/// autocommit transaction, over several connections. A chunk's rows depend only on its keys, so a
/// retried or re-run chunk sends the same statement and a duplicate-key answer means it already
/// committed: the load can be resumed. The row in <c>tpcc_meta</c> is written last and marks the load
/// complete.</para>
/// </summary>
public static class TpccLoader
{
    private const int ItemsPerChunk = 1_000;
    private const int StockPerChunk = 500;
    private const int CustomersPerChunk = 250;

    /// <summary>An order chunk carries its lines too: 1,500 at most, under the 20,000-mutation cap.</summary>
    private const int OrdersPerChunk = 100;

    private static readonly TimeSpan RetryBudget = TimeSpan.FromMinutes(5);

    public static async Task LoadAsync(
        Func<Task<CamusConnection>> open, TpccGenerator generator, ulong seed, int connections, Action<string> log,
        CancellationToken ct)
    {
        connections = Math.Clamp(connections, 1, 64);
        TpccScale scale = generator.Scale;
        string seedText = seed.ToString(CultureInfo.InvariantCulture);

        await using CamusConnection setup = await open().ConfigureAwait(false);

        foreach ((string sql, string what) in TpccSchema.Ddl)
            await Dataset.EnsureDdlAsync(setup, sql, what, ct).ConfigureAwait(false);

        TpccMeta? existing = await ReadMetaAsync(setup, ct).ConfigureAwait(false);
        if (existing is not null)
        {
            if (existing.Seed != seedText || existing.Warehouses != scale.Warehouses || existing.Scale != scale.FractionText)
                throw new InvalidOperationException(
                    $"the database already holds TPC-C data for seed {existing.Seed}, {existing.Warehouses} warehouse(s) and " +
                    $"scale {existing.Scale}; seed {seedText}, {scale.Warehouses} warehouse(s) and scale {scale.FractionText} " +
                    "need a fresh database");

            log($"TPC-C data for seed {seedText}, {scale.Warehouses} warehouse(s) and scale {scale.FractionText} is already loaded.");
            return;
        }

        Stopwatch elapsed = Stopwatch.StartNew();
        long warehouses = scale.Warehouses;
        long districts = warehouses * TpccScale.Districts;

        StringBuilder small = new();
        for (long w = 1; w <= warehouses; w++)
        {
            TpccWarehouse row = generator.Warehouse(w);
            Begin(small, "warehouse", TpccSchema.WarehouseColumns, w - 1);
            small.Append('(').Append(row.Id).Append(", ").Append(Q(row.Name)).Append(", ").Append(Q(row.Street1)).Append(", ")
                .Append(Q(row.Street2)).Append(", ").Append(Q(row.City)).Append(", ").Append(Q(row.State)).Append(", ")
                .Append(Q(row.Zip)).Append(", ").Append(Rate(row.TaxBasisPoints)).Append(", ").Append(Money(row.YtdCents)).Append(')');
        }
        await ExecuteAsync(setup, small.ToString(), ct).ConfigureAwait(false);

        for (long w = 1; w <= warehouses; w++)
        {
            small.Clear();
            for (long d = 1; d <= TpccScale.Districts; d++)
            {
                TpccDistrict row = generator.District(w, d);
                Begin(small, "district", TpccSchema.DistrictColumns, d - 1);
                small.Append('(').Append(row.WarehouseId).Append(", ").Append(row.Id).Append(", ").Append(Q(row.Name)).Append(", ")
                    .Append(Q(row.Street1)).Append(", ").Append(Q(row.Street2)).Append(", ").Append(Q(row.City)).Append(", ")
                    .Append(Q(row.State)).Append(", ").Append(Q(row.Zip)).Append(", ").Append(Rate(row.TaxBasisPoints)).Append(", ")
                    .Append(Money(row.YtdCents)).Append(", ").Append(row.NextOrderId).Append(')');
            }
            await ExecuteAsync(setup, small.ToString(), ct).ConfigureAwait(false);
        }

        long orderLines;
        CamusConnection[] pool = new CamusConnection[connections];
        try
        {
            for (int i = 0; i < pool.Length; i++)
                pool[i] = await open().ConfigureAwait(false);

            await RunPhaseAsync(pool, "item", scale.Items, ItemsPerChunk, log,
                (from, to, into) => ItemChunk(generator, from, to, into), ct).ConfigureAwait(false);
            await RunPhaseAsync(pool, "stock", warehouses * scale.Items, StockPerChunk, log,
                (from, to, into) => StockChunk(generator, from, to, into), ct).ConfigureAwait(false);
            await RunPhaseAsync(pool, "customer and history", districts * scale.Customers, CustomersPerChunk, log,
                (from, to, into) => CustomerChunk(generator, from, to, into), ct).ConfigureAwait(false);
            orderLines = await RunPhaseAsync(pool, "orders, order_line and new_order", districts * scale.Customers, OrdersPerChunk, log,
                (from, to, into) => OrderChunk(generator, from, to, into), ct).ConfigureAwait(false);
        }
        finally
        {
            foreach (CamusConnection c in pool)
            {
                if (c is not null)
                    await c.DisposeAsync().ConfigureAwait(false);
            }
        }

        Dictionary<string, long> expected = new()
        {
            ["item"] = scale.Items,
            ["warehouse"] = warehouses,
            ["district"] = districts,
            ["stock"] = warehouses * scale.Items,
            ["customer"] = districts * scale.Customers,
            ["history"] = districts * scale.Customers,
            ["orders"] = districts * scale.Customers,
            ["order_line"] = orderLines,
            ["new_order"] = districts * (scale.Customers - scale.DeliveredOrders),
        };
        foreach (string table in TpccSchema.Tables)
        {
            long found = await TpchLoader.CountAsync(setup, table, ct).ConfigureAwait(false);
            if (found != expected[table])
                throw new InvalidOperationException(
                    $"table {table} holds {found} row(s) after the load, the generator produced {expected[table]}; " +
                    "the database held other rows or the load lost some (use a fresh database)");
        }

        await ExecuteAsync(
            setup,
            $"INSERT INTO {TpccSchema.MetaTable} (id, seed, warehouses, scale) VALUES (1, {Q(seedText)}, {scale.Warehouses}, " +
            $"{Q(scale.FractionText)})",
            ct).ConfigureAwait(false);

        long total = expected.Values.Sum();
        log($"Loaded {total:N0} TPC-C row(s) in {elapsed.Elapsed.TotalSeconds:F0}s " +
            $"({total / Math.Max(1, elapsed.Elapsed.TotalSeconds):N0} rows/s over {connections} connection(s)).");

        // Statistics decide the access path of the range reads. A failure costs plan quality, not the load.
        foreach (string table in TpccSchema.Tables)
        {
            try
            {
                // ANALYZE is served by the query path, not the non-query one.
                using CamusCommand analyze = setup.CreateCamusCommand($"ANALYZE TABLE {table}");
                using CamusDataReader analyzed = await analyze.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await analyzed.ReadAsync(ct).ConfigureAwait(false))
                {
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                log($"ANALYZE TABLE {table} failed ({ex.GetType().Name}: {ex.Message}); the automatic statistics will have to do.");
            }
        }
    }

    /// <summary>The load marker, or null when the database holds no finished load.</summary>
    public static async Task<TpccMeta?> ReadMetaAsync(CamusConnection conn, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand($"SELECT seed, warehouses, scale FROM {TpccSchema.MetaTable} WHERE id = 1");
                using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                    return null;
                return new TpccMeta(reader.GetString(0), (int)reader.GetInt64(1), reader.GetString(2));
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                if (Dataset.IsMissingTable(ex))
                    return null;
                if (!IsRetryable(ex) || waited.Elapsed > RetryBudget)
                    throw;
                await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Fills the statements of the keys <c>from..to</c> (1-based, inclusive) and returns a count
    /// the phase adds up.</summary>
    private delegate long ChunkBuilder(long from, long to, List<string> into);

    private static async Task<long> RunPhaseAsync(
        CamusConnection[] pool, string what, long keys, int perChunk, Action<string> log, ChunkBuilder build, CancellationToken ct)
    {
        long chunks = (keys + perChunk - 1) / perChunk;
        long next = -1;
        long counted = 0;
        Stopwatch elapsed = Stopwatch.StartNew();
        log($"Loading {what}: {keys:N0} key(s) in {chunks:N0} chunk(s)...");

        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task[] workers = new Task[pool.Length];
        for (int w = 0; w < workers.Length; w++)
        {
            CamusConnection conn = pool[w];
            workers[w] = Task.Run(async () =>
            {
                try
                {
                    List<string> statements = new(3);
                    long chunk;
                    while ((chunk = Interlocked.Increment(ref next)) < chunks)
                    {
                        long from = chunk * perChunk + 1;
                        long to = Math.Min(keys, from + perChunk - 1);
                        statements.Clear();
                        long count = build(from, to, statements);
                        foreach (string statement in statements)
                            await ExecuteAsync(conn, statement, linked.Token).ConfigureAwait(false);
                        Interlocked.Add(ref counted, count);
                    }
                }
                catch
                {
                    // One connection giving up ends the phase: the others would only load a dataset the
                    // run then refuses.
                    linked.Cancel();
                    throw;
                }
            }, linked.Token);
        }

        try
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Exception? cause = workers.Where(t => t.IsFaulted).Select(t => t.Exception!.GetBaseException()).FirstOrDefault();
            if (cause is not null)
                throw new InvalidOperationException($"loading {what} failed: {cause.GetType().Name}: {cause.Message}", cause);
            throw;
        }

        log($"  {what} done in {elapsed.Elapsed.TotalSeconds:F0}s.");
        return Interlocked.Read(ref counted);
    }

    private static long ItemChunk(TpccGenerator g, long from, long to, List<string> into)
    {
        StringBuilder sql = new();
        for (long id = from; id <= to; id++)
        {
            TpccItem i = g.Item(id);
            Begin(sql, "item", TpccSchema.ItemColumns, id - from);
            sql.Append('(').Append(i.Id).Append(", ").Append(i.ImId).Append(", ").Append(Q(i.Name)).Append(", ")
                .Append(Money(i.PriceCents)).Append(", ").Append(Q(i.Data)).Append(')');
        }
        into.Add(sql.ToString());
        return to - from + 1;
    }

    private static long StockChunk(TpccGenerator g, long from, long to, List<string> into)
    {
        int items = g.Scale.Items;
        StringBuilder sql = new();
        for (long key = from; key <= to; key++)
        {
            TpccStock s = g.Stock((key - 1) / items + 1, (key - 1) % items + 1);
            Begin(sql, "stock", TpccSchema.StockColumns, key - from);
            sql.Append('(').Append(s.WarehouseId).Append(", ").Append(s.ItemId).Append(", ").Append(s.Quantity);
            foreach (string dist in s.Dist)
                sql.Append(", ").Append(Q(dist));
            sql.Append(", 0, 0, 0, ").Append(Q(s.Data)).Append(')');
        }
        into.Add(sql.ToString());
        return to - from + 1;
    }

    /// <summary>The warehouse, district and row number (customer or order id) of a 1-based position in the
    /// run of every district's rows.</summary>
    private static (long Warehouse, long District, long Id) Locate(TpccScale scale, long key)
    {
        long index = key - 1;
        long id = index % scale.Customers + 1;
        long district = index / scale.Customers;
        return (district / TpccScale.Districts + 1, district % TpccScale.Districts + 1, id);
    }

    private static long CustomerChunk(TpccGenerator g, long from, long to, List<string> into)
    {
        StringBuilder customers = new();
        StringBuilder history = new();
        for (long key = from; key <= to; key++)
        {
            (long w, long d, long id) = Locate(g.Scale, key);
            TpccCustomer c = g.Customer(w, d, id);

            Begin(customers, "customer", TpccSchema.CustomerColumns, key - from);
            customers.Append('(').Append(w).Append(", ").Append(d).Append(", ").Append(id).Append(", ").Append(Q(c.First))
                .Append(", 'OE', ").Append(Q(c.Last)).Append(", ").Append(Q(c.Street1)).Append(", ").Append(Q(c.Street2)).Append(", ")
                .Append(Q(c.City)).Append(", ").Append(Q(c.State)).Append(", ").Append(Q(c.Zip)).Append(", ").Append(Q(c.Phone))
                .Append(", ").Append(TpccGenerator.LoadEpochMs).Append(", ").Append(Q(c.Credit)).Append(", NUMERIC '50000.00', ")
                .Append(Rate(c.DiscountBasisPoints)).Append(", ").Append(Money(-TpccScale.InitialPaymentCents)).Append(", ")
                .Append(Money(TpccScale.InitialPaymentCents)).Append(", 1, 0, ").Append(Q(c.Data)).Append(')');

            Begin(history, "history", TpccSchema.HistoryColumns, key - from);
            history.Append('(').Append(Q(InitialHistoryId(w, d, id))).Append(", ").Append(id).Append(", ").Append(d).Append(", ")
                .Append(w).Append(", ").Append(d).Append(", ").Append(w).Append(", ").Append(TpccGenerator.LoadEpochMs).Append(", ")
                .Append(Money(TpccScale.InitialPaymentCents)).Append(", 'initial payment')");
        }
        into.Add(customers.ToString());
        into.Add(history.ToString());
        return to - from + 1;
    }

    public static string InitialHistoryId(long warehouse, long district, long customer)
        => string.Create(CultureInfo.InvariantCulture, $"L-{warehouse}-{district}-{customer}");

    /// <summary>Returns the number of order lines, which only the generator knows.</summary>
    private static long OrderChunk(TpccGenerator g, long from, long to, List<string> into)
    {
        StringBuilder orders = new();
        StringBuilder lines = new();
        StringBuilder newOrders = new();
        long lineRows = 0, newOrderRows = 0;

        for (long key = from; key <= to; key++)
        {
            (long w, long d, long id) = Locate(g.Scale, key);
            TpccOrder o = g.Order(w, d, id);

            Begin(orders, "orders", TpccSchema.OrdersColumns, key - from);
            orders.Append('(').Append(w).Append(", ").Append(d).Append(", ").Append(id).Append(", ").Append(o.CustomerId).Append(", ")
                .Append(TpccGenerator.LoadEpochMs).Append(", ");
            if (o.CarrierId is long carrier)
                orders.Append(carrier);
            else
                orders.Append("NULL");
            orders.Append(", ").Append(o.Lines.Length).Append(", 1)");

            foreach (TpccOrderLine l in o.Lines)
            {
                Begin(lines, "order_line", TpccSchema.OrderLineColumns, lineRows++);
                lines.Append('(').Append(w).Append(", ").Append(d).Append(", ").Append(id).Append(", ").Append(l.Number).Append(", ")
                    .Append(l.ItemId).Append(", ").Append(w).Append(", ");
                if (o.Delivered)
                    lines.Append(TpccGenerator.LoadEpochMs);
                else
                    lines.Append("NULL");
                lines.Append(", 5, ").Append(Money(l.AmountCents)).Append(", ").Append(Q(l.DistInfo)).Append(')');
            }

            if (!o.Delivered)
            {
                Begin(newOrders, "new_order", TpccSchema.NewOrderColumns, newOrderRows++);
                newOrders.Append('(').Append(w).Append(", ").Append(d).Append(", ").Append(id).Append(')');
            }
        }

        into.Add(orders.ToString());
        into.Add(lines.ToString());
        if (newOrderRows > 0)
            into.Add(newOrders.ToString());
        return lineRows;
    }

    private static void Begin(StringBuilder sql, string table, string columns, long rowInStatement)
    {
        if (rowInStatement == 0)
            sql.Append("INSERT INTO ").Append(table).Append(" (").Append(columns).Append(") VALUES ");
        else
            sql.Append(", ");
    }

    private static string Q(string text) => TpchLoader.Quote(text);

    private static string Money(long cents) => TpchLoader.Money(cents);

    /// <summary>A NUMERIC literal for a rate in basis points: 1234 is 0.1234.</summary>
    private static string Rate(int basisPoints)
        => string.Create(CultureInfo.InvariantCulture, $"NUMERIC '0.{basisPoints % 10_000:D4}'");

    private static bool IsRetryable(Exception ex)
        => ErrorClassifier.Classify(ex).Status is OperationStatus.Conflict or OperationStatus.Transient
           || ErrorClassifier.IsRetryableForIdempotentSetup(ex);

    /// <summary>
    /// Runs one INSERT, retrying a transient failure. A duplicate key means an earlier attempt of this
    /// same statement committed, so it counts as done.
    /// </summary>
    private static async Task ExecuteAsync(CamusConnection conn, string sql, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand(sql);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                if (ErrorClassifier.IsDuplicateKey(ex))
                    return;
                if (!(IsRetryable(ex) || Dataset.IsMissingTable(ex)) || waited.Elapsed > RetryBudget)
                    throw;
                await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }
}
