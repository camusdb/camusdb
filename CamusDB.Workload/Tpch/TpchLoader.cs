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
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Tpch;

/// <summary>What <c>tpch_meta</c> says about the load in a database.</summary>
public sealed record TpchMeta(string Seed, string Scale, TpchFacts Facts);

/// <summary>
/// Creates the TPC-H tables and loads them.
///
/// <para>CamusDB has no bulk import, so rows go in as multi-row <c>INSERT ... VALUES</c> statements with
/// literal values, each its own autocommit transaction. The key space of every table is cut into chunks
/// that several connections take in turn. A chunk's rows depend only on its keys, so a retried or
/// re-run chunk sends the same statement, and a duplicate-key answer means that statement already
/// committed: the load is idempotent and can be resumed.</para>
///
/// <para>The row in <c>tpch_meta</c> is written last and marks the load complete. It carries the seed,
/// the scale and the generator's <see cref="TpchFacts"/>.</para>
/// </summary>
public static class TpchLoader
{
    /// <summary>Keys per chunk. An orders chunk also carries its lines (four on average, seven at most), so
    /// the largest statement is 1,400 line rows: 4,200 mutations with the index, under the 20,000 cap.</summary>
    private const int OrdersPerChunk = 200;
    private const int PartsPerChunk = 250;
    private const int RowsPerChunk = 1_000;

    private static readonly TimeSpan RetryBudget = TimeSpan.FromMinutes(5);

    public static async Task<TpchFacts> LoadAsync(
        Func<Task<CamusConnection>> open, TpchGenerator generator, ulong seed, int connections, Action<string> log,
        CancellationToken ct)
    {
        connections = Math.Clamp(connections, 1, 64);
        string seedText = seed.ToString(CultureInfo.InvariantCulture);
        string scaleText = TpchGenerator.ScaleText(generator.Scale);

        await using CamusConnection setup = await open().ConfigureAwait(false);

        foreach ((string sql, string what) in TpchSchema.Ddl)
            await Dataset.EnsureDdlAsync(setup, sql, what, ct).ConfigureAwait(false);

        TpchMeta? existing = await ReadMetaAsync(setup, ct).ConfigureAwait(false);
        if (existing is not null)
        {
            if (existing.Seed != seedText || existing.Scale != scaleText)
                throw new InvalidOperationException(
                    $"the database already holds TPC-H data for seed {existing.Seed} and scale {existing.Scale}; " +
                    $"seed {seedText} and scale {scaleText} need a fresh database");

            log($"TPC-H data for seed {seedText} and scale {scaleText} is already loaded.");
            return existing.Facts;
        }

        Stopwatch elapsed = Stopwatch.StartNew();
        TpchFacts facts = new();

        StringBuilder small = new();
        long regions = 0;
        foreach (TpchRegion r in generator.RegionRows())
        {
            Begin(small, "region", TpchSchema.RegionColumns, regions++);
            small.Append('(').Append(r.Key).Append(", ").Append(Quote(r.Name)).Append(", ").Append(Quote(r.Comment)).Append(')');
        }
        await ExecuteAsync(setup, small.ToString(), ct).ConfigureAwait(false);
        facts.AddRows("region", regions);

        small.Clear();
        long nations = 0;
        foreach (TpchNation n in generator.NationRows())
        {
            Begin(small, "nation", TpchSchema.NationColumns, nations++);
            small.Append('(').Append(n.Key).Append(", ").Append(Quote(n.Name)).Append(", ").Append(n.RegionKey).Append(", ")
                .Append(Quote(n.Comment)).Append(')');
        }
        await ExecuteAsync(setup, small.ToString(), ct).ConfigureAwait(false);
        facts.AddRows("nation", nations);

        CamusConnection[] pool = new CamusConnection[connections];
        try
        {
            for (int i = 0; i < pool.Length; i++)
                pool[i] = await open().ConfigureAwait(false);

            await RunPhaseAsync(pool, "supplier", generator.Suppliers, RowsPerChunk, facts, log,
                (from, to, into) => SupplierChunk(generator, from, to, into), ct).ConfigureAwait(false);
            await RunPhaseAsync(pool, "part and partsupp", generator.Parts, PartsPerChunk, facts, log,
                (from, to, into) => PartChunk(generator, from, to, into), ct).ConfigureAwait(false);
            await RunPhaseAsync(pool, "customer", generator.Customers, RowsPerChunk, facts, log,
                (from, to, into) => CustomerChunk(generator, from, to, into), ct).ConfigureAwait(false);
            await RunPhaseAsync(pool, "orders and lineitem", generator.Orders, OrdersPerChunk, facts, log,
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

        foreach (string table in TpchSchema.Tables)
        {
            long found = await CountAsync(setup, table, ct).ConfigureAwait(false);
            long expected = facts.Counts.GetValueOrDefault(table);
            if (found != expected)
                throw new InvalidOperationException(
                    $"table {table} holds {found} row(s) after the load, the generator produced {expected}; " +
                    "the database held other rows or the load lost some (use a fresh database)");
        }

        await ExecuteAsync(
            setup,
            $"INSERT INTO {TpchSchema.MetaTable} (id, seed, scale, facts) VALUES (1, {Quote(seedText)}, {Quote(scaleText)}, " +
            $"{Quote(facts.ToJson())})",
            ct).ConfigureAwait(false);

        long total = facts.Counts.Values.Sum();
        log($"Loaded {total:N0} TPC-H row(s) in {elapsed.Elapsed.TotalSeconds:F0}s " +
            $"({total / Math.Max(1, elapsed.Elapsed.TotalSeconds):N0} rows/s over {connections} connection(s)).");

        // Statistics decide the join order of every query. A failure costs plan quality, not the load.
        foreach (string table in TpchSchema.Tables)
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

        return facts;
    }

    /// <summary>The load marker, or null when the database holds no finished load.</summary>
    public static async Task<TpchMeta?> ReadMetaAsync(CamusConnection conn, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand($"SELECT seed, scale, facts FROM {TpchSchema.MetaTable} WHERE id = 1");
                using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                    return null;
                return new TpchMeta(reader.GetString(0), reader.GetString(1), TpchFacts.FromJson(reader.GetString(2)));
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

    public static async Task<long> CountAsync(CamusConnection conn, string table, CancellationToken ct)
    {
        Stopwatch waited = Stopwatch.StartNew();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand($"SELECT COUNT(*) FROM {table}");
                using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                return await reader.ReadAsync(ct).ConfigureAwait(false) ? reader.GetInt64(0) : 0;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                if (!(IsRetryable(ex) || Dataset.IsMissingTable(ex)) || waited.Elapsed > RetryBudget)
                    throw;
                await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    private delegate void ChunkBuilder(long fromIndex, long toIndex, ChunkOutput into);

    /// <summary>The statements of one chunk and what the generator knows about its rows.</summary>
    private sealed class ChunkOutput
    {
        public List<string> Statements { get; } = new(2);
        public TpchFacts Facts { get; } = new();
    }

    private static async Task RunPhaseAsync(
        CamusConnection[] pool, string what, long keys, int perChunk, TpchFacts facts, Action<string> log, ChunkBuilder build,
        CancellationToken ct)
    {
        long chunks = (keys + perChunk - 1) / perChunk;
        long next = -1;
        Stopwatch elapsed = Stopwatch.StartNew();
        log($"Loading {what}: {keys:N0} key(s) in {chunks:N0} chunk(s)...");

        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<TpchFacts>[] workers = new Task<TpchFacts>[pool.Length];
        for (int w = 0; w < workers.Length; w++)
        {
            CamusConnection conn = pool[w];
            workers[w] = Task.Run(async () =>
            {
                TpchFacts mine = new();
                try
                {
                    long chunk;
                    while ((chunk = Interlocked.Increment(ref next)) < chunks)
                    {
                        long from = chunk * perChunk + 1;
                        long to = Math.Min(keys, from + perChunk - 1);
                        ChunkOutput output = new();
                        build(from, to, output);
                        foreach (string statement in output.Statements)
                            await ExecuteAsync(conn, statement, linked.Token).ConfigureAwait(false);
                        mine.Merge(output.Facts);
                    }
                }
                catch
                {
                    // One connection giving up ends the phase: the others would only load a dataset the
                    // run then refuses.
                    linked.Cancel();
                    throw;
                }
                return mine;
            }, linked.Token);
        }

        try
        {
            await Task.WhenAll(workers).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Rethrow the failure that caused the cancellation, not the cancellation.
            Exception? cause = workers.Where(t => t.IsFaulted).Select(t => t.Exception!.GetBaseException()).FirstOrDefault();
            if (cause is not null)
                throw new InvalidOperationException($"loading {what} failed: {cause.GetType().Name}: {cause.Message}", cause);
            throw;
        }

        foreach (Task<TpchFacts> worker in workers)
            facts.Merge(worker.Result);
        log($"  {what} done in {elapsed.Elapsed.TotalSeconds:F0}s.");
    }

    private static void SupplierChunk(TpchGenerator g, long from, long to, ChunkOutput into)
    {
        StringBuilder sql = new();
        for (long key = from; key <= to; key++)
        {
            TpchSupplier s = g.Supplier(key);
            Begin(sql, "supplier", TpchSchema.SupplierColumns, key - from);
            sql.Append('(').Append(s.Key).Append(", ").Append(Quote(s.Name)).Append(", ").Append(Quote(s.Address)).Append(", ")
                .Append(s.NationKey).Append(", ").Append(Quote(s.Phone)).Append(", ").Append(Money(s.AcctBalCents)).Append(", ")
                .Append(Quote(s.Comment)).Append(')');
        }
        into.Statements.Add(sql.ToString());
        into.Facts.AddRows("supplier", to - from + 1);
    }

    private static void PartChunk(TpchGenerator g, long from, long to, ChunkOutput into)
    {
        StringBuilder parts = new();
        StringBuilder partSupps = new();
        long partSuppRows = 0;
        for (long key = from; key <= to; key++)
        {
            TpchPart p = g.Part(key);
            Begin(parts, "part", TpchSchema.PartColumns, key - from);
            parts.Append('(').Append(p.Key).Append(", ").Append(Quote(p.Name)).Append(", ").Append(Quote(p.Mfgr)).Append(", ")
                .Append(Quote(p.Brand)).Append(", ").Append(Quote(p.Type)).Append(", ").Append(p.Size).Append(", ")
                .Append(Quote(p.Container)).Append(", ").Append(Money(p.RetailCents)).Append(", ").Append(Quote(p.Comment)).Append(')');

            for (int position = 0; position < 4; position++)
            {
                TpchPartSupp ps = g.PartSupp(key, position);
                Begin(partSupps, "partsupp", TpchSchema.PartSuppColumns, partSuppRows++);
                partSupps.Append('(').Append(ps.PartKey).Append(", ").Append(ps.SuppKey).Append(", ").Append(ps.AvailQty).Append(", ")
                    .Append(Money(ps.SupplyCostCents)).Append(", ").Append(Quote(ps.Comment)).Append(')');
            }
        }
        into.Statements.Add(parts.ToString());
        into.Statements.Add(partSupps.ToString());
        into.Facts.AddRows("part", to - from + 1);
        into.Facts.AddRows("partsupp", partSuppRows);
    }

    private static void CustomerChunk(TpchGenerator g, long from, long to, ChunkOutput into)
    {
        StringBuilder sql = new();
        for (long key = from; key <= to; key++)
        {
            TpchCustomer c = g.Customer(key);
            Begin(sql, "customer", TpchSchema.CustomerColumns, key - from);
            sql.Append('(').Append(c.Key).Append(", ").Append(Quote(c.Name)).Append(", ").Append(Quote(c.Address)).Append(", ")
                .Append(c.NationKey).Append(", ").Append(Quote(c.Phone)).Append(", ").Append(Money(c.AcctBalCents)).Append(", ")
                .Append(Quote(c.Segment)).Append(", ").Append(Quote(c.Comment)).Append(')');
        }
        into.Statements.Add(sql.ToString());
        into.Facts.AddRows("customer", to - from + 1);
    }

    private static void OrderChunk(TpchGenerator g, long from, long to, ChunkOutput into)
    {
        StringBuilder orders = new();
        StringBuilder lines = new();
        long lineRows = 0;
        for (long index = from; index <= to; index++)
        {
            TpchOrder o = g.Order(index);
            Begin(orders, "orders", TpchSchema.OrdersColumns, index - from);
            orders.Append('(').Append(o.Key).Append(", ").Append(o.CustKey).Append(", '").Append(o.Status).Append("', ")
                .Append(Money(o.TotalCents)).Append(", ").Append(Date(o.OrderDate)).Append(", ").Append(Quote(o.Priority)).Append(", ")
                .Append(Quote(o.Clerk)).Append(", ").Append(o.ShipPriority).Append(", ").Append(Quote(o.Comment)).Append(')');

            foreach (TpchLineItem l in o.Lines)
            {
                Begin(lines, "lineitem", TpchSchema.LineItemColumns, lineRows++);
                lines.Append('(').Append(l.OrderKey).Append(", ").Append(l.PartKey).Append(", ").Append(l.SuppKey).Append(", ")
                    .Append(l.LineNumber).Append(", NUMERIC '").Append(l.Quantity).Append("', ").Append(Money(l.ExtendedCents)).Append(", ")
                    .Append(Money(l.DiscountPct)).Append(", ").Append(Money(l.TaxPct)).Append(", '").Append(l.ReturnFlag).Append("', '")
                    .Append(l.LineStatus).Append("', ").Append(Date(l.ShipDate)).Append(", ").Append(Date(l.CommitDate)).Append(", ")
                    .Append(Date(l.ReceiptDate)).Append(", ").Append(Quote(l.ShipInstruct)).Append(", ").Append(Quote(l.ShipMode)).Append(", ")
                    .Append(Quote(l.Comment)).Append(')');
                into.Facts.Add(l);
            }
        }
        into.Statements.Add(orders.ToString());
        into.Statements.Add(lines.ToString());
        into.Facts.AddRows("orders", to - from + 1);
        into.Facts.AddRows("lineitem", lineRows);
    }

    private static void Begin(StringBuilder sql, string table, string columns, long rowInStatement)
    {
        if (rowInStatement == 0)
            sql.Append("INSERT INTO ").Append(table).Append(" (").Append(columns).Append(") VALUES ");
        else
            sql.Append(", ");
    }

    /// <summary>A NUMERIC literal for an amount in hundredths: cents, or a percentage as a fraction.</summary>
    internal static string Money(long hundredths)
    {
        long magnitude = Math.Abs(hundredths);
        return string.Create(
            CultureInfo.InvariantCulture, $"NUMERIC '{(hundredths < 0 ? "-" : "")}{magnitude / 100}.{magnitude % 100:D2}'");
    }

    internal static string Date(int dayNumber)
        => string.Create(CultureInfo.InvariantCulture, $"DATE '{DateOnly.FromDayNumber(dayNumber):yyyy-MM-dd}'");

    internal static string Quote(string text) => "'" + text.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static bool IsRetryable(Exception ex)
        => ErrorClassifier.Classify(ex).Status is OperationStatus.Conflict or OperationStatus.Transient
           || ErrorClassifier.IsRetryableForIdempotentSetup(ex);

    /// <summary>
    /// Runs one INSERT, retrying a transient failure. A duplicate key means an earlier attempt of this
    /// same statement committed (the statement is atomic and its rows are a function of its keys), so it
    /// counts as done.
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
