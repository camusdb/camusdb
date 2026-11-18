/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Workload.Util;

namespace CamusDB.Workload.Tpcc;

public readonly record struct TpccItem(long Id, long ImId, string Name, long PriceCents, string Data);

public readonly record struct TpccWarehouse(
    long Id, string Name, string Street1, string Street2, string City, string State, string Zip, int TaxBasisPoints, long YtdCents);

public readonly record struct TpccDistrict(
    long WarehouseId, long Id, string Name, string Street1, string Street2, string City, string State, string Zip,
    int TaxBasisPoints, long YtdCents, long NextOrderId);

public readonly record struct TpccStock(long WarehouseId, long ItemId, int Quantity, string[] Dist, string Data);

public readonly record struct TpccCustomer(
    long WarehouseId, long DistrictId, long Id, string First, string Last, string Street1, string Street2, string City,
    string State, string Zip, string Phone, string Credit, int DiscountBasisPoints, string Data);

public readonly record struct TpccOrderLine(int Number, long ItemId, long AmountCents, string DistInfo);

public sealed record TpccOrder(
    long WarehouseId, long DistrictId, long Id, long CustomerId, long? CarrierId, TpccOrderLine[] Lines)
{
    public bool Delivered => CarrierId is not null;
}

/// <summary>
/// The size of a TPC-C database: the number of warehouses, and a fraction that shrinks what the
/// specification fixes for each warehouse (100,000 items, 3,000 customers and 3,000 orders in each of
/// ten districts). The fraction is 1 for a database of the specified size; a smaller one exists so a
/// test can load in seconds. It is not a TPC-C scale.
/// </summary>
public sealed record TpccScale
{
    public const int Districts = 10;
    public const int MaxWarehouses = 1000;
    public const double MinFraction = 0.01;

    public int Warehouses { get; }
    public double Fraction { get; }

    /// <summary>Rows of <c>item</c>, and of <c>stock</c> in each warehouse.</summary>
    public int Items { get; }

    /// <summary>Customers in each district. The load also gives each one order, so this is the number of
    /// initial orders of a district.</summary>
    public int Customers { get; }

    /// <summary>The initial orders with an id above this are undelivered and have a <c>new_order</c> row:
    /// the last 30 percent, 900 of 3,000 at the specified size.</summary>
    public int DeliveredOrders { get; }

    public TpccScale(int warehouses, double fraction)
    {
        Warehouses = warehouses;
        Fraction = fraction;
        Items = Math.Max(100, (int)Math.Round(100_000 * fraction));
        Customers = Math.Max(30, (int)Math.Round(3_000 * fraction));
        DeliveredOrders = Customers - Customers * 3 / 10;
    }

    public static string? Error(int warehouses, double fraction)
    {
        if (warehouses < 1 || warehouses > MaxWarehouses)
            return $"the number of warehouses must be between 1 and {MaxWarehouses} (got {warehouses})";
        if (double.IsNaN(fraction) || fraction < MinFraction || fraction > 1)
            return $"the scale must be between {MinFraction.ToString(CultureInfo.InvariantCulture)} and 1 " +
                   $"(got {fraction.ToString(CultureInfo.InvariantCulture)})";
        return null;
    }

    public string FractionText => Fraction.ToString("0.####", CultureInfo.InvariantCulture);

    /// <summary>Order ids start here for the measured run: one past the initial orders.</summary>
    public long FirstNewOrderId => Customers + 1;

    /// <summary>What every customer starts with: one payment of 10.00 in <c>history</c>.</summary>
    public const long InitialPaymentCents = 1_000;

    /// <summary>The initial <c>d_ytd</c>: the initial payments of the district's customers (30,000.00 at the
    /// specified size), so the year-to-date conditions hold from the first row.</summary>
    public long DistrictYtdCents => Customers * InitialPaymentCents;

    public long WarehouseYtdCents => Districts * DistrictYtdCents;

    /// <summary>The constant A of NURand for a range of this many values: the specification's value where
    /// it gives one (1,000 names, 3,000 customers, 100,000 items), otherwise the largest power of two
    /// minus one that is at most a third of the range.</summary>
    public static int NuRandA(int range)
    {
        switch (range)
        {
            case 1_000: return 255;
            case 3_000: return 1023;
            case 100_000: return 8191;
        }
        int a = 1;
        while ((a << 1) + 1 <= range / 3)
            a = (a << 1) + 1;
        return a;
    }
}

/// <summary>
/// Generates the initial TPC-C population (clause 4.3.3 of the specification). Every row is a function
/// of the seed and its own key, so any chunk can be generated on any connection and a retried chunk is
/// the same statement.
/// </summary>
public sealed class TpccGenerator
{
    /// <summary>The timestamp of every loaded row: 2026-01-01T00:00:00Z in milliseconds.</summary>
    public const long LoadEpochMs = 1_767_225_600_000;

    private static readonly string[] Syllables =
        ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"];

    private const string Alphanumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private readonly ulong _seed;

    public TpccScale Scale { get; }

    public TpccGenerator(ulong seed, TpccScale scale)
    {
        _seed = seed;
        Scale = scale;
    }

    /// <summary>The customer last name for a number in 0..999: three syllables, one per digit.</summary>
    public static string LastName(int number)
        => Syllables[number / 100 % 10] + Syllables[number / 10 % 10] + Syllables[number % 10];

    /// <summary>How many distinct last-name numbers the customers of a district use.</summary>
    public int LastNames => Math.Min(1_000, Scale.Customers);

    /// <summary>NURand(A, x, y) of clause 2.1.6, with the run-time constant C.</summary>
    public static long NuRand(DeterministicRandom rng, int a, long c, long x, long y)
        => (((rng.NextLong(a + 1) | (x + rng.NextLong(y - x + 1))) + c) % (y - x + 1)) + x;

    public TpccItem Item(long id)
    {
        DeterministicRandom rng = Rng(1, id);
        return new TpccItem(
            id, 1 + rng.NextLong(10_000), Text(rng, 14, 24), 100 + rng.NextLong(9_901), OriginalData(rng));
    }

    public TpccWarehouse Warehouse(long id)
    {
        DeterministicRandom rng = Rng(2, id);
        return new TpccWarehouse(
            id, Text(rng, 6, 10), Text(rng, 10, 20), Text(rng, 10, 20), Text(rng, 10, 20), Letters(rng, 2), Zip(rng),
            (int)rng.NextLong(2_001), Scale.WarehouseYtdCents);
    }

    public TpccDistrict District(long warehouse, long id)
    {
        DeterministicRandom rng = Rng(3, warehouse, id);
        return new TpccDistrict(
            warehouse, id, Text(rng, 6, 10), Text(rng, 10, 20), Text(rng, 10, 20), Text(rng, 10, 20), Letters(rng, 2), Zip(rng),
            (int)rng.NextLong(2_001), Scale.DistrictYtdCents, Scale.FirstNewOrderId);
    }

    public TpccStock Stock(long warehouse, long item)
    {
        DeterministicRandom rng = Rng(4, warehouse, item);
        int quantity = 10 + (int)rng.NextLong(91);
        string[] dist = new string[TpccScale.Districts];
        for (int i = 0; i < dist.Length; i++)
            dist[i] = Text(rng, 24, 24);
        return new TpccStock(warehouse, item, quantity, dist, OriginalData(rng));
    }

    public TpccCustomer Customer(long warehouse, long district, long id)
    {
        DeterministicRandom rng = Rng(5, warehouse, district, id);

        // The first thousand customers of a district take the names in order, so every name exists; the
        // rest draw theirs with the non-uniform distribution the transactions use.
        int names = LastNames;
        int name = id <= names ? (int)(id - 1) : (int)NuRand(rng, TpccScale.NuRandA(names), 157, 0, names - 1);

        return new TpccCustomer(
            warehouse, district, id, Text(rng, 8, 16), LastName(name), Text(rng, 10, 20), Text(rng, 10, 20), Text(rng, 10, 20),
            Letters(rng, 2), Zip(rng), Digits(rng, 16), rng.NextLong(10) == 0 ? "BC" : "GC", (int)rng.NextLong(5_001),
            Text(rng, 300, 500));
    }

    /// <summary>
    /// The initial order with this id. The customers of a district each get one order: the order ids map
    /// onto the customer ids through a permutation that depends on the district.
    /// </summary>
    public TpccOrder Order(long warehouse, long district, long id)
    {
        DeterministicRandom rng = Rng(6, warehouse, district, id);
        bool delivered = id <= Scale.DeliveredOrders;
        int lineCount = 5 + (int)rng.NextLong(11);

        TpccOrderLine[] lines = new TpccOrderLine[lineCount];
        for (int i = 0; i < lineCount; i++)
            lines[i] = new TpccOrderLine(
                i + 1, 1 + rng.NextLong(Scale.Items), delivered ? 0 : 1 + rng.NextLong(999_999), Text(rng, 24, 24));

        return new TpccOrder(
            warehouse, district, id, CustomerOfOrder(warehouse, district, id), delivered ? 1 + rng.NextLong(10) : null, lines);
    }

    /// <summary>The customer of an initial order: a bijection from 1..Customers onto itself.</summary>
    public long CustomerOfOrder(long warehouse, long district, long orderId)
    {
        long n = Scale.Customers;
        long step = StepFor(n);
        long offset = (long)(Mix(_seed, 7, warehouse, district, 0) % (ulong)n);
        return ((orderId - 1) * step + offset) % n + 1;
    }

    /// <summary>A prime that does not divide <paramref name="n"/>, so multiplying by it permutes 0..n-1.</summary>
    private static long StepFor(long n)
    {
        foreach (long prime in (ReadOnlySpan<long>)[7919, 7907, 7901, 7883])
        {
            if (n % prime != 0)
                return prime;
        }
        return 1;
    }

    private DeterministicRandom Rng(int table, long a, long b = 0, long c = 0) => new(Mix(_seed, table, a, b, c));

    private static ulong Mix(ulong seed, int table, long a, long b, long c)
    {
        ulong z = seed;
        z = Stir(z ^ ((ulong)table * 0x9E3779B97F4A7C15UL));
        z = Stir(z ^ ((ulong)a * 0xC2B2AE3D27D4EB4FUL));
        z = Stir(z ^ ((ulong)b * 0x165667B19E3779F9UL));
        z = Stir(z ^ ((ulong)c * 0x27D4EB2F165667C5UL));
        return z;
    }

    private static ulong Stir(ulong z)
    {
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    /// <summary>A string of letters and digits whose length is uniform in [min, max].</summary>
    public static string Text(DeterministicRandom rng, int min, int max)
    {
        int length = min + (int)rng.NextLong(max - min + 1);
        return string.Create(length, rng, static (span, r) =>
        {
            for (int i = 0; i < span.Length; i++)
                span[i] = Alphanumeric[(int)r.NextLong(Alphanumeric.Length)];
        });
    }

    private static string Letters(DeterministicRandom rng, int length)
        => string.Create(length, rng, static (span, r) =>
        {
            for (int i = 0; i < span.Length; i++)
                span[i] = (char)('A' + (int)r.NextLong(26));
        });

    private static string Digits(DeterministicRandom rng, int length)
        => string.Create(length, rng, static (span, r) =>
        {
            for (int i = 0; i < span.Length; i++)
                span[i] = (char)('0' + (int)r.NextLong(10));
        });

    private static string Zip(DeterministicRandom rng) => Digits(rng, 4) + "11111";

    /// <summary>26 to 50 characters; one row in ten carries the word ORIGINAL at a random position.</summary>
    private static string OriginalData(DeterministicRandom rng)
    {
        string data = Text(rng, 26, 50);
        if (rng.NextLong(10) != 0)
            return data;
        int at = (int)rng.NextLong(data.Length - 8 + 1);
        return new StringBuilder(data).Remove(at, 8).Insert(at, "ORIGINAL").ToString();
    }
}
