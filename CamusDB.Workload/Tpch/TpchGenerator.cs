/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Workload.Util;

namespace CamusDB.Workload.Tpch;

public readonly record struct TpchRegion(long Key, string Name, string Comment);

public readonly record struct TpchNation(long Key, string Name, long RegionKey, string Comment);

public readonly record struct TpchSupplier(
    long Key, string Name, string Address, long NationKey, string Phone, long AcctBalCents, string Comment);

public readonly record struct TpchPart(
    long Key, string Name, string Mfgr, string Brand, string Type, int Size, string Container, long RetailCents, string Comment);

public readonly record struct TpchPartSupp(long PartKey, long SuppKey, int AvailQty, long SupplyCostCents, string Comment);

public readonly record struct TpchCustomer(
    long Key, string Name, string Address, long NationKey, string Phone, long AcctBalCents, string Segment, string Comment);

/// <summary>Dates are <see cref="DateOnly.DayNumber"/> values; percentages are whole hundredths.</summary>
public readonly record struct TpchLineItem(
    long OrderKey, int LineNumber, long PartKey, long SuppKey, int Quantity, long ExtendedCents, int DiscountPct, int TaxPct,
    char ReturnFlag, char LineStatus, int ShipDate, int CommitDate, int ReceiptDate, string ShipInstruct, string ShipMode,
    string Comment);

public sealed record TpchOrder(
    long Key, long CustKey, char Status, long TotalCents, int OrderDate, string Priority, string Clerk, int ShipPriority,
    string Comment, TpchLineItem[] Lines);

/// <summary>
/// A seeded generator for the eight TPC-H tables. It follows the row counts, key structure and value
/// distributions of the TPC-H specification (sparse order keys, four suppliers per part, customers whose
/// key is a multiple of three never order, 1 to 7 lines per order, the date and flag rules), but it is not
/// dbgen: the text and the random streams differ, so results are not comparable with published TPC-H
/// numbers or with the official answer sets.
///
/// <para>Every row is a pure function of (seed, table, key). A loader can therefore generate any key
/// range on any connection in any order, and a retried chunk produces the same rows.</para>
/// </summary>
public sealed class TpchGenerator
{
    public const double MinScale = 0.01;

    /// <summary>The date the specification treats as "today" when it sets return flags and line status.</summary>
    public static readonly int CurrentDate = new DateOnly(1995, 6, 17).DayNumber;

    private static readonly int StartDate = new DateOnly(1992, 1, 1).DayNumber;

    /// <summary>Last order date: the end of the data (1998-12-31) less the 151 days a line may still take.</summary>
    private static readonly int LastOrderDate = new DateOnly(1998, 12, 31).DayNumber - 151;

    public static readonly string[] Regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

    public static readonly (string Name, int Region)[] Nations =
    [
        ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4), ("ETHIOPIA", 0), ("FRANCE", 3),
        ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2), ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
        ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3), ("SAUDI ARABIA", 4), ("VIETNAM", 2),
        ("RUSSIA", 3), ("UNITED KINGDOM", 3), ("UNITED STATES", 1),
    ];

    private static readonly string[] Colors =
    [
        "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown",
        "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan",
        "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod",
        "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime",
        "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive",
        "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose",
        "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel",
        "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow",
    ];

    private static readonly string[] TypeSize = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
    private static readonly string[] TypeFinish = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"];
    private static readonly string[] TypeMetal = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"];
    private static readonly string[] ContainerSize = ["SM", "LG", "MED", "JUMBO", "WRAP"];
    private static readonly string[] ContainerKind = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"];
    private static readonly string[] Segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
    private static readonly string[] Priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
    private static readonly string[] Instructions = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
    private static readonly string[] Modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

    private static readonly string[] Words =
    [
        "special", "requests", "pending", "deposits", "packages", "accounts", "furiously", "carefully", "quickly", "express",
        "regular", "final", "ironic", "bold", "even", "unusual", "theodolites", "foxes", "pinto", "beans", "instructions",
        "platelets", "sleep", "nag", "haggle", "above", "the", "blithely", "slyly", "ideas",
    ];

    private const string AddressAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";

    private readonly ulong _seed;

    public TpchGenerator(ulong seed, double scale)
    {
        if (ScaleError(scale) is string error)
            throw new ArgumentException(error, nameof(scale));

        _seed = seed;
        Scale = scale;
        Suppliers = Count(10_000, scale);
        Parts = Count(200_000, scale);
        Customers = Count(150_000, scale);
        Orders = Count(1_500_000, scale);
        Clerks = Math.Max(1, Count(1_000, scale));
    }

    public double Scale { get; }
    public long Suppliers { get; }
    public long Parts { get; }
    public long Customers { get; }
    public long Orders { get; }
    public long Clerks { get; }
    public long PartSupps => Parts * 4;

    /// <summary>The scale as it is written to artifacts and compared between init and run.</summary>
    public static string ScaleText(double scale) => scale.ToString("0.####", CultureInfo.InvariantCulture);

    /// <summary>
    /// Why a scale cannot be generated, or null. The four suppliers of a part are spread by a fixed step,
    /// and for a few small supplier counts two of the four would land on the same supplier and break the
    /// partsupp primary key.
    /// </summary>
    public static string? ScaleError(double scale)
    {
        if (double.IsNaN(scale) || scale < MinScale)
            return $"the TPC-H scale must be at least {ScaleText(MinScale)} (got {scale.ToString(CultureInfo.InvariantCulture)}).";

        long suppliers = Count(10_000, scale);
        long parts = Count(200_000, scale);
        long maxStep = suppliers / 4 + (parts - 1) / suppliers;
        for (long step = suppliers / 4; step <= maxStep; step++)
        {
            if (step % suppliers == 0 || 2 * step % suppliers == 0 || 3 * step % suppliers == 0)
                return $"scale {ScaleText(scale)} gives {suppliers} suppliers, for which two suppliers of one part coincide; " +
                       "use another scale (0.01, 0.1 and 1 are sound).";
        }
        return null;
    }

    private static long Count(long perUnit, double scale) => (long)Math.Round(perUnit * scale);

    /// <summary>The sparse order key of the index-th order (1-based): the first 8 of every 32 keys.</summary>
    public static long OrderKey(long index) => (index - 1) / 8 * 32 + (index - 1) % 8 + 1;

    /// <summary>The retail price of a part in cents; a function of the key alone, as in the specification.</summary>
    public static long RetailCents(long partKey) => 90_000 + partKey / 10 % 20_001 + 100 * (partKey % 1_000);

    /// <summary>The supplier in position 0..3 of a part.</summary>
    public long SupplierOf(long partKey, int position)
        => (partKey + position * (Suppliers / 4 + (partKey - 1) / Suppliers)) % Suppliers + 1;

    public IEnumerable<TpchRegion> RegionRows()
    {
        for (int i = 0; i < Regions.Length; i++)
            yield return new TpchRegion(i, Regions[i], Text(Rng(1, i), 3, 6));
    }

    public IEnumerable<TpchNation> NationRows()
    {
        for (int i = 0; i < Nations.Length; i++)
            yield return new TpchNation(i, Nations[i].Name, Nations[i].Region, Text(Rng(2, i), 3, 6));
    }

    public TpchSupplier Supplier(long key)
    {
        DeterministicRandom rng = Rng(3, key);
        long nation = rng.NextLong(Nations.Length);
        string comment = Text(rng, 3, 6);

        // The specification marks 5 suppliers in 10,000 each way. That would leave none at the small
        // scales this workload runs at, so the share here is 1 in 50.
        long mark = rng.NextLong(50);
        if (mark == 0)
            comment = "Customer " + comment + " Complaints";
        else if (mark == 1)
            comment = "Customer " + comment + " Recommends";

        return new TpchSupplier(
            key, string.Create(CultureInfo.InvariantCulture, $"Supplier#{key:D9}"), Address(rng), nation, Phone(rng, nation),
            rng.NextLong(1_099_999) - 99_999, comment);
    }

    public TpchPart Part(long key)
    {
        DeterministicRandom rng = Rng(4, key);

        string[] name = new string[5];
        for (int i = 0; i < name.Length; i++)
        {
            string color;
            do
                color = Colors[rng.NextLong(Colors.Length)];
            while (Array.IndexOf(name, color, 0, i) >= 0);
            name[i] = color;
        }

        long mfgr = rng.NextLong(5) + 1;
        long brand = rng.NextLong(5) + 1;
        string type = $"{Pick(rng, TypeSize)} {Pick(rng, TypeFinish)} {Pick(rng, TypeMetal)}";
        int size = (int)rng.NextLong(50) + 1;
        string container = $"{Pick(rng, ContainerSize)} {Pick(rng, ContainerKind)}";

        return new TpchPart(
            key, string.Join(' ', name), $"Manufacturer#{mfgr}", $"Brand#{mfgr}{brand}", type, size, container,
            RetailCents(key), Text(rng, 1, 3));
    }

    public TpchPartSupp PartSupp(long partKey, int position)
    {
        DeterministicRandom rng = Rng(5, partKey * 4 + position);
        return new TpchPartSupp(
            partKey, SupplierOf(partKey, position), (int)rng.NextLong(9_999) + 1, rng.NextLong(99_901) + 100, Text(rng, 4, 8));
    }

    public TpchCustomer Customer(long key)
    {
        DeterministicRandom rng = Rng(6, key);
        long nation = rng.NextLong(Nations.Length);
        return new TpchCustomer(
            key, string.Create(CultureInfo.InvariantCulture, $"Customer#{key:D9}"), Address(rng), nation, Phone(rng, nation),
            rng.NextLong(1_099_999) - 99_999, Pick(rng, Segments), Text(rng, 4, 8));
    }

    /// <summary>The index-th order (1-based) with its lines.</summary>
    public TpchOrder Order(long index)
    {
        long key = OrderKey(index);
        DeterministicRandom rng = Rng(7, key);

        long customer;
        do
            customer = rng.NextLong(Customers) + 1;
        while (customer % 3 == 0);

        int orderDate = StartDate + (int)rng.NextLong(LastOrderDate - StartDate + 1);
        string priority = Pick(rng, Priorities);
        string clerk = string.Create(CultureInfo.InvariantCulture, $"Clerk#{rng.NextLong(Clerks) + 1:D9}");
        string comment = Text(rng, 4, 8);

        TpchLineItem[] lines = new TpchLineItem[rng.NextLong(7) + 1];
        long total = 0;
        int shipped = 0;
        for (int i = 0; i < lines.Length; i++)
        {
            long part = rng.NextLong(Parts) + 1;
            long supplier = SupplierOf(part, (int)rng.NextLong(4));
            int quantity = (int)rng.NextLong(50) + 1;
            long extended = quantity * RetailCents(part);
            int discount = (int)rng.NextLong(11);
            int tax = (int)rng.NextLong(9);
            int shipDate = orderDate + (int)rng.NextLong(121) + 1;
            int commitDate = orderDate + (int)rng.NextLong(61) + 30;
            int receiptDate = shipDate + (int)rng.NextLong(30) + 1;
            char returnFlag = receiptDate <= CurrentDate ? (rng.NextLong(2) == 0 ? 'R' : 'A') : 'N';
            char lineStatus = shipDate > CurrentDate ? 'O' : 'F';
            if (lineStatus == 'F')
                shipped++;

            lines[i] = new TpchLineItem(
                key, i + 1, part, supplier, quantity, extended, discount, tax, returnFlag, lineStatus, shipDate, commitDate,
                receiptDate, Pick(rng, Instructions), Pick(rng, Modes), Text(rng, 2, 4));

            // Rounded to the cent per line, half up.
            total += (extended * (100 + tax) * (100 - discount) + 5_000) / 10_000;
        }

        char status = shipped == lines.Length ? 'F' : shipped == 0 ? 'O' : 'P';
        return new TpchOrder(key, customer, status, total, orderDate, priority, clerk, 0, comment, lines);
    }

    private DeterministicRandom Rng(int table, long key)
        => new(unchecked(_seed ^ (0xD6E8FEB86659FD93UL * (ulong)table) ^ (0x9E3779B97F4A7C15UL * (ulong)(key + 1))));

    private static string Pick(DeterministicRandom rng, string[] values) => values[rng.NextLong(values.Length)];

    private static string Text(DeterministicRandom rng, int minWords, int maxWords)
    {
        int words = minWords + (int)rng.NextLong(maxWords - minWords + 1);
        string[] parts = new string[words];
        for (int i = 0; i < words; i++)
            parts[i] = Words[rng.NextLong(Words.Length)];
        return string.Join(' ', parts);
    }

    private static string Address(DeterministicRandom rng)
    {
        char[] buffer = new char[10 + rng.NextLong(16)];
        for (int i = 0; i < buffer.Length; i++)
            buffer[i] = AddressAlphabet[(int)rng.NextLong(AddressAlphabet.Length)];
        return new string(buffer).Trim();
    }

    /// <summary>The country code is the nation key plus 10, which Q22 selects on.</summary>
    private static string Phone(DeterministicRandom rng, long nation)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{nation + 10:D2}-{rng.NextLong(900) + 100:D3}-{rng.NextLong(900) + 100:D3}-{rng.NextLong(9000) + 1000:D4}");
}
