/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

namespace CamusDB.Workload.Tpch;

/// <summary>The exact Q1 totals of one (return flag, line status) group.</summary>
public sealed class TpchQ1Group
{
    public decimal SumQty { get; set; }
    public decimal SumBasePrice { get; set; }
    public decimal SumDiscPrice { get; set; }
    public decimal SumCharge { get; set; }
    public decimal SumDiscount { get; set; }
    public long Count { get; set; }
}

/// <summary>
/// What the generator knows about the data it produced, computed while the rows are generated and
/// stored with the load: the row count of every table and the exact answers of Q1 and Q6. These are the
/// only expected values that do not come from the database itself, so they are what catches a load or a
/// scan that lost rows before the reference pass could record the loss as the truth.
/// </summary>
public sealed class TpchFacts
{
    /// <summary>Q1 counts lines shipped on or before this date (1998-12-01 less 90 days).</summary>
    public static readonly int Q1Cutoff = new DateOnly(1998, 9, 2).DayNumber;

    private static readonly int Q6From = new DateOnly(1994, 1, 1).DayNumber;
    private static readonly int Q6Until = new DateOnly(1995, 1, 1).DayNumber;

    public Dictionary<string, long> Counts { get; set; } = new();

    /// <summary>Keyed by return flag and line status, e.g. "A|F".</summary>
    public SortedDictionary<string, TpchQ1Group> Q1 { get; set; } = new(StringComparer.Ordinal);

    public decimal Q6Revenue { get; set; }
    public long Q6Rows { get; set; }

    public void AddRows(string table, long rows)
        => Counts[table] = Counts.GetValueOrDefault(table) + rows;

    public void Add(in TpchLineItem line)
    {
        decimal extended = line.ExtendedCents / 100m;
        decimal discount = line.DiscountPct / 100m;

        if (line.ShipDate <= Q1Cutoff)
        {
            string key = $"{line.ReturnFlag}|{line.LineStatus}";
            if (!Q1.TryGetValue(key, out TpchQ1Group? group))
                Q1[key] = group = new TpchQ1Group();

            decimal discounted = extended * (1 - discount);
            group.SumQty += line.Quantity;
            group.SumBasePrice += extended;
            group.SumDiscPrice += discounted;
            group.SumCharge += discounted * (1 + line.TaxPct / 100m);
            group.SumDiscount += discount;
            group.Count++;
        }

        if (line.ShipDate >= Q6From && line.ShipDate < Q6Until && line.DiscountPct is >= 5 and <= 7 && line.Quantity < 24)
        {
            Q6Revenue += extended * discount;
            Q6Rows++;
        }
    }

    public void Merge(TpchFacts other)
    {
        foreach ((string table, long rows) in other.Counts)
            AddRows(table, rows);

        foreach ((string key, TpchQ1Group theirs) in other.Q1)
        {
            if (!Q1.TryGetValue(key, out TpchQ1Group? mine))
                Q1[key] = mine = new TpchQ1Group();
            mine.SumQty += theirs.SumQty;
            mine.SumBasePrice += theirs.SumBasePrice;
            mine.SumDiscPrice += theirs.SumDiscPrice;
            mine.SumCharge += theirs.SumCharge;
            mine.SumDiscount += theirs.SumDiscount;
            mine.Count += theirs.Count;
        }

        Q6Revenue += other.Q6Revenue;
        Q6Rows += other.Q6Rows;
    }

    public string ToJson() => JsonSerializer.Serialize(this);

    public static TpchFacts FromJson(string json)
        => JsonSerializer.Deserialize<TpchFacts>(json) ?? throw new FormatException("the stored TPC-H facts are empty");
}
