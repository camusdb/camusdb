/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text;
using System.Text.Json;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// The rule that produced one factor of a row estimate. Each value names one branch of the
/// cardinality estimator or of the cost annotation, so the plan-quality harness can attribute the
/// error of each plan node to the branch that made its estimate. A rule is a label on a
/// <see cref="EstimateFactor"/>, never a decision: adding a value here changes no estimate.
/// </summary>
public enum EstimateRule
{
    /// <summary>A node with an estimate that no rule explains; the harness reports its share.</summary>
    Unknown = 0,

    /// <summary>An equality on a heavy value: its share from the most-common-value list or the whole buckets it fills.</summary>
    EqualityHeavy,

    /// <summary>An equality on a non-heavy value of a column with heavy values: the residual share over the residual distinct values.</summary>
    EqualityResidual,

    /// <summary>An equality priced at <c>1 / NDV</c>.</summary>
    EqualityNdv,

    /// <summary>An equality priced at the density of its histogram bucket, with no distinct count.</summary>
    EqualityBucket,

    /// <summary>An equality on a value outside the histogram's domain: one row.</summary>
    EqualityOutOfDomain,

    /// <summary>An equality with a NULL constant: no row.</summary>
    EqualityNull,

    /// <summary>An equality with no statistics: the fallback selectivity.</summary>
    EqualityFallback,

    /// <summary>A <c>&lt;&gt;</c> comparison: the complement of the equality.</summary>
    NotEqual,

    /// <summary>A range read from the histogram, one- or two-sided, on a filter or on an index scan.</summary>
    RangeHistogram,

    /// <summary>A range comparison with no histogram or no constant: the fallback selectivity.</summary>
    RangeFallback,

    /// <summary>An <c>IN</c> list on a column with statistics: the sum of the per-value equalities, capped at 1.</summary>
    InList,

    /// <summary>An <c>IN</c> list with no statistics: the fallback selectivity for each value, capped at 1.</summary>
    InListFallback,

    /// <summary>An <c>IN</c> list through a unique index: at most one row for each value.</summary>
    InListUnique,

    /// <summary>Equalities on a composite-index prefix priced together at <c>1 / KeyNdv</c>.</summary>
    CompositeKeyNdv,

    /// <summary>A <c>LIKE</c> with a fixed prefix and one trailing wildcard: the histogram range of the prefix.</summary>
    LikePrefix,

    /// <summary>A <c>LIKE</c> with a prefix and more pattern after it: the prefix range scaled by the matching bounds inside it.</summary>
    LikePrefixScaled,

    /// <summary>A pattern priced by the share of the histogram's bucket bounds that match.</summary>
    LikeBounds,

    /// <summary>Several patterns on one column priced jointly over the bucket bounds.</summary>
    LikeJoint,

    /// <summary>A pattern without a wildcard: an equality.</summary>
    LikeEquality,

    /// <summary>A pattern with no histogram: the fallback selectivity.</summary>
    LikeFallback,

    /// <summary>A whole filter that no rule could price: the fallback selectivity.</summary>
    FilterFallback,

    /// <summary>An index equality on a composite prefix priced at <c>rows / KeyNdv</c>.</summary>
    IndexEqualityKeyNdv,

    /// <summary>An index equality on one column priced at <c>rows / NDV</c>, without the histogram.</summary>
    IndexEqualityNdv,

    /// <summary>An index equality on a composite prefix with no key count: the product of the column NDV inverses.</summary>
    IndexEqualityNdvProduct,

    /// <summary>The equality prefix of a composite index step, before its range column.</summary>
    IndexPrefixNdv,

    /// <summary>An index range on a column with no histogram: linear interpolation between the column's min and max.</summary>
    IndexRangeMinMax,

    /// <summary>An index range with no statistics: the fixed one-bound or two-bound constant.</summary>
    IndexRangeFixed,

    /// <summary>A scan with no filter: the table's row count.</summary>
    TableRows,

    /// <summary>A function in <c>FROM</c> with constant arguments: its exact count.</summary>
    FunctionRows,

    /// <summary>A small dimension leaf whose rows the planner read at plan time: the kept count.</summary>
    LeafLookup,

    /// <summary>A point lookup through a unique index: one row.</summary>
    IndexLookup,

    /// <summary>An equi-join with the keys of one side known at plan time: the mean share of those keys in the other column.</summary>
    JoinKnownKeys,

    /// <summary>An equi-join of two skewed columns: the sum over the common values of the two distributions.</summary>
    JoinSkewPair,

    /// <summary>An equi-join where one side's key covers a unique index: <c>1 / rows</c> of that side.</summary>
    JoinUniqueKey,

    /// <summary>An equi-join priced at the textbook <c>1 / max(NDV)</c>.</summary>
    JoinTextbook,

    /// <summary>An equi-join with no distinct count on either side: the fallback selectivity.</summary>
    JoinFallback,

    /// <summary>A join conjunct that is not a column equality: the fallback selectivity.</summary>
    JoinNonEqui,

    /// <summary>One column class factor from the skew-aware pair between the anchor and another member.</summary>
    ClassSkewPair,

    /// <summary>One column class pair priced at the textbook <c>1 / max(d)</c>, because the two distributions cannot be compared.</summary>
    ClassPairTextbook,

    /// <summary>One column class pair priced at the known keys' mean share.</summary>
    ClassKnownKeys,

    /// <summary>One column class factor <c>1 / d</c> for a member other than the one with the smallest count.</summary>
    ClassTextbook,

    /// <summary>A column class with no distinct count on any member: the fallback selectivity for each extra member.</summary>
    ClassFallback,

    /// <summary>A join step that adds a table with no conjunct to the tables before it: a cross product.</summary>
    JoinCross,

    /// <summary>The plan annotation of a join whose right key is unique: the keyed left rows.</summary>
    JoinUniqueRight,

    /// <summary>The plan annotation of a join whose left key is unique: the keyed right rows.</summary>
    JoinUniqueLeft,

    /// <summary>A grouped aggregate: the fixed group-by selectivity.</summary>
    Aggregate,

    /// <summary>An aggregate without <c>GROUP BY</c>: one row.</summary>
    AggregateSingle,

    /// <summary><c>DISTINCT</c>: the fixed distinct selectivity.</summary>
    Distinct,

    /// <summary><c>HAVING</c>: the fixed filter selectivity.</summary>
    HavingFilter,

    /// <summary>A semi-join or anti-join: the fixed filter selectivity.</summary>
    SemiJoin,

    /// <summary>A sort bounded by a <c>LIMIT</c>: the smaller of the bound and the input.</summary>
    SortBounded,

    /// <summary>A <c>LIMIT</c>: the smaller of the limit and the input.</summary>
    Limit,

    /// <summary>A node that passes its input rows through unchanged.</summary>
    PassThrough,

    /// <summary>The plan annotation of an index nested-loop join: one row for each outer row.</summary>
    IndexNestedLoop,
}

/// <summary>
/// Where a factor entered the estimate. The estimator clamps the product of each stage to
/// <c>[0, 1]</c> before it multiplies the stages, so a replica of the estimate must group the
/// factors the same way.
/// </summary>
public enum EstimateStage
{
    /// <summary>The comparisons and <c>IN</c> lists of a filter (<c>EstimatePredicateSelectivity</c>).</summary>
    Comparison,

    /// <summary>The pattern predicates of a filter (<c>PatternConjunctsSelectivity</c>).</summary>
    Pattern,

    /// <summary>The bounds of an index range scan (<c>EstimateRangeScanRows</c>).</summary>
    Index,

    /// <summary>An equi-join selectivity, in the join-order search or in the plan annotation.</summary>
    Join,

    /// <summary>A fixed per-node constant of the plan annotation.</summary>
    Node,
}

/// <summary>
/// How the factors of an <see cref="EstimateTrace"/> combine into its estimate. The harness
/// replica follows the same arithmetic, including the truncation to a whole number of rows.
/// </summary>
public enum EstimateFormula
{
    /// <summary><c>max(1, trunc(base × Π stage products))</c>, each stage product clamped to <c>[0, 1]</c>.</summary>
    Product,

    /// <summary><c>max(1, ceil(base × Π factors))</c>: the stats-free index range fallback.</summary>
    CeilingProduct,

    /// <summary>The estimate is given directly; the factors, if any, only explain it.</summary>
    Fixed,

    /// <summary>A join of the join-order search: the product of the leaf rows, the group selectivities and the class factors of its table subset.</summary>
    JoinSearch,

    /// <summary>A join annotation with a unique right key: <c>max(1, trunc(left × leftKeyed))</c>.</summary>
    JoinUniqueRight,

    /// <summary>A join annotation with a unique left key: <c>max(1, trunc(right × rightKeyed))</c>.</summary>
    JoinUniqueLeft,

    /// <summary>A join annotation: <c>max(1, trunc(left × leftKeyed × right × rightKeyed × selectivity))</c>.</summary>
    JoinProduct,
}

/// <summary>
/// One factor of a row estimate: the rule that produced it, the value the estimator multiplied
/// into the estimate (<see cref="Selectivity"/>), and the rule's inputs. <see cref="Raw"/> is the
/// rule's own value before a negation or the non-null share were applied, so a reader sees what
/// the statistics said and what the estimate did with it. The optional fields hold what the rule
/// read: the distinct count, the alternative branch's value for an equality (the histogram share
/// when the distinct count was used, the distinct-count share when the histogram was), the
/// textbook selectivity next to a skew-aware one, and the non-null share of a join.
/// </summary>
public sealed class EstimateFactor
{
    public required EstimateRule Rule { get; init; }

    public EstimateStage Stage { get; init; }

    public string? Column { get; init; }

    /// <summary>The value multiplied into the estimate, after a negation and the non-null share.</summary>
    public double Selectivity { get; init; }

    /// <summary>The rule's value before a negation and the non-null share.</summary>
    public double Raw { get; init; }

    /// <summary>True when the factor is <c>1 − rule</c> (a <c>NOT LIKE</c>).</summary>
    public bool Negated { get; init; }

    /// <summary>
    /// True when the estimator divided the base rows by <see cref="Ndv"/> instead of multiplying
    /// by <see cref="Raw"/>. The two differ in the last bit, and a truncation to whole rows can see
    /// that bit, so a reader who rebuilds the estimate from the trace must divide too.
    /// </summary>
    public bool Divided { get; init; }

    /// <summary>The value of the branch the estimator did not take, when one exists.</summary>
    public double? Alternative { get; init; }

    /// <summary>The textbook <c>1 / max(d)</c> next to a skew-aware or known-key selectivity.</summary>
    public double? Textbook { get; init; }

    /// <summary>The non-null share multiplied into a join selectivity.</summary>
    public double? Keyed { get; init; }

    public long? Ndv { get; init; }

    /// <summary>The rows the statistics describe (histogram total rows, or the table rows of a join side).</summary>
    public long? Rows { get; init; }

    public int? Buckets { get; init; }

    /// <summary>The values of an <c>IN</c> list or the known keys of a lookup.</summary>
    public int? Values { get; init; }
}

/// <summary>One leaf of a join-order subset estimate: its index in the join graph, its alias, the rows the search used, and the trace of that leaf estimate when it had one.</summary>
public sealed class EstimateLeafTrace
{
    public required int Leaf { get; init; }

    public required string Alias { get; init; }

    public required double Rows { get; init; }

    public EstimateTrace? Trace { get; init; }
}

/// <summary>One edge or pair group inside a join-order subset: the leaves it covers and its factor.</summary>
public sealed class EstimateGroupTrace
{
    public required ulong Mask { get; init; }

    public required EstimateFactor Factor { get; init; }
}

/// <summary>One member of a column equivalence class inside a subset, as the class factor reads it.</summary>
public sealed class EstimateClassMember
{
    public required int Leaf { get; init; }

    public long? Distinct { get; init; }

    public bool Skewed { get; init; }

    public double NonNull { get; init; }
}

/// <summary>One pair selectivity of a column class, between members <see cref="I"/> and <see cref="J"/> of the recorded member list.</summary>
public sealed class EstimateClassPair
{
    public required int I { get; init; }

    public required int J { get; init; }

    public required EstimateRule Rule { get; init; }

    /// <summary>The pair selectivity the class factor multiplies.</summary>
    public double Selectivity { get; init; }

    /// <summary>The textbook <c>1 / max(d)</c> of the pair, for a reader to see how far the skew-aware value moved.</summary>
    public double Textbook { get; init; }
}

/// <summary>
/// One column equivalence class over the members inside a subset. The class factor is the product
/// of the anchored pair selectivities when the pairs exist and a member is skewed, else
/// <c>1 / d</c> for every member but the one with the smallest count; the harness replica runs the
/// same rule over these fields.
/// </summary>
public sealed class EstimateClassTrace
{
    public required List<EstimateClassMember> Members { get; init; }

    /// <summary>The pairs among the members, or null when the class has no pair matrix.</summary>
    public List<EstimateClassPair>? Pairs { get; init; }
}

/// <summary>
/// How one plan node's row estimate was produced, for <c>EXPLAIN (ANALYZE)</c> and the plan-quality
/// harness. The planner fills it only when it was built to trace estimates (the <c>ANALYZE</c>
/// path); a normal statement allocates none of it. <see cref="Estimate"/> is the engine's own
/// number before any rounding for display, and the recorded factors and inputs are enough to
/// rebuild it.
///
/// <para>A leaf, filter or index node holds <see cref="BaseRows"/> and <see cref="Factors"/>. A
/// join of the join-order search holds the subset <see cref="Mask"/>, the leaves inside it with
/// their own traces, the groups whose leaves the subset covers and the column classes over its
/// members; the estimate is their product in that order. A join of the plan annotation holds the
/// two input row counts and the selectivity factor. <see cref="Rule"/> is the rule that dominates
/// the node: its most selective factor, or for a search join the most selective factor the step
/// added.</para>
/// </summary>
public sealed class EstimateTrace
{
    public EstimateFormula Formula { get; set; }

    public EstimateRule Rule { get; set; }

    /// <summary>The input rows of the formula: the table rows of a leaf, the input rows of a filter or a node constant, the left rows of an annotated join.</summary>
    public double BaseRows { get; set; }

    /// <summary>The right input rows of an annotated join.</summary>
    public double RightRows { get; set; }

    /// <summary>The non-null share of the left key of an annotated join; 1 when unknown.</summary>
    public double LeftKeyed { get; set; } = 1.0;

    /// <summary>The non-null share of the right key of an annotated join; 1 when unknown.</summary>
    public double RightKeyed { get; set; } = 1.0;

    /// <summary>A floor the estimate cannot go under (the left rows of a left outer join); 0 when none.</summary>
    public double Floor { get; set; }

    /// <summary>The engine's estimate, before display rounding.</summary>
    public double Estimate { get; set; }

    public List<EstimateFactor> Factors { get; } = [];

    /// <summary>The leaf mask of a join-order subset estimate.</summary>
    public ulong Mask { get; set; }

    /// <summary>The leaf a join-order step added to the tables before it; -1 when not a step.</summary>
    public int StepLeaf { get; set; } = -1;

    public List<EstimateLeafTrace>? Leaves { get; set; }

    public List<EstimateGroupTrace>? Groups { get; set; }

    public List<EstimateClassTrace>? Classes { get; set; }

    /// <summary>Records the formula and its result once the factors are in place, and names the dominant rule from the factors.</summary>
    public void Complete(EstimateFormula formula, double baseRows, double estimate)
    {
        Formula = formula;
        BaseRows = baseRows;
        Estimate = estimate;
        if (Rule == EstimateRule.Unknown)
            Rule = DominantRule(Factors);
    }

    /// <summary>The rule of the smallest factor, the one that decides the estimate; <see cref="EstimateRule.Unknown"/> for no factor.</summary>
    public static EstimateRule DominantRule(IReadOnlyList<EstimateFactor> factors)
    {
        EstimateRule rule = EstimateRule.Unknown;
        double smallest = double.PositiveInfinity;

        foreach (EstimateFactor factor in factors)
        {
            if (factor.Selectivity < smallest)
            {
                smallest = factor.Selectivity;
                rule = factor.Rule;
            }
        }

        return rule;
    }
}

/// <summary>
/// The names of the estimation rules, in the form the <c>estimate_trace</c> column of
/// <c>EXPLAIN (ANALYZE)</c> prints them: the kebab-case form of the enum member.
/// </summary>
public static class EstimateRuleNames
{
    private static readonly string[] Names = BuildNames();

    private static readonly Dictionary<string, EstimateRule> ByName = BuildIndex();

    private static string[] BuildNames()
    {
        string[] names = new string[Enum.GetValues<EstimateRule>().Length];
        foreach (EstimateRule rule in Enum.GetValues<EstimateRule>())
            names[(int)rule] = ToKebab(rule.ToString());
        return names;
    }

    private static Dictionary<string, EstimateRule> BuildIndex()
    {
        Dictionary<string, EstimateRule> index = new(Names.Length, StringComparer.Ordinal);
        foreach (EstimateRule rule in Enum.GetValues<EstimateRule>())
            index[Names[(int)rule]] = rule;
        return index;
    }

    private static string ToKebab(string pascal)
    {
        StringBuilder sb = new(pascal.Length + 4);
        for (int i = 0; i < pascal.Length; i++)
        {
            char c = pascal[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    sb.Append('-');
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    /// <summary>The kebab-case name of a rule, for example <c>equality-ndv</c>.</summary>
    public static string Name(EstimateRule rule) => Names[(int)rule];

    /// <summary>The rule a kebab-case name denotes, or false for an unknown name.</summary>
    public static bool TryParse(string name, out EstimateRule rule) => ByName.TryGetValue(name, out rule);

    /// <summary>Every rule name, in enum order.</summary>
    public static IReadOnlyList<string> All => Names;
}

/// <summary>
/// Renders an <see cref="EstimateTrace"/> as one JSON object for the <c>estimate_trace</c> column.
/// Hand-written over <see cref="Utf8JsonWriter"/>, so the engine needs no serializer metadata and
/// doubles round-trip exactly. Optional fields are left out when null, so a leaf trace stays short.
/// </summary>
public static class EstimateTraceJson
{
    public static string Render(EstimateTrace trace)
    {
        ArrayBufferWriter<byte> buffer = new(512);
        using (Utf8JsonWriter writer = new(buffer))
            WriteTrace(writer, trace);
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void WriteTrace(Utf8JsonWriter w, EstimateTrace t)
    {
        w.WriteStartObject();
        w.WriteString("formula", FormulaName(t.Formula));
        w.WriteString("rule", EstimateRuleNames.Name(t.Rule));
        w.WriteNumber("base", t.BaseRows);
        if (t.RightRows != 0)
            w.WriteNumber("right", t.RightRows);
        if (t.LeftKeyed != 1.0)
            w.WriteNumber("leftKeyed", t.LeftKeyed);
        if (t.RightKeyed != 1.0)
            w.WriteNumber("rightKeyed", t.RightKeyed);
        if (t.Floor != 0)
            w.WriteNumber("floor", t.Floor);
        w.WriteNumber("estimate", t.Estimate);

        if (t.Factors.Count > 0)
        {
            w.WritePropertyName("factors");
            w.WriteStartArray();
            foreach (EstimateFactor f in t.Factors)
                WriteFactor(w, f);
            w.WriteEndArray();
        }

        if (t.Formula == EstimateFormula.JoinSearch)
        {
            w.WriteNumber("mask", t.Mask);
            w.WriteNumber("step", t.StepLeaf);

            w.WritePropertyName("leaves");
            w.WriteStartArray();
            foreach (EstimateLeafTrace leaf in t.Leaves ?? [])
            {
                w.WriteStartObject();
                w.WriteNumber("leaf", leaf.Leaf);
                w.WriteString("alias", leaf.Alias);
                w.WriteNumber("rows", leaf.Rows);
                if (leaf.Trace is { } inner)
                {
                    w.WritePropertyName("trace");
                    WriteTrace(w, inner);
                }
                w.WriteEndObject();
            }
            w.WriteEndArray();

            w.WritePropertyName("groups");
            w.WriteStartArray();
            foreach (EstimateGroupTrace group in t.Groups ?? [])
            {
                w.WriteStartObject();
                w.WriteNumber("mask", group.Mask);
                w.WritePropertyName("factor");
                WriteFactor(w, group.Factor);
                w.WriteEndObject();
            }
            w.WriteEndArray();

            w.WritePropertyName("classes");
            w.WriteStartArray();
            foreach (EstimateClassTrace cls in t.Classes ?? [])
            {
                w.WriteStartObject();
                w.WritePropertyName("members");
                w.WriteStartArray();
                foreach (EstimateClassMember m in cls.Members)
                {
                    w.WriteStartObject();
                    w.WriteNumber("leaf", m.Leaf);
                    if (m.Distinct is { } d)
                        w.WriteNumber("distinct", d);
                    w.WriteBoolean("skewed", m.Skewed);
                    w.WriteNumber("nonNull", m.NonNull);
                    w.WriteEndObject();
                }
                w.WriteEndArray();

                if (cls.Pairs is { } pairs)
                {
                    w.WritePropertyName("pairs");
                    w.WriteStartArray();
                    foreach (EstimateClassPair p in pairs)
                    {
                        w.WriteStartObject();
                        w.WriteNumber("i", p.I);
                        w.WriteNumber("j", p.J);
                        w.WriteString("rule", EstimateRuleNames.Name(p.Rule));
                        w.WriteNumber("sel", p.Selectivity);
                        w.WriteNumber("textbook", p.Textbook);
                        w.WriteEndObject();
                    }
                    w.WriteEndArray();
                }
                w.WriteEndObject();
            }
            w.WriteEndArray();
        }

        w.WriteEndObject();
    }

    private static void WriteFactor(Utf8JsonWriter w, EstimateFactor f)
    {
        w.WriteStartObject();
        w.WriteString("rule", EstimateRuleNames.Name(f.Rule));
        w.WriteString("stage", StageName(f.Stage));
        if (f.Column is not null)
            w.WriteString("column", f.Column);
        w.WriteNumber("sel", f.Selectivity);
        w.WriteNumber("raw", f.Raw);
        if (f.Negated)
            w.WriteBoolean("negated", true);
        if (f.Divided)
            w.WriteBoolean("divided", true);
        if (f.Alternative is { } alt)
            w.WriteNumber("alternative", alt);
        if (f.Textbook is { } tb)
            w.WriteNumber("textbook", tb);
        if (f.Keyed is { } keyed)
            w.WriteNumber("keyed", keyed);
        if (f.Ndv is { } ndv)
            w.WriteNumber("ndv", ndv);
        if (f.Rows is { } rows)
            w.WriteNumber("rows", rows);
        if (f.Buckets is { } buckets)
            w.WriteNumber("buckets", buckets);
        if (f.Values is { } values)
            w.WriteNumber("values", values);
        w.WriteEndObject();
    }

    /// <summary>The JSON name of a formula, in kebab case.</summary>
    public static string FormulaName(EstimateFormula formula) => formula switch
    {
        EstimateFormula.Product => "product",
        EstimateFormula.CeilingProduct => "ceiling-product",
        EstimateFormula.Fixed => "fixed",
        EstimateFormula.JoinSearch => "join-search",
        EstimateFormula.JoinUniqueRight => "join-unique-right",
        EstimateFormula.JoinUniqueLeft => "join-unique-left",
        EstimateFormula.JoinProduct => "join-product",
        _ => "unknown",
    };

    /// <summary>The JSON name of a stage, in lower case.</summary>
    public static string StageName(EstimateStage stage) => stage switch
    {
        EstimateStage.Comparison => "comparison",
        EstimateStage.Pattern => "pattern",
        EstimateStage.Index => "index",
        EstimateStage.Join => "join",
        _ => "node",
    };
}
