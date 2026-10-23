/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;

namespace CamusDB.PlanQuality.Tests.Estimates;

/// <summary>
/// The <c>estimate_trace</c> column of <c>EXPLAIN (ANALYZE)</c>, as the harness reads it back: one
/// node's formula, its inputs and its factors, in the shape the engine's
/// <c>EstimateTraceJson</c> writes. Rules, stages and formulas stay strings here, so a rule the
/// engine adds later still loads and shows up by name in the reports.
/// </summary>
internal sealed class TraceNode
{
    public string Formula { get; set; } = "";

    public string Rule { get; set; } = "";

    public double Base { get; set; }

    public double Right { get; set; }

    public double LeftKeyed { get; set; } = 1.0;

    public double RightKeyed { get; set; } = 1.0;

    public double Floor { get; set; }

    public double Estimate { get; set; }

    public List<TraceFactor> Factors { get; set; } = [];

    public ulong Mask { get; set; }

    public int Step { get; set; } = -1;

    public List<TraceLeaf>? Leaves { get; set; }

    public List<TraceGroup>? Groups { get; set; }

    public List<TraceClass>? Classes { get; set; }

    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
    };

    public static TraceNode Parse(string json) =>
        JsonSerializer.Deserialize<TraceNode>(json, Options) ?? throw new InvalidOperationException("the estimate trace is empty");
}

/// <summary>One factor of a traced estimate; see the engine's <c>EstimateFactor</c>.</summary>
internal sealed class TraceFactor
{
    public string Rule { get; set; } = "";

    public string Stage { get; set; } = "";

    public string? Column { get; set; }

    public double Sel { get; set; }

    public double Raw { get; set; }

    public bool Negated { get; set; }

    public bool Divided { get; set; }

    public double? Alternative { get; set; }

    public double? Textbook { get; set; }

    public double? Keyed { get; set; }

    public long? Ndv { get; set; }

    public long? Rows { get; set; }

    public int? Buckets { get; set; }

    public int? Values { get; set; }
}

internal sealed class TraceLeaf
{
    public int Leaf { get; set; }

    public string Alias { get; set; } = "";

    public double Rows { get; set; }

    public TraceNode? Trace { get; set; }
}

internal sealed class TraceGroup
{
    public ulong Mask { get; set; }

    public TraceFactor Factor { get; set; } = new();
}

internal sealed class TraceClassMember
{
    public int Leaf { get; set; }

    public long? Distinct { get; set; }

    public bool Skewed { get; set; }

    public double NonNull { get; set; } = 1.0;
}

internal sealed class TraceClassPair
{
    public int I { get; set; }

    public int J { get; set; }

    public string Rule { get; set; } = "";

    public double Sel { get; set; }

    public double Textbook { get; set; }
}

internal sealed class TraceClass
{
    public List<TraceClassMember> Members { get; set; } = [];

    public List<TraceClassPair>? Pairs { get; set; }
}
