
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config.Models;

/// <summary>
/// Opt-in observability settings surfaced through the <c>diagnostics:</c> YAML section. Everything is
/// off by default: with <see cref="Enabled"/> false the server registers no exporter, endpoint, or
/// background collector and emits nothing, so an unconfigured node pays no diagnostics cost. Exporting
/// is wired only for a standalone node (<c>!IsClusterMode</c>) with <see cref="Enabled"/> true — this
/// phase deliberately leaves cluster behavior unchanged. The Prometheus scrape endpoint exposes
/// operational metadata and must be protected or bound to a trusted interface when enabled.
/// </summary>
public sealed class DiagnosticsConfig
{
    internal static readonly HashSet<string> AllowedYamlKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "enabled",
        "prometheus_enabled",
        "prometheus_path",
        "otlp_endpoint",
        "trace_sample_ratio",
        "include_runtime_metrics",
    };

    /// <summary>Master switch. When false nothing below takes effect and no diagnostics are emitted.</summary>
    public bool Enabled { get; set; }

    /// <summary>Binds the Prometheus scrape endpoint on the existing HTTP listener when true.</summary>
    public bool PrometheusEnabled { get; set; }

    /// <summary>Path the Prometheus endpoint is mapped at; must begin with '/'.</summary>
    public string PrometheusPath { get; set; } = "/metrics";

    /// <summary>Optional OTLP collector endpoint. Its presence enables the OTLP push exporter.</summary>
    public string? OtlpEndpoint { get; set; }

    /// <summary>Head trace sampling ratio in [0, 1]. Default 1% keeps tracing overhead bounded.</summary>
    public double TraceSampleRatio { get; set; } = 0.01;

    /// <summary>Registers standard .NET runtime/process metrics (GC, thread-pool, CPU) alongside server metrics.</summary>
    public bool IncludeRuntimeMetrics { get; set; } = true;

    /// <summary>
    /// Validates the diagnostics fields. Called from <see cref="ConfigDefinition.Validate"/>. Fails fast
    /// on a sample ratio outside [0,1], a Prometheus path that is not rooted, or a malformed OTLP URL —
    /// a misconfigured exporter should stop startup, not silently emit nowhere.
    /// </summary>
    public void Validate()
    {
        if (TraceSampleRatio is < 0 or > 1 || double.IsNaN(TraceSampleRatio))
            throw InvalidConfig($"'diagnostics.trace_sample_ratio' must be in [0, 1], got {TraceSampleRatio}");

        if (string.IsNullOrWhiteSpace(PrometheusPath) || !PrometheusPath.StartsWith('/'))
            throw InvalidConfig($"'diagnostics.prometheus_path' must start with '/', got '{PrometheusPath}'");

        if (OtlpEndpoint is not null &&
            !Uri.TryCreate(OtlpEndpoint, UriKind.Absolute, out _))
            throw InvalidConfig($"'diagnostics.otlp_endpoint' must be an absolute URL, got '{OtlpEndpoint}'");
    }

    private static CamusDBException InvalidConfig(string message)
        => new(CamusDBErrorCodes.InvalidConfig, message);
}
