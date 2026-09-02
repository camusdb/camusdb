/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CamusDB.Client;
using CamusDB.Workload.Reporting;

namespace CamusDB.Workload.Cluster;

/// <summary>
/// Asks the cluster to describe itself, once per run, so the result bundle records what answered
/// rather than what was asked for.
///
/// <para>Three facts drive the whole thing. The <b>versions</b> come from each node's
/// <c>/v1/version</c>, because a package version read from a project file describes the repository,
/// not the image that ran. The <b>configuration</b> comes from each node's <c>SHOW VARIABLES</c>,
/// which reports the values the engine resolved rather than what a configuration file contains — and
/// it is node-local, so it must be asked of every node. The <b>placement</b> comes from
/// <c>SHOW RANGES</c>, which is the only way to tell a hot single partition from a distributed one.</para>
///
/// <para>Nothing here can fail a run. Every probe records its own error and the capture continues:
/// this is provenance for a measurement that already happened, and losing the measurement to a
/// refused metadata request would be the worse outcome by far.</para>
/// </summary>
public sealed class ClusterProbe
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly IReadOnlyList<NodeTarget> _nodes;
    private readonly string _database;
    private readonly TimeSpan _timeout;

    public ClusterProbe(IReadOnlyList<NodeTarget> nodes, string database, TimeSpan? timeout = null)
    {
        _nodes = nodes;
        _database = database;
        _timeout = timeout ?? TimeSpan.FromSeconds(20);
    }

    /// <summary>
    /// Captures every node's facts plus the placement of the workload's tables.
    ///
    /// <para><paramref name="rangeReader"/> is the run's own connection: range placement is a cluster
    /// fact, so one node's answer is enough and it should come from the endpoint the run used.
    /// <c>SHOW RANGES</c> is table-scoped, so it is asked once per workload table and each row is
    /// tagged with the table it describes — a multi-table run is exactly the case where placement
    /// matters most, and an untagged union of rows would not say which table sat where.</para>
    /// </summary>
    public async Task<ClusterFacts> CaptureAsync(
        CamusConnection? rangeReader, IReadOnlyList<string> tables, CancellationToken ct)
    {
        using HttpClient http = new() { Timeout = _timeout };

        List<NodeFacts> nodes = new();
        Dictionary<string, PlacementView> placements = new(StringComparer.Ordinal);
        foreach (NodeTarget node in _nodes)
        {
            nodes.Add(await CaptureNodeAsync(http, node, placements, ct).ConfigureAwait(false));
        }

        List<string> errors = new();
        List<IReadOnlyDictionary<string, string>> ranges = new();
        if (rangeReader is not null)
        {
            foreach (string table in tables)
            {
                try
                {
                    foreach (IReadOnlyDictionary<string, string> row in
                             await QueryAsync(rangeReader, $"SHOW RANGES FROM TABLE {table}", ct).ConfigureAwait(false))
                    {
                        Dictionary<string, string> tagged = new(row, StringComparer.OrdinalIgnoreCase) { ["table"] = table };
                        ranges.Add(tagged);
                    }
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    errors.Add($"SHOW RANGES FROM TABLE {table}: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }

        return new ClusterFacts(
            CapturedAtUtc: DateTime.UtcNow.ToString("O"),
            Nodes: nodes,
            Ranges: ranges,
            Errors: errors,
            DurabilityFingerprint: Fingerprint(nodes))
        {
            Partitions = MergePlacement(placements),
        };
    }

    /// <summary>
    /// Folds every node's answer into one partition map with leadership resolved to node names.
    ///
    /// <para>The committed map itself is cluster-wide, so any answering node supplies a partition's
    /// state, generation, replication factor and replica set; the first answer wins and later ones
    /// only contribute leadership. Leadership is the part that is local belief, and it is taken from
    /// whichever nodes claimed it — none, one, or, during an election, more than one.</para>
    /// </summary>
    private static IReadOnlyList<PartitionFacts> MergePlacement(IReadOnlyDictionary<string, PlacementView> placements)
    {
        SortedDictionary<int, PartitionFacts> merged = new();
        SortedDictionary<int, List<string>> claimants = new();

        foreach ((string node, PlacementView view) in placements.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            foreach (PartitionView partition in view.Partitions)
            {
                if (!merged.ContainsKey(partition.PartitionId))
                {
                    merged[partition.PartitionId] = new PartitionFacts(
                        PartitionId: partition.PartitionId,
                        State: partition.State,
                        Generation: partition.Generation,
                        EffectiveReplicationFactor: partition.EffectiveReplicationFactor,
                        Leader: null,
                        Replicas: partition.Replicas);
                }

                if (!partition.LeaderLocal)
                    continue;

                if (!claimants.TryGetValue(partition.PartitionId, out List<string>? who))
                    claimants[partition.PartitionId] = who = new List<string>();
                who.Add(node);
            }
        }

        List<PartitionFacts> result = new();
        foreach ((int id, PartitionFacts partition) in merged)
        {
            string? leader = claimants.TryGetValue(id, out List<string>? who) ? string.Join(" + ", who) : null;
            result.Add(partition with { Leader = leader });
        }
        return result;
    }

    /// <summary>What one node answered at <c>/v1/cluster/placement</c>, reduced to what a report uses.</summary>
    private sealed record PlacementView(IReadOnlyList<PartitionView> Partitions);

    private sealed record PartitionView(
        int PartitionId,
        string State,
        long Generation,
        int EffectiveReplicationFactor,
        bool LeaderLocal,
        IReadOnlyList<string> Replicas);

    private async Task<NodeFacts> CaptureNodeAsync(
        HttpClient http, NodeTarget node, IDictionary<string, PlacementView> placements, CancellationToken ct)
    {
        List<string> errors = new();
        List<int> leads = new();
        string? server = null;
        string? runtime = null;
        List<NodeComponent> components = new();
        bool? ready = null;
        Dictionary<string, string> variables = new(StringComparer.Ordinal);
        Dictionary<string, string> engineSettings = new(StringComparer.Ordinal);

        try
        {
            using JsonDocument doc = JsonDocument.Parse(
                await http.GetStringAsync(new Uri(node.MetricsUrl, "/v1/version"), ct).ConfigureAwait(false));

            if (doc.RootElement.TryGetProperty("server", out JsonElement s))
                server = s.GetString();
            if (doc.RootElement.TryGetProperty("runtime", out JsonElement r))
                runtime = r.GetString();
            if (doc.RootElement.TryGetProperty("components", out JsonElement list) && list.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement item in list.EnumerateArray())
                {
                    string? name = item.TryGetProperty("name", out JsonElement n) ? n.GetString() : null;
                    string? version = item.TryGetProperty("version", out JsonElement v) ? v.GetString() : null;
                    if (name is not null && version is not null)
                        components.Add(new NodeComponent(name, version));
                }
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            errors.Add($"/v1/version: {ex.GetType().Name}: {ex.Message}");
        }

        try
        {
            using JsonDocument doc = JsonDocument.Parse(
                await http.GetStringAsync(new Uri(node.MetricsUrl, "/v1/cluster/health"), ct).ConfigureAwait(false));
            if (doc.RootElement.TryGetProperty("ready", out JsonElement readyElement))
                ready = readyElement.GetBoolean();
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            errors.Add($"/v1/cluster/health: {ex.GetType().Name}: {ex.Message}");
        }

        // The resolved engine configuration. This is the durability evidence a comparison rests on:
        // SHOW VARIABLES below reports the configuration layer, which says nothing about a setting the
        // operator never wrote down, and the baseline underneath it is exactly where the WAL knobs
        // live.
        try
        {
            using JsonDocument doc = JsonDocument.Parse(
                await http.GetStringAsync(new Uri(node.MetricsUrl, "/v1/engine-settings"), ct).ConfigureAwait(false));

            if (doc.RootElement.TryGetProperty("settings", out JsonElement settings) &&
                settings.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty setting in settings.EnumerateObject())
                    engineSettings[setting.Name] = setting.Value.GetString() ?? "";
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            errors.Add($"/v1/engine-settings: {ex.GetType().Name}: {ex.Message}");
        }

        // Placement, which is where a hot partition becomes attributable. The committed map is
        // cluster-wide, but leadership is local belief, so this has to be asked of every node: each
        // partition has exactly one leader, and collecting what every node claims is the only way to
        // see the whole distribution.
        try
        {
            using JsonDocument doc = JsonDocument.Parse(
                await http.GetStringAsync(new Uri(node.MetricsUrl, "/v1/cluster/placement"), ct).ConfigureAwait(false));

            List<PartitionView> partitions = new();
            if (doc.RootElement.TryGetProperty("partitions", out JsonElement list) && list.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement item in list.EnumerateArray())
                {
                    if (!item.TryGetProperty("partitionId", out JsonElement idElement) ||
                        !idElement.TryGetInt32(out int partitionId))
                    {
                        continue;
                    }

                    bool leaderLocal = item.TryGetProperty("leaderLocal", out JsonElement leaderElement) &&
                                       leaderElement.ValueKind == JsonValueKind.True;

                    List<string> replicas = new();
                    if (item.TryGetProperty("replicas", out JsonElement replicaList) &&
                        replicaList.ValueKind == JsonValueKind.Array)
                    {
                        foreach (JsonElement replica in replicaList.EnumerateArray())
                        {
                            string? endpoint = replica.TryGetProperty("endpoint", out JsonElement e) ? e.GetString() : null;
                            string? role = replica.TryGetProperty("role", out JsonElement r) ? r.GetString() : null;
                            if (endpoint is not null)
                                replicas.Add(role is null ? endpoint : $"{endpoint} ({role})");
                        }
                    }

                    partitions.Add(new PartitionView(
                        PartitionId: partitionId,
                        State: item.TryGetProperty("state", out JsonElement s) ? s.GetString() ?? "" : "",
                        Generation: item.TryGetProperty("generation", out JsonElement g) && g.TryGetInt64(out long gen) ? gen : 0,
                        EffectiveReplicationFactor: item.TryGetProperty("effectiveReplicationFactor", out JsonElement f) &&
                                                    f.TryGetInt32(out int rf) ? rf : 0,
                        LeaderLocal: leaderLocal,
                        Replicas: replicas));

                    if (leaderLocal)
                        leads.Add(partitionId);
                }
            }

            leads.Sort();
            placements[node.Name] = new PlacementView(partitions);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            errors.Add($"/v1/cluster/placement: {ex.GetType().Name}: {ex.Message}");
        }

        // SHOW VARIABLES is node-local by design: it reports the configuration the engine that answered
        // resolved, which is exactly why it has to be asked of each node rather than of the pool.
        try
        {
            string connectionString =
                $"Endpoint={node.MetricsUrl.GetLeftPart(UriPartial.Authority)};Database={_database};Protocol=rest";
            await using CamusConnection conn = new(new CamusConnectionStringBuilder(connectionString));
            await conn.OpenAsync(ct).ConfigureAwait(false);

            foreach (IReadOnlyDictionary<string, string> row in await QueryAsync(conn, "SHOW VARIABLES", ct).ConfigureAwait(false))
            {
                if (row.TryGetValue("variable", out string? name) && row.TryGetValue("value", out string? value))
                    variables[name] = value;
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            errors.Add($"SHOW VARIABLES: {ex.GetType().Name}: {ex.Message}");
        }

        return new NodeFacts(
            Node: node.Name,
            BaseUrl: node.MetricsUrl.GetLeftPart(UriPartial.Authority),
            Server: server,
            Runtime: runtime,
            Components: components,
            Ready: ready,
            Variables: variables,
            EngineSettings: engineSettings,
            Errors: errors)
        {
            LeadsPartitions = leads,
        };
    }

    /// <summary>Runs a statement and materializes its rows as string maps, which is all a manifest needs.</summary>
    private static async Task<IReadOnlyList<IReadOnlyDictionary<string, string>>> QueryAsync(
        CamusConnection conn, string sql, CancellationToken ct)
    {
        List<IReadOnlyDictionary<string, string>> rows = new();

        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            Dictionary<string, string> row = new(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? "" : reader.GetValue(i)?.ToString() ?? "";
            rows.Add(row);
        }

        return rows;
    }

    /// <summary>
    /// Reduces the facts that must not differ between two comparable runs to one string: every node's
    /// engine configuration and every node's component versions.
    ///
    /// <para>Engine keys only. The full variable set includes values that legitimately differ between
    /// two runs of the same configuration — a node name, a data directory, a listening port — and
    /// folding those in would make every fingerprint unique and the check useless. What remains is the
    /// storage, WAL and durability surface, which is precisely what may not change under a
    /// comparison.</para>
    ///
    /// <para><b>What it proves, exactly:</b> the same configured engine settings on the same build. An
    /// engine key the operator never set reports as empty, because CamusDB applies its mode-specific
    /// Kahuna baseline below the configuration layer and <c>SHOW VARIABLES</c> reports the configured
    /// value, not that baseline. Two runs with an equal fingerprint therefore share their explicit
    /// settings and their component versions — and since the baseline is part of the build, an equal
    /// fingerprint still means an equal effective configuration. Two runs whose fingerprints differ
    /// are never comparable; that direction is exact.</para>
    /// </summary>
    public static string Fingerprint(IReadOnlyList<NodeFacts> nodes)
    {
        StringBuilder material = new();

        foreach (NodeFacts node in nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            material.Append(node.Node).Append('\n');

            foreach (NodeComponent component in node.Components.OrderBy(c => c.Name, StringComparer.Ordinal))
                material.Append(component.Name).Append('=').Append(component.Version).Append('\n');

            foreach (KeyValuePair<string, string> variable in node.Variables
                         .Where(v => v.Key.StartsWith("kahuna.", StringComparison.OrdinalIgnoreCase))
                         .OrderBy(v => v.Key, StringComparer.Ordinal))
            {
                material.Append(variable.Key).Append('=').Append(variable.Value).Append('\n');
            }

            // The resolved engine settings, excluding the ones that legitimately differ between two
            // runs of one configuration: a path under a per-run temp directory, or a node identity.
            // Folding those in would make every fingerprint unique and the check useless.
            foreach (KeyValuePair<string, string> setting in node.EngineSettings
                         .Where(e => !IsRunSpecific(e.Key))
                         .OrderBy(e => e.Key, StringComparer.Ordinal))
            {
                material.Append("engine.").Append(setting.Key).Append('=').Append(setting.Value).Append('\n');
            }
        }

        if (material.Length == 0)
            return "none";

        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(material.ToString()));
        return "sha256:" + Convert.ToHexStringLower(hash)[..16];
    }

    /// <summary>
    /// Settings that differ between two runs of the same configuration and so must not enter the
    /// fingerprint: filesystem paths and node identity. Everything else — every durability, batching
    /// and threading knob — is exactly what the fingerprint exists to pin down.
    /// </summary>
    private static bool IsRunSpecific(string name)
        => name.EndsWith("Path", StringComparison.Ordinal)
        || name.EndsWith("Dir", StringComparison.Ordinal)
        || name.Contains("NodeName", StringComparison.Ordinal)
        || name.Contains("NodeId", StringComparison.Ordinal)
        || name.Contains("Host", StringComparison.Ordinal)
        || name.Contains("Port", StringComparison.Ordinal);

    /// <summary>
    /// A fingerprint over the component versions alone — which build answered, ignoring how it was
    /// configured.
    ///
    /// <para>Separate from the full <see cref="Fingerprint"/> so a comparison can pin the build while
    /// still permitting a deliberate, named configuration change. A single combined fingerprint can
    /// only be accepted or waived whole, and waiving it whole would hide a dependency bump alongside
    /// the one setting an experiment meant to vary.</para>
    /// </summary>
    public static string VersionFingerprint(IReadOnlyList<NodeFacts> nodes)
    {
        StringBuilder material = new();

        foreach (NodeFacts node in nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            material.Append(node.Node).Append('\n');
            foreach (NodeComponent component in node.Components.OrderBy(c => c.Name, StringComparer.Ordinal))
                material.Append(component.Name).Append('=').Append(component.Version).Append('\n');
        }

        if (material.Length == 0)
            return "none";

        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(material.ToString()));
        return "sha256:" + Convert.ToHexStringLower(hash)[..16];
    }

    /// <summary>
    /// The cluster's resolved engine settings as one map, for a field-by-field comparison.
    ///
    /// <para>Run-specific values are dropped for the reason given on <see cref="IsRunSpecific"/>.
    /// When nodes disagree on a setting the disagreement is reported as the value rather than hidden
    /// behind one node's answer: a fleet configured inconsistently is a finding in itself, and it must
    /// not read as equal to another fleet that happens to match one of its nodes.</para>
    /// </summary>
    public static IReadOnlyDictionary<string, string> EngineSettings(ClusterFacts facts)
    {
        SortedDictionary<string, string> settings = new(StringComparer.Ordinal);

        IEnumerable<string> names = facts.Nodes
            .SelectMany(n => n.EngineSettings.Keys)
            .Where(k => !IsRunSpecific(k))
            .Distinct(StringComparer.Ordinal);

        foreach (string name in names)
        {
            List<string> values = facts.Nodes
                .Select(n => n.EngineSettings.TryGetValue(name, out string? v) ? v : "<absent>")
                .Distinct(StringComparer.Ordinal)
                .OrderBy(v => v, StringComparer.Ordinal)
                .ToList();

            settings[name] = values.Count == 1 ? values[0] : $"<nodes disagree: {string.Join(" | ", values)}>";
        }

        return settings;
    }

    public static string Serialize(ClusterFacts facts)
        => JsonSerializer.Serialize(facts, new JsonSerializerOptions(Json) { WriteIndented = true });

    public static ClusterFacts? Deserialize(string json)
        => JsonSerializer.Deserialize<ClusterFacts>(json, Json);
}
