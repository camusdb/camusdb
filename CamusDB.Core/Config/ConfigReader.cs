
using System.Collections;
using CamusDB.Core.Config.Models;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Config;

public class ConfigReader
{
	public ConfigReader()
	{
        
    }

    /// <summary>
    /// Top-level YAML keys recognised by <see cref="ConfigDefinition"/> (underscored form).
    /// Any other root key is a typo or a stale option and is rejected, since YamlDotNet would
    /// otherwise silently drop it (e.g. a misspelled <c>htttp_port</c> would leave the real
    /// <c>http_port</c> at its default with no warning).
    /// </summary>
    private static readonly HashSet<string> AllowedRootKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "data_dir",
        "mode",
        "node_name",
        "raft_host",
        "raft_port",
        "initial_partitions",
        "peers",
        "http_peers",
        "schema_ack_wait_timeout_ms",
        "schema_ack_live_node_lease_ms",
        "stats_flush_interval_ms",
        "sql_parser_cache_ttl_seconds",
        "sql_parser_cache_max_entries",
        "sql_parser_cache_sweep_seconds",
        "raft_node_id",
        "http_port",
        "https_port",
        "https_certificate",
        "raft_certificate",
        "default_isolation_level",
        "range_lock_expires_ms",
        "range_lock_heartbeat_interval_ms",
        "max_serializable_transaction_lifetime_ms",
        "lock_escalation_threshold",
        "lock_wait_deadline_ms",
        "key_range_sharding",
        "kahuna",
    };

    public ConfigDefinition Read(string yml)
    {
        ValidateUnknownKeys(yml);

        IDeserializer deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        ConfigDefinition config = deserializer.Deserialize<ConfigDefinition>(yml) ?? new ConfigDefinition();

        // Fail fast on a malformed config rather than producing confusing behaviour later
        // (e.g. a zero ack timeout that makes the two-version gate give up instantly, or
        // an http_peers list that silently disables the explicit forwarding map).
        config.Validate();

        return config;
    }

    private static void ValidateUnknownKeys(string yml)
    {
        if (string.IsNullOrWhiteSpace(yml))
            return;

        IDeserializer raw = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        Dictionary<string, object>? root = raw.Deserialize<Dictionary<string, object>>(yml);
        if (root is null)
            return;

        foreach (string rootKey in root.Keys)
        {
            if (!AllowedRootKeys.Contains(rootKey))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidConfig,
                    $"Unknown config option '{rootKey}'; allowed keys: " +
                    string.Join(", ", AllowedRootKeys.OrderBy(k => k)));
        }

        if (!root.TryGetValue("kahuna", out object? kahunaRaw) || kahunaRaw is null)
            return;

        if (kahunaRaw is not IDictionary kahunaDict)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                "'kahuna' must be a mapping of option names to values");

        foreach (object key in kahunaDict.Keys)
        {
            string name = key.ToString() ?? "";
            if (!KahunaOptionsConfig.AllowedYamlKeys.Contains(name))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidConfig,
                    $"Unknown 'kahuna' option '{name}'; allowed keys: " +
                    string.Join(", ", KahunaOptionsConfig.AllowedYamlKeys.OrderBy(k => k)));
            }
        }
    }
}
