
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
    internal static readonly HashSet<string> AllowedRootKeys = new(StringComparer.OrdinalIgnoreCase)
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
        "stats_analyze_sample_rows",
        "stats_histogram_buckets",
        "auto_analyze_enabled",
        "auto_analyze_check_interval_ms",
        "auto_analyze_fraction_stale_rows",
        "auto_analyze_min_stale_rows",
        "auto_analyze_max_concurrent",
        "auto_analyze_max_rows_per_second",
        "auto_analyze_histogram_sample_rows",
        "auto_analyze_hll_precision",
        "auto_analyze_load_pause_threshold",
        "auto_analyze_ownership_check_rows",
        "ttl_enabled",
        "ttl_default_job_cron",
        "ttl_default_select_batch_size",
        "ttl_default_delete_batch_size",
        "ttl_default_select_rate_limit",
        "ttl_default_delete_rate_limit",
        "ttl_spans_per_table",
        "ttl_max_concurrent_spans_per_node",
        "ttl_load_pause_threshold",
        "ttl_span_lease_ms",
        "ttl_span_lease_renew_interval_ms",
        "sql_parser_cache_ttl_seconds",
        "sql_parser_cache_max_entries",
        "sql_parser_cache_sweep_seconds",
        "cost_based_access_path_enabled",
        "cost_based_join_order_enabled",
        "plan_cache_enabled",
        "plan_cache_max_entries",
        "regex_match_timeout_ms",
        "regex_cache_max_entries",
        "raft_node_id",
        "http_port",
        "https_port",
        "https_certificate",
        "raft_certificate",
        "require_tls_when_auth_enabled",
        "grpc_enabled",
        "grpc_port",
        "grpc_batch_max_in_flight",
        "default_isolation_level",
        "default_transaction_locking",
        "default_transaction_priority",
        "range_lock_expires_ms",
        "max_serializable_transaction_lifetime_ms",
        "transaction_idle_timeout_ms",
        "transaction_reaper_interval_ms",
        "prepared_statement_idle_timeout_ms",
        "prepared_statement_sweep_interval_ms",
        "grpc_max_prepared_statements_per_stream",
        "rest_max_prepared_statements_per_principal",
        "rest_max_prepared_statements",
        "max_prepared_statement_bytes",
        "rest_max_prepared_statement_bytes",
        "rest_max_prepared_statement_bytes_per_principal",
        "grpc_max_prepared_statement_bytes_per_stream",
        "lock_escalation_threshold",
        "lock_wait_deadline_ms",
        "key_range_sharding",
        "max_identifier_length",
        "max_columns_per_table",
        "max_indexes_per_table",
        "max_tables_per_database",
        "max_index_columns",
        "max_index_include_tuple_bytes",
        "max_mutations_per_transaction",
        "branch_snapshot_hold_lease_ms",
        "spill_enabled",
        "spill_threshold_rows",
        "spill_merge_fan_in",
        "query_result_cache_enabled",
        "query_result_cache_default_ttl_ms",
        "query_result_cache_max_entries",
        "query_result_cache_max_bytes",
        "query_result_cache_max_entry_bytes",
        "query_result_cache_max_entry_rows",
        "query_result_cache_max_deps",
        "query_result_cache_max_point_deps",
        "query_result_cache_max_ranges",
        "query_result_cache_singleflight_wait_ms",
        "query_result_cache_strict_validation_max_keys",
        "query_result_cache_sweep_interval_ms",
        "orphan_retention_ms",
        "orphan_reclaim_interval_ms",
        "engine_metrics_enabled",
        "kahuna",
        "diagnostics",
    };

    public ConfigDefinition Read(string yml)
    {
        IReadOnlyCollection<string> providedKeys = ValidateUnknownKeys(yml);

        IDeserializer deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        ConfigDefinition config = deserializer.Deserialize<ConfigDefinition>(yml) ?? new ConfigDefinition();

        // Record what the document actually said, so a setting whose default depends on the rest of
        // the configuration can distinguish an explicit choice from an untouched default.
        foreach (string key in providedKeys)
            config.ProvidedKeys.Add(key);

        // Fail fast on a malformed config rather than producing confusing behaviour later
        // (e.g. a zero ack timeout that makes the two-version gate give up instantly, or
        // an http_peers list that silently disables the explicit forwarding map).
        config.Validate();

        return config;
    }

    /// <summary>
    /// Rejects root and nested keys that <see cref="ConfigDefinition"/> does not model, and returns
    /// the root keys the document did provide.
    /// </summary>
    private static IReadOnlyCollection<string> ValidateUnknownKeys(string yml)
    {
        if (string.IsNullOrWhiteSpace(yml))
            return [];

        IDeserializer raw = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        Dictionary<string, object>? root = raw.Deserialize<Dictionary<string, object>>(yml);
        if (root is null)
            return [];

        foreach (string rootKey in root.Keys)
        {
            if (!AllowedRootKeys.Contains(rootKey))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidConfig,
                    $"Unknown config option '{rootKey}'; allowed keys: " +
                    string.Join(", ", AllowedRootKeys.OrderBy(k => k)));
        }

        ValidateNestedKeys(root, "kahuna", KahunaOptionsConfig.AllowedYamlKeys);
        ValidateNestedKeys(root, "diagnostics", DiagnosticsConfig.AllowedYamlKeys);

        return root.Keys;
    }

    private static void ValidateNestedKeys(
        Dictionary<string, object> root, string section, HashSet<string> allowedKeys)
    {
        if (!root.TryGetValue(section, out object? raw) || raw is null)
            return;

        if (raw is not IDictionary dict)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"'{section}' must be a mapping of option names to values");

        foreach (object key in dict.Keys)
        {
            string name = key.ToString() ?? "";
            if (!allowedKeys.Contains(name))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidConfig,
                    $"Unknown '{section}' option '{name}'; allowed keys: " +
                    string.Join(", ", allowedKeys.OrderBy(k => k)));
            }
        }
    }
}
