
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
        "memory_profile",
        "node_name",
        "raft_host",
        "raft_port",
        "initial_partitions",
        "peers",
        "http_peers",
        "join_existing",
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
        "bound_query_cache_enabled",
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
        "transaction_admission_wait_ms",
        "range_lock_expires_ms",
        "max_serializable_transaction_lifetime_ms",
        "transaction_finalize_retry_budget_ms",
        "sequence_retry_budget_ms",
        "transaction_idle_timeout_ms",
        "transaction_reaper_interval_ms",
        "abandoned_transaction_release_after_ms",
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
        "distributed_query_execution",
        "max_identifier_length",
        "max_columns_per_table",
        "max_indexes_per_table",
        "max_tables_per_database",
        "max_index_columns",
        "max_index_include_tuple_bytes",
        "max_mutations_per_transaction",
        "min_free_disk_bytes",
        "max_view_expansion_depth",
        "materialized_view_refresh_chunk_rows",
        "materialized_view_refresh_enabled",
        "materialized_view_refresh_takeover_attempts",
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
        "database_idle_eviction_ms",
        "schema_freshness_check_interval_ms",
        "engine_metrics_enabled",
        "slow_query_log_enabled",
        "slow_query_log_threshold_ms",
        "slow_query_log_max_entries",
        "slow_query_log_max_sql_length",
        "dashboard_enabled",
        "dashboard_refresh_seconds",
        "query_tracing_enabled",
        "lock_tracing_enabled",
        "fence_lease_ms",
        "fence_lease_renew_interval_ms",
        "keyspace_purge_batch_size",
        "index_scan_fetch_batch_size",
        "max_query_parallelism",
        "broadcast_join_max_build_rows",
        "hash_join_max_build_rows",
        "net_weight",
        "slot_backed_decode",
        "borrowed_decode",
        "spill_max_frame_bytes",
        "default_read_validation",
        "default_decision_durability",
        "password_hash_iterations",
        "login_kdf_max_concurrency",
        "login_max_attempts_per_minute",
        "login_rate_limit_max_entries",
        "authentication_cache_ttl",
        "authentication_cache_max_entries",
        "access_token_ttl",
        "kahuna",
        "diagnostics",
    };

    public ConfigDefinition Read(string yml)
    {
        List<string> providedKeys = [];
        ValidateUnknownKeys(yml, providedKeys);

        IDeserializer deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        ConfigDefinition config = deserializer.Deserialize<ConfigDefinition>(yml) ?? new ConfigDefinition();

        // Record what the document actually said, so a setting whose default depends on the rest of
        // the configuration can distinguish an explicit choice from an untouched default.
        //
        // The same keys are recorded as file-sourced for reporting. Root keys and the dotted sub-keys
        // of the nested sections are both recorded: a reader asking where kahuna.wal_sync_writes came
        // from wants the sub-key answered, not the section it happens to live under.
        foreach (string key in providedKeys)
        {
            if (!key.Contains('.'))
                config.ProvidedKeys.Add(key);

            config.RecordSource(key, ConfigValueSource.ConfigFile);
        }

        // Fail fast on a malformed config rather than producing confusing behaviour later
        // (e.g. a zero ack timeout that makes the two-version gate give up instantly, or
        // an http_peers list that silently disables the explicit forwarding map).
        config.Validate();

        return config;
    }

    /// <summary>
    /// Rejects root and nested keys that <see cref="ConfigDefinition"/> does not model, appending
    /// every key the document did provide to <paramref name="providedKeys"/> — root keys by name, and
    /// the keys of the nested sections in dotted <c>section.key</c> form.
    /// </summary>
    private static void ValidateUnknownKeys(string yml, List<string> providedKeys)
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

        providedKeys.AddRange(root.Keys);

        ValidateNestedKeys(root, "kahuna", KahunaOptionsConfig.AllowedYamlKeys, providedKeys);
        ValidateNestedKeys(root, "diagnostics", DiagnosticsConfig.AllowedYamlKeys, providedKeys);
    }

    private static void ValidateNestedKeys(
        Dictionary<string, object> root, string section, HashSet<string> allowedKeys, List<string> providedKeys)
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

            providedKeys.Add($"{section}.{name}");
        }
    }
}
