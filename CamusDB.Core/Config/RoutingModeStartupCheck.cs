/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Config;

/// <summary>
/// Fixes a cluster's routing mode (<c>key_range_sharding</c>) when the cluster is created, and keeps
/// every later boot on it.
///
/// <para><b>Why the mode cannot change on existing data.</b> Hash routing and the key-range seed pick a
/// key space's partition with different hash functions, so turning key-range sharding on moves most
/// tables to another partition. Their rows stay reachable only while Kahuna's storage is node-global
/// and every node holds every partition. And turning it off does not undo it: Kahuna rebuilds its
/// key-space registry from the replicated range map at startup, so a space that has a range
/// descriptor stays key-range routed, while the CamusDB code that reads the flag then assumes hash
/// routing for it. Both directions are pinned by <c>TestRoutingModeChangeOnExistingData</c>. The only
/// supported way to change the mode of data is a logical dump and reimport into a new cluster.</para>
///
/// <para><b>The record.</b> One persistent key, <see cref="MarkerKey"/>, in the hash-routed
/// <c>_system</c> space, written once with <see cref="KeyValueFlags.SetIfNotExists"/>. Every node reads
/// it at boot through the key's partition leader, so a node with an empty data directory learns the
/// cluster's mode as well. Concurrent first boots race on the conditional write; the loser re-reads
/// and follows the winner.</para>
///
/// <para><b>The rule</b> (<see cref="Decide"/>). An unset <c>key_range_sharding</c> follows the recorded
/// mode, so a change of the compiled default applies only to clusters created after it. An explicit
/// value (config file or <c>CAMUS_KEY_RANGE_SHARDING</c>) that differs from the recorded mode refuses
/// the boot. A cluster that predates the record has its mode inferred once from its data and then
/// recorded: a CamusDB key space with a range descriptor means key-range, a registered database with
/// none means hash, and a cluster with no database takes the configured value.</para>
///
/// <para><b>Limit.</b> The inference reads this node's applied range map, which a node restores from its
/// own WAL inside <c>EmbeddedKahuna.StartAsync</c>. A node that joins a pre-record key-range cluster
/// with an empty data directory, before any existing node has booted the new version, has no
/// descriptors yet and would infer hash. Boot an existing node first. <c>TableOpener</c> refuses to
/// open a table whose space has a range descriptor under hash routing, so the split state is reported
/// at first use and is never served silently.</para>
///
/// <para>Runs on the server host after the embedded node has started and before the cluster-settings
/// service publishes the boot configuration, so every component built from that configuration sees
/// the resolved mode.</para>
/// </summary>
public static class RoutingModeStartupCheck
{
    /// <summary>The persistent record of the cluster's routing mode. Hash-routed, like every <c>_system</c> key.</summary>
    internal const string MarkerKey = "_system/routing/mode";

    internal const string HashValue = "hash";

    internal const string KeyRangeValue = "key_range";

    private const string ConfigKey = "key_range_sharding";

    /// <summary>The outcome of <see cref="Decide"/>: the mode to run in, and whether the record must be written.</summary>
    internal readonly record struct RoutingModeDecision(bool KeyRange, bool Persist);

    /// <summary>
    /// Reads or creates the cluster's routing-mode record and returns the mode this node must run
    /// in. Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidConfig"/>
    /// when an explicit <c>key_range_sharding</c> disagrees with the cluster. Transient failures
    /// (a partition between leaders) are retried under the startup retry budget.
    /// </summary>
    /// <param name="configured">The value the node's local configuration resolved to.</param>
    /// <param name="explicitlySet">Whether the config file or the environment set that value.</param>
    public static async Task<bool> ResolveAsync(
        EmbeddedKahuna node, bool configured, bool explicitlySet, ILogger logger, CancellationToken cancellationToken)
    {
        // Two rounds: a node that loses the first-write race re-reads the winner's record. The record
        // is never deleted, so a second loss cannot happen.
        for (int round = 0; round < 2; round++)
        {
            bool? recorded = await ClusterSettingsService.RunBootStepAsync(
                () => ReadMarkerAsync(node, cancellationToken), cancellationToken).ConfigureAwait(false);

            bool? existing = recorded is null
                ? await ClusterSettingsService.RunBootStepAsync(
                    () => InferExistingModeAsync(node, cancellationToken), cancellationToken).ConfigureAwait(false)
                : null;

            RoutingModeDecision decision = Decide(recorded, existing, configured, explicitlySet);

            if (decision.Persist)
            {
                bool written = await ClusterSettingsService.RunBootStepAsync(
                    () => TryWriteMarkerAsync(node, decision.KeyRange, cancellationToken), cancellationToken).ConfigureAwait(false);

                if (!written)
                    continue;

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "Recorded the cluster's routing mode as {Mode} ({Reason})",
                        Name(decision.KeyRange),
                        existing is null ? "a new cluster" : "inferred from the existing data");
            }

            if (!explicitlySet && decision.KeyRange != configured && logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "{Key} is not set; following the cluster's routing mode, {Mode}", ConfigKey, Name(decision.KeyRange));

            return decision.KeyRange;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidConfig,
            $"The cluster's routing-mode record '{MarkerKey}' was neither readable nor writable at startup");
    }

    /// <summary>
    /// The rule, free of I/O. <paramref name="recorded"/> is the record's mode, or null when absent;
    /// <paramref name="existing"/> is the mode inferred from existing data when there is no record, or
    /// null for a cluster with no database.
    /// </summary>
    internal static RoutingModeDecision Decide(bool? recorded, bool? existing, bool configured, bool explicitlySet)
    {
        if (recorded is bool cluster)
        {
            if (explicitlySet && configured != cluster)
                throw Mismatch(configured, cluster, "was created with");

            return new RoutingModeDecision(cluster, Persist: false);
        }

        if (existing is bool data)
        {
            if (explicitlySet && configured != data)
                throw Mismatch(configured, data, "holds data placed under");

            return new RoutingModeDecision(data, Persist: true);
        }

        return new RoutingModeDecision(configured, Persist: true);
    }

    private static CamusDBException Mismatch(bool configured, bool cluster, string relation) =>
        new(CamusDBErrorCodes.InvalidConfig,
            $"'{ConfigKey}' is {(configured ? "true" : "false")} in this node's configuration, but this cluster {relation} " +
            $"{Name(cluster)} routing. A cluster's routing mode is fixed when it is created, because the two modes place " +
            $"a table's keys on different partitions. Remove '{ConfigKey}' (and CAMUS_KEY_RANGE_SHARDING) to follow the " +
            $"cluster, or set it to {(cluster ? "true" : "false")}. To move data to the other mode, dump it and reimport it " +
            "into a new cluster (see docs/logical-dump-and-reimport.md).");

    private static string Name(bool keyRange) => keyRange ? "key-range" : "hash";

    private static async Task<bool?> ReadMarkerAsync(EmbeddedKahuna node, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, MarkerKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);

        if (type == KeyValueResponseType.DoesNotExist || (type == KeyValueResponseType.Get && entry?.Value is null))
            return null;

        if (type != KeyValueResponseType.Get)
            throw new InvalidOperationException($"routing-mode record read did not answer: {type} (retried at boot)");

        string value = System.Text.Encoding.UTF8.GetString(entry!.Value!);

        return value switch
        {
            HashValue => false,
            KeyRangeValue => true,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"The cluster's routing-mode record '{MarkerKey}' holds '{value}', which is neither '{HashValue}' nor '{KeyRangeValue}'"),
        };
    }

    /// <summary>
    /// The mode a cluster without a record was running in, or null when it has no database. A CamusDB
    /// table key space always names its placement group before <see cref="HashPlacement.GroupSeparator"/>
    /// and the system spaces never do, so only table spaces count as evidence of key-range routing.
    /// </summary>
    private static async Task<bool?> InferExistingModeAsync(EmbeddedKahuna node, CancellationToken cancellationToken)
    {
        KahunaRangeMapResponse map = node.Kahuna.GetRangeMap();

        // Until initialized, an empty map means "not applied yet", not "no ranges".
        if (!map.Initialized)
            throw new InvalidOperationException("the range map is not initialized yet (retried at boot)");

        foreach (KahunaKeySpaceRangesResponse space in map.KeySpaces)
        {
            if (space.Descriptors.Count > 0 && space.KeySpace.Contains(HashPlacement.GroupSeparator))
                return true;
        }

        return await AnyDatabaseRegisteredAsync(node, cancellationToken).ConfigureAwait(false) ? false : null;
    }

    private static async Task<bool> AnyDatabaseRegisteredAsync(EmbeddedKahuna node, CancellationToken cancellationToken)
    {
        string bucket = DatabaseRegistry.RegistryBucketOf(DatabaseRegistry.SystemKeyPrefix);
        string namePrefix = DatabaseRegistry.NameKeyPrefixOf(DatabaseRegistry.SystemKeyPrefix);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in node.Kahuna.LocateAndScanRange(
            KvTransaction.CreateReadOnly().TransactionId, bucket, null, true, null, true, 16,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false))
        {
            if (entry.Value is not null && key.StartsWith(namePrefix, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>True when this call wrote the record; false when another node's record was already there.</summary>
    private static async Task<bool> TryWriteMarkerAsync(EmbeddedKahuna node, bool keyRange, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, MarkerKey, System.Text.Encoding.UTF8.GetBytes(keyRange ? KeyRangeValue : HashValue), null, -1,
            KeyValueFlags.SetIfNotExists, 0, KeyValueDurability.Persistent, cancellationToken).ConfigureAwait(false);

        return type switch
        {
            KeyValueResponseType.Set => true,
            KeyValueResponseType.NotSet => false,
            _ => throw new InvalidOperationException($"routing-mode record write did not answer: {type} (retried at boot)"),
        };
    }
}
