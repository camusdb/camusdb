/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kahuna;
using Kahuna.Communication.External;
using Kahuna.Shared.Communication.Rest;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

/// <summary>
/// Cluster topology admin API: the committed membership roster, per-node readiness, the partition
/// placement table, graceful decommission of the local node, and per-partition replication-factor
/// overrides. These surface the consensus layer's own membership/placement records — the same data
/// Kahuna's server exposes — so an external orchestrator (health probes, a chaos/reliability
/// harness, a scale-down script) can observe and steer the cluster without in-process access.
///
/// <para><b>Who may read what.</b> The readiness probe answers without a credential, because an
/// orchestrator probe has none, but with authentication on it tells an anonymous or non-superuser
/// caller only whether the node serves: its role, hosted-partition count and stalled partition ids
/// are withheld. The four detail reads — membership, placement, backfill status, snapshot status —
/// name peer endpoints, partition ids, leaders and commit indexes, which is the cluster topology
/// <c>SHOW ENGINE STATS</c> is superuser-gated for; they follow that statement's bar: superuser when
/// authentication is on. With authentication off they stay open, exactly as that statement does, since
/// any client that reaches the port can already run every statement.</para>
///
/// <para>The four mutations (leave, replication-factor, transfer-leadership, split-range) are held higher:
/// superuser when authentication is enabled, loopback-only otherwise, and never a credential over
/// plaintext. Unlike a read, one unauthenticated call can take a node out of the cluster, or move a
/// partition's leadership under a running workload.</para>
/// </summary>
[ApiController]
public sealed class ClusterController : CommandsController
{
    private readonly EmbeddedKahuna kahuna;

    public ClusterController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger,
        CamusDBOptions options, EmbeddedKahuna kahuna)
        : base(executor, transactions, logger, options)
    {
        this.kahuna = kahuna;
    }

    /// <summary>
    /// Readiness probe. 200 when the node can serve key/value requests, 503 while it cannot — a
    /// node answers HTTP (and membership queries) long before cluster initialization completes, so
    /// <c>/ping</c> alone cannot distinguish "up" from "able to serve".
    ///
    /// <para>Exempt from authentication, because a probe has no credential. With authentication on,
    /// only a superuser sees <c>LocalRole</c>, <c>HostedPartitions</c>, <c>StalledPartitions</c>
    /// and the storage fault texts; everyone else gets the status code, <c>Ready</c>, <c>Initialized</c>
    /// and <c>CommitStalled</c>.
    /// The stalled bit stays public on purpose: it is the one signal a monitor alarms on, and it names
    /// no partition, peer or endpoint.</para>
    /// </summary>
    [HttpGet]
    [Route("/v1/cluster/health")]
    public async Task<JsonResult> GetHealth()
    {
        ClusterHealthResponse health = BuildHealth(kahuna.Raft, kahuna.Kahuna);

        if (!await CanReadTopologyQuietlyAsync().ConfigureAwait(false))
            health = new ClusterHealthResponse
            {
                Ready = health.Ready,
                Initialized = health.Initialized,
                CommitStalled = health.CommitStalled,
            };

        return new JsonResult(health)
        {
            StatusCode = health.Ready ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable,
        };
    }

    /// <summary>
    /// Leader-side backfill refusal diagnostics — the query the refusal log line tells the
    /// operator to run (<c>IRaft.GetBackfillStatuses</c>), previously unreachable from a running
    /// node. Empty partitions list = this node refuses nobody. Each node reports only the
    /// partitions it leads; union the answers across nodes for the full cluster picture.
    /// </summary>
    [HttpGet]
    [Route("/v1/cluster/backfill-status")]
    public async Task<JsonResult> GetBackfillStatus()
    {
        JsonResult? refusal = await RefuseTopologyReadAsync().ConfigureAwait(false);
        if (refusal is not null)
            return refusal;

        IRaft raft = kahuna.Raft;

        ClusterBackfillStatusResponse response = new()
        {
            LocalEndpoint = raft.GetLocalEndpoint(),
            Initialized = raft.IsInitialized,
        };

        foreach (int partitionId in KnownPartitionIds(raft))
        {
            IReadOnlyList<RaftBackfillStatus> statuses = raft.GetBackfillStatuses(partitionId);
            if (statuses.Count == 0)
                continue;

            ClusterPartitionBackfillModel partition = new()
            {
                PartitionId = partitionId,
                CommitIndex = raft.GetCommitIndex(partitionId),
            };

            foreach (RaftBackfillStatus status in statuses)
                partition.Peers.Add(new ClusterPeerBackfillModel
                {
                    FollowerEndpoint = status.FollowerEndpoint,
                    AnchorIndex = status.AnchorIndex,
                    FirstAvailableIndex = status.FirstAvailableIndex,
                    LastCheckpoint = status.LastCheckpoint,
                    Occurrences = status.Occurrences,
                    FirstRefusedAt = status.FirstRefusedAt,
                    LastRefusedAt = status.LastRefusedAt,
                });

            response.Partitions.Add(partition);
        }

        return new JsonResult(response);
    }

    /// <summary>
    /// Leader-side snapshot transfer diagnostics (<c>IRaft.GetSnapshotStatuses</c>): whether a
    /// rescue for a below-the-compaction-floor follower is in flight, backing off after failures,
    /// or unproducible. Empty partitions list = no transfer activity and no failures. This is the
    /// endpoint that distinguishes "escalation declined", "transfer failing" and "no escalation"
    /// on a live cluster — the question the wedge investigations could not ask.
    /// </summary>
    [HttpGet]
    [Route("/v1/cluster/snapshot-status")]
    public async Task<JsonResult> GetSnapshotStatus()
    {
        JsonResult? refusal = await RefuseTopologyReadAsync().ConfigureAwait(false);
        if (refusal is not null)
            return refusal;

        IRaft raft = kahuna.Raft;

        ClusterSnapshotStatusResponse response = new()
        {
            LocalEndpoint = raft.GetLocalEndpoint(),
            Initialized = raft.IsInitialized,
        };

        foreach (int partitionId in KnownPartitionIds(raft))
        {
            IReadOnlyList<RaftSnapshotStatus> statuses = raft.GetSnapshotStatuses(partitionId);
            if (statuses.Count == 0)
                continue;

            ClusterPartitionSnapshotModel partition = new()
            {
                PartitionId = partitionId,
                CommitIndex = raft.GetCommitIndex(partitionId),
            };

            foreach (RaftSnapshotStatus status in statuses)
                partition.Peers.Add(new ClusterPeerSnapshotModel
                {
                    FollowerEndpoint = status.FollowerEndpoint,
                    FailedAttempts = status.FailedAttempts,
                    LastError = status.LastError,
                    Unproducible = status.Unproducible,
                    InFlight = status.InFlight,
                    InFlightForMs = status.InFlightFor?.TotalMilliseconds,
                    FirstFailureAt = status.FirstFailureAt,
                    LastFailureAt = status.LastFailureAt,
                    RetryBackoffRemainingMs = status.RetryBackoffRemaining.TotalMilliseconds,
                });

            response.Partitions.Add(partition);
        }

        return new JsonResult(response);
    }

    [HttpGet]
    [Route("/v1/cluster/membership")]
    public async Task<JsonResult> GetMembership()
    {
        JsonResult? refusal = await RefuseTopologyReadAsync().ConfigureAwait(false);
        if (refusal is not null)
            return refusal;

        return new JsonResult(BuildMembership(kahuna.Raft));
    }

    /// <summary>
    /// The committed membership roster as this node sees it. Internal so the dashboard's cluster
    /// panel serves the same answer from its own route, which accepts the dashboard session: the
    /// browser cannot authenticate to this controller, whose routes take the bearer header only.
    /// </summary>
    internal static ClusterMembershipResponse BuildMembership(IRaft raft)
    {
        ClusterMembership membership = raft.GetMembership();
        string localEndpoint = raft.GetLocalEndpoint();

        ClusterMembershipResponse response = new()
        {
            MembershipVersion = membership.MembershipVersion,
            Initialized = raft.IsInitialized,
            LocalRole = nameof(ClusterMemberRole.NotMember),
        };

        foreach (ClusterMember member in membership.Members)
        {
            response.Members.Add(new ClusterMemberModel
            {
                Endpoint = member.Endpoint,
                NodeId = member.NodeId,
                Role = member.Role.ToString(),
                JoinedVersion = member.JoinedVersion,
            });

            if (member.Endpoint == localEndpoint)
                response.LocalRole = member.Role.ToString();
        }

        return response;
    }

    [HttpGet]
    [Route("/v1/cluster/placement")]
    public async Task<JsonResult> GetPlacement()
    {
        JsonResult? refusal = await RefuseTopologyReadAsync().ConfigureAwait(false);
        if (refusal is not null)
            return refusal;

        IRaft raft = kahuna.Raft;

        ClusterPlacementResponse response = new()
        {
            ReplicationFactor = raft.Configuration.ReplicationFactor,
            RebalancerEnabled = raft.Configuration.EnablePlacementRebalancer,
            Initialized = raft.IsInitialized,
            LocalEndpoint = raft.GetLocalEndpoint(),
        };

        foreach (RaftPartitionRange range in raft.GetPartitionMap())
        {
            bool hosted = raft.HostsPartition(range.PartitionId);

            ClusterPartitionPlacementModel partition = new()
            {
                PartitionId = range.PartitionId,
                State = range.State.ToString(),
                Generation = range.Generation,
                EffectiveReplicationFactor = raft.GetEffectiveReplicationFactor(range.PartitionId),
                HostedLocally = hosted,
                // Only a hosting node can lead a partition; ask its local belief (non-blocking).
                LeaderLocal = hosted && range.State != RaftPartitionState.Removed
                    && await raft.AmILeaderQuick(range.PartitionId).ConfigureAwait(false),
            };

            foreach (RaftReplica replica in range.Replicas)
                partition.Replicas.Add(new ClusterPartitionReplicaModel
                {
                    Endpoint = replica.Endpoint,
                    Role = replica.Role.ToString(),
                });

            if (hosted && range.State != RaftPartitionState.Removed)
                response.HostedPartitionCount++;

            response.Partitions.Add(partition);
        }

        return new JsonResult(response);
    }

    /// <summary>
    /// Graceful decommission of the local node: its replicas are evacuated onto survivors first,
    /// then the removal commits. The node keeps serving afterwards so the caller can read the
    /// committed verdict; stopping the process is the caller's next step. Only one node may drain
    /// at a time, and consensus refuses a removal that would leave the cluster without a voter.
    /// </summary>
    [HttpPost]
    [Route("/v1/cluster/leave")]
    public async Task<JsonResult> Leave()
    {
        Principal? principal = null;
        try
        {
            principal = await EnsureClusterAdminAllowedAsync().ConfigureAwait(false);

            LeaveClusterResult result = await ClusterLeave.ExecuteAsync(kahuna.Raft, HttpContext.RequestAborted)
                .ConfigureAwait(false);

            AuditCluster($"leave:{result.Outcome}", principal, result.Left ? "ok" : "not-left", failure: false);

            return new JsonResult(new ClusterLeaveResponse
            {
                Left = result.Left,
                Drained = result.Drained,
                Outcome = result.Outcome.ToString(),
                MembershipVersion = result.MembershipVersion,
                Retryable = ClusterLeave.IsRetryable(result.Outcome),
                Reason = ClusterLeave.ToReason(result.Outcome),
            })
            {
                StatusCode = ClusterLeave.ToStatusCode(result.Outcome),
            };
        }
        catch (CamusDBException e)
        {
            AuditCluster("leave", principal, e.Code, failure: true);
            return Failure(e);
        }
    }

    /// <summary>
    /// Commits a per-partition replication-factor override (0 clears it). Leader-only like every
    /// placement-map mutation: a follower refuses with the reason and the caller retries against
    /// the meta-partition leader. The change adjusts the target only; with the rebalancer on, the
    /// replica set converges on later passes.
    /// </summary>
    [HttpPost]
    [Route("/v1/cluster/replication-factor")]
    public async Task<JsonResult> SetReplicationFactor()
    {
        Principal? principal = null;
        try
        {
            principal = await EnsureClusterAdminAllowedAsync().ConfigureAwait(false);

            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync(HttpContext.RequestAborted).ConfigureAwait(false);

            SetReplicationFactorRequest? request = string.IsNullOrWhiteSpace(body)
                ? null
                : JsonSerializer.Deserialize<SetReplicationFactorRequest>(body, jsonOptions);

            if (request is null || request.PartitionId <= 0 || request.ReplicationFactor < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "'partitionId' must be a data partition (> 0) and 'replicationFactor' must be >= 0 (0 clears the override)");

            SetReplicationFactorResponse response;
            try
            {
                RaftPartitionLifecycleResult result = await kahuna.Raft.SetReplicationFactorAsync(
                    request.PartitionId, request.ReplicationFactor, HttpContext.RequestAborted).ConfigureAwait(false);

                response = new()
                {
                    Success = result.Success,
                    Status = result.Status.ToString(),
                    Generation = result.Generation,
                    Reason = result.Success ? null : "The override was not committed; see status.",
                };
            }
            catch (RaftException ex)
            {
                // Kommander refuses by throwing (not initialized, system partition, follower);
                // surface the reason so the caller can retry against the leader instead of a 500.
                response = new()
                {
                    Success = false,
                    Status = "Refused",
                    Reason = ex.Message,
                };
            }

            AuditCluster($"replication-factor:{request.PartitionId}={request.ReplicationFactor}",
                principal, response.Success ? "ok" : response.Status, failure: !response.Success);

            return new JsonResult(response)
            {
                StatusCode = response.Success ? StatusCodes.Status200OK : StatusCodes.Status409Conflict,
            };
        }
        catch (CamusDBException e)
        {
            AuditCluster("replication-factor", principal, e.Code, failure: true);
            return Failure(e);
        }
    }

    /// <summary>
    /// Hands leadership of one data partition from this node to another replica of it: a graceful
    /// handover, not a failover. The local node must lead the partition; the consensus layer asks
    /// the target to campaign once it is caught up and waits up to ten seconds for the new leader
    /// to be observed. This is the lever a reliability harness needs for "leader transfer at chosen
    /// commit points" — exercising the handover path with commits in flight, which no crash fault
    /// reaches — and what an operator uses to drain leadership off a node before maintenance.
    ///
    /// <para>Refusals are reported as an outcome rather than a 500: <c>NodeIsNotLeader</c> when this
    /// node does not lead the partition (the caller retries against the leader from the placement
    /// table), <c>LeaderAlreadyElected</c> when leadership moved elsewhere during the handover, and
    /// <c>Refused</c> when consensus would not take the request at all (not initialized, unknown
    /// partition). A 409 carries every non-success outcome so a script can branch on the body.</para>
    /// </summary>
    [HttpPost]
    [Route("/v1/cluster/transfer-leadership")]
    public async Task<JsonResult> TransferLeadership()
    {
        Principal? principal = null;
        ClusterTransferLeadershipRequest? request = null;
        try
        {
            principal = await EnsureClusterAdminAllowedAsync().ConfigureAwait(false);

            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync(HttpContext.RequestAborted).ConfigureAwait(false);

            request = string.IsNullOrWhiteSpace(body)
                ? null
                : JsonSerializer.Deserialize<ClusterTransferLeadershipRequest>(body, jsonOptions);

            if (request is null || request.PartitionId <= 0 || string.IsNullOrWhiteSpace(request.TargetEndpoint))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "'partitionId' must be a data partition (> 0) and 'targetEndpoint' the consensus endpoint of a replica that hosts it");

            ClusterTransferLeadershipResponse response = new()
            {
                PartitionId = request.PartitionId,
                TargetEndpoint = request.TargetEndpoint,
            };

            try
            {
                RaftOperationStatus status = await kahuna.Raft.TransferLeadershipAsync(
                    request.PartitionId, request.TargetEndpoint, HttpContext.RequestAborted).ConfigureAwait(false);

                response.Success = status == RaftOperationStatus.Success;
                response.Status = status.ToString();
                response.Reason = status switch
                {
                    RaftOperationStatus.Success => null,
                    RaftOperationStatus.NodeIsNotLeader => "This node does not lead the partition; send the request to its leader.",
                    RaftOperationStatus.LeaderAlreadyElected => "Leadership moved to another replica during the handover.",
                    RaftOperationStatus.Errored => "Consensus refused the request: the node is not initialized or the partition is unknown here.",
                    _ => "The handover did not complete; see status.",
                };
            }
            catch (RaftException ex)
            {
                // Kommander refuses by throwing for a system partition or an unknown target; surface
                // the reason so the caller can correct the request instead of reading a 500.
                response.Success = false;
                response.Status = "Refused";
                response.Reason = ex.Message;
            }

            AuditCluster($"transfer-leadership:{request.PartitionId}->{request.TargetEndpoint}",
                principal, response.Success ? "ok" : response.Status, failure: !response.Success);

            return new JsonResult(response)
            {
                StatusCode = response.Success ? StatusCodes.Status200OK : StatusCodes.Status409Conflict,
            };
        }
        catch (CamusDBException e)
        {
            AuditCluster("transfer-leadership", principal, e.Code, failure: true);
            return new JsonResult(new ClusterTransferLeadershipResponse
            {
                Success = false,
                Status = "Refused",
                PartitionId = request?.PartitionId ?? 0,
                TargetEndpoint = request?.TargetEndpoint ?? "",
                Reason = e.Message,
            })
            {
                // A malformed request is the caller's error (400); everything else follows the code's mapping.
                StatusCode = e.Code == CamusDBErrorCodes.InvalidInput
                    ? StatusCodes.Status400BadRequest
                    : CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
    }

    /// <summary>
    /// Divides one key space of a relation at its median key, through Kahuna's manual range split.
    /// <c>database</c> and <c>table</c> name the relation; <c>index</c> null names its row space, a name
    /// names one of its readable indexes. The split key is chosen here (<see cref="RangeSplitKeyChooser"/>):
    /// a client cannot name a row key, because row keys are ordered by the stored row id and not by any
    /// column.
    ///
    /// <para>Why this exists: a split's timing is otherwise a function of load and of the trigger's
    /// settle windows. A fault-injection harness that must place a cutover at a chosen moment, such as
    /// inside the window in which one node is down, needs a call that starts one now. The split itself
    /// is the same quiesce, copy, hand-off and cutover the trigger runs; nothing about the split policy
    /// is bypassed, so a split the trigger would refuse is refused here for the same reason.</para>
    ///
    /// <para>Kahuna runs a split only on the node that leads the partition owning the range map; any
    /// other node answers <c>NotLeader</c> and names the leader when gossip knows it, so the caller tries
    /// there. Refusals are reported as an outcome rather than a 500, with Kahuna's own status names; an
    /// indeterminate status (<c>QuiesceFailed</c>, <c>TransferFailed</c>, <c>CutoverFailed</c>,
    /// <c>ConcurrentSplit</c>) means the map may still change and the caller re-reads the ranges before
    /// concluding anything. Held to the bar of the other mutations: superuser when authentication is
    /// enabled, loopback-only otherwise.</para>
    /// </summary>
    [HttpPost]
    [Route("/v1/cluster/split-range")]
    public async Task<JsonResult> SplitRange()
    {
        Principal? principal = null;
        ClusterSplitRangeRequest? request = null;
        ClusterSplitRangeResponse response = new();

        try
        {
            principal = await EnsureClusterAdminAllowedAsync().ConfigureAwait(false);

            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync(HttpContext.RequestAborted).ConfigureAwait(false);

            request = string.IsNullOrWhiteSpace(body)
                ? null
                : JsonSerializer.Deserialize<ClusterSplitRangeRequest>(body, jsonOptions);

            if (request is null || string.IsNullOrWhiteSpace(request.Database) || string.IsNullOrWhiteSpace(request.Table))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "'database' and 'table' must name the relation whose key space to split; 'index' names one of its indexes, or is omitted for the row space");

            if (request.MaxKeys < 2)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "'maxKeys' must be at least 2: a split needs a key on each side of the split key");

            DatabaseDescriptor database = await executor.OpenDatabase(request.Database).ConfigureAwait(false);
            TableDescriptor table = await executor
                .OpenTableWithDescriptor(database, new OpenTableTicket(request.Database, request.Table))
                .ConfigureAwait(false);

            string keySpace = ShowRangesReader.ResolveKeySpace(table, request.Index);
            response.KeySpace = keySpace;

            // The node-local applied view of the space's ranges: how many there are, and where each
            // starts, which the chooser needs so the split key is never a range's own start.
            HashSet<string> rangeStarts = new(StringComparer.Ordinal);
            int descriptors = 0;
            string routingMode = "";

            foreach (KahunaKeySpaceRangesResponse space in kahuna.Kahuna.GetRangeMap(keySpace).KeySpaces)
            {
                if (!string.Equals(space.KeySpace, keySpace, StringComparison.Ordinal))
                    continue;

                routingMode = space.RoutingMode;
                descriptors = space.Descriptors.Count;

                foreach (KahunaRangeDescriptorResponse descriptor in space.Descriptors)
                {
                    if (descriptor.StartKey is not null)
                        rangeStarts.Add(descriptor.StartKey);
                }
            }

            response.DescriptorsBefore = descriptors;

            if (descriptors == 0 || !string.Equals(routingMode, "KeyRange", StringComparison.Ordinal))
            {
                response.Status = "NoRange";
                response.Reason = $"Key space '{keySpace}' has no key-range descriptor on this node: the space is hash-routed, not yet seeded, or the node has not applied the range map yet";
            }
            else
            {
                RangeSplitKeyChooser.Choice choice = await RangeSplitKeyChooser
                    .PickMedianAsync(kahuna.Kahuna, keySpace, rangeStarts, request.MaxKeys, HttpContext.RequestAborted)
                    .ConfigureAwait(false);

                response.KeysSeen = choice.KeysSeen;

                if (choice.Truncated)
                {
                    response.Status = "Refused";
                    response.Reason = $"Key space '{keySpace}' holds more than {request.MaxKeys} keys; raise 'maxKeys' to let the node scan it for a median";
                }
                else if (choice.SplitKey is null)
                {
                    response.Status = "BelowMinRangeSize";
                    response.Reason = $"Key space '{keySpace}' holds {choice.KeysSeen} key(s) from its median up that are not already range starts; a split needs a key on each side";
                }
                else
                {
                    response.SplitKey = choice.SplitKey;

                    KahunaSplitRangeResponse outcome = await kahuna.Kahuna
                        .SplitRangeAtKeyWithOutcomeAsync(keySpace, choice.SplitKey, HttpContext.RequestAborted)
                        .ConfigureAwait(false);

                    response.Success = outcome.Success;
                    response.Status = outcome.Status;
                    response.Determinate = outcome.Determinate;
                    response.NewPartitionId = outcome.NewPartitionId;
                    response.LeaderHint = outcome.LeaderHint;
                    response.Reason = outcome.Reason;
                }
            }

            AuditCluster($"split-range:{keySpace}@{response.SplitKey ?? "-"}",
                principal, response.Success ? "ok" : response.Status, failure: !response.Success);

            return new JsonResult(response)
            {
                StatusCode = response.Success ? StatusCodes.Status200OK : StatusCodes.Status409Conflict,
            };
        }
        catch (CamusDBException e)
        {
            AuditCluster("split-range", principal, e.Code, failure: true);
            response.Success = false;
            response.Status = "Refused";
            response.Reason = e.Message;

            return new JsonResult(response)
            {
                StatusCode = e.Code == CamusDBErrorCodes.InvalidInput
                    ? StatusCodes.Status400BadRequest
                    : CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
    }

    /// <summary>
    /// Builds the readiness answer for one node. Internal rather than private so the dashboard's
    /// summary endpoint reports readiness from this one implementation: a second copy of the
    /// "which roles serve" and "what counts as stalled" rules would drift from this one, and the
    /// dashboard would then disagree with the probe an orchestrator trusts.
    /// </summary>
    internal static ClusterHealthResponse BuildHealth(IRaft raft, IKahuna kahuna)
    {
        bool initialized;
        string localRole;
        int hostedPartitions = 0;
        List<string>? partitionStorageFaults = null;

        try
        {
            initialized = raft.IsInitialized;
            localRole = raft.LocalRole.ToString();
        }
        catch (Exception)
        {
            // A node so early in boot that membership state is not constructed yet cannot serve;
            // fail closed (503) rather than surface an unclassifiable 500 to orchestrator probes.
            return new()
            {
                Ready = false,
                Initialized = false,
                LocalRole = nameof(ClusterMemberRole.NotMember),
            };
        }

        // Informational only, never a readiness condition: a node hosting zero data partitions
        // still serves every key by forwarding — so a failure computing the count must not flip
        // readiness either.
        if (initialized)
        {
            try
            {
                foreach (RaftPartitionRange range in raft.GetPartitionMap())
                {
                    if (range.State == RaftPartitionState.Removed || !raft.HostsPartition(range.PartitionId))
                        continue;

                    hostedPartitions++;

                    // A partition with a storage verdict is not served from this node: its log lost
                    // acknowledged entries, its store lacks a prefix the log no longer holds, its log
                    // engine fail-stopped, or its restore failed. A restore in progress is not a fault.
                    RaftPartitionStorageHealth? storage = raft.GetPartitionStorageHealth(range.PartitionId);
                    if (storage is not null && storage.State is not (RaftPartitionStorageState.Healthy or RaftPartitionStorageState.Restoring))
                        (partitionStorageFaults ??= []).Add($"#{range.PartitionId} {storage}");
                }
            }
            catch (Exception)
            {
                hostedPartitions = 0;
            }
        }

        // The node's own store: fail-stopped on damage, or found empty at start while the log holds
        // history. Each is terminal for the process, so this one fails closed. A node that cannot read
        // its own storage health must not report ready.
        string? storageFault;
        try
        {
            storageFault = kahuna.GetStorageHealth().Fault;
        }
        catch (Exception)
        {
            storageFault = "The storage health of this node could not be read.";
        }

        // Neither NotMember (evicted while down) nor Leaving (decommissioned, on its way out) is a
        // serving role: reporting a decommissioned node as ready would keep load balancers routing
        // to a node the cluster already dropped.
        ClusterHealthResponse response = new()
        {
            Ready = initialized
                && localRole != nameof(ClusterMemberRole.NotMember)
                && localRole != nameof(ClusterMemberRole.Leaving)
                && storageFault is null
                && partitionStorageFaults is null,
            Initialized = initialized,
            LocalRole = localRole,
            HostedPartitions = hostedPartitions,
            StorageFault = storageFault,
            PartitionStorageFaults = partitionStorageFaults,
        };

        // Replication liveness, joined into health so a stalled partition is externally
        // detectable: a partition this node leads with an open backfill-refusal episode, or with
        // a snapshot rescue that is failing or unproducible, is degraded even while its healthy
        // quorum keeps committing — the Caraxes soaks watched exactly this state report every
        // green signal for 83 dead minutes. Deliberately NOT a readiness condition (see the
        // response model), and fail-open: a failure computing it must not flip a healthy probe.
        try
        {
            foreach (int partitionId in KnownPartitionIds(raft))
            {
                IReadOnlyList<RaftBackfillStatus> backfill = raft.GetBackfillStatuses(partitionId);
                IReadOnlyList<RaftSnapshotStatus> snapshots = raft.GetSnapshotStatuses(partitionId);

                bool snapshotTrouble = false;
                bool unproducible = false;
                bool inFlight = false;
                int failedAttempts = 0;

                foreach (RaftSnapshotStatus status in snapshots)
                {
                    unproducible |= status.Unproducible;
                    inFlight |= status.InFlight;
                    if (status.FailedAttempts > failedAttempts)
                        failedAttempts = status.FailedAttempts;
                    snapshotTrouble |= status.Unproducible || status.FailedAttempts > 0;
                }

                // An in-flight transfer with no failures is the rescue working, not a stall.
                if (backfill.Count == 0 && !snapshotTrouble)
                    continue;

                response.StalledPartitions.Add(new ClusterStalledPartitionModel
                {
                    PartitionId = partitionId,
                    OpenBackfillRefusals = backfill.Count,
                    SnapshotUnproducible = unproducible,
                    SnapshotFailedAttempts = failedAttempts,
                    SnapshotInFlight = inFlight,
                    CommitIndex = raft.GetCommitIndex(partitionId),
                });
            }

            response.CommitStalled = response.StalledPartitions.Count > 0;
        }
        catch (Exception)
        {
            response.CommitStalled = false;
            response.StalledPartitions.Clear();
        }

        return response;
    }

    /// <summary>
    /// The partition ids worth querying for replication diagnostics: the system partition plus
    /// every non-removed partition in the committed map. Non-hosted partitions answer with empty
    /// statuses (and a -1 commit index), so callers need no hosting check.
    /// </summary>
    private static IEnumerable<int> KnownPartitionIds(IRaft raft)
    {
        yield return RaftSystemConfig.SystemPartition;

        if (!raft.IsInitialized)
            yield break;

        foreach (RaftPartitionRange range in raft.GetPartitionMap())
        {
            if (range.State != RaftPartitionState.Removed)
                yield return range.PartitionId;
        }
    }

    /// <summary>
    /// Fail-closed gate for the two topology mutations, mirroring the backup admin surface: never a
    /// credential over plaintext; superuser required when authentication is enabled (enforced here —
    /// there is no executor downstream of these endpoints); loopback-only when it is not, so a
    /// network peer can never decommission a node of an unauthenticated cluster.
    /// </summary>
    private async Task<Principal?> EnsureClusterAdminAllowedAsync()
    {
        EnsureSecureTransport();

        if (options.AuthenticationEnabled)
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);
            if (principal is null || !principal.IsSuperuser)
                throw new CamusDBException(
                    CamusDBErrorCodes.InsufficientPrivilege,
                    "Cluster administration requires a superuser");

            return principal;
        }

        IPAddress? remote = HttpContext.Connection.RemoteIpAddress;
        if (remote is null || !IPAddress.IsLoopback(remote))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                "Cluster administration over the network requires authentication to be enabled");

        return null;
    }

    /// <summary>
    /// One audit record per topology mutation and its outcome, so a privileged cluster operation is
    /// always attributable: the operation, the acting user (or anonymous/unauthenticated), the
    /// remote address, and the result. Failures log at Warning so they surface in a security review.
    /// </summary>
    private void AuditCluster(string operation, Principal? principal, string outcome, bool failure)
    {
        string user = principal?.UserName ?? (options.AuthenticationEnabled ? "unauthenticated" : "anonymous");
        string remote = HttpContext.Connection.RemoteIpAddress?.ToString() ?? "unknown";

        if (failure)
            logger.LogWarning("Cluster admin audit: op={Operation} user={User} remote={Remote} outcome={Outcome}",
                operation, user, remote, outcome);
        else if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Cluster admin audit: op={Operation} user={User} remote={Remote} outcome={Outcome}",
                operation, user, remote, outcome);
    }

    /// <summary>
    /// Refuses a topology read to a caller who is not a superuser while authentication is on, and
    /// returns null when the read may proceed. See the class summary for why the bar sits here.
    /// </summary>
    private async Task<JsonResult?> RefuseTopologyReadAsync()
    {
        if (!options.AuthenticationEnabled)
            return null;

        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);
            if (principal is not null && principal.IsSuperuser)
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege, "Cluster topology requires a superuser");
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return new JsonResult(new { status = "failed", code = e.Code, message = e.Message })
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
    }

    /// <summary>
    /// Whether the readiness probe may include topology detail for this caller. Never throws: the
    /// probe route is exempt from authentication, so a missing, invalid or plaintext credential only
    /// narrows the answer — failing the probe over it would report a healthy node as down.
    /// </summary>
    private async Task<bool> CanReadTopologyQuietlyAsync()
    {
        if (!options.AuthenticationEnabled)
            return true;

        if (string.IsNullOrEmpty(Request.Headers.Authorization.ToString()))
            return false;

        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);
            return principal is not null && principal.IsSuperuser;
        }
        catch (CamusDBException)
        {
            return false;
        }
    }

    private static JsonResult Failure(CamusDBException e) => new(new ClusterLeaveResponse
    {
        Left = false,
        Drained = false,
        Outcome = "Refused",
        Retryable = false,
        Reason = e.Message,
    })
    {
        StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
    };
}
