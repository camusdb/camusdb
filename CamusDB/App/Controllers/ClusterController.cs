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
using Kahuna.Communication.External;
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
/// <para>Reads are unauthenticated like <c>/ping</c>: probes must work before credentials exist,
/// and the roster/placement expose operational metadata only. The two mutations (leave,
/// replication-factor) follow the backup admin gate: superuser when authentication is enabled,
/// loopback-only otherwise, and never a credential over plaintext.</para>
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
    /// </summary>
    [HttpGet]
    [Route("/v1/cluster/health")]
    public JsonResult GetHealth()
    {
        ClusterHealthResponse health = BuildHealth(kahuna.Raft);
        return new JsonResult(health)
        {
            StatusCode = health.Ready ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable,
        };
    }

    [HttpGet]
    [Route("/v1/cluster/membership")]
    public JsonResult GetMembership()
    {
        IRaft raft = kahuna.Raft;

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

        return new JsonResult(response);
    }

    [HttpGet]
    [Route("/v1/cluster/placement")]
    public JsonResult GetPlacement()
    {
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
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

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

    private static ClusterHealthResponse BuildHealth(IRaft raft)
    {
        bool initialized;
        string localRole;
        int hostedPartitions = 0;

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
                    if (range.State != RaftPartitionState.Removed && raft.HostsPartition(range.PartitionId))
                        hostedPartitions++;
            }
            catch (Exception)
            {
                hostedPartitions = 0;
            }
        }

        // Neither NotMember (evicted while down) nor Leaving (decommissioned, on its way out) is a
        // serving role: reporting a decommissioned node as ready would keep load balancers routing
        // to a node the cluster already dropped.
        return new()
        {
            Ready = initialized
                && localRole != nameof(ClusterMemberRole.NotMember)
                && localRole != nameof(ClusterMemberRole.Leaving),
            Initialized = initialized,
            LocalRole = localRole,
            HostedPartitions = hostedPartitions,
        };
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
