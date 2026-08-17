/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Tests.Cluster;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// Tests for <see cref="SchemaChangeCoordinator"/> (D1).
///
/// Exercises the staged online-schema-change sequence on a 3-node in-process cluster,
/// verifying that each intermediate version is acked by all nodes before the coordinator
/// advances to the next step.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSchemaChangeCoordinatorCluster
{
    // Pure path-computation unit tests (no cluster) live in CamusDB.Tests
    // (TestSchemaChangeCoordinatorPath) so they stay in the fast suite.

    private static async Task<(InProcessSchemaCluster cluster, string db)> SetUpClusterAndTableAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version);
        return (cluster, db);
    }

    [Test]
    public async Task AddColumn_DrivesDeleteOnlyWriteOnlyToPublic_AllNodesAckEachStep()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create the table.
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Run the coordinator on the leader, recording each step.
        List<(SchemaElementState state, long schemaVersion)> steps = [];

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            coordinator.OnStepCompleted = async (state, schemaVersion) =>
            {
                steps.Add((state, schemaVersion));

                // Verify every live node has acked this version BEFORE the coordinator
                // proceeds to the next transition — this is the two-version invariant.
                await cluster.WaitForSchemaConvergenceAsync(db, schemaVersion, timeout: TimeSpan.FromSeconds(10));

                // Verify the column is in the expected intermediate state on all nodes.
                foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                {
                    TableColumnSchema? col = node.Database?.Schema.Tables
                        .GetValueOrDefault("robots")?.Columns?
                        .FirstOrDefault(c => c.Name == "year");

                    if (state == SchemaElementState.DeleteOnly || state == SchemaElementState.WriteOnly)
                    {
                        Assert.IsNotNull(col, $"Column must exist on node {node.Index} at state {state}");
                        Assert.AreEqual(state, col!.State,
                            $"Column must be in {state} on node {node.Index} after that step");
                    }
                    else if (state == SchemaElementState.Public)
                    {
                        Assert.IsNotNull(col, $"Column must exist on node {node.Index} at Public");
                        Assert.AreEqual(SchemaElementState.Public, col!.State,
                            $"Column must be Public on node {node.Index} after final step");
                    }
                }
            };

            string robotsId = leader.Database!.Schema.Tables["robots"].Id ?? "";
            await coordinator.RunJobAsync(
                leader.Database!,
                new SchemaChangeJob(db, "robots", robotsId, "year", SchemaElementState.Public),
                columnDefinition: new ColumnInfo("year", ColumnType.Integer64)
            );

            version = leader.Database!.Schema.SchemaVersion;
        });

        // Three steps: DeleteOnly → WriteOnly → Public.
        Assert.AreEqual(3, steps.Count, "coordinator must emit exactly 3 transitions for Absent → Public");
        Assert.AreEqual(SchemaElementState.DeleteOnly, steps[0].state);
        Assert.AreEqual(SchemaElementState.WriteOnly, steps[1].state);
        Assert.AreEqual(SchemaElementState.Public, steps[2].state);

        // Each step advances the schema version by exactly 1.
        Assert.AreEqual(steps[0].schemaVersion + 1, steps[1].schemaVersion,
            "WriteOnly step must be one version after DeleteOnly step");
        Assert.AreEqual(steps[1].schemaVersion + 1, steps[2].schemaVersion,
            "Public step must be one version after WriteOnly step");

        // Final convergence and column verification on all nodes.
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "year");

            Assert.IsNotNull(col, $"Column 'year' must exist on node {node.Index}");
            Assert.AreEqual(SchemaElementState.Public, col!.State,
                $"Column 'year' must be Public on node {node.Index} after coordinator finishes");
        }
    }

    [Test]
    public async Task DropColumn_DrivesPublicThroughWriteOnlyDeleteOnlyToAbsent_AllNodesAckEachStep()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table with a column that starts at Public.
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("year", ColumnType.Integer64),
                ],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        List<SchemaElementState> steps = [];

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            coordinator.OnStepCompleted = async (state, schemaVersion) =>
            {
                steps.Add(state);
                await cluster.WaitForSchemaConvergenceAsync(db, schemaVersion, timeout: TimeSpan.FromSeconds(10));
            };

            string robotsId = leader.Database!.Schema.Tables["robots"].Id ?? "";
            await coordinator.RunJobAsync(
                leader.Database!,
                new SchemaChangeJob(db, "robots", robotsId, "year", SchemaElementState.Absent)
            );

            version = leader.Database!.Schema.SchemaVersion;
        });

        Assert.AreEqual(3, steps.Count, "coordinator must emit exactly 3 transitions for Public → Absent");
        Assert.AreEqual(SchemaElementState.WriteOnly, steps[0]);
        Assert.AreEqual(SchemaElementState.DeleteOnly, steps[1]);
        Assert.AreEqual(SchemaElementState.Absent, steps[2]);

        await cluster.WaitForSchemaConvergenceAsync(db, version);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            bool hasYear = node.Database?.Schema.Tables["robots"].Columns?
                .Any(c => c.Name == "year") ?? false;
            Assert.IsFalse(hasYear, $"Column 'year' must be absent on node {node.Index} after coordinator finishes");
        }
    }

    [Test]
    public async Task ResumeFromIntermediateState_CompletesRemainingStepsOnly()
    {
        // When the column already exists in WriteOnly state (e.g. coordinator was interrupted
        // after step 2 and resumed), only the remaining WriteOnly → Public step is emitted.
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Build a table; then drive the column to WriteOnly using a first coordinator run.
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // First run: stop at WriteOnly by targeting WriteOnly.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);
            string robotsId = leader.Database!.Schema.Tables["robots"].Id ?? "";
            await coordinator.RunJobAsync(
                leader.Database!,
                new SchemaChangeJob(db, "robots", robotsId, "year", SchemaElementState.WriteOnly),
                columnDefinition: new ColumnInfo("year", ColumnType.Integer64)
            );
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Verify all nodes are at WriteOnly.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?.FirstOrDefault(c => c.Name == "year");
            Assert.IsNotNull(col, $"Column must exist on node {node.Index}");
            Assert.AreEqual(SchemaElementState.WriteOnly, col!.State, $"Node {node.Index} must be at WriteOnly");
        }

        // Second run: resume from WriteOnly → Public (only one step).
        List<SchemaElementState> resumeSteps = [];
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);
            coordinator.OnStepCompleted = (state, _) => { resumeSteps.Add(state); return Task.CompletedTask; };

            string robotsId = leader.Database!.Schema.Tables["robots"].Id ?? "";
            await coordinator.RunJobAsync(
                leader.Database!,
                new SchemaChangeJob(db, "robots", robotsId, "year", SchemaElementState.Public)
            );
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        Assert.AreEqual(1, resumeSteps.Count, "resuming from WriteOnly must emit exactly one step");
        Assert.AreEqual(SchemaElementState.Public, resumeSteps[0]);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?.FirstOrDefault(c => c.Name == "year");
            Assert.IsNotNull(col, $"Column must exist on node {node.Index} after resume");
            Assert.AreEqual(SchemaElementState.Public, col!.State, $"Column must be Public on node {node.Index} after resume");
        }
    }

    [Test]
    public async Task AlreadyAtTarget_NoStepsEmitted()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("year", ColumnType.Integer64),
                ],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Column is already Public; running the coordinator toward Public must be a no-op.
        int stepCount = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);
            coordinator.OnStepCompleted = (_, _) => { stepCount++; return Task.CompletedTask; };

            string robotsId = leader.Database!.Schema.Tables["robots"].Id ?? "";
            await coordinator.RunJobAsync(
                leader.Database!,
                new SchemaChangeJob(db, "robots", robotsId, "year", SchemaElementState.Public)
            );
        });

        Assert.AreEqual(0, stepCount, "no steps must be emitted when already at target state");
    }

    // ── leader-change resume ──────────────────────────────────────────────────

    /// <summary>
    /// Simulates a coordinator interrupted after its first step: the column is at
    /// <c>DeleteOnly</c> and the job is persisted, but no further steps ran.
    /// A fresh <see cref="SchemaChangeCoordinator"/> on the same node calls
    /// <see cref="SchemaChangeCoordinator.ResumeJobsAsync"/>, reads the persisted
    /// job, and drives the column to <c>Public</c>.
    /// </summary>
    [Test]
    public async Task PersistedJob_ResumesFromDeleteOnlyToPublic()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Step 1: create the table.
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Step 2: simulate the first coordinator step completing — column lands at DeleteOnly
        // and the coordinator job is persisted to KV.  The coordinator is then "lost"
        // (leadership change, crash, etc.) before advancing to WriteOnly.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);

            await catalogs.ReplicateAddColumnInStateAsync(
                leader.Database!, "robots",
                new ColumnInfo("year", ColumnType.Integer64),
                SchemaElementState.DeleteOnly
            );

            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = leader.Database!.Schema.Tables["robots"].Id ?? "",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
                ColumnType = ColumnType.Integer64,
            });

            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(10));

        // All nodes must see the column at DeleteOnly.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "year");
            Assert.IsNotNull(col, $"Column must exist at DeleteOnly on node {node.Index}");
            Assert.AreEqual(SchemaElementState.DeleteOnly, col!.State,
                $"Column must be DeleteOnly on node {node.Index}");
        }

        // Step 3: a fresh coordinator on the current leader reads the persisted job and
        // resumes the sequence.  In production this is invoked by the OnBecameLeader callback;
        // here we call it directly to avoid the flakiness of a forced leader change.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);
            await coordinator.ResumeJobsAsync(leader.Database!);
            version = leader.Database!.Schema.SchemaVersion;
        });

        // All nodes must converge with the column at Public.
        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(10));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "year");
            Assert.IsNotNull(col, $"Column 'year' must exist on node {node.Index} after resume");
            Assert.AreEqual(SchemaElementState.Public, col!.State,
                $"Column 'year' must be Public on node {node.Index} after coordinator resume");
        }
    }

    [Test]
    public async Task PersistedJob_IsDeletedAfterSuccessfulCompletion()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            string robotsId = leader.Database!.Schema.Tables["robots"].Id ?? "";
            await coordinator.RunJobAsync(
                leader.Database!,
                new SchemaChangeJob(db, "robots", robotsId, "year", SchemaElementState.Public),
                columnDefinition: new ColumnInfo("year", ColumnType.Integer64)
            );

            // After successful completion the job must be deleted from KV.
            List<PersistedCoordinatorJob> remaining = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(0, remaining.Count,
                "Persisted coordinator job must be cleaned up after successful completion");

            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(10));
    }

    [Test]
    public async Task ResumeJobsAsync_AbandonsJobThatExceededAttemptBudget()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            // A job that has already burned far more than the retry budget.
            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = leader.Database!.Schema.Tables["robots"].Id ?? "",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
                ColumnType = ColumnType.Integer64,
                Attempts = 100,
            });

            await coordinator.ResumeJobsAsync(leader.Database!);

            // The over-budget job must be abandoned (deleted), not driven.
            List<PersistedCoordinatorJob> remaining = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(0, remaining.Count, "over-budget job must be deleted, not retried");

            bool hasYear = leader.Database!.Schema.Tables["robots"].Columns?.Any(c => c.Name == "year") ?? false;
            Assert.IsFalse(hasYear, "abandoned job must NOT be driven — the column must not be created");
        });
    }

    [Test]
    public async Task ResumeJobsAsync_DeletesAlreadyCompletedJob()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("year", ColumnType.Integer64)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            // Stale job whose target ('year' Public) is already reached — e.g. the previous
            // leader finished the last step but crashed before deleting the record.
            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = leader.Database!.Schema.Tables["robots"].Id ?? "",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
            });

            long before = leader.Database!.Schema.SchemaVersion;
            await coordinator.ResumeJobsAsync(leader.Database!);

            List<PersistedCoordinatorJob> remaining = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(0, remaining.Count, "already-completed job must be cleaned up on resume");
            Assert.AreEqual(before, leader.Database!.Schema.SchemaVersion,
                "no transition should be emitted for a job already at its target");
        });
    }

    // ── DROP TABLE cleans up coordinator job records ─────────────────────────

    /// <summary>
    /// After <c>DROP TABLE T</c>, no coordinator job record keyed on <c>T</c> should remain
    /// in the KV store, even when a job was persisted mid-sequence before the drop.
    /// </summary>
    [Test]
    public async Task DropTable_DeletesPersistedCoordinatorJobsForTable()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Persist a coordinator job simulating an interrupted add-column sequence.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);

            await catalogs.ReplicateAddColumnInStateAsync(
                leader.Database!, "robots",
                new ColumnInfo("year", ColumnType.Integer64),
                SchemaElementState.DeleteOnly
            );

            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = leader.Database!.Schema.Tables["robots"].Id ?? "",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
                ColumnType = ColumnType.Integer64,
            });

            List<PersistedCoordinatorJob> before = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(1, before.Count, "job must be present before drop");

            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Drop the table — this should also clean up the coordinator job record.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.DropTable(new DropTableTicket(db, "robots", ifExists: false));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // No coordinator job record for the dropped table should remain.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            List<PersistedCoordinatorJob> remaining = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(0, remaining.Count,
                "DROP TABLE must delete all coordinator job records for the table");
        });
    }

    /// <summary>
    /// Drop/recreate-same-name aliasing guard: a coordinator job persisted for the old table T
    /// must not be resumed against the new T after DROP + CREATE TABLE with the same name.
    /// After the drop-time cleanup, <c>ResumeJobsAsync</c> must find no jobs, leaving the new
    /// table completely untouched.
    /// </summary>
    [Test]
    public async Task DropTable_ThenRecreateWithSameName_ResumeDoesNotTouchNewTable()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Simulate an interrupted add-column on the old table.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);

            await catalogs.ReplicateAddColumnInStateAsync(
                leader.Database!, "robots",
                new ColumnInfo("year", ColumnType.Integer64),
                SchemaElementState.DeleteOnly
            );

            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = leader.Database!.Schema.Tables["robots"].Id ?? "",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
                ColumnType = ColumnType.Integer64,
            });

            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Drop the old table (this deletes the coordinator job record).
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.DropTable(new DropTableTicket(db, "robots", ifExists: false));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Create a brand-new table with the same name (different internal id).
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Force a coordinator resume — must find no jobs (cleaned up at drop time).
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            long versionBefore = leader.Database!.Schema.SchemaVersion;
            await coordinator.ResumeJobsAsync(leader.Database!);

            // Schema version must not advance — no job was resumed.
            Assert.AreEqual(versionBefore, leader.Database!.Schema.SchemaVersion,
                "ResumeJobsAsync must not drive any transitions when no jobs remain after DROP TABLE");

            // The new 'robots' table must have only the original 'id' column, no 'year'.
            TableSchema? newTable = leader.Database!.Schema.Tables.GetValueOrDefault("robots");
            Assert.IsNotNull(newTable, "recreated 'robots' table must exist");
            bool hasYear = newTable!.Columns?.Any(c => c.Name == "year") ?? false;
            Assert.IsFalse(hasYear, "new 'robots' table must not have 'year' column added by stale coordinator job");
        });
    }

    // ── table-id anchor prevents aliasing even without drop-time cleanup ────────

    /// <summary>
    /// Id-mismatch guard: a persisted coordinator job whose <c>TableId</c> does not
    /// correspond to any live table is deleted and skipped on resume — no schema transition
    /// fires. This is the structural backstop that prevents aliasing even if the drop-time
    /// cleanup ever misses a record.
    /// </summary>
    [Test]
    public async Task ResumeJobsAsync_DeletesJobWhoseTableIdNoLongerExists()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Persist a job with a TableId that doesn't match the live robots table (simulates
        // a stale job left over after a DROP + CREATE cycle where drop-time cleanup was skipped).
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);
            SchemaChangeCoordinator coordinator = new(catalogs);

            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = "stale_table_id_that_does_not_exist",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
                ColumnType = ColumnType.Integer64,
            });

            long versionBefore = leader.Database!.Schema.SchemaVersion;
            await coordinator.ResumeJobsAsync(leader.Database!);

            // No transition must fire — the stale job must be deleted, not driven.
            Assert.AreEqual(versionBefore, leader.Database!.Schema.SchemaVersion,
                "ResumeJobsAsync must not drive any transitions for a job with a stale TableId");

            // The stale job record must have been deleted.
            List<PersistedCoordinatorJob> remaining = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(0, remaining.Count,
                "Stale job (unknown TableId) must be deleted by ResumeJobsAsync, not retried");

            // The live 'robots' table must be untouched — no 'year' column.
            bool hasYear = leader.Database!.Schema.Tables["robots"].Columns?.Any(c => c.Name == "year") ?? false;
            Assert.IsFalse(hasYear,
                "ResumeJobsAsync must not add 'year' to the live table when the job's TableId is stale");
        });
    }

    /// <summary>
    /// Full leader-change scenario: a coordinator runs the first step on node A, persists its job,
    /// then node B wins a new election (<c>ForceLeaderForTestingAsync</c>).  A fresh
    /// coordinator on node B calls <c>ResumeJobsAsync</c>, reads the persisted job, and
    /// drives the column to <c>Public</c>.  All nodes converge.
    /// </summary>
    [Test]
    public async Task LeaderChange_NewLeaderResumesPersistedCoordinatorJob()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Step 1: create the table.
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Step 2: simulate "coordinator ran first step" — column at DeleteOnly, job persisted.
        int originalLeaderIndex = -1;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            originalLeaderIndex = leader.Index;
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);

            await catalogs.ReplicateAddColumnInStateAsync(
                leader.Database!, "robots",
                new ColumnInfo("year", ColumnType.Integer64),
                SchemaElementState.DeleteOnly
            );

            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName = "robots",
                TableId = leader.Database!.Schema.Tables["robots"].Id ?? "",
                ElementName = "year",
                TargetState = SchemaElementState.Public,
                ColumnType = ColumnType.Integer64,
            });

            // Verify job is readable immediately after write on the same node.
            List<PersistedCoordinatorJob> writtenJobs = await catalogs.LoadCoordinatorJobsAsync(leader.Database!);
            Assert.AreEqual(1, writtenJobs.Count, $"Job must be readable right after write on node {leader.Index}");

            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(10));

        // All nodes see the column at DeleteOnly.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "year");
            Assert.IsNotNull(col, $"Column must exist at DeleteOnly on node {node.Index}");
            Assert.AreEqual(SchemaElementState.DeleteOnly, col!.State,
                $"Column must be DeleteOnly on node {node.Index}");
        }

        // Step 3: elect a DIFFERENT node as leader using ForceLeaderForTestingAsync.
        // Unlike ForceLeaderChangeAsync, this does NOT block the old leader — the
        // cluster remains fully stable, so WaitForSchemaAcksAsync succeeds with all 3 nodes.
        InProcessSchemaCluster.Node newLeader = cluster.Nodes.First(n => n.Index != originalLeaderIndex);
        await cluster.TransferSchemaLeadershipAsync(db, newLeader, timeout: TimeSpan.FromSeconds(30));

        // Step 4: the production OnBecameLeader callback (wired by DatabaseOpener via
        // SchemaReplicator.Register) fires ResumeJobsAsync automatically on the new leader.
        // ResumeJobsAsync retries with backoff to handle the KV apply race (OnLeaderChanged
        // fires before the new leader's KV state machine has applied all committed entries).
        // Wait for all nodes to converge two versions ahead: DeleteOnly → WriteOnly → Public.
        await cluster.WaitForSchemaConvergenceAsync(db, version + 2, timeout: TimeSpan.FromSeconds(60));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "year");
            Assert.IsNotNull(col, $"Column 'year' must exist on node {node.Index}");
            Assert.AreEqual(SchemaElementState.Public, col!.State,
                $"Column 'year' must be Public on node {node.Index} after new leader resumes job");
        }
    }
}
