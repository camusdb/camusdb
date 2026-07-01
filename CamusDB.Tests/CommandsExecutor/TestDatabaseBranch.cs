
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for BR2: CREATE DATABASE name BRANCH FROM source — SQL syntax, ticket API,
/// ancestry threading, schema copy, and error paths.
/// </summary>
[NonParallelizable]
internal sealed class TestDatabaseBranch : BaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    // -----------------------------------------------------------------------
    // SQL parsing: both BRANCH FROM alternatives round-trip through ExecuteDDLSQL
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_Via_SQL_ProducesDescriptorWithAncestor()
    {
        // Use prefixed names so both are always valid SQL identifiers (no leading digit).
        string rootName = NewName();
        CommandExecutor rootExecutor = CreateCommandExecutor();
        DatabaseDescriptor rootDescriptor = await rootExecutor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, rootExecutor);

        string branchName = NewName();
        DatabaseDescriptor branchDescriptor = (await rootExecutor.ExecuteDDLSQL(
            new ExecuteSQLTicket(txnState: null!, database: rootName, sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null))
        ).Database!;
        TrackDatabase(branchName, rootExecutor);

        Assert.IsNotNull(branchDescriptor, "ExecuteDDLSQL must return the branch descriptor");
        Assert.AreEqual(1, branchDescriptor.Ancestors.Count, "branch must carry one ancestor");
        Assert.AreEqual(rootDescriptor.Id, branchDescriptor.Ancestors[0].DatabaseId,
            "ancestor DatabaseId must match the source");
        Assert.IsFalse(branchDescriptor.Ancestors[0].ForkTimestamp.IsNull(),
            "fork timestamp must be non-null");
    }

    [Test]
    public async Task CreateBranch_Via_SQL_IfNotExists_Idempotent()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        string branchName = NewName();

        // First creation succeeds.
        await executor.ExecuteDDLSQL(
            new ExecuteSQLTicket(txnState: null!, database: rootName, sql: $"CREATE DATABASE IF NOT EXISTS {branchName} BRANCH FROM {rootName}", parameters: null));
        TrackDatabase(branchName, executor);

        // Second creation is idempotent (IF NOT EXISTS).
        Assert.DoesNotThrowAsync(async () =>
            await executor.ExecuteDDLSQL(
                new ExecuteSQLTicket(txnState: null!, database: rootName, sql: $"CREATE DATABASE IF NOT EXISTS {branchName} BRANCH FROM {rootName}", parameters: null)));
    }

    [Test]
    public void CreateBranch_Via_SQL_Duplicate_Throws()
    {
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            string rootName = NewName();
            CommandExecutor executor = CreateCommandExecutor();
            await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
            TrackDatabase(rootName, executor);

            string branchName = NewName();

            await executor.ExecuteDDLSQL(
                new ExecuteSQLTicket(txnState: null!, database: rootName, sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null));
            TrackDatabase(branchName, executor);

            // Second creation without IF NOT EXISTS must throw.
            await executor.ExecuteDDLSQL(
                new ExecuteSQLTicket(txnState: null!, database: rootName, sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null));
        });
    }

    // -----------------------------------------------------------------------
    // Ticket API
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_Via_Ticket_ProducesDescriptorWithAncestor()
    {
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        Assert.AreEqual(1, branch.Ancestors.Count);
        Assert.AreEqual(rootDescriptor.Id, branch.Ancestors[0].DatabaseId);
    }

    [Test]
    public async Task CreateBranch_SourceNotExist_Throws()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateDatabase(
                new CreateDatabaseTicket(NewName(), ifNotExists: false, branchFrom: "nonexistent_db_xyz"))
        )!;

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex.Code,
            "must fail with DatabaseDoesntExist when source is missing");
    }

    [Test]
    public async Task CreateBranch_TargetExists_Throws()
    {
        (string rootName, _, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        string branchName = NewName();
        await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateDatabase(
                new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName))
        )!;

        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex.Code,
            "duplicate branch target without IF NOT EXISTS must fail");
    }

    [Test]
    public async Task CreateBranch_TargetExists_IfNotExists_Succeeds()
    {
        (string rootName, _, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        string branchName = NewName();
        DatabaseDescriptor first = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // IF NOT EXISTS on a duplicate must succeed and return the existing descriptor.
        DatabaseDescriptor second = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: true, branchFrom: rootName));

        Assert.AreEqual(first.Id, second.Id, "both opens must resolve to the same database id");
    }

    // -----------------------------------------------------------------------
    // Schema copy: branch opens with the source's tables
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_SchemaCopied_TablesVisibleOnBranch()
    {
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        // Create a table in the root database.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE products (id OBJECT_ID PRIMARY KEY, name STRING, price FLOAT64)",
            parameters: null));

        // Branch the root.
        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // The branch must see the same table.
        Assert.IsTrue(branch.Schema.Tables.ContainsKey("products"),
            "branch must inherit the 'products' table from the source schema copy");
    }

    [Test]
    public async Task CreateBranch_SchemaVersion_Matches_Source()
    {
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE orders (id OBJECT_ID PRIMARY KEY, amount INT64)",
            parameters: null));

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        Assert.AreEqual(rootDescriptor.Schema.SchemaVersion, branch.Schema.SchemaVersion,
            "branch schema version must match the source at fork time");
    }

    // -----------------------------------------------------------------------
    // Ancestry chain: deep branching
    // -----------------------------------------------------------------------

    [Test]
    public async Task DeepBranch_AncestryChainIsCorrect()
    {
        (string aName, DatabaseDescriptor aDesc, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(aName, executor);

        string bName = NewName();
        DatabaseDescriptor bDesc = await executor.CreateDatabase(
            new CreateDatabaseTicket(bName, ifNotExists: false, branchFrom: aName));
        TrackDatabase(bName, executor);

        string cName = NewName();
        DatabaseDescriptor cDesc = await executor.CreateDatabase(
            new CreateDatabaseTicket(cName, ifNotExists: false, branchFrom: bName));
        TrackDatabase(cName, executor);

        Assert.AreEqual(1, bDesc.Ancestors.Count, "B must have 1 ancestor (A)");
        Assert.AreEqual(aDesc.Id, bDesc.Ancestors[0].DatabaseId);

        Assert.AreEqual(2, cDesc.Ancestors.Count, "C must have 2 ancestors (B, A)");
        Assert.AreEqual(bDesc.Id, cDesc.Ancestors[0].DatabaseId, "C[0] must be B");
        Assert.AreEqual(aDesc.Id, cDesc.Ancestors[1].DatabaseId, "C[1] must be A");
    }

    // -----------------------------------------------------------------------
    // Reopen: ancestry and schema survive a second open
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_AncestryAndSchema_SurviveReopen()
    {
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE catalog (id OBJECT_ID PRIMARY KEY, label STRING)",
            parameters: null));

        string branchName = NewName();
        await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Close and reopen to confirm persistence.
        await executor.CloseDatabase(new CloseDatabaseTicket(branchName));

        DatabaseDescriptor reopened = await executor.OpenDatabase(branchName);

        Assert.AreEqual(1, reopened.Ancestors.Count, "ancestry must survive reopen");
        Assert.AreEqual(rootDescriptor.Id, reopened.Ancestors[0].DatabaseId);
        Assert.IsTrue(reopened.Schema.Tables.ContainsKey("catalog"),
            "copied schema must survive reopen");
    }

    // -----------------------------------------------------------------------
    // Table and index id preservation: branch schema reuses source ids
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_TableIds_PreservedFromSource()
    {
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE widgets (id OBJECT_ID PRIMARY KEY, label STRING)",
            parameters: null));

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        string sourceTableId = rootDescriptor.Schema.Tables["widgets"].Id!;
        string branchTableId = branch.Schema.Tables["widgets"].Id!;

        Assert.AreEqual(sourceTableId, branchTableId,
            "branch must reuse the source table id so row keys route to the source namespace");
    }

    // -----------------------------------------------------------------------
    // No rows copied: branch table is empty even if source has rows
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_NoRowsCopied_BranchTableIsEmpty()
    {
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE events (id OBJECT_ID PRIMARY KEY, kind STRING)",
            parameters: null));

        // Insert a row in the source.
        KvTransaction ins = await rootDescriptor.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            ins, rootName, "INSERT INTO events (id, kind) VALUES (gen_id(), \"click\")", null));
        await rootDescriptor.Transactions.CommitAsync(ins);

        // Branch — only schema metadata is copied, not data.
        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        KvTransaction sel = await branch.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(sel, branchName, "SELECT id FROM events", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await branch.Transactions.CommitAsync(sel);

        Assert.AreEqual(0, rows.Count,
            "branching must not copy source rows; branch data namespace must be empty");
    }

    // -----------------------------------------------------------------------
    // No half-created branch: if branch creation fails after the name is
    // reserved but before metadata is written, the name is not wedged.
    // (Tested via the IfNotExists path: failed duplicate without IF NOT EXISTS
    // must leave no registry entry that blocks a subsequent creation.)
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_DuplicateTarget_OriginalUntouched()
    {
        (string rootName, _, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        string branchName = NewName();
        DatabaseDescriptor first = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Confirm duplicate throws.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateDatabase(
                new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));

        // The original branch must still be openable with the same id.
        DatabaseDescriptor reopened = await executor.OpenDatabase(branchName);
        Assert.AreEqual(first.Id, reopened.Id,
            "original branch must be untouched after a failed duplicate create");
    }

    // -----------------------------------------------------------------------
    // Stability gate after reopen: HeadSchemaVersion fence must be seeded
    // from the on-disk SchemaVersion so a reopened source can be branched.
    // Without the fence seed in LoadMetaAsync, HeadSchemaVersion stays 0
    // after reopen while SchemaVersion is restored from disk (e.g. 1), and
    // the != guard incorrectly fires for any source that has had prior DDL.
    // -----------------------------------------------------------------------

    [Test]
    public async Task CreateBranch_SourceReopened_StillBranchable()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        // Give the source at least one DDL so SchemaVersion > 0.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE items (id OBJECT_ID PRIMARY KEY, name STRING)",
            parameters: null));

        // Close and reopen — this is the scenario that was broken.  After reopen,
        // HeadSchemaVersion resets to 0 while SchemaVersion is restored from disk (1).
        // The fence seed in LoadMetaAsync must correct this before the stability gate runs.
        await executor.CloseDatabase(new CloseDatabaseTicket(rootName));
        await executor.OpenDatabase(rootName);

        string branchName = NewName();
        Assert.DoesNotThrowAsync(async () =>
        {
            DatabaseDescriptor branch = await executor.CreateDatabase(
                new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
            TrackDatabase(branchName, executor);

            Assert.AreEqual(1, branch.Ancestors.Count);
            Assert.IsTrue(branch.Schema.Tables.ContainsKey("items"),
                "schema must be copied from the reopened source");
        }, "branching a reopened source must not throw");
    }
}
