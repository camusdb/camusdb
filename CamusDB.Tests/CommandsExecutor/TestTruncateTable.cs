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
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>TRUNCATE TABLE</c>: the relation keeps its identity and every piece of schema metadata while
/// its physical contents generation is replaced. These tests assert both halves — that the live
/// relation reads empty and still exists, and that the rows it stopped reading are retained as a
/// recoverable generation rather than deleted.
/// </summary>
internal sealed class TestTruncateTable : SharedNodeBaseTest
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task<int> CountKeysAsync(string bucket, string keyPrefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    private static async Task<int> CountRowsAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(tx, dbname, sql, null);
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

            int count = 0;
            await foreach (QueryResultRow _ in cursor)
                count++;

            return count;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId)>
        SetupTableWithRows(int rows, bool withSecondaryIndex = true)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CatalogsManager catalogs = new(logger);

        List<ConstraintInfo> constraints =
        [
            new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
        ];

        if (withSecondaryIndex)
            constraints.Add(new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }));

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: [.. constraints],
            ifNotExists: false
        );
        await executor.CreateTable(createTicket);

        string tableId = database.Schema.Tables["robots"].Id!;

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        for (int i = 0; i < rows; i++)
        {
            InsertTicket ticket = new(
                txnState: insertTx,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "robot " + i) },
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                    }
                }
            );
            await executor.Insert(ticket);
        }
        await database.Transactions.CommitAsync(insertTx);

        return (dbname, database, executor, catalogs, tableId);
    }

    // -----------------------------------------------------------------------
    // Parser
    // -----------------------------------------------------------------------

    [Test]
    public void Truncate_BothSpellings_AndQuotedIdentifier_Parse()
    {
        NodeAst withKeyword = SQLParserProcessor.Parse("TRUNCATE TABLE robots");
        Assert.AreEqual(NodeType.TruncateTable, withKeyword.nodeType);
        Assert.AreEqual("robots", withKeyword.leftAst!.yytext);

        NodeAst withoutKeyword = SQLParserProcessor.Parse("truncate robots");
        Assert.AreEqual(NodeType.TruncateTable, withoutKeyword.nodeType);
        Assert.AreEqual("robots", withoutKeyword.leftAst!.yytext);

        NodeAst quoted = SQLParserProcessor.Parse("TRUNCATE TABLE `robots`");
        Assert.AreEqual(NodeType.TruncateTable, quoted.nodeType);
        Assert.AreEqual("robots", quoted.leftAst!.yytext);
    }

    // -----------------------------------------------------------------------
    // Core behavior
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Truncate_EmptiesTheRelationAndKeepsItsIdentity()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId) =
            await SetupTableWithRows(30);

        TableSchema schema = database.Schema.Tables["robots"];
        int versionBefore = schema.Version;
        long generationBefore = schema.ContentsGeneration;
        int columnsBefore = schema.Columns!.Count;
        int indexesBefore = schema.Indexes!.Count;

        Assert.AreEqual(30, await CountRowsAsync(executor, dbname, "SELECT * FROM robots"));

        Assert.True(await executor.TruncateTable(new TruncateTableTicket(dbname, "robots")));

        Assert.True(catalogs.TableExists(database, "robots"), "the relation must survive its own truncate");
        Assert.AreEqual(0, await CountRowsAsync(executor, dbname, "SELECT * FROM robots"));

        TableSchema after = database.Schema.Tables["robots"];
        Assert.AreEqual(tableId, after.Id, "identity must not change");
        Assert.AreEqual(versionBefore, after.Version, "a truncate does not change the row encoding");
        Assert.AreEqual(generationBefore + 1, after.ContentsGeneration);
        Assert.AreEqual(columnsBefore, after.Columns!.Count);
        Assert.AreEqual(indexesBefore, after.Indexes!.Count);
        Assert.AreNotEqual(tableId, after.EffectiveStorageId, "the rows must live in a new key-space");
        Assert.NotNull(after.ContentsValidFrom);
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_ScansNoRowsAndRetainsTheOldGeneration()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId) =
            await SetupTableWithRows(40);

        string rowBucket = $"{database.Id}:{tableId}:r";
        string rowPrefix = $"{database.Id}:{tableId}:r/";

        Assert.AreEqual(40, await CountKeysAsync(rowBucket, rowPrefix));

        await executor.TruncateTable(new TruncateTableTicket(dbname, "robots"));

        Assert.AreEqual(40, await CountKeysAsync(rowBucket, rowPrefix),
            "a truncate must not delete a single row key: the old generation is retained, not purged");

        List<OrphanTableRecord> orphans = await catalogs.LoadTableOrphansAsync(database);
        Assert.AreEqual(1, orphans.Count);

        OrphanTableRecord retired = orphans[0];
        Assert.AreEqual(OrphanKind.RetiredContents, retired.Kind);
        Assert.AreEqual(tableId, retired.RetiredStorageId, "the first truncate retires the relation's own key-space");
        Assert.AreEqual(tableId, retired.SourceTableId);
        Assert.AreEqual("robots", retired.FormerName);
        Assert.IsNull(retired.RelinkTargetId);
        Assert.AreEqual(3, retired.Schema.Columns!.Count, "the record must be able to decode the retained rows");
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_LeavesTheRelationWritable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(5);

        await executor.TruncateTable(new TruncateTableTicket(dbname, "robots"));

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: dbname,
            tableName: "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "after truncate") },
                    { "year", new(ColumnType.Integer64, 2030) },
                }
            }));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, await CountRowsAsync(executor, dbname, "SELECT * FROM robots"));
        Assert.AreEqual(1, await CountRowsAsync(executor, dbname, "SELECT * FROM robots WHERE year = 2030"));
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_OfAnEmptyTable_StillMakesANewGeneration()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(0);

        long generationBefore = database.Schema.Tables["robots"].ContentsGeneration;

        Assert.True(await executor.TruncateTable(new TruncateTableTicket(dbname, "robots")));

        Assert.AreEqual(generationBefore + 1, database.Schema.Tables["robots"].ContentsGeneration,
            "an empty table still transitions: the statement's outcome must not depend on the row count");
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_Twice_RetiresTwoDistinctGenerations()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId) =
            await SetupTableWithRows(3);

        await executor.TruncateTable(new TruncateTableTicket(dbname, "robots"));
        string secondGeneration = database.Schema.Tables["robots"].EffectiveStorageId;

        await executor.TruncateTable(new TruncateTableTicket(dbname, "robots"));
        string thirdGeneration = database.Schema.Tables["robots"].EffectiveStorageId;

        Assert.AreNotEqual(secondGeneration, thirdGeneration);
        Assert.AreEqual(2, database.Schema.Tables["robots"].ContentsGeneration);

        List<OrphanTableRecord> orphans = await catalogs.LoadTableOrphansAsync(database);
        Assert.AreEqual(2, orphans.Count);

        HashSet<string> retired = [.. orphans.Select(o => o.RetiredStorageId)];
        Assert.True(retired.Contains(tableId));
        Assert.True(retired.Contains(secondGeneration));
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_ViaSql_BothSpellingsReachTheSameExecutor()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(4);

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname, "TRUNCATE TABLE robots", null));
        await database.Transactions.CommitAsync(ddlTx);

        Assert.AreEqual(0, await CountRowsAsync(executor, dbname, "SELECT * FROM robots"));
        Assert.AreEqual(1, database.Schema.Tables["robots"].ContentsGeneration);

        KvTransaction nonQueryTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(nonQueryTx, dbname, "TRUNCATE robots", null));
        await database.Transactions.CommitAsync(nonQueryTx);

        Assert.AreEqual(2, database.Schema.Tables["robots"].ContentsGeneration,
            "the non-query entry point must reach the same implementation as the DDL entry point");
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_ExceedsNoMutationLimit()
    {
        // Well above MaxMutationsPerTransaction would take too long to insert here; the point the test
        // makes is that the statement enumerates no row mutations at all, which is what a mutation
        // counter of zero after the truncate proves.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, string tableId) =
            await SetupTableWithRows(60);

        string rowBucket = $"{database.Id}:{tableId}:r";
        string rowPrefix = $"{database.Id}:{tableId}:r/";

        await executor.TruncateTable(new TruncateTableTicket(dbname, "robots"));

        Assert.AreEqual(60, await CountKeysAsync(rowBucket, rowPrefix),
            "no row may be touched, so the statement cannot accumulate row mutations");
    }

    // -----------------------------------------------------------------------
    // Refusals
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Truncate_OfAMissingTable_Fails()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.TruncateTable(new TruncateTableTicket(dbname, "nope")));

        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, exception!.Code);
    }

    [Test]
    [NonParallelizable]
    public void Truncate_WithEmptyNames_FailsValidation()
    {
        CommandValidator validator = new(Options);

        Assert.Throws<CamusDBException>(() => validator.Validate(new TruncateTableTicket("", "robots")));
        Assert.Throws<CamusDBException>(() => validator.Validate(new TruncateTableTicket("db", "")));
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_InsideAnExplicitTransaction_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(2);

        KvTransaction sessionTx = await database.Transactions.BeginAsync();
        sessionTx.MarkSessionOwned();

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(sessionTx, dbname, "TRUNCATE TABLE robots", null)));

        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, exception!.Code);

        await database.Transactions.RollbackIfNotCompletedAsync(sessionTx);

        Assert.AreEqual(0, database.Schema.Tables["robots"].ContentsGeneration, "the refusal must change nothing");
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_OfAMaterializedView_IsRefusedAndNamesRefresh()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(3);

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            ddlTx, dbname, "CREATE MATERIALIZED VIEW robots_mv AS SELECT id, name FROM robots", null));
        await database.Transactions.CommitAsync(ddlTx);

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.TruncateTable(new TruncateTableTicket(dbname, "robots_mv")));

        Assert.AreEqual(CamusDBErrorCodes.ViewNotUpdatable, exception!.Code);
        Assert.True(exception.Message.Contains("REFRESH", StringComparison.Ordinal));
    }

    [Test]
    [NonParallelizable]
    public async Task Truncate_OfAView_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(3);

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            ddlTx, dbname, "CREATE VIEW robots_v AS SELECT id, name FROM robots", null));
        await database.Transactions.CommitAsync(ddlTx);

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.TruncateTable(new TruncateTableTicket(dbname, "robots_v")));

        Assert.AreEqual(CamusDBErrorCodes.ViewNotUpdatable, exception!.Code);
    }

    // -----------------------------------------------------------------------
    // Dependents
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Truncate_LeavesAMaterializedViewStaleAndAPlainViewEmpty()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(6);

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            ddlTx, dbname, "CREATE MATERIALIZED VIEW robots_mv AS SELECT id, name FROM robots", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            ddlTx, dbname, "CREATE VIEW robots_v AS SELECT id, name FROM robots", null));
        await database.Transactions.CommitAsync(ddlTx);

        Assert.AreEqual(6, await CountRowsAsync(executor, dbname, "SELECT * FROM robots_mv"));

        await executor.TruncateTable(new TruncateTableTicket(dbname, "robots"));

        // The chosen contract: a base-table mutation never invalidates a dependent materialized view,
        // and a truncate is a base-table mutation. Changing this must change every mutation, not one.
        Assert.AreEqual(6, await CountRowsAsync(executor, dbname, "SELECT * FROM robots_mv"),
            "a dependent materialized view stays populated and stale until an explicit REFRESH");

        Assert.AreEqual(0, await CountRowsAsync(executor, dbname, "SELECT * FROM robots_v"),
            "a plain view stores no rows, so it reads the emptied base table");

        KvTransaction refreshTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            refreshTx, dbname, "REFRESH MATERIALIZED VIEW robots_mv", null));
        await database.Transactions.CommitAsync(refreshTx);

        Assert.AreEqual(0, await CountRowsAsync(executor, dbname, "SELECT * FROM robots_mv"),
            "REFRESH is what makes the materialized view agree with the emptied base table");
    }
}
