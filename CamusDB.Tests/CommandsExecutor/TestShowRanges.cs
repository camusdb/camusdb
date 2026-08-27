/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>SHOW RANGES</c> / <c>SHOW RANGE</c>, driven through
/// <see cref="ExecuteSQLTicket"/> so the statement runs the way a session reaches it — parse,
/// authorization, target resolution, placement read and row projection included.
///
/// <para>This fixture is standalone (one node, hash routing), which is the single-span arm of the
/// feature. The multi-span arm lives in <c>TestShowRangesAcrossSplit</c>, which derives from the
/// key-range split fixture; a standalone-only fixture would leave the shape that motivates the
/// statement unverified.</para>
///
/// <para><c>[NonParallelizable]</c> because each test boots an embedded Kahuna node.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowRanges : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Creates "robots": id (Id PK), name (String, unique index), year (Integer64, indexed).</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",       new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx",  new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<List<string>> InsertRobots(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int count)
    {
        List<string> ids = [];
        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);

            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, id) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(2000 + i)) },
                    }
                }));
        }

        await database.Transactions.CommitAsync(txn);
        return ids;
    }

    private static async Task<TableDescriptor> OpenTable(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    /// <summary>Runs a row-returning statement and captures both the rows and the declared schema.</summary>
    private static async Task<(List<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema>? schema)> Query(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: parameters),
            schemaOut: schemaHolder);

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await database.Transactions.CommitAsync(txn);
        return (rows, schemaHolder.Schema);
    }

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.StrValue : null;

    private static long? Number(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.LongValue : null;

    private static bool? Flag(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.BoolValue : null;

    private static bool IsNull(QueryResultRow row, string column)
        => !row.Row.TryGetValue(column, out ColumnValue? v) || v.Type == ColumnType.Null;

    // ─────────────────────────────────────────────────────────────────────────
    // Standalone shape
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A standalone node routes by hash and never splits, so the honest answer is exactly one
    /// unbounded span — and <c>routing = hash</c> is precisely the diagnostic an operator came for.
    /// </summary>
    [Test]
    public async Task Standalone_TableReportsExactlyOneUnboundedSpan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 5);

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW RANGES FROM TABLE robots");

        Assert.AreEqual(1, rows.Count);
        QueryResultRow row = rows[0];

        Assert.AreEqual("robots", Text(row, "relation"));
        Assert.AreEqual("hash", Text(row, "routing"));
        Assert.AreEqual(1, Number(row, "span"));
        Assert.IsTrue(IsNull(row, "start_key"), "An unbounded lower bound is NULL");
        Assert.IsTrue(IsNull(row, "end_key"), "An unbounded upper bound is NULL");
        Assert.IsTrue(IsNull(row, "raw_start_key"));
        Assert.IsTrue(IsNull(row, "raw_end_key"));
        Assert.AreEqual(0, Number(row, "generation"), "A hash span carries no split fence");
        Assert.IsTrue(Flag(row, "leader_is_local"));
        Assert.IsTrue(Flag(row, "hosted_locally"));
        Assert.AreEqual("", Text(row, "replicas"), "Empty means legacy full replication, not 'no replicas'");
        Assert.IsTrue(IsNull(row, "probe_key"), "The plural form locates no single key");
    }

    /// <summary>The reported key space must be the store's, which is the one routing actually uses.</summary>
    [Test]
    public async Task KeySpaceMatchesTheStore()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        TableDescriptor table = await OpenTable(database, "robots");

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW RANGES FROM TABLE robots");
        Assert.AreEqual(table.Store.RowKeySpace, Text(rows[0], "key_space"));

        (List<QueryResultRow> indexRows, _) = await Query(
            executor, database, dbname, "SHOW RANGES FROM INDEX robots@year_idx");

        string yearKvId = table.Indexes["year_idx"].KvId;
        Assert.AreEqual(table.Store.IndexKeySpace(yearKvId), Text(indexRows[0], "key_space"));
        Assert.AreEqual("robots@year_idx", Text(indexRows[0], "relation"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Target resolution and its aliases
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The primary index is stored as <c>~pk</c>, which nobody guesses. Both familiar spellings must
    /// reach the same key space, or the natural form of the statement simply does not work.
    /// </summary>
    [Test]
    public async Task PrimaryKeyAliasesResolveToTheSameKeySpace()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        (List<QueryResultRow> internalName, _) = await Query(
            executor, database, dbname, "SHOW RANGES FROM INDEX robots@~pk");
        (List<QueryResultRow> pkeySuffix, _) = await Query(
            executor, database, dbname, "SHOW RANGES FROM INDEX robots@robots_pkey");
        (List<QueryResultRow> primaryWord, _) = await Query(
            executor, database, dbname, "SHOW RANGES FROM INDEX robots@primary");

        string keySpace = Text(internalName[0], "key_space")!;
        Assert.AreEqual(keySpace, Text(pkeySuffix[0], "key_space"));
        Assert.AreEqual(keySpace, Text(primaryWord[0], "key_space"));
    }

    /// <summary>An unknown index must say what is available rather than fail opaquely.</summary>
    [Test]
    public async Task UnknownIndexIsRejectedAndListsTheReadableOnes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(executor, database, dbname, "SHOW RANGES FROM INDEX robots@nosuchindex"))!;

        Assert.AreEqual(CamusDBErrorCodes.IndexDoesntExist, ex.Code);
        StringAssert.Contains("year_idx", ex.Message);
    }

    [Test]
    public async Task UnknownTableIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(executor, database, dbname, "SHOW RANGES FROM TABLE nosuchtable"));
    }

    /// <summary>
    /// A plain view stores no rows, so it has no key space of its own. The message must say that
    /// rather than let the table-open path reject a read with "cannot be written to".
    /// </summary>
    [Test]
    public async Task PlainViewIsRejectedWithItsOwnMessage()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 3);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddl, database: dbname,
            sql: "CREATE VIEW recent_robots AS SELECT id, name FROM robots", parameters: null));
        await database.Transactions.CommitAsync(ddl);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(executor, database, dbname, "SHOW RANGES FROM TABLE recent_robots"))!;

        StringAssert.Contains("is a view", ex.Message);
        StringAssert.Contains("key space", ex.Message);
    }

    /// <summary>
    /// A materialized view is a real relation with a real store, so it reports ranges like any
    /// table. The key space must come from the store rather than from the relation id — a refresh
    /// moves the contents to a fresh storage id while the relation keeps its identity.
    /// </summary>
    [Test]
    public async Task MaterializedViewReportsItsOwnKeySpace()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 4);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddl, database: dbname,
            sql: "CREATE MATERIALIZED VIEW robot_years AS SELECT id, year FROM robots", parameters: null));
        await database.Transactions.CommitAsync(ddl);

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW RANGES FROM TABLE robot_years");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("robot_years", Text(rows[0], "relation"));

        TableDescriptor matview = await OpenTable(database, "robot_years");
        Assert.AreEqual(matview.Store.RowKeySpace, Text(rows[0], "key_space"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FOR ROW
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The index form is a routing question, not a data question: it must answer for a key nothing
    /// has ever stored. That is the whole reason it is the well-defined form.
    /// </summary>
    [Test]
    public async Task ForRowOnAnIndex_ResolvesAKeyThatDoesNotExist()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 3);

        (List<QueryResultRow> rows, _) = await Query(
            executor, database, dbname, "SHOW RANGE FROM INDEX robots@year_idx FOR ROW (999999)");

        Assert.AreEqual(1, rows.Count);
        Assert.IsNotNull(Text(rows[0], "probe_key"), "The located key must be reported");
        StringAssert.StartsWith(Text(rows[0], "key_space")! + "/", Text(rows[0], "probe_key")!);
    }

    /// <summary>A value that will not convert to the key column's type must be an error, not a guess.</summary>
    [Test]
    public async Task ForRowOnAnIndex_UncoercibleValueIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(
                executor, database, dbname, "SHOW RANGE FROM INDEX robots@year_idx FOR ROW (true)"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>More values than key columns would locate some span while looking plausible.</summary>
    [Test]
    public async Task ForRowOnAnIndex_TooManyValuesIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(
                executor, database, dbname, "SHOW RANGE FROM INDEX robots@year_idx FOR ROW (1, 2)"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("key column", ex.Message);
    }

    /// <summary>
    /// On a table the statement must resolve through the primary index, because a row's KV key is
    /// built from the stored row id and not from the primary key. The reported probe key must be
    /// the row's actual key.
    /// </summary>
    [Test]
    public async Task ForRowOnATable_ResolvesThroughThePrimaryIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        List<string> ids = await InsertRobots(executor, database, dbname, 5);

        (List<QueryResultRow> rows, _) = await Query(
            executor, database, dbname, $"SHOW RANGE FROM TABLE robots FOR ROW ('{ids[2]}')");

        Assert.AreEqual(1, rows.Count);

        TableDescriptor table = await OpenTable(database, "robots");

        // The row's KV key is built from its stored row id, which is NOT the value of its `id`
        // column — that is the whole reason this form needs a primary-index read. So the expected
        // key comes from the row id the engine assigned, read back through a SELECT.
        string expected = table.Store.RowPointKey(await RowIdOf(executor, database, dbname, ids[2]));

        Assert.AreEqual(expected, Text(rows[0], "probe_key"));
        Assert.AreEqual(table.Store.RowKeySpace, Text(rows[0], "key_space"));
    }

    /// <summary>
    /// The engine-assigned row id of the row whose <c>id</c> column holds <paramref name="idValue"/>.
    /// The two are distinct values: the row id orders the row space, the id column is user data.
    /// </summary>
    private static async Task<ObjectIdValue> RowIdOf(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string idValue)
    {
        (List<QueryResultRow> rows, _) = await Query(
            executor, database, dbname, $"SELECT * FROM robots WHERE id = '{idValue}'");

        return rows.Single().RowId;
    }

    /// <summary>
    /// A primary key nothing carries must raise, and the message must explain why the table form
    /// cannot answer for a key that does not exist. Zero rows would be indistinguishable from a
    /// filter that matched nothing.
    /// </summary>
    [Test]
    public async Task ForRowOnATable_MissingPrimaryKeyRaisesAndExplains()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 3);

        string absent = ObjectIdGenerator.Generate().ToString();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(
                executor, database, dbname, $"SHOW RANGE FROM TABLE robots FOR ROW ('{absent}')"))!;

        Assert.AreEqual(CamusDBErrorCodes.UnknownKey, ex.Code);
        StringAssert.Contains("row id", ex.Message);
        StringAssert.Contains("SHOW RANGE FROM INDEX", ex.Message);
    }

    /// <summary>A bound placeholder must reach the value list, so the statement can be parameterized.</summary>
    [Test]
    public async Task ForRow_AcceptsABoundPlaceholder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        List<string> ids = await InsertRobots(executor, database, dbname, 4);

        (List<QueryResultRow> rows, _) = await Query(
            executor, database, dbname,
            "SHOW RANGE FROM TABLE robots FOR ROW (@id)",
            new Dictionary<string, ColumnValue> { { "@id", new ColumnValue(ColumnType.Id, ids[1]) } });

        Assert.AreEqual(1, rows.Count);

        TableDescriptor table = await OpenTable(database, "robots");
        ObjectIdValue rowId = await RowIdOf(executor, database, dbname, ids[1]);
        Assert.AreEqual(table.Store.RowPointKey(rowId), Text(rows[0], "probe_key"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // The statement must not disturb what it inspects
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The probe behind <c>FOR ROW</c> must acquire no lock and fold nothing into the read set, so a
    /// serializable transaction commits exactly as it would have without the statement. A
    /// <c>SHOW</c> that can abort its caller's transaction is a bug users would spend a long time
    /// not believing.
    /// </summary>
    [Test]
    public async Task ForRowInsideASerializableTransactionDoesNotChangeItsOutcome()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        List<string> ids = await InsertRobots(executor, database, dbname, 5);

        TableDescriptor probeTable = await OpenTable(database, "robots");
        string rowKeySpace = probeTable.Store.RowKeySpace;
        string pkKeySpace = probeTable.Store.IndexKeySpace(probeTable.Indexes["~pk"].KvId);

        KvTransaction txn = await database.Transactions.BeginAsync();

        int rangeLocksBefore = txn.GetAcquiredRangeLocks().Count;
        int pointLocksBefore = txn.CountPointLocksForBucket(rowKeySpace);
        int modifiedBefore = txn.GetModifiedKeyPairs().Count;

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(
                txnState: txn, database: dbname,
                sql: $"SHOW RANGE FROM TABLE robots FOR ROW ('{ids[0]}')", parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(rangeLocksBefore, txn.GetAcquiredRangeLocks().Count, "The probe must acquire no range lock");
        Assert.AreEqual(pointLocksBefore, txn.CountPointLocksForBucket(rowKeySpace), "The probe must acquire no point lock");
        Assert.AreEqual(pointLocksBefore, txn.CountPointLocksForBucket(pkKeySpace), "The probe must not lock the primary index entry it read");
        Assert.AreEqual(modifiedBefore, txn.GetModifiedKeyPairs().Count, "The probe must modify nothing");

        // The surrounding transaction still commits, which is the outcome that matters.
        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// The read must be cache-neutral: it neither serves the planner a stale layout nor leaves one
    /// behind. Reporting from that cache would be exactly the staleness an operator runs this
    /// statement to rule out.
    /// </summary>
    [Test]
    public async Task StatementLeavesThePlannerPlacementCacheUnchanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 3);

        TableDescriptor table = await OpenTable(database, "robots");
        string keySpace = table.Store.RowKeySpace;

        // A placement the planner cached before the statement must still be the same instance after,
        // which proves nothing was evicted; and running the statement first must not have created
        // one either.
        TablePlacement planned = database.Kahuna.GetPlacement(keySpace);

        await Query(executor, database, dbname, "SHOW RANGES FROM TABLE robots");

        Assert.AreSame(planned, database.Kahuna.GetPlacement(keySpace),
            "SHOW RANGES must neither evict nor replace the planner's cached placement");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Emitted schema
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The column set, its order and its types are the statement's contract. Most cells are
    /// nullable, so a transport that renders nulls implicitly would drop columns without this pin.
    /// </summary>
    [Test]
    public async Task EmittedColumnSchemaIsPinned()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        (List<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema>? schema) =
            await Query(executor, database, dbname, "SHOW RANGES FROM TABLE robots");

        Assert.IsNotNull(schema);

        string[] expectedNames =
        [
            "relation", "key_space", "routing", "span",
            "start_key", "end_key", "raw_start_key", "raw_end_key",
            "partition_id", "generation",
            "leader", "leader_is_local", "hosted_locally", "replicas", "probe_key",
        ];

        CollectionAssert.AreEqual(expectedNames, schema!.Select(c => c.Name).ToArray());

        Assert.AreEqual(ColumnType.String, schema[0].Type);
        Assert.AreEqual(ColumnType.Integer64, schema[3].Type);
        Assert.AreEqual(ColumnType.Integer64, schema[8].Type);
        Assert.AreEqual(ColumnType.Bool, schema[11].Type);
        Assert.AreEqual(ColumnType.Bool, schema[12].Type);

        // Every declared column must actually be produced, or the two drift silently.
        foreach (DerivedColumnSchema column in schema)
            Assert.IsTrue(rows[0].Row.ContainsKey(column.Name), $"Row is missing column '{column.Name}'");
    }

    /// <summary>The statement is database-scoped and must not be answerable without one.</summary>
    [Test]
    public void RequiresAContextDatabase()
    {
        Assert.IsFalse(
            CamusDB.Core.SQLParser.StatementScope.AllowsEmptyContextDatabase(
                CamusDB.Core.SQLParser.NodeType.ShowRanges));
    }
}
