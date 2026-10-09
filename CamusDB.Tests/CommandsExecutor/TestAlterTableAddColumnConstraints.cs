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

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>ALTER TABLE … ADD COLUMN</c> with each column constraint <c>CREATE TABLE</c> accepts. Each one
/// must either work as it does in <c>CREATE TABLE</c>, filling the rows that already exist, or fail with
/// an error and leave no column. None may succeed and drop part of the statement.
///
/// <para>The scenarios run twice, on a standalone engine and on a cluster-mode engine. Both stage the
/// column through the schema-change coordinator and fill it at its <c>WriteOnly</c> step, in committed
/// batches that lock and read each row again.</para>
/// </summary>
[NonParallelizable]
internal abstract class AddColumnConstraintScenarios : SharedNodeBaseTest
{
    protected abstract bool ClusterMode { get; }

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase));

            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>A database with table <c>items</c> holding <paramref name="rows"/> rows, ids 1..rows.</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable(int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)");

        for (int i = 1; i <= rows; i++)
            await ExecuteNonQuery(executor, dbname, $"INSERT INTO items (id, name) VALUES ({i}, 'n{i}')");

        return (dbname, database, executor);
    }

    private static TableColumnSchema? Column(DatabaseDescriptor database, string name) =>
        database.Schema.Tables["items"].Columns!.Find(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));

    private static async Task<List<ColumnValue>> ColumnValues(CommandExecutor executor, string dbname, string column)
    {
        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, $"SELECT id, {column} FROM items ORDER BY id");
        return rows.Select(r => r[column]).ToList();
    }

    private static async Task<CamusDBException> AddColumnFails(CommandExecutor executor, string dbname, string sql)
    {
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await ExecuteDdl(executor, dbname, sql));
        Assert.That(ex, Is.Not.Null);
        return await Task.FromResult(ex!);
    }

    // -----------------------------------------------------------------------
    // NOT NULL
    // -----------------------------------------------------------------------

    [Test]
    public async Task NotNullWithoutDefault_TableWithRows_IsRefusedAndLeavesNoColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 2);

        CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN must STRING NOT NULL");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.NotNullViolation));
        Assert.That(ex.Message, Does.Contain("contains null values"));
        Assert.That(Column(database, "must"), Is.Null);

        // Nothing was half-added, so the statement can be corrected and run again.
        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN must STRING NOT NULL DEFAULT ('x')");
        Assert.That(await ColumnValues(executor, dbname, "must"), Has.All.Property(nameof(ColumnValue.StrValue)).EqualTo("x"));
    }

    [Test]
    public async Task NotNullWithoutDefault_EmptyTable_IsAcceptedAndEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 0);

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN must STRING NOT NULL");

        TableColumnSchema? must = Column(database, "must");
        Assert.That(must, Is.Not.Null);
        Assert.That(must!.NotNull, Is.True);
        Assert.That(must.State, Is.EqualTo(SchemaElementState.Public));

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name) VALUES (1, 'a')"));

        await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name, must) VALUES (1, 'a', 'm')");
        Assert.That((await ColumnValues(executor, dbname, "must")).Single().StrValue, Is.EqualTo("m"));
    }

    /// <summary>
    /// A row that arrives after the empty-table check: the fill finds it, the statement fails with the
    /// same error, and the column is removed again, so the catalog never claims NOT NULL over a NULL.
    /// </summary>
    [Test]
    public async Task NotNullWithoutDefault_RowArrivesAfterCheck_FailsAndRemovesColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 0);
        long versionBefore = database.Schema.SchemaVersion;

        executor.TestInterceptAfterAddColumnPrecheck = () =>
            ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name) VALUES (7, 'late')");
        try
        {
            CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN must STRING NOT NULL");
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.NotNullViolation));
        }
        finally
        {
            executor.TestInterceptAfterAddColumnPrecheck = null;
        }

        Assert.That(Column(database, "must"), Is.Null);

        // Both modes stage the column: Absent → DeleteOnly → WriteOnly, then back WriteOnly → DeleteOnly → Absent.
        Assert.That(database.Schema.SchemaVersion - versionBefore, Is.EqualTo(4));
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "no job may be left behind");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT * FROM items");
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].ContainsKey("must"), Is.False);

        // The name is free again.
        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN must STRING NOT NULL DEFAULT ('d')");
        Assert.That((await ColumnValues(executor, dbname, "must")).Single().StrValue, Is.EqualTo("d"));
    }

    [Test]
    public async Task NamedNotNull_KeepsItsConstraintName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 2);

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 CONSTRAINT qty_nn NOT NULL DEFAULT (5)");

        TableColumnSchema? qty = Column(database, "qty");
        Assert.That(qty!.NotNull, Is.True);
        Assert.That(qty.NotNullConstraintName, Is.EqualTo("qty_nn"));
        Assert.That((await ColumnValues(executor, dbname, "qty")).Select(v => v.LongValue), Is.EqualTo(new long[] { 5, 5 }));
    }

    // -----------------------------------------------------------------------
    // Defaults evaluated per existing row
    // -----------------------------------------------------------------------

    [Test]
    public async Task ConstantDefault_FillsExistingRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 3);
        long versionBefore = database.Schema.SchemaVersion;

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN score FLOAT64 DEFAULT (1.5)");

        // Both modes stage the column through DeleteOnly and WriteOnly, so it is readable only once filled.
        Assert.That(database.Schema.SchemaVersion - versionBefore, Is.EqualTo(3));
        Assert.That(Column(database, "score")!.State, Is.EqualTo(SchemaElementState.Public));

        Assert.That((await ColumnValues(executor, dbname, "score")).Select(v => v.FloatValue), Is.EqualTo(new[] { 1.5, 1.5, 1.5 }));
    }

    [Test]
    public async Task FunctionDefault_IsEvaluatedForEachExistingRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 3);

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN created DATETIME DEFAULT (now())");
        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN token UUID DEFAULT (gen_uuid_v7())");

        Assert.That(await ColumnValues(executor, dbname, "created"), Has.None.Property(nameof(ColumnValue.Type)).EqualTo(ColumnType.Null));

        List<ColumnValue> tokens = await ColumnValues(executor, dbname, "token");
        Assert.That(tokens, Has.None.Property(nameof(ColumnValue.Type)).EqualTo(ColumnType.Null));
        Assert.That(tokens.Select(t => t.ToString()).Distinct().Count(), Is.EqualTo(3), "each row gets its own value");

        Assert.That(Column(database, "token")!.DefaultFunction, Is.EqualTo("gen_uuid_v7"));
    }

    [Test]
    public async Task FunctionDefaultWithNotNull_TableWithRows_IsAccepted()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTable(rows: 2);

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN created DATETIME NOT NULL DEFAULT (now())");

        Assert.That(await ColumnValues(executor, dbname, "created"), Has.None.Property(nameof(ColumnValue.Type)).EqualTo(ColumnType.Null));
    }

    [Test]
    public async Task NextvalDefault_DrawsOneValuePerExistingRow_AndKeepsTheDefault()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 3);

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE item_seq");
        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN seqcol INT64 DEFAULT (nextval('item_seq'))");

        TableColumnSchema? seqcol = Column(database, "seqcol");
        Assert.That(seqcol!.DefaultSequenceId, Is.EqualTo(database.Schema.Sequences["item_seq"].Id));

        List<long> filled = (await ColumnValues(executor, dbname, "seqcol")).Select(v => v.LongValue).ToList();
        Assert.That(filled.Distinct().Count(), Is.EqualTo(3));
        Assert.That(filled.OrderBy(v => v), Is.EqualTo(new long[] { 1, 2, 3 }));

        // A later INSERT that omits the column draws from the same sequence.
        await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name) VALUES (4, 'n4')");
        Assert.That((await ColumnValues(executor, dbname, "seqcol")).Last().LongValue, Is.EqualTo(4));
    }

    [Test]
    public async Task NextvalDefault_UnknownSequence_IsRefusedAndLeavesNoColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 1);

        CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN seqcol INT64 DEFAULT (nextval('missing_seq'))");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SequenceDoesntExist));
        Assert.That(Column(database, "seqcol"), Is.Null);
    }

    [Test]
    public async Task Identity_CreatesOwnedSequence_FillsExistingRows_AndIsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 3);

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN rid INT64 GENERATED ALWAYS AS IDENTITY");

        TableColumnSchema? rid = Column(database, "rid");
        Assert.That(rid!.NotNull, Is.True);
        Assert.That(rid.IdentityAlways, Is.True);

        Assert.That(database.Schema.Sequences.TryGetValue("items_rid_seq", out SequenceSchema? owned), Is.True);
        Assert.That(owned!.OwnedByTableId, Is.EqualTo(database.Schema.Tables["items"].Id));
        Assert.That(rid.DefaultSequenceId, Is.EqualTo(owned.Id));

        List<long> filled = (await ColumnValues(executor, dbname, "rid")).Select(v => v.LongValue).ToList();
        Assert.That(filled.OrderBy(v => v), Is.EqualTo(new long[] { 1, 2, 3 }));

        // ALWAYS refuses a supplied value, and an omitted one draws the next number.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name, rid) VALUES (9, 'x', 100)"));

        await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name) VALUES (4, 'n4')");
        Assert.That((await ColumnValues(executor, dbname, "rid")).Last().LongValue, Is.EqualTo(4));
    }

    [Test]
    public async Task Identity_OnNonIntegerColumn_IsRefusedBeforeASequenceIsCreated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 1);

        CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN rid STRING GENERATED BY DEFAULT AS IDENTITY");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(Column(database, "rid"), Is.Null);
        Assert.That(database.Schema.Sequences.ContainsKey("items_rid_seq"), Is.False);
    }

    [Test]
    public async Task Identity_DuplicateColumn_DoesNotLeaveASequence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 1);

        CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN name INT64 GENERATED BY DEFAULT AS IDENTITY");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(database.Schema.Sequences.ContainsKey("items_name_seq"), Is.False);
    }

    // -----------------------------------------------------------------------
    // Constraints that need their own statement
    // -----------------------------------------------------------------------

    [TestCase("ALTER TABLE items ADD COLUMN pk2 INT64 PRIMARY KEY", "pk2", "ADD PRIMARY KEY")]
    [TestCase("ALTER TABLE items ADD COLUMN u INT64 UNIQUE", "u", "CREATE UNIQUE INDEX")]
    [TestCase("ALTER TABLE items ADD COLUMN c INT64 DEFAULT (1) CHECK (c > 0)", "c", "ADD CONSTRAINT")]
    public async Task ConstraintNeedingItsOwnRollout_IsRefusedAndLeavesNoColumn(string sql, string column, string named)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 2);
        int indexesBefore = database.Schema.Tables["items"].Indexes?.Count ?? 0;

        CamusDBException ex = await AddColumnFails(executor, dbname, sql);

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.FeatureNotSupported));
        Assert.That(ex.Message, Does.Contain(named));
        Assert.That(Column(database, column), Is.Null);
        Assert.That(database.Schema.Tables["items"].Indexes?.Count ?? 0, Is.EqualTo(indexesBefore));
        Assert.That(database.Schema.Tables["items"].CheckConstraints ?? [], Is.Empty);
    }

    // -----------------------------------------------------------------------
    // Concurrent writes during the add
    // -----------------------------------------------------------------------

    /// <summary>
    /// An UPDATE and a DELETE that commit between the fill's unlocked scan and its write. The fill must
    /// write the updated row, not the image it scanned, and must not bring the deleted row back.
    /// </summary>
    [Test]
    public async Task UpdateAndDeleteDuringFill_AreNotOverwritten()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTable(rows: 3);
        await ExecuteDdl(executor, dbname, "CREATE INDEX name_idx ON items (name)");

        executor.TestInterceptBeforeFillBatchLock = async () =>
        {
            executor.TestInterceptBeforeFillBatchLock = null;
            await ExecuteNonQuery(executor, dbname, "UPDATE items SET name = 'changed' WHERE id = 1");
            await ExecuteNonQuery(executor, dbname, "DELETE FROM items WHERE id = 2");
        };
        try
        {
            await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 NOT NULL DEFAULT (7)");
        }
        finally
        {
            executor.TestInterceptBeforeFillBatchLock = null;
        }

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT id, name, qty FROM items ORDER BY id");
        Assert.That(rows.Select(r => r["id"].LongValue), Is.EqualTo(new long[] { 1, 3 }), "the deleted row stays deleted");
        Assert.That(rows[0]["name"].StrValue, Is.EqualTo("changed"), "the committed update survives the fill");
        Assert.That(rows.Select(r => r["qty"].LongValue), Is.EqualTo(new long[] { 7, 7 }));

        // The row and its index entry agree.
        Assert.That(await Query(executor, dbname, "SELECT id FROM items WHERE name = 'changed'"), Has.Count.EqualTo(1));
        Assert.That(await Query(executor, dbname, "SELECT id FROM items WHERE name = 'n1'"), Is.Empty);
        Assert.That(await Query(executor, dbname, "SELECT id FROM items WHERE name = 'n2'"), Is.Empty);
    }

    /// <summary>
    /// An UPDATE of an older row while the column is WriteOnly stores the decoder's injected default,
    /// which is NULL for a function default. The fill must still give that row a value.
    /// </summary>
    [Test]
    public async Task UpdateDuringFill_OfAFunctionDefaultColumn_StillGetsAValue()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTable(rows: 3);

        executor.TestInterceptBeforeFillBatchLock = async () =>
        {
            executor.TestInterceptBeforeFillBatchLock = null;
            await ExecuteNonQuery(executor, dbname, "UPDATE items SET name = 'changed' WHERE id = 2");
        };
        try
        {
            await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN token UUID DEFAULT (gen_uuid_v7())");
        }
        finally
        {
            executor.TestInterceptBeforeFillBatchLock = null;
        }

        List<ColumnValue> tokens = await ColumnValues(executor, dbname, "token");
        Assert.That(tokens, Has.None.Property(nameof(ColumnValue.Type)).EqualTo(ColumnType.Null));
        Assert.That(tokens.Select(t => t.ToString()).Distinct().Count(), Is.EqualTo(3));
    }

    /// <summary>
    /// A row written while the column is DeleteOnly holds a NULL placeholder for it, because the writer
    /// may not write the column yet. The fill must treat it as a row without a value.
    /// </summary>
    [TestCase("qty INT64 NOT NULL DEFAULT (7)")]
    // Nullable with a constant: the placeholder NULL is caught only by the layout rule.
    [TestCase("qty INT64 DEFAULT (7)")]
    [TestCase("qty INT64 GENERATED BY DEFAULT AS IDENTITY")]
    [TestCase("qty DATETIME NOT NULL DEFAULT (now())")]
    public async Task RowsWrittenWhileDeleteOnly_AreFilled(string definition)
    {
        (string dbname, _, CommandExecutor executor) = await SetupTable(rows: 2);

        executor.TestInterceptAfterAddColumnStep = async state =>
        {
            if (state != SchemaElementState.DeleteOnly)
                return;

            await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, name) VALUES (10, 'during')");
            await ExecuteNonQuery(executor, dbname, "UPDATE items SET name = 'touched' WHERE id = 1");
        };
        try
        {
            await ExecuteDdl(executor, dbname, $"ALTER TABLE items ADD COLUMN {definition}");
        }
        finally
        {
            executor.TestInterceptAfterAddColumnStep = null;
        }

        List<ColumnValue> values = await ColumnValues(executor, dbname, "qty");
        Assert.That(values, Has.Count.EqualTo(3));
        Assert.That(values, Has.None.Property(nameof(ColumnValue.Type)).EqualTo(ColumnType.Null));

        if (definition.Contains("DEFAULT (7)"))
            Assert.That(values.Select(v => v.LongValue), Is.EqualTo(new long[] { 7, 7, 7 }));

        if (definition.Contains("IDENTITY"))
            Assert.That(values.Select(v => v.LongValue).Distinct().Count(), Is.EqualTo(3));
    }

    /// <summary>Another statement adds the same column between the early check and the gate.</summary>
    [Test]
    public async Task SameColumnAddedAfterTheEarlyCheck_IsRefusedUnderTheGate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 1);

        executor.TestInterceptAfterAddColumnPrecheck = async () =>
        {
            executor.TestInterceptAfterAddColumnPrecheck = null;
            await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN value STRING DEFAULT ('x')");
        };
        try
        {
            CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN value INT64 DEFAULT (1)");
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(ex.Message, Does.Contain("Duplicate column"));
        }
        finally
        {
            executor.TestInterceptAfterAddColumnPrecheck = null;
        }

        Assert.That(Column(database, "value")!.Type, Is.EqualTo(ColumnType.String), "the first definition stays");
    }

    /// <summary>Another statement claims the same NOT NULL constraint name between the early check and the gate.</summary>
    [Test]
    public async Task ConstraintNameClaimedAfterTheEarlyCheck_IsRefusedUnderTheGate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 1);

        executor.TestInterceptAfterAddColumnPrecheck = async () =>
        {
            executor.TestInterceptAfterAddColumnPrecheck = null;
            await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN b INT64 CONSTRAINT shared_nn NOT NULL DEFAULT (1)");
        };
        try
        {
            CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN a INT64 CONSTRAINT shared_nn NOT NULL DEFAULT (1)");
            Assert.That(ex.Message, Does.Contain("shared_nn"));
        }
        finally
        {
            executor.TestInterceptAfterAddColumnPrecheck = null;
        }

        Assert.That(Column(database, "a"), Is.Null);
        Assert.That(database.Schema.Tables["items"].Columns!.Count(c => c.NotNullConstraintName == "shared_nn"), Is.EqualTo(1));
    }

    [Test]
    public async Task ColumnLimit_IsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { MaxColumnsPerTable = 3 });
        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)");

        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN c1 INT64");

        CamusDBException ex = await AddColumnFails(executor, dbname, "ALTER TABLE items ADD COLUMN c2 INT64 GENERATED BY DEFAULT AS IDENTITY");
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SchemaLimitExceeded));
        Assert.That(Column(database, "c2"), Is.Null);
        Assert.That(database.Schema.Sequences.ContainsKey("items_c2_seq"), Is.False, "no sequence is left behind");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "no job is left behind");
    }

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------

    /// <summary>
    /// A failed add whose removal was recorded but not finished: the column is still DeleteOnly, and the
    /// job targets Absent. A resume removes the column and the identity sequence, and a second resume,
    /// as after a failed job delete, adds nothing.
    /// </summary>
    [Test]
    public async Task RemovalJob_IsFinishedByResume_AndNeverAddsTheColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable(rows: 2);

        executor.TestInterceptAfterAddColumnStep = state =>
            state == SchemaElementState.DeleteOnly ? throw new InvalidOperationException("stopped after DeleteOnly") : Task.CompletedTask;
        try
        {
            Assert.CatchAsync(async () =>
                await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN rid INT64 GENERATED BY DEFAULT AS IDENTITY"));
        }
        finally
        {
            executor.TestInterceptAfterAddColumnStep = null;
        }

        TableColumnSchema? rid = Column(database, "rid");
        Assert.That(rid!.State, Is.EqualTo(SchemaElementState.DeleteOnly));
        Assert.That(database.Schema.Sequences.ContainsKey("items_rid_seq"), Is.True, "the column still draws from it");

        // The removal intent a take-back records first, as if the process stopped right after it.
        TableSchema items = database.Schema.Tables["items"];
        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = items.Id,
            ElementName = "rid",
            TargetState = SchemaElementState.Absent,
            ElementKind = SchemaElementKind.Column,
            ColumnType = ColumnType.Integer64,
            ColumnNotNull = true,
            ColumnDefaultSequenceId = rid.DefaultSequenceId,
        });

        SchemaChangeCoordinator resume = new(executor.Catalogs, logger)
        {
            BackfillAsync = executor.BackfillColumnDefaultsAsync,
            ReleaseOwnedSequenceAsync = executor.ReleaseOwnedSequenceAsync,
        };

        await resume.ResumeJobsAsync(database);

        Assert.That(Column(database, "rid"), Is.Null);
        Assert.That(database.Schema.Sequences.ContainsKey("items_rid_seq"), Is.False);
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);

        // A removal job that survives (a failed delete) finds the column gone and adds nothing.
        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = items.Id,
            ElementName = "rid",
            TargetState = SchemaElementState.Absent,
            ElementKind = SchemaElementKind.Column,
            ColumnType = ColumnType.Integer64,
        });

        await resume.ResumeJobsAsync(database);

        Assert.That(Column(database, "rid"), Is.Null);
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);

        // The name is free again.
        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN rid INT64 GENERATED BY DEFAULT AS IDENTITY");
        Assert.That((await ColumnValues(executor, dbname, "rid")).Select(v => v.LongValue).OrderBy(v => v), Is.EqualTo(new long[] { 1, 2 }));
    }
}

/// <summary><see cref="AddColumnConstraintScenarios"/> on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestAlterTableAddColumnConstraintsStandalone : AddColumnConstraintScenarios
{
    protected override bool ClusterMode => false;

    /// <summary>
    /// An add that stopped after WriteOnly and before the fill, as a crash would leave it. The column is
    /// not readable, and the next open of the database finishes the job: a standalone node never sees a
    /// schema-leader change that would resume it.
    /// </summary>
    [Test]
    public async Task AddStoppedBeforeFill_IsNotReadable_AndIsFinishedWhenTheDatabaseReopens()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdlSql(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)");
        await ExecuteNonQuerySql(executor, dbname, "INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b')");

        executor.TestInterceptAfterAddColumnStep = state =>
            state == SchemaElementState.WriteOnly ? throw new InvalidOperationException("stopped before the fill") : Task.CompletedTask;
        try
        {
            Assert.CatchAsync(async () =>
                await ExecuteDdlSql(executor, dbname, "ALTER TABLE items ADD COLUMN token UUID NOT NULL DEFAULT (gen_uuid_v7())"));
        }
        finally
        {
            executor.TestInterceptAfterAddColumnStep = null;
        }

        Assert.That(database.Schema.Tables["items"].Columns!.Single(c => c.Name == "token").State, Is.EqualTo(SchemaElementState.WriteOnly));
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Has.Count.EqualTo(1));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        DateTime deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline
               && reopened.Schema.Tables["items"].Columns!.Find(c => c.Name == "token")?.State != SchemaElementState.Public)
            await Task.Delay(50);

        Assert.That(reopened.Schema.Tables["items"].Columns!.Single(c => c.Name == "token").State, Is.EqualTo(SchemaElementState.Public));

        DateTime jobDeadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < jobDeadline && (await executor.Catalogs.LoadCoordinatorJobsAsync(reopened)).Count > 0)
            await Task.Delay(50);
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(reopened), Is.Empty);

        KvTransaction tx = await reopened.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "SELECT token FROM items", null));

            List<ColumnValue> tokens = [];
            await foreach (QueryResultRow row in cursor)
                tokens.Add(row.Row["token"]);

            Assert.That(tokens, Has.Count.EqualTo(2));
            Assert.That(tokens, Has.None.Property(nameof(ColumnValue.Type)).EqualTo(ColumnType.Null));
        }
        finally
        {
            await reopened.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteDdlSql(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteNonQuerySql(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}

/// <summary><see cref="AddColumnConstraintScenarios"/> on a cluster-mode engine (staged coordinator path).</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestAlterTableAddColumnConstraintsCluster : AddColumnConstraintScenarios
{
    protected override bool ClusterMode => true;
}
