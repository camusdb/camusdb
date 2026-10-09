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
/// <para>The scenarios run twice: on a standalone engine, where the column is added and filled inside
/// one DDL transaction, and on a cluster-mode engine, where the schema-change coordinator stages the
/// column and fills it at its <c>WriteOnly</c> step.</para>
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

        // Proof of the path each engine took. Standalone: the add and the removal, one version each.
        // Cluster: Absent → DeleteOnly → WriteOnly, then back WriteOnly → DeleteOnly → Absent.
        Assert.That(database.Schema.SchemaVersion - versionBefore, Is.EqualTo(ClusterMode ? 4 : 2));
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

        // Standalone adds the column Public in one version; a cluster stages it through three.
        Assert.That(database.Schema.SchemaVersion - versionBefore, Is.EqualTo(ClusterMode ? 3 : 1));
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
}

/// <summary><see cref="AddColumnConstraintScenarios"/> on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestAlterTableAddColumnConstraintsStandalone : AddColumnConstraintScenarios
{
    protected override bool ClusterMode => false;
}

/// <summary><see cref="AddColumnConstraintScenarios"/> on a cluster-mode engine (staged coordinator path).</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestAlterTableAddColumnConstraintsCluster : AddColumnConstraintScenarios
{
    protected override bool ClusterMode => true;
}
