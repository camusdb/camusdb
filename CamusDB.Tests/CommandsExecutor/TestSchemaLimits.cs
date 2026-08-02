
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Validates the schema limits:
///   identifier length (MaxIdentifierLength)
///   columns per table (MaxColumnsPerTable)
///   indexes per table (MaxIndexesPerTable)
///   tables per database (MaxTablesPerDatabase)
/// Each test restores the overridden config knob in TearDown via NUnit's
/// [TearDown] attribute on a nested helper or via the shared TearDown in BaseTest.
/// Config is overridden locally because it's a static knob — tests that modify it
/// are marked [NonParallelizable] to prevent interference with parallel fixtures.
/// </summary>
[TestFixture]
public sealed class TestSchemaLimits : BaseTest
{

    /// <summary>
    /// A second engine over the same node whose identifier ceiling is <paramref name="maxIdentifierLength"/>.
    /// Validators fix their limits when the engine is built, so a case that must create a database under
    /// the default ceiling and then exercise a tighter one needs two engines rather than a flag flipped
    /// between the two steps.
    /// </summary>
    private CommandExecutor ExecutorWithIdentifierLimit(int maxIdentifierLength)
        => new(new CommandValidator(Options with { MaxIdentifierLength = maxIdentifierLength }),
               new CatalogsManager(logger), logger, Options with { MaxIdentifierLength = maxIdentifierLength },
               sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
    // ── helpers ────────────────────────────────────────────────────────────────

    private static CreateTableTicket BasicTable(string dbname, string tableName, int extraColumns = 0)
    {
        ColumnInfo[] cols =
        [
            new("id", ColumnType.Id),
            .. Enumerable.Range(0, extraColumns)
                         .Select(i => new ColumnInfo($"col_{i}", ColumnType.String))
        ];

        return new CreateTableTicket(
            databaseName: dbname,
            tableName: tableName,
            columns: cols,
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false
        );
    }

    private static AlterIndexTicket AddIndexTicket(string dbname, string tableName, string colName, string indexName)
        => new(
            databaseName: dbname,
            tableName: tableName,
            indexName: indexName,
            columns: [new(colName, OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        );

    // ── identifier length ──────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CreateTable_NameTooLong_ThrowsSchemaLimitExceeded()
    {
        // The database is created under the default limit — its auto-generated name is 32 chars — and
        // the table is created through an engine whose ceiling is 8, so the limit bites only the table
        // name under test.
        (string dbname, _, _) = await CreateDatabase();

        CommandExecutor tightExecutor = ExecutorWithIdentifierLimit(8);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await tightExecutor.CreateTable(BasicTable(dbname, "toolongname")));
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
        Assert.That(ex.Message, Does.Contain("toolongname"));
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_NameAtLimit_Succeeds()
    {
        (string dbname, _, _) = await CreateDatabase();

        bool ok = (await ExecutorWithIdentifierLimit(8).CreateTable(BasicTable(dbname, "exactly8"))).Success;
        Assert.IsTrue(ok);
    }

    [Test]
    [NonParallelizable]
    public async Task CreateDatabase_NameTooLong_ThrowsSchemaLimitExceeded()
    {
        CommandExecutor executor = ExecutorWithIdentifierLimit(6);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.CreateDatabase(new CreateDatabaseTicket("toolong_db", ifNotExists: false)));
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task AlterTable_ColumnNameTooLong_ThrowsSchemaLimitExceeded()
    {
        // The table is created under the default ceiling; the ALTER runs through an engine whose
        // ceiling is 10, so the limit bites only the column name under test.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await executor.CreateTable(BasicTable(dbname, "mytable"));

        {
            CommandExecutor tightExecutor = ExecutorWithIdentifierLimit(10);

            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await tightExecutor.AlterTable(new AlterTableTicket(
                    databaseName: dbname,
                    tableName: "mytable",
                    column: new ColumnInfo("col_toolong_name", ColumnType.String),
                    operation: AlterTableOperation.AddColumn)));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task AlterIndex_IndexNameTooLong_ThrowsSchemaLimitExceeded()
    {
        // The table is created under the default ceiling; the index is added through an engine whose
        // ceiling is 8, so the limit bites only the index name under test.
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await executor.CreateTable(BasicTable(dbname, "t", extraColumns: 1));

        CommandExecutor tightExecutor = ExecutorWithIdentifierLimit(8);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await tightExecutor.AlterIndex(AddIndexTicket(dbname, "t", "col_0", "toolong_idx")));
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public void IdentifierLimitDisabled_ZeroMeansNoCheck()
    {
        int saved = CamusDBConfig.MaxIdentifierLength;
        CamusDBConfig.MaxIdentifierLength = 0;
        try
        {
            // ValidateIdentifier must not throw for a very long name when limit is 0.
            Assert.DoesNotThrow(() =>
            {
                // Invoke via CreateDatabaseValidator directly — avoids spinning up a DB.
                var validator = new CamusDB.Core.CommandsValidator.CommandValidator(CamusDBConfig.Ambient);
                // A 300-char name — would have failed under the old 255 limit.
                string longName = new('a', 300);
                validator.Validate(new CreateDatabaseTicket(longName, ifNotExists: false));
            });
        }
        finally { CamusDBConfig.MaxIdentifierLength = saved; }
    }

    // ── columns per table ──────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CreateTable_TooManyColumns_ThrowsSchemaLimitExceeded()
    {
        int saved = CamusDBConfig.MaxColumnsPerTable;
        CamusDBConfig.MaxColumnsPerTable = 3;
        try
        {
            // 4 columns (id + col_0 + col_1 + col_2) — one over the limit of 3.
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();

            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.CreateTable(BasicTable(dbname, "wide", extraColumns: 3)));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
            Assert.That(ex.Message, Does.Contain("3"));
        }
        finally { CamusDBConfig.MaxColumnsPerTable = saved; }
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_AtColumnLimit_Succeeds()
    {
        int saved = CamusDBConfig.MaxColumnsPerTable;
        CamusDBConfig.MaxColumnsPerTable = 3;
        try
        {
            // exactly 3 columns (id + col_0 + col_1).
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            bool ok = (await executor.CreateTable(BasicTable(dbname, "t", extraColumns: 2))).Success;
            Assert.IsTrue(ok);
        }
        finally { CamusDBConfig.MaxColumnsPerTable = saved; }
    }

    [Test]
    [NonParallelizable]
    public async Task AlterTable_AddColumnExceedsLimit_ThrowsSchemaLimitExceeded()
    {
        int saved = CamusDBConfig.MaxColumnsPerTable;
        CamusDBConfig.MaxColumnsPerTable = 2;
        try
        {
            // Create table with 2 columns (id + col_0) — at the limit.
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            await executor.CreateTable(BasicTable(dbname, "t", extraColumns: 1));

            // Adding a third column should be rejected.
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.AlterTable(new AlterTableTicket(
                    databaseName: dbname,
                    tableName: "t",
                    column: new ColumnInfo("extra", ColumnType.String),
                    operation: AlterTableOperation.AddColumn)));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
        }
        finally { CamusDBConfig.MaxColumnsPerTable = saved; }
    }

    // ── indexes per table ──────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task AlterIndex_TooManyIndexes_ThrowsSchemaLimitExceeded()
    {
        int saved = CamusDBConfig.MaxIndexesPerTable;
        CamusDBConfig.MaxIndexesPerTable = 1;
        try
        {
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            await executor.CreateTable(BasicTable(dbname, "t", extraColumns: 2));

            // First user index — allowed.
            await executor.AlterIndex(AddIndexTicket(dbname, "t", "col_0", "idx0"));

            // Second user index — exceeds limit.
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.AlterIndex(AddIndexTicket(dbname, "t", "col_1", "idx1")));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
        }
        finally { CamusDBConfig.MaxIndexesPerTable = saved; }
    }

    [Test]
    [NonParallelizable]
    public async Task AlterIndex_PrimaryKeyDoesNotCountTowardLimit()
    {
        int saved = CamusDBConfig.MaxIndexesPerTable;
        CamusDBConfig.MaxIndexesPerTable = 1;
        try
        {
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            await executor.CreateTable(BasicTable(dbname, "t", extraColumns: 1));

            // The PK is already present (~pk); adding one user index must succeed.
            bool ok = (await executor.AlterIndex(AddIndexTicket(dbname, "t", "col_0", "idx0")));
            Assert.IsTrue(ok);
        }
        finally { CamusDBConfig.MaxIndexesPerTable = saved; }
    }

    // ── tables per database ────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CreateTable_TooManyTables_ThrowsSchemaLimitExceeded()
    {
        int saved = CamusDBConfig.MaxTablesPerDatabase;
        CamusDBConfig.MaxTablesPerDatabase = 2;
        try
        {
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            await executor.CreateTable(BasicTable(dbname, "t1"));
            await executor.CreateTable(BasicTable(dbname, "t2"));

            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.CreateTable(BasicTable(dbname, "t3")));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
            Assert.That(ex.Message, Does.Contain("2"));
        }
        finally { CamusDBConfig.MaxTablesPerDatabase = saved; }
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_IfNotExists_ExistingTableAtLimit_IsNoOp()
    {
        int saved = CamusDBConfig.MaxTablesPerDatabase;
        CamusDBConfig.MaxTablesPerDatabase = 1;
        try
        {
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            await executor.CreateTable(BasicTable(dbname, "t1"));

            // IF NOT EXISTS on the existing table must succeed even though db is at the limit.
            CreateTableTicket ifNotExists = new(
                databaseName: dbname,
                tableName: "t1",
                columns: [new("id", ColumnType.Id)],
                constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
                ifNotExists: true
            );
            bool result = (await executor.CreateTable(ifNotExists)).Success;
            Assert.IsFalse(result, "IF NOT EXISTS on an existing table returns false");
        }
        finally { CamusDBConfig.MaxTablesPerDatabase = saved; }
    }
}
