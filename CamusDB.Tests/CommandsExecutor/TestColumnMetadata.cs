
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// T6 acceptance tests: MaxLength / ArrayElementType plumbing through the full
/// SQL → AST → ColumnInfo → TableColumnSchema pipeline.
/// </summary>
[NonParallelizable]
internal sealed class TestColumnMetadata : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs)> Setup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return (dbname, database, executor, new CatalogsManager(logger));
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
    }

    // ── Basic new scalar types ────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Float32Column_HasCorrectType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, v float32, PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "v")!;

        Assert.AreEqual(ColumnType.Float32, col.Type);
        Assert.IsNull(col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesColumn_HasCorrectType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, b bytes, PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "b")!;

        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.IsNull(col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task DateColumn_HasCorrectType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, d date, PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "d")!;

        Assert.AreEqual(ColumnType.Date, col.Type);
        Assert.IsNull(col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task DateTimeColumn_HasCorrectType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, ts datetime, PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "ts")!;

        Assert.AreEqual(ColumnType.DateTime, col.Type);
        Assert.IsNull(col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    // ── string(N) — sized string ──────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task StringSized_YieldsMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, nm string(32), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "nm")!;

        Assert.AreEqual(ColumnType.String, col.Type);
        Assert.AreEqual(32, col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task BareString_YieldsNullMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, nm string, PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "nm")!;

        Assert.AreEqual(ColumnType.String, col.Type);
        Assert.IsNull(col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task StringSized_LargeN_YieldsCorrectMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, bio string(4096), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "bio")!;

        Assert.AreEqual(ColumnType.String, col.Type);
        Assert.AreEqual(4096, col.MaxLength);
    }

    // ── array<T> — element type plumbing ─────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ArrayInt64_YieldsArrayTypeAndElementType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, tags array(int64), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "tags")!;

        Assert.AreEqual(ColumnType.Array, col.Type);
        Assert.AreEqual(ColumnType.Integer64, col.ArrayElementType);
        Assert.IsNull(col.MaxLength);
    }

    [Test]
    [NonParallelizable]
    public async Task ArrayString_YieldsArrayTypeAndStringElementType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, labels array(string), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "labels")!;

        Assert.AreEqual(ColumnType.Array, col.Type);
        Assert.AreEqual(ColumnType.String, col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task ArrayFloat32_YieldsArrayTypeAndFloat32ElementType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, nums array(float32), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "nums")!;

        Assert.AreEqual(ColumnType.Array, col.Type);
        Assert.AreEqual(ColumnType.Float32, col.ArrayElementType);
    }

    // ── nested array rejection ────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task NestedArray_IsRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecDDL(executor, db,
                "CREATE TABLE t (id OID NOT NULL, bad array(array(int64)), PRIMARY KEY (id))"));

        Assert.IsNotNull(ex);
        StringAssert.Contains("Nested arrays", ex!.Message);
    }

    // ── array-in-index rejection ──────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ArrayInPrimaryKey_IsRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecDDL(executor, db,
                "CREATE TABLE t (id OID NOT NULL, tags array(int64), PRIMARY KEY (tags))"));

        Assert.IsNotNull(ex);
        StringAssert.Contains("Array", ex!.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task ArrayInInlineUniqueConstraint_IsRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecDDL(executor, db,
                "CREATE TABLE t (id OID NOT NULL, tags array(int64) UNIQUE, PRIMARY KEY (id))"));

        Assert.IsNotNull(ex);
        StringAssert.Contains("Array", ex!.Message);
    }

    // ── DefaultStringMaxLength / DefaultBytesMaxLength constants ─────────────

    [Test]
    public void DefaultStringMaxLength_Is2621440()
    {
        Assert.AreEqual(2_621_440, CamusDBConfig.DefaultStringMaxLength);
    }

    [Test]
    public void DefaultBytesMaxLength_Is10MB()
    {
        Assert.AreEqual(10_485_760, CamusDBConfig.DefaultBytesMaxLength);
    }

    // ── Schema persist/reload round-trip ─────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task StringSized_PersistsAndReloads()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, nm string(128), PRIMARY KEY (id))");

        // Reload by re-opening the same database (exercises the JSON round-trip).
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        CatalogsManager catalogs2 = new(logger);
        TableSchema schema = catalogs2.GetTableSchema(reopened, "t");

        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "nm")!;
        Assert.AreEqual(ColumnType.String, col.Type);
        Assert.AreEqual(128, col.MaxLength);
    }

    [Test]
    [NonParallelizable]
    public async Task ArrayColumn_PersistsAndReloads()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, tags array(int64), PRIMARY KEY (id))");

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        CatalogsManager catalogs2 = new(logger);
        TableSchema schema = catalogs2.GetTableSchema(reopened, "t");

        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "tags")!;
        Assert.AreEqual(ColumnType.Array,    col.Type);
        Assert.AreEqual(ColumnType.Integer64, col.ArrayElementType);
        Assert.IsNull(col.MaxLength);
    }
}
