
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
/// Acceptance tests: MaxLength / ArrayElementType plumbing through the full
/// SQL → AST → ColumnInfo → TableColumnSchema pipeline.
/// </summary>
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

    [Test]
    [NonParallelizable]
    public async Task StringSized_ViaAlterAddColumn_YieldsMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, PRIMARY KEY (id))");
        await ExecDDL(executor, db, "ALTER TABLE t ADD COLUMN nm string(32)");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "nm")!;

        Assert.AreEqual(ColumnType.String, col.Type);
        Assert.AreEqual(32, col.MaxLength);
    }

    // ── bytes(N) — sized bytes ────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task BytesSized_YieldsMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, embedding bytes(3072), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "embedding")!;

        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.AreEqual(3072, col.MaxLength);
        Assert.IsNull(col.ArrayElementType);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_ViaAlterAddColumn_YieldsMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, PRIMARY KEY (id))");
        await ExecDDL(executor, db, "ALTER TABLE t ADD COLUMN embedding bytes(3072)");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "embedding")!;

        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.AreEqual(3072, col.MaxLength);
    }

    [Test]
    [NonParallelizable]
    public async Task BareBytes_YieldsNullMaxLength()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, payload bytes, PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "payload")!;

        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.IsNull(col.MaxLength);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_AboveDefaultCap_IsAcceptedAsTheCeiling()
    {
        // An explicit N is the column ceiling, matching string(N). It is not clamped to
        // DefaultBytesMaxLength, which applies only when no size is declared.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, CatalogsManager catalogs) = await Setup();
        int n = CamusDBConstants.DefaultBytesMaxLength + 1024;
        await ExecDDL(executor, db, $"CREATE TABLE t (id OID NOT NULL, payload bytes({n}), PRIMARY KEY (id))");

        TableSchema schema = catalogs.GetTableSchema(db, "t");
        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "payload")!;

        Assert.AreEqual(n, col.MaxLength);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_ZeroSize_IsRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecDDL(executor, db,
                "CREATE TABLE t (id OID NOT NULL, payload bytes(0), PRIMARY KEY (id))"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("positive integer", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_SizeAboveInt32Range_IsRejected()
    {
        // int.TryParse fails rather than wrapping, so an out-of-range size is a client error.
        (_, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecDDL(executor, db,
                "CREATE TABLE t (id OID NOT NULL, payload bytes(99999999999), PRIMARY KEY (id))"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("bytes size", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_ViaAlterAddColumn_ZeroSize_IsRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, PRIMARY KEY (id))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecDDL(executor, db, "ALTER TABLE t ADD COLUMN payload bytes(0)"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("positive integer", ex.Message);
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
        Assert.AreEqual(2_621_440, CamusDBConstants.DefaultStringMaxLength);
    }

    [Test]
    public void DefaultBytesMaxLength_Is10MB()
    {
        Assert.AreEqual(10_485_760, CamusDBConstants.DefaultBytesMaxLength);
    }

    // ── Schema persist/reload round-trip ─────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task BytesSized_PersistsAndReloads()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, _) = await Setup();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, embedding bytes(3072), PRIMARY KEY (id))");

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        CatalogsManager catalogs2 = new(logger);
        TableSchema schema = catalogs2.GetTableSchema(reopened, "t");

        TableColumnSchema col = schema.Columns!.Find(c => c.Name == "embedding")!;
        Assert.AreEqual(ColumnType.Bytes, col.Type);
        Assert.AreEqual(3072, col.MaxLength);
    }

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
