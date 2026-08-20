
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests: <c>octet_length</c> and <c>vector_dims</c> in a projection, in a WHERE clause,
/// and inside a CHECK constraint — including after the constraint is persisted and re-parsed at
/// table open, which is the path a stored dimension check actually takes in production.
/// </summary>
internal sealed class TestVectorScalarFunctions : SharedNodeBaseTest
{
    // ── Setup helpers ─────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTable(string ddl)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, ddl, null));
        return (dbname, db, executor);
    }

    private static async Task ExecInsert(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecSelect(
        CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, null));
        return await cursor.ToListAsync();
    }

    private static string OID => ObjectIdGenerator.Generate().ToString();

    /// <summary>A vector of <paramref name="dimensions"/> float32 elements, as packed bytes.</summary>
    private static ColumnValue Vector(int dimensions) => new(new byte[dimensions * 4]);

    private static Task InsertVector(CommandExecutor executor, DatabaseDescriptor db, ColumnValue value)
        => ExecInsert(executor, db, "INSERT INTO t (id, v) VALUES (@id, @v)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@v", value } });

    // ── octet_length ──────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task OctetLength_Bytes_ReturnsByteCount()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), PRIMARY KEY (id))");
        await InsertVector(executor, db, Vector(768));

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT octet_length(v) AS n FROM t");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["n"].Type);
        Assert.AreEqual(3072L, rows[0].Row["n"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task OctetLength_String_CountsUtf8BytesNotCharacters()
    {
        // 'áé' is 2 characters and 4 UTF-8 bytes. A character count here would be a real defect,
        // because the whole point of octet_length is to measure storage rather than text.
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, s string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, "INSERT INTO t (id, s) VALUES (@id, @s)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@s", new(ColumnType.String, "áé") } });

        List<QueryResultRow> rows = await ExecSelect(executor, db,
            "SELECT octet_length(s) AS octets, length(s) AS chars FROM t");

        Assert.AreEqual(4L, rows[0].Row["octets"].LongValue);
        Assert.AreEqual(2L, rows[0].Row["chars"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task OctetLength_Null_ReturnsNull()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), PRIMARY KEY (id))");
        await InsertVector(executor, db, ColumnValue.Null);

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT octet_length(v) AS n FROM t");

        Assert.AreEqual(ColumnType.Null, rows[0].Row["n"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task OctetLength_RejectsATypeItCannotMeasure()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, n int64, PRIMARY KEY (id))");
        await ExecInsert(executor, db, "INSERT INTO t (id, n) VALUES (@id, @n)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@n", new(ColumnType.Integer64, 7L) } });

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecSelect(executor, db, "SELECT octet_length(n) AS x FROM t"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("octet_length", ex.Message);
    }

    // ── vector_dims ───────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task VectorDims_ReturnsElementCount()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), PRIMARY KEY (id))");
        await InsertVector(executor, db, Vector(768));

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT vector_dims(v) AS d FROM t");

        Assert.AreEqual(768L, rows[0].Row["d"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task VectorDims_Null_ReturnsNull()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), PRIMARY KEY (id))");
        await InsertVector(executor, db, ColumnValue.Null);

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT vector_dims(v) AS d FROM t");

        Assert.AreEqual(ColumnType.Null, rows[0].Row["d"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task VectorDims_ByteCountNotDivisibleByFour_RaisesMalformedVector()
    {
        // 3070 bytes would floor to 767 dimensions and quietly satisfy a check written for 767.
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), PRIMARY KEY (id))");
        await InsertVector(executor, db, new ColumnValue(new byte[3070]));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecSelect(executor, db, "SELECT vector_dims(v) AS d FROM t"));

        Assert.AreEqual(CamusDBErrorCodes.MalformedVector, ex!.Code);
        StringAssert.Contains("3070", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task VectorDims_RejectsString()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, s string, PRIMARY KEY (id))");
        await ExecInsert(executor, db, "INSERT INTO t (id, s) VALUES (@id, @s)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@s", new(ColumnType.String, "abcd") } });

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecSelect(executor, db, "SELECT vector_dims(s) AS d FROM t"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task VectorDims_FiltersInWhereClause()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(4096), PRIMARY KEY (id))");
        await InsertVector(executor, db, Vector(768));
        await InsertVector(executor, db, Vector(512));
        await InsertVector(executor, db, Vector(768));

        List<QueryResultRow> rows = await ExecSelect(executor, db,
            "SELECT vector_dims(v) AS d FROM t WHERE vector_dims(v) = 768");

        Assert.AreEqual(2, rows.Count);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual(768L, row.Row["d"].LongValue);
    }

    // ── CHECK constraints — the reason both functions exist ───────────────────

    [Test]
    [NonParallelizable]
    public async Task Check_VectorDims_AcceptsTheExactWidth()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), " +
            "CONSTRAINT v_is_768d CHECK (vector_dims(v) = 768), PRIMARY KEY (id))");

        await InsertVector(executor, db, Vector(768));

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT vector_dims(v) AS d FROM t");
        Assert.AreEqual(768L, rows[0].Row["d"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Check_VectorDims_RejectsAShortVector()
    {
        // 767 floats fit inside bytes(3072), so the column's own maximum accepts this row.
        // Only the CHECK can reject it — which is exactly why this task exists.
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), " +
            "CONSTRAINT v_is_768d CHECK (vector_dims(v) = 768), PRIMARY KEY (id))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await InsertVector(executor, db, Vector(767)));

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);
        StringAssert.Contains("v_is_768d", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Check_OctetLength_RejectsAShortVector()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), " +
            "CONSTRAINT v_is_3072b CHECK (octet_length(v) = 3072), PRIMARY KEY (id))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await InsertVector(executor, db, Vector(767)));

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task Check_VectorDims_NullPassesUnderThreeValuedLogic()
    {
        // vector_dims(NULL) is NULL, so `NULL = 768` is unknown, and a CHECK is violated only on
        // false. A NOT NULL column is the way to forbid the missing vector, not the CHECK.
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), " +
            "CONSTRAINT v_is_768d CHECK (vector_dims(v) = 768), PRIMARY KEY (id))");

        await InsertVector(executor, db, ColumnValue.Null);

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT vector_dims(v) AS d FROM t");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["d"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task Check_VectorDims_StillEnforcedAfterCloseAndReopen()
    {
        // The stored condition is text; the AST is re-parsed at table open. A dimension check that
        // works only before a restart would be worse than none.
        (string dbname, _, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(3072), " +
            "CONSTRAINT v_is_768d CHECK (vector_dims(v) = 768), PRIMARY KEY (id))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await InsertVector(executor, reopened, Vector(767)));

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code);

        await InsertVector(executor, reopened, Vector(768));

        List<QueryResultRow> rows = await ExecSelect(executor, reopened, "SELECT vector_dims(v) AS d FROM t");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(768L, rows[0].Row["d"].LongValue);
    }
}
