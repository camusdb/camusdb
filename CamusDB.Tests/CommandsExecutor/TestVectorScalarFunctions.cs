
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers.Binary;
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

    // ── Distance metrics ──────────────────────────────────────────────────────

    /// <summary>Packs float32 elements little-endian, matching the documented wire contract.</summary>
    private static ColumnValue Pack(float[] elements)
    {
        byte[] bytes = new byte[elements.Length * 4];

        for (int i = 0; i < elements.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), elements[i]);

        return new ColumnValue(bytes);
    }

    private static float[] RandomElements(Random random, int dimensions)
    {
        float[] elements = new float[dimensions];

        for (int i = 0; i < dimensions; i++)
            elements[i] = (float)(random.NextDouble() * 2d - 1d);

        return elements;
    }

    /// <summary>
    /// Compensated (Kahan) summation, used only by the reference values below. Summing in a different
    /// way from the engine is the point: a reference that repeated the engine's own naive loop would
    /// agree with it even when both are wrong.
    /// </summary>
    private static double KahanSum(IEnumerable<double> terms)
    {
        double sum = 0d;
        double compensation = 0d;

        foreach (double term in terms)
        {
            double adjusted = term - compensation;
            double next = sum + adjusted;
            compensation = next - sum - adjusted;
            sum = next;
        }

        return sum;
    }

    private static double ReferenceL2(float[] a, float[] b)
        => Math.Sqrt(KahanSum(a.Zip(b, (x, y) => ((double)x - y) * ((double)x - y))));

    private static double ReferenceInnerProduct(float[] a, float[] b)
        => KahanSum(a.Zip(b, (x, y) => (double)x * y));

    private static double ReferenceCosineDistance(float[] a, float[] b)
    {
        double dot = ReferenceInnerProduct(a, b);
        double magnitudeA = Math.Sqrt(KahanSum(a.Select(x => (double)x * x)));
        double magnitudeB = Math.Sqrt(KahanSum(b.Select(x => (double)x * x)));
        return 1d - dot / (magnitudeA * magnitudeB);
    }

    /// <summary>
    /// Relative tolerance for a 768-dimension double accumulation compared against a compensated
    /// reference. Naive summation of ~768 terms loses far less than this; a wider gap means an
    /// arithmetic error, not rounding.
    /// </summary>
    private const double Tolerance = 1e-9;

    private static void AssertClose(double expected, double actual, string label)
    {
        double allowed = Tolerance * Math.Max(1d, Math.Abs(expected));
        Assert.That(actual, Is.EqualTo(expected).Within(allowed), label);
    }

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupVectorTable()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id OID NOT NULL, v bytes(4096), PRIMARY KEY (id))");
        return (db, executor);
    }

    private static async Task<double> Metric(
        CommandExecutor executor, DatabaseDescriptor db, string metric, ColumnValue query)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, $"SELECT {metric}(v, @q) AS d FROM t", new() { { "@q", query } }));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Float64, rows[0].Row["d"].Type);
        return rows[0].Row["d"].FloatValue;
    }

    [Test]
    [NonParallelizable]
    public async Task Metrics_MatchAnIndependentReference()
    {
        // Fixed seed: a failure must be reproducible.
        Random random = new(20260819);
        float[] stored = RandomElements(random, 768);
        float[] query = RandomElements(random, 768);

        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Pack(stored));

        ColumnValue queryVector = Pack(query);

        AssertClose(ReferenceL2(stored, query),
            await Metric(executor, db, "l2_distance", queryVector), "l2_distance");
        AssertClose(ReferenceInnerProduct(stored, query),
            await Metric(executor, db, "inner_product", queryVector), "inner_product");
        AssertClose(ReferenceCosineDistance(stored, query),
            await Metric(executor, db, "cosine_distance", queryVector), "cosine_distance");
    }

    [Test]
    [NonParallelizable]
    public async Task L2Distance_OfIdenticalVectorsIsZero()
    {
        float[] elements = RandomElements(new Random(7), 128);

        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Pack(elements));

        Assert.AreEqual(0d, await Metric(executor, db, "l2_distance", Pack(elements)));
    }

    [Test]
    [NonParallelizable]
    public async Task CosineDistance_OfIdenticalVectorsIsZeroAndNeverNegative()
    {
        // Without clamping, rounding can push the similarity a few ulps past 1 and produce a small
        // negative distance, which would sort ahead of a genuine exact match.
        float[] elements = RandomElements(new Random(11), 768);

        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Pack(elements));

        double distance = await Metric(executor, db, "cosine_distance", Pack(elements));

        Assert.GreaterOrEqual(distance, 0d, "cosine distance must never be negative");
        Assert.LessOrEqual(distance, Tolerance);
    }

    [Test]
    [NonParallelizable]
    public async Task CosineDistance_OfOppositeVectorsIsTwo()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Pack([1f, 0f, 0f, 0f]));

        AssertClose(2d, await Metric(executor, db, "cosine_distance", Pack([-1f, 0f, 0f, 0f])), "opposite");
    }

    [Test]
    [NonParallelizable]
    public async Task Metrics_RankInTheDocumentedDirection()
    {
        // near is closer to the query than far. L2 and cosine must report near as smaller;
        // inner_product must report it as larger. A sign error would still return plausible rows.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();

        ColumnValue query = Pack([1f, 0f, 0f, 0f]);
        ColumnValue near = Pack([0.9f, 0.1f, 0f, 0f]);
        ColumnValue far = Pack([0f, 1f, 0f, 0f]);

        await ExecInsert(executor, db, "INSERT INTO t (id, v) VALUES (@id, @v)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@v", near } });

        double nearL2 = await Metric(executor, db, "l2_distance", query);
        double nearDot = await Metric(executor, db, "inner_product", query);
        double nearCosine = await Metric(executor, db, "cosine_distance", query);

        (DatabaseDescriptor db2, CommandExecutor executor2) = await SetupVectorTable();
        await ExecInsert(executor2, db2, "INSERT INTO t (id, v) VALUES (@id, @v)",
            new() { { "@id", new(ColumnType.Id, OID) }, { "@v", far } });

        double farL2 = await Metric(executor2, db2, "l2_distance", query);
        double farDot = await Metric(executor2, db2, "inner_product", query);
        double farCosine = await Metric(executor2, db2, "cosine_distance", query);

        Assert.Less(nearL2, farL2, "l2_distance: nearer row must score lower");
        Assert.Greater(nearDot, farDot, "inner_product: nearer row must score HIGHER");
        Assert.Less(nearCosine, farCosine, "cosine_distance: nearer row must score lower");
    }

    [Test]
    [NonParallelizable]
    public async Task Metrics_DoNotOverflowOnExtremeFiniteValues()
    {
        // Every intermediate is widened to double before multiplying. In float, MaxValue squared is
        // infinity, so a float accumulator would return Infinity or NaN here.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        float[] extreme = Enumerable.Repeat(float.MaxValue, 512).ToArray();
        await InsertVector(executor, db, Pack(extreme));

        double dot = await Metric(executor, db, "inner_product", Pack(extreme));
        double l2 = await Metric(executor, db, "l2_distance", Pack(extreme));

        Assert.IsTrue(double.IsFinite(dot), $"inner_product overflowed: {dot}");
        AssertClose(512d * (double)float.MaxValue * float.MaxValue, dot, "inner_product");
        Assert.AreEqual(0d, l2);
    }

    [Test]
    [NonParallelizable]
    public async Task Metrics_RejectMismatchedDimensions()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Vector(768));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Metric(executor, db, "l2_distance", Vector(512)));

        Assert.AreEqual(CamusDBErrorCodes.VectorDimensionMismatch, ex!.Code);
        StringAssert.Contains("768", ex.Message);
        StringAssert.Contains("512", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Metrics_RejectANonFiniteElement()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Pack([1f, float.NaN, 0f, 0f]));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Metric(executor, db, "l2_distance", Pack([1f, 1f, 1f, 1f])));

        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task CosineDistance_RejectsAZeroMagnitudeOperand()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, Pack([0f, 0f, 0f, 0f]));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Metric(executor, db, "cosine_distance", Pack([1f, 0f, 0f, 0f])));

        Assert.AreEqual(CamusDBErrorCodes.InvalidVectorValue, ex!.Code);
        StringAssert.Contains("cosine_distance", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Metrics_ReturnNullForANullOperand()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupVectorTable();
        await InsertVector(executor, db, ColumnValue.Null);

        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name,
                "SELECT l2_distance(v, @q) AS a, inner_product(v, @q) AS b, cosine_distance(v, @q) AS c FROM t",
                new() { { "@q", Pack([1f, 0f, 0f, 0f]) } }));

        List<QueryResultRow> rows = await cursor.ToListAsync();

        Assert.AreEqual(ColumnType.Null, rows[0].Row["a"].Type);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["b"].Type);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["c"].Type);
    }
}
