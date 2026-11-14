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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests of the trigonometric functions, the degree forms, <c>radians</c>, <c>degrees</c>
/// and <c>pi()</c>. Every expected value and every error was measured on PostgreSQL 16 and written
/// here by hand, so this fixture does not compare the engine with itself.
/// <para>
/// A radian form calls the platform math library, as PostgreSQL does, so its expected value is
/// compared within one unit in the last place: two correct libraries can round the last bit in
/// different directions. The degree forms at the common angles, <c>radians</c>, <c>degrees</c> and
/// <c>pi()</c> are compared exactly, because their exactness is the behavior under test.
/// </para>
/// </summary>
[NonParallelizable]
public sealed class TestTrigonometricFunctions : SharedNodeBaseTest
{
    private const string Infinity = "CAST('Infinity' AS float64)";
    private const string NegativeInfinity = "CAST('-Infinity' AS float64)";
    private const string NaN = "CAST('NaN' AS float64)";
    private const string NegativeZero = "CAST('-0' AS float64)";

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE angles (id INT64 PRIMARY KEY, d FLOAT64, i INT64, n NUMERIC)");
        await Dml(database, executor, dbname,
            "INSERT INTO angles (id, d, i, n) VALUES (1, 0, 0, NUMERIC '0'), (2, 90, 1, NUMERIC '0.5'), (3, 180, 2, NUMERIC '1'), (4, 270, -1, NUMERIC '-0.5')");
        return (dbname, database, executor);
    }

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Dml(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> Query(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static CamusDBException Refused(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await Query(database, executor, dbname, sql))!;

    private async Task<ColumnValue> Scalar(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        return (await Query(database, executor, dbname, sql))[0].Row.First().Value;
    }

    private static List<long> Ids(List<QueryResultRow> rows) => rows.Select(r => r.Row["id"].LongValue).ToList();

    // ── Radian forms ──

    [TestCase("SELECT sin(0)", 0.0, TestName = "Sin_OfZero")]
    [TestCase("SELECT sin(1)", 0.8414709848078965, TestName = "Sin_OfOne")]
    [TestCase("SELECT sin(-1)", -0.8414709848078965, TestName = "Sin_OfANegativeAngle")]
    [TestCase("SELECT sin(-pi() / 2)", -1.0, TestName = "Sin_OfMinusHalfPi")]
    [TestCase("SELECT sin(pi())", 1.2246467991473532e-16, TestName = "Sin_OfPi_IsTheLibraryValue")]
    [TestCase("SELECT sin(1e22)", -0.8522008497671888, TestName = "Sin_OfALargeAngle")]
    [TestCase("SELECT sin(1e300)", -0.8178819121159085, TestName = "Sin_OfAVeryLargeAngle")]
    [TestCase("SELECT cos(0)", 1.0, TestName = "Cos_OfZero")]
    [TestCase("SELECT cos(1)", 0.5403023058681398, TestName = "Cos_OfOne")]
    [TestCase("SELECT cos(-1)", 0.5403023058681398, TestName = "Cos_IsEven")]
    [TestCase("SELECT cos(pi() / 2)", 6.123233995736766e-17, TestName = "Cos_OfHalfPi_IsTheLibraryValue")]
    [TestCase("SELECT cos(pi())", -1.0, TestName = "Cos_OfPi")]
    [TestCase("SELECT cos(1e300)", -0.5753861119575491, TestName = "Cos_OfAVeryLargeAngle")]
    [TestCase("SELECT tan(0)", 0.0, TestName = "Tan_OfZero")]
    [TestCase("SELECT tan(1)", 1.557407724654902, TestName = "Tan_OfOne")]
    [TestCase("SELECT tan(-2)", 2.185039863261519, TestName = "Tan_OfANegativeAngle")]
    [TestCase("SELECT tan(pi() / 4)", 0.9999999999999999, TestName = "Tan_OfQuarterPi")]
    [TestCase("SELECT tan(pi() / 2)", 1.633123935319537e+16, TestName = "Tan_OfHalfPi_IsFinite")]
    [TestCase("SELECT cot(1)", 0.6420926159343308, TestName = "Cot_OfOne")]
    [TestCase("SELECT cot(-2)", 0.45765755436028577, TestName = "Cot_OfANegativeAngle")]
    [TestCase("SELECT asin(0)", 0.0, TestName = "Asin_OfZero")]
    [TestCase("SELECT asin(1)", 1.5707963267948966, TestName = "Asin_OfOne")]
    [TestCase("SELECT asin(-1)", -1.5707963267948966, TestName = "Asin_OfMinusOne")]
    [TestCase("SELECT acos(1)", 0.0, TestName = "Acos_OfOne")]
    [TestCase("SELECT acos(0)", 1.5707963267948966, TestName = "Acos_OfZero")]
    [TestCase("SELECT acos(-1)", 3.141592653589793, TestName = "Acos_OfMinusOne")]
    [TestCase("SELECT atan(1)", 0.7853981633974483, TestName = "Atan_OfOne")]
    [TestCase("SELECT atan(-2)", -1.1071487177940906, TestName = "Atan_OfANegativeValue")]
    [TestCase("SELECT atan(" + Infinity + ")", 1.5707963267948966, TestName = "Atan_OfInfinity")]
    [TestCase("SELECT atan(" + NegativeInfinity + ")", -1.5707963267948966, TestName = "Atan_OfNegativeInfinity")]
    [TestCase("SELECT atan2(0, 0)", 0.0, TestName = "Atan2_OfTheOrigin")]
    [TestCase("SELECT atan2(1, 1)", 0.7853981633974483, TestName = "Atan2_FirstQuadrant")]
    [TestCase("SELECT atan2(1, -1)", 2.356194490192345, TestName = "Atan2_SecondQuadrant")]
    [TestCase("SELECT atan2(-1, -1)", -2.356194490192345, TestName = "Atan2_ThirdQuadrant")]
    [TestCase("SELECT atan2(-1, 1)", -0.7853981633974483, TestName = "Atan2_FourthQuadrant")]
    [TestCase("SELECT atan2(0, -1)", 3.141592653589793, TestName = "Atan2_NegativeXAxis")]
    [TestCase("SELECT atan2(" + NegativeZero + ", -1)", -3.141592653589793, TestName = "Atan2_NegativeZeroYSelectsTheLowerSide")]
    [TestCase("SELECT atan2(" + Infinity + ", 1)", 1.5707963267948966, TestName = "Atan2_InfiniteY")]
    [TestCase("SELECT atan2(1, " + Infinity + ")", 0.0, TestName = "Atan2_InfiniteX")]
    [TestCase("SELECT atan2(" + Infinity + ", " + Infinity + ")", 0.7853981633974483, TestName = "Atan2_BothInfinite")]
    public async Task RadianForm_HasTheValueOfPostgreSql(string sql, double expected)
    {
        ColumnValue value = await Scalar(sql);
        Assert.AreEqual(ColumnType.Float64, value.Type);
        Assert.That(value.FloatValue, Is.EqualTo(expected).Within(1).Ulps);
    }

    [TestCase("SELECT cot(0)", double.PositiveInfinity, TestName = "Cot_OfZero_IsInfinity")]
    [TestCase("SELECT cot(" + NegativeZero + ")", double.NegativeInfinity, TestName = "Cot_OfNegativeZero_IsNegativeInfinity")]
    public async Task Cot_OfZero_IsAnInfinityNotAnError(string sql, double expected)
    {
        Assert.AreEqual(expected, (await Scalar(sql)).FloatValue);
    }

    [TestCase("SELECT sin(" + NegativeZero + ")", TestName = "Sin_KeepsTheSignOfZero")]
    [TestCase("SELECT asin(" + NegativeZero + ")", TestName = "Asin_KeepsTheSignOfZero")]
    [TestCase("SELECT sind(-180)", TestName = "Sind_OfMinus180_IsNegativeZero")]
    public async Task SignedZero_IsKept(string sql)
    {
        double value = (await Scalar(sql)).FloatValue;
        Assert.AreEqual(0.0, value);
        Assert.IsTrue(double.IsNegative(value), "PostgreSQL 16 gives -0");
    }

    // ── Degree forms ──

    [TestCase("SELECT sind(0)", 0.0, TestName = "Sind_Of0")]
    [TestCase("SELECT sind(30)", 0.5, TestName = "Sind_Of30")]
    [TestCase("SELECT sind(90)", 1.0, TestName = "Sind_Of90")]
    [TestCase("SELECT sind(150)", 0.5, TestName = "Sind_Of150")]
    [TestCase("SELECT sind(180)", 0.0, TestName = "Sind_Of180")]
    [TestCase("SELECT sind(210)", -0.5, TestName = "Sind_Of210")]
    [TestCase("SELECT sind(270)", -1.0, TestName = "Sind_Of270")]
    [TestCase("SELECT sind(360)", 0.0, TestName = "Sind_Of360")]
    [TestCase("SELECT sind(390)", 0.5, TestName = "Sind_Of390_WrapsAround")]
    [TestCase("SELECT sind(1e300)", 0.0, TestName = "Sind_OfAVeryLargeAngle_ReducesExactly")]
    [TestCase("SELECT cosd(0)", 1.0, TestName = "Cosd_Of0")]
    [TestCase("SELECT cosd(60)", 0.5, TestName = "Cosd_Of60")]
    [TestCase("SELECT cosd(-60)", 0.5, TestName = "Cosd_OfMinus60")]
    [TestCase("SELECT cosd(90)", 0.0, TestName = "Cosd_Of90")]
    [TestCase("SELECT cosd(120)", -0.5, TestName = "Cosd_Of120")]
    [TestCase("SELECT cosd(180)", -1.0, TestName = "Cosd_Of180")]
    [TestCase("SELECT cosd(240)", -0.5, TestName = "Cosd_Of240")]
    [TestCase("SELECT cosd(1e300)", 1.0, TestName = "Cosd_OfAVeryLargeAngle_ReducesExactly")]
    [TestCase("SELECT tand(0)", 0.0, TestName = "Tand_Of0")]
    [TestCase("SELECT tand(45)", 1.0, TestName = "Tand_Of45")]
    [TestCase("SELECT tand(135)", -1.0, TestName = "Tand_Of135")]
    [TestCase("SELECT tand(225)", 1.0, TestName = "Tand_Of225")]
    [TestCase("SELECT tand(90)", double.PositiveInfinity, TestName = "Tand_Of90_IsInfinity")]
    [TestCase("SELECT tand(-90)", double.NegativeInfinity, TestName = "Tand_OfMinus90_IsNegativeInfinity")]
    [TestCase("SELECT tand(270)", double.NegativeInfinity, TestName = "Tand_Of270_IsNegativeInfinity")]
    [TestCase("SELECT cotd(45)", 1.0, TestName = "Cotd_Of45")]
    [TestCase("SELECT cotd(90)", 0.0, TestName = "Cotd_Of90")]
    [TestCase("SELECT cotd(0)", double.PositiveInfinity, TestName = "Cotd_Of0_IsInfinity")]
    [TestCase("SELECT cotd(180)", double.NegativeInfinity, TestName = "Cotd_Of180_IsNegativeInfinity")]
    [TestCase("SELECT cotd(" + NegativeZero + ")", double.NegativeInfinity, TestName = "Cotd_OfNegativeZero_IsNegativeInfinity")]
    [TestCase("SELECT asind(0.5)", 30.0, TestName = "Asind_OfHalf")]
    [TestCase("SELECT asind(-0.5)", -30.0, TestName = "Asind_OfMinusHalf")]
    [TestCase("SELECT asind(1)", 90.0, TestName = "Asind_OfOne")]
    [TestCase("SELECT asind(-1)", -90.0, TestName = "Asind_OfMinusOne")]
    [TestCase("SELECT acosd(0)", 90.0, TestName = "Acosd_OfZero")]
    [TestCase("SELECT acosd(0.5)", 60.0, TestName = "Acosd_OfHalf")]
    [TestCase("SELECT acosd(-0.5)", 120.0, TestName = "Acosd_OfMinusHalf")]
    [TestCase("SELECT acosd(1)", 0.0, TestName = "Acosd_OfOne")]
    [TestCase("SELECT acosd(-1)", 180.0, TestName = "Acosd_OfMinusOne")]
    [TestCase("SELECT atand(1)", 45.0, TestName = "Atand_OfOne")]
    [TestCase("SELECT atand(-1)", -45.0, TestName = "Atand_OfMinusOne")]
    [TestCase("SELECT atand(" + Infinity + ")", 90.0, TestName = "Atand_OfInfinity")]
    [TestCase("SELECT atand(" + NegativeInfinity + ")", -90.0, TestName = "Atand_OfNegativeInfinity")]
    [TestCase("SELECT atan2d(0, 0)", 0.0, TestName = "Atan2d_OfTheOrigin")]
    [TestCase("SELECT atan2d(1, 1)", 45.0, TestName = "Atan2d_FirstQuadrant")]
    [TestCase("SELECT atan2d(1, -1)", 135.0, TestName = "Atan2d_SecondQuadrant")]
    [TestCase("SELECT atan2d(-1, -1)", -135.0, TestName = "Atan2d_ThirdQuadrant")]
    [TestCase("SELECT atan2d(-1, 1)", -45.0, TestName = "Atan2d_FourthQuadrant")]
    [TestCase("SELECT atan2d(0, -1)", 180.0, TestName = "Atan2d_NegativeXAxis")]
    [TestCase("SELECT atan2d(-1, 0)", -90.0, TestName = "Atan2d_NegativeYAxis")]
    [TestCase("SELECT atan2d(" + Infinity + ", " + Infinity + ")", 45.0, TestName = "Atan2d_BothInfinite")]
    public async Task DegreeForm_IsExactAtTheCommonAngles(string sql, double expected)
    {
        ColumnValue value = await Scalar(sql);
        Assert.AreEqual(ColumnType.Float64, value.Type);
        Assert.AreEqual(expected, value.FloatValue);
        if (expected == 0.0)
            Assert.IsFalse(double.IsNegative(value.FloatValue), "PostgreSQL 16 gives +0");
    }

    [TestCase("SELECT sind(1)", 0.017452406437283515, TestName = "Sind_OfOne")]
    [TestCase("SELECT cosd(1)", 0.9998476951563913, TestName = "Cosd_OfOne")]
    [TestCase("SELECT tand(1)", 0.01745506492821759, TestName = "Tand_OfOne")]
    [TestCase("SELECT cotd(1)", 57.28996163075942, TestName = "Cotd_OfOne")]
    [TestCase("SELECT cotd(-2)", -28.6362532829156, TestName = "Cotd_OfANegativeAngle")]
    [TestCase("SELECT sind(45)", 0.7071067811865475, TestName = "Sind_Of45")]
    [TestCase("SELECT atand(1.5)", 56.309932474020215, TestName = "Atand_OfOneAndAHalf")]
    public async Task DegreeForm_HasTheValueOfPostgreSqlBetweenTheCommonAngles(string sql, double expected)
    {
        Assert.That((await Scalar(sql)).FloatValue, Is.EqualTo(expected).Within(1).Ulps);
    }

    // ── radians, degrees and pi ──

    [TestCase("SELECT pi()", 3.141592653589793, TestName = "Pi")]
    [TestCase("SELECT PI()", 3.141592653589793, TestName = "Pi_IsCaseInsensitive")]
    [TestCase("SELECT radians(180)", 3.141592653589793, TestName = "Radians_Of180")]
    [TestCase("SELECT radians(1)", 0.017453292519943295, TestName = "Radians_OfOne")]
    [TestCase("SELECT radians(-2)", -0.03490658503988659, TestName = "Radians_OfANegativeAngle")]
    [TestCase("SELECT radians(1e300)", 1.7453292519943297e+298, TestName = "Radians_OfALargeAngle")]
    [TestCase("SELECT radians(1e-320)", 1.73e-322, TestName = "Radians_SubnormalResult_IsAValue")]
    [TestCase("SELECT degrees(1)", 57.29577951308232, TestName = "Degrees_OfOne")]
    [TestCase("SELECT degrees(pi())", 180.0, TestName = "Degrees_OfPi")]
    [TestCase("SELECT degrees(-2)", -114.59155902616465, TestName = "Degrees_OfANegativeAngle")]
    [TestCase("SELECT degrees(5e-324)", 2.8e-322, TestName = "Degrees_OfTheSmallestSubnormal")]
    [TestCase("SELECT radians(" + Infinity + ")", double.PositiveInfinity, TestName = "Radians_OfInfinity")]
    [TestCase("SELECT degrees(" + NegativeInfinity + ")", double.NegativeInfinity, TestName = "Degrees_OfNegativeInfinity")]
    public async Task AngleConversion_HasTheValueOfPostgreSql(string sql, double expected)
    {
        ColumnValue value = await Scalar(sql);
        Assert.AreEqual(ColumnType.Float64, value.Type);
        Assert.AreEqual(expected, value.FloatValue);
    }

    // ── Errors ──

    [TestCase("SELECT sin(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Sin_OfInfinity_IsRefused")]
    [TestCase("SELECT sin(" + NegativeInfinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Sin_OfNegativeInfinity_IsRefused")]
    [TestCase("SELECT cos(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Cos_OfInfinity_IsRefused")]
    [TestCase("SELECT tan(" + NegativeInfinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Tan_OfNegativeInfinity_IsRefused")]
    [TestCase("SELECT cot(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Cot_OfInfinity_IsRefused")]
    [TestCase("SELECT sind(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Sind_OfInfinity_IsRefused")]
    [TestCase("SELECT cosd(" + NegativeInfinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Cosd_OfNegativeInfinity_IsRefused")]
    [TestCase("SELECT tand(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Tand_OfInfinity_IsRefused")]
    [TestCase("SELECT cotd(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Cotd_OfInfinity_IsRefused")]
    [TestCase("SELECT asin(1.5)", CamusDBErrorCodes.InvalidInput, TestName = "Asin_AboveOne_IsRefused")]
    [TestCase("SELECT asin(1.0000000000000002)", CamusDBErrorCodes.InvalidInput, TestName = "Asin_JustAboveOne_IsRefused")]
    [TestCase("SELECT asin(-2)", CamusDBErrorCodes.InvalidInput, TestName = "Asin_BelowMinusOne_IsRefused")]
    [TestCase("SELECT asin(" + Infinity + ")", CamusDBErrorCodes.InvalidInput, TestName = "Asin_OfInfinity_IsRefused")]
    [TestCase("SELECT acos(-1.0000000000000002)", CamusDBErrorCodes.InvalidInput, TestName = "Acos_JustBelowMinusOne_IsRefused")]
    [TestCase("SELECT acos(2)", CamusDBErrorCodes.InvalidInput, TestName = "Acos_AboveOne_IsRefused")]
    [TestCase("SELECT asind(2)", CamusDBErrorCodes.InvalidInput, TestName = "Asind_AboveOne_IsRefused")]
    [TestCase("SELECT acosd(-1.0000000000000002)", CamusDBErrorCodes.InvalidInput, TestName = "Acosd_JustBelowMinusOne_IsRefused")]
    [TestCase("SELECT degrees(1e308)", CamusDBErrorCodes.FloatValueOutOfRange, TestName = "Degrees_Overflow_IsRefused")]
    [TestCase("SELECT radians(5e-324)", CamusDBErrorCodes.FloatValueOutOfRange, TestName = "Radians_Underflow_IsRefused")]
    [TestCase("SELECT sin('x')", CamusDBErrorCodes.InvalidInput, TestName = "Sin_OfAString_IsRefused")]
    [TestCase("SELECT atan2('x', 1)", CamusDBErrorCodes.InvalidInput, TestName = "Atan2_StringY_IsRefused")]
    [TestCase("SELECT atan2(1, 'x')", CamusDBErrorCodes.InvalidInput, TestName = "Atan2_StringX_IsRefused")]
    [TestCase("SELECT sin(true)", CamusDBErrorCodes.InvalidInput, TestName = "Sin_OfABool_IsRefused")]
    [TestCase("SELECT sin()", CamusDBErrorCodes.InvalidInput, TestName = "Sin_NoArgument_IsRefused")]
    [TestCase("SELECT sin(1, 2)", CamusDBErrorCodes.InvalidInput, TestName = "Sin_TwoArguments_IsRefused")]
    [TestCase("SELECT atan2(1)", CamusDBErrorCodes.InvalidInput, TestName = "Atan2_OneArgument_IsRefused")]
    [TestCase("SELECT pi(1)", CamusDBErrorCodes.InvalidInput, TestName = "Pi_WithAnArgument_IsRefused")]
    public async Task Error_MatchesPostgreSql(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        Assert.AreEqual(code, Refused(database, executor, dbname, sql).Code);
    }

    [Test]
    public async Task DomainError_NamesTheFunctionAndTheRule()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        StringAssert.Contains("Function 'asin' domain error", Refused(database, executor, dbname, "SELECT asin(2)").Message);
        StringAssert.Contains("between -1 and 1", Refused(database, executor, dbname, "SELECT ASIN(2)").Message);
        StringAssert.Contains("Function 'cos' domain error", Refused(database, executor, dbname, "SELECT cos(" + Infinity + ")").Message);
        StringAssert.Contains("must be finite", Refused(database, executor, dbname, "SELECT cos(" + Infinity + ")").Message);
    }

    // ── NULL and NaN ──

    [TestCase("sin")]
    [TestCase("cos")]
    [TestCase("tan")]
    [TestCase("cot")]
    [TestCase("asin")]
    [TestCase("acos")]
    [TestCase("atan")]
    [TestCase("sind")]
    [TestCase("cosd")]
    [TestCase("tand")]
    [TestCase("cotd")]
    [TestCase("asind")]
    [TestCase("acosd")]
    [TestCase("atand")]
    [TestCase("radians")]
    [TestCase("degrees")]
    public async Task UnaryFunction_NullGivesNullAndNaNGivesNaN(string function)
    {
        Assert.AreEqual(ColumnType.Null, (await Scalar($"SELECT {function}(NULL)")).Type);
        Assert.IsTrue(double.IsNaN((await Scalar($"SELECT {function}({NaN})")).FloatValue));
    }

    [TestCase("atan2")]
    [TestCase("atan2d")]
    public async Task BinaryFunction_NullGivesNullAndNaNGivesNaN(string function)
    {
        Assert.AreEqual(ColumnType.Null, (await Scalar($"SELECT {function}(NULL, 1)")).Type);
        Assert.AreEqual(ColumnType.Null, (await Scalar($"SELECT {function}(1, NULL)")).Type);
        Assert.IsTrue(double.IsNaN((await Scalar($"SELECT {function}({NaN}, 1)")).FloatValue));
        Assert.IsTrue(double.IsNaN((await Scalar($"SELECT {function}(1, {NaN})")).FloatValue));
    }

    // ── Argument types and expression positions ──

    [Test]
    public async Task EveryNumericArgumentType_GivesAFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT sind(d) AS s, sin(i) AS si, asind(n) AS a FROM angles WHERE id = 2");

        Assert.AreEqual(ColumnType.Float64, rows[0].Row["s"].Type);
        Assert.AreEqual(1.0, rows[0].Row["s"].FloatValue);
        Assert.That(rows[0].Row["si"].FloatValue, Is.EqualTo(0.8414709848078965).Within(1).Ulps);
        Assert.AreEqual(30.0, rows[0].Row["a"].FloatValue);
    }

    [Test]
    public async Task InWhere_FiltersRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Assert.AreEqual(new List<long> { 4 }, Ids(await Query(database, executor, dbname, "SELECT id FROM angles WHERE sind(d) < 0")));
        Assert.AreEqual(new List<long> { 2, 4 }, Ids(await Query(database, executor, dbname, "SELECT id FROM angles WHERE cosd(d) = 0 ORDER BY id")));
        Assert.AreEqual(new List<long> { 3 }, Ids(await Query(database, executor, dbname, "SELECT id FROM angles WHERE radians(d) = pi()")));
    }

    [Test]
    public async Task InOrderBy_SortsRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // sind(d) is 0, 1, 0 and -1 for the ids 1 to 4.
        Assert.AreEqual(new List<long> { 2, 1, 3, 4 }, Ids(await Query(database, executor, dbname, "SELECT id FROM angles ORDER BY sind(d) DESC, id")));
    }

    [Test]
    public async Task InACheckConstraint_RefusesABadRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE headings (id INT64 PRIMARY KEY, h FLOAT64, CHECK (cosd(h) >= 0))");

        await Dml(database, executor, dbname, "INSERT INTO headings (id, h) VALUES (1, 45), (2, 90)");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Dml(database, executor, dbname, "INSERT INTO headings (id, h) VALUES (3, 120)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        Assert.AreEqual(new List<long> { 1, 2 }, Ids(await Query(database, executor, dbname, "SELECT id FROM headings ORDER BY id")));
    }

    [Test]
    public async Task InAColumnDefault_FillsTheValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname,
            "CREATE TABLE circles (id INT64 PRIMARY KEY, half_turn FLOAT64 DEFAULT (pi()), right_angle FLOAT64 DEFAULT (degrees(pi() / 2)))");

        await Dml(database, executor, dbname, "INSERT INTO circles (id) VALUES (1)");

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT half_turn, right_angle FROM circles WHERE id = 1");
        Assert.AreEqual(System.Math.PI, rows[0].Row["half_turn"].FloatValue);
        Assert.AreEqual(90.0, rows[0].Row["right_angle"].FloatValue);
    }

    [Test]
    public async Task InAView_IsEvaluatedForEachRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE VIEW unit_circle AS SELECT id, cosd(d) AS x, sind(d) AS y, atan2d(sind(d), cosd(d)) AS a FROM angles");

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT id, x, y, a FROM unit_circle WHERE y >= 0 ORDER BY id");

        Assert.AreEqual(new List<long> { 1, 2, 3 }, Ids(rows));
        Assert.AreEqual(-1.0, rows[2].Row["x"].FloatValue);
        Assert.AreEqual(90.0, rows[1].Row["a"].FloatValue);
        Assert.AreEqual(180.0, rows[2].Row["a"].FloatValue);
    }

    [Test]
    public async Task WithParameters_UsesTheBoundValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT asind(@x) AS a, atan2(@y, @x) AS b",
            new() { ["@x"] = new ColumnValue(ColumnType.Float64, 0.5), ["@y"] = new ColumnValue(ColumnType.Float64, 0.5) });
        Assert.AreEqual(30.0, rows[0].Row["a"].FloatValue);
        Assert.That(rows[0].Row["b"].FloatValue, Is.EqualTo(0.7853981633974483).Within(1).Ulps);

        List<QueryResultRow> filtered = await Query(database, executor, dbname, "SELECT id FROM angles WHERE sind(d) = @s ORDER BY id",
            new() { ["@s"] = new ColumnValue(ColumnType.Integer64, 0L) });
        Assert.AreEqual(new List<long> { 1, 3 }, Ids(filtered));
    }

    [Test]
    public async Task Haversine_GivesTheGreatCircleDistance()
    {
        // The distance from London (51.5074, -0.1278) to Paris (48.8566, 2.3522) on a sphere of radius
        // 6371 km. PostgreSQL 16 gives 343.55606034104164 for this expression in float8.
        const string sql =
            "SELECT 2 * 6371 * asin(sqrt(pow(sin(radians(48.8566 - 51.5074) / 2), 2) " +
            "+ cos(radians(51.5074)) * cos(radians(48.8566)) * pow(sin(radians(2.3522 - -0.1278) / 2), 2)))";

        Assert.That((await Scalar(sql)).FloatValue, Is.EqualTo(343.55606034104164).Within(1e-9));
    }
}
