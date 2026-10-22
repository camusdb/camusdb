/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Scalar functions from <c>Fixture.Good</c>, called through SQL: the value mapping for every CLR
/// type, NULL in and out, optional parameters and aliases, error wrapping, use in WHERE, CHECK and
/// DEFAULT, and isolation between two engines in one process.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionFunctions : ExtensionTestBase
{
    // ── value mapping ─────────────────────────────────────────────────────────

    [Test]
    public async Task Projection_StringFunction()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        Assert.AreEqual("hello-world", (await Scalar(executor, db, "SELECT fx_slugify('  Hello, World ')")).StrValue);
    }

    [Test]
    public async Task TypeMatrix_EachMappedType()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        ColumnValue add = await Scalar(executor, db, "SELECT fx_add(2, 3)");
        Assert.AreEqual(ColumnType.Integer64, add.Type);
        Assert.AreEqual(5L, add.LongValue);

        ColumnValue half = await Scalar(executor, db, "SELECT fx_half(5.0)");
        Assert.AreEqual(ColumnType.Float64, half.Type);
        Assert.AreEqual(2.5, half.FloatValue);

        ColumnValue halfOfInteger = await Scalar(executor, db, "SELECT fx_half(5)");
        Assert.AreEqual(2.5, halfOfInteger.FloatValue, "an Integer64 argument converts to double");

        ColumnValue twice = await Scalar(executor, db, "SELECT fx_float_twice(1.5)");
        Assert.AreEqual(ColumnType.Float32, twice.Type);
        Assert.AreEqual(3.0, twice.FloatValue);

        ColumnValue not = await Scalar(executor, db, "SELECT fx_not(true)");
        Assert.AreEqual(ColumnType.Bool, not.Type);
        Assert.IsFalse(not.BoolValue);

        const string uuid = "0190a8b2-7c3d-7e4f-8a1b-2c3d4e5f6a7b";
        ColumnValue parsed = await Scalar(executor, db, $"SELECT fx_parse_uuid('{uuid}')");
        Assert.AreEqual(ColumnType.Uuid, parsed.Type);
        Assert.AreEqual(Guid.Parse(uuid), parsed.ToGuid());

        ColumnValue text = await Scalar(executor, db, $"SELECT fx_uuid_text(fx_parse_uuid('{uuid}'))");
        Assert.AreEqual(uuid, text.StrValue, "a Uuid argument maps to Guid");

        ColumnValue length = await Scalar(executor, db, "SELECT fx_bytes_len(X'010203')");
        Assert.AreEqual(3L, length.LongValue);

        ColumnValue reversed = await Scalar(executor, db, "SELECT fx_bytes_rev(X'010203')");
        Assert.AreEqual(ColumnType.Bytes, reversed.Type);
        CollectionAssert.AreEqual(new byte[] { 3, 2, 1 }, reversed.BytesValue);

        ColumnValue nextDay = await Scalar(executor, db, "SELECT fx_next_day(make_date(2024, 2, 28))");
        Assert.AreEqual(ColumnType.DateTime, nextDay.Type);
        Assert.AreEqual(new DateTime(2024, 2, 29, 0, 0, 0, DateTimeKind.Utc).Ticks, nextDay.LongValue);

        ColumnValue doubled = await Scalar(executor, db, "SELECT fx_dec_double(NUMERIC '1.25')");
        Assert.AreEqual(ColumnType.Numeric, doubled.Type);
        Assert.AreEqual("2.5", NumericMath.Format(doubled.NumericUnscaled).TrimEnd('0').TrimEnd('.'));
    }

    [Test]
    public async Task Numeric_TooManyDigitsForDecimal_IsRefusedNotTruncated()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => Scalar(executor, db, "SELECT fx_dec_double(NUMERIC '99999999999999999999999999999.123456789')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionValueOutOfRange, ex.Code);
    }

    [Test]
    public void DecimalMapping_RoundTripsAndRounds()
    {
        Assert.AreEqual(1.25m, ExtensionValueMapping.UnscaledToDecimal(ExtensionValueMapping.DecimalToUnscaled(1.25m)));
        Assert.AreEqual(-7.000000001m, ExtensionValueMapping.UnscaledToDecimal(ExtensionValueMapping.DecimalToUnscaled(-7.000000001m)));
        Assert.AreEqual(decimal.MaxValue, ExtensionValueMapping.UnscaledToDecimal(ExtensionValueMapping.DecimalToUnscaled(decimal.MaxValue)));

        // More than nine fractional digits round half away from zero, as text cast to NUMERIC does.
        Assert.AreEqual(0.000000001m, ExtensionValueMapping.UnscaledToDecimal(ExtensionValueMapping.DecimalToUnscaled(0.0000000005m)));
        Assert.AreEqual(-0.000000001m, ExtensionValueMapping.UnscaledToDecimal(ExtensionValueMapping.DecimalToUnscaled(-0.0000000005m)));
        Assert.AreEqual(0m, ExtensionValueMapping.UnscaledToDecimal(ExtensionValueMapping.DecimalToUnscaled(0.0000000004m)));
    }

    // ── NULL rules ────────────────────────────────────────────────────────────

    [Test]
    public async Task Null_PropagatesForNonNullableParameters()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT fx_add(NULL, 1)")).Type);
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT fx_slugify(NULL)")).Type);
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT fx_coalesce('a', NULL)")).Type,
            "the second parameter is not nullable, so NULL there propagates");
    }

    [Test]
    public async Task Null_ReachesNullableParameters()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        Assert.AreEqual("b", (await Scalar(executor, db, "SELECT fx_coalesce(NULL, 'b')")).StrValue, "string? receives null");
        Assert.AreEqual(-1L, (await Scalar(executor, db, "SELECT fx_nullable_len(NULL)")).LongValue, "long? receives null");
        Assert.AreEqual(4L, (await Scalar(executor, db, "SELECT fx_nullable_len(4)")).LongValue);
    }

    [Test]
    public async Task Null_ReturnedNullIsSqlNull()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, "SELECT fx_null_if_empty('')")).Type);
        Assert.AreEqual("x", (await Scalar(executor, db, "SELECT fx_null_if_empty('x')")).StrValue);
    }

    // ── arity, aliases, errors ────────────────────────────────────────────────

    [Test]
    public async Task OptionalParameter_MakesAnArityRange()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        Assert.AreEqual(11L, (await Scalar(executor, db, "SELECT fx_optional(1)")).LongValue);
        Assert.AreEqual(3L, (await Scalar(executor, db, "SELECT fx_optional(1, 2)")).LongValue);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT fx_optional()"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("fx_optional", ex.Message);
    }

    [Test]
    public async Task Alias_CallsTheSameFunction()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        Assert.AreEqual("[x]", (await Scalar(executor, db, "SELECT fx_tag_alias('x')")).StrValue);
        Assert.AreEqual("[x]", (await Scalar(executor, db, "SELECT FX_TAG('x')")).StrValue, "names are case-insensitive");
    }

    [Test]
    public async Task WrongArgumentType_NamesTheFunctionAndArgument()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT fx_add('a', 1)"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("'fx_add' expects argument 1", ex.Message);
    }

    [Test]
    public async Task UserException_IsWrappedWithTheFunctionName()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT fx_throw('boom')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, ex.Code);
        StringAssert.Contains("fx_throw", ex.Message);
        StringAssert.Contains("boom", ex.Message);
        Assert.IsInstanceOf<InvalidOperationException>(ex.InnerException);
    }

    [Test]
    public async Task CallPath_HasNoReflectionInvoke()
    {
        // The adapter is a compiled delegate: the user method's frame sits directly under it, with no
        // MethodBase.Invoke between them.
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(executor, db, "SELECT fx_throw('trace')"))!;
        string trace = ex.InnerException!.StackTrace ?? "";

        StringAssert.Contains("CamusDB.Fixtures.Good.Functions.Throw", trace);
        StringAssert.DoesNotContain("MethodBase.Invoke", trace);
        StringAssert.DoesNotContain("RuntimeMethodInfo.Invoke", trace);
        StringAssert.DoesNotContain("MethodBaseInvoker", trace);
    }

    // ── WHERE, CHECK, DEFAULT, views ──────────────────────────────────────────

    [Test]
    public async Task Where_FiltersRows()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        await ExecDdl(executor, db, "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'Hello World'), (2, 'Other Post'), (3, 'hello   world!')");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(
            executor, db, "SELECT id FROM posts WHERE fx_slugify(title) = 'hello-world' ORDER BY id");

        CollectionAssert.AreEqual(new[] { 1L, 3L }, rows.Select(r => r["id"].LongValue).ToArray());
    }

    [Test]
    public async Task Check_DeterministicFunctionEnforced()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        await ExecDdl(executor, db, "CREATE TABLE slugs (id INT64 PRIMARY KEY, s STRING, CHECK (fx_slugify(s) = s))");
        await ExecNonQuery(executor, db, "INSERT INTO slugs (id, s) VALUES (1, 'already-a-slug')");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO slugs (id, s) VALUES (2, 'Not A Slug')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    [Test]
    public async Task Check_VolatileFunctionRefused()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        Assert.ThrowsAsync<CamusDBException>(
            () => ExecDdl(executor, db, "CREATE TABLE bad (id INT64 PRIMARY KEY, n INT64, CHECK (n < fx_counter()))"));
    }

    [Test]
    public async Task Default_VolatileFunctionRunsPerRow_DeterministicIsConstant()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        await ExecDdl(executor, db,
            "CREATE TABLE d (id INT64 PRIMARY KEY, per_row INT64 DEFAULT (fx_counter()), fixed INT64 DEFAULT (fx_add(40, 2)))");
        await ExecNonQuery(executor, db, "INSERT INTO d (id) VALUES (1)");
        await ExecNonQuery(executor, db, "INSERT INTO d (id) VALUES (2)");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT per_row, fixed FROM d ORDER BY id");

        Assert.AreEqual(2, rows.Count);
        Assert.AreNotEqual(rows[0]["per_row"].LongValue, rows[1]["per_row"].LongValue, "a volatile default runs per row");
        Assert.AreEqual(42L, rows[0]["fixed"].LongValue);
        Assert.AreEqual(42L, rows[1]["fixed"].LongValue);
    }

    [Test]
    public async Task AddColumn_DefaultBackfillCallsTheFunction()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        await ExecDdl(executor, db, "CREATE TABLE b (id INT64 PRIMARY KEY, name STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO b (id, name) VALUES (1, 'a'), (2, 'b')");
        await ExecDdl(executor, db, "ALTER TABLE b ADD COLUMN tag STRING DEFAULT (fx_tag('t'))");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT tag FROM b ORDER BY id");
        Assert.IsTrue(rows.All(r => r["tag"].StrValue == "[t]"));
    }

    [Test]
    public async Task View_And_MaterializedView_CallTheFunction()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, title STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO src (id, title) VALUES (1, 'A B'), (2, 'C D')");
        await ExecDdl(executor, db, "CREATE VIEW v AS SELECT id, fx_slugify(title) AS slug FROM src");
        await ExecDdl(executor, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, fx_slugify(title) AS slug FROM src");

        List<Dictionary<string, ColumnValue>> viewRows = await ExecQuery(executor, db, "SELECT slug FROM v ORDER BY id");
        CollectionAssert.AreEqual(new[] { "a-b", "c-d" }, viewRows.Select(r => r["slug"].StrValue).ToArray());

        await ExecNonQuery(executor, db, "INSERT INTO src (id, title) VALUES (3, 'E F')");
        await ExecNonQuery(executor, db, "REFRESH MATERIALIZED VIEW mv");

        List<Dictionary<string, ColumnValue>> mvRows = await ExecQuery(executor, db, "SELECT slug FROM mv ORDER BY id");
        CollectionAssert.AreEqual(new[] { "a-b", "c-d", "e-f" }, mvRows.Select(r => r["slug"].StrValue).ToArray());
    }

    // ── per-engine scope ──────────────────────────────────────────────────────

    [Test]
    public async Task TwoEngines_EachSeesOnlyItsOwnFunctions()
    {
        (string withDb, _, CommandExecutor withExtensions) = await CreateDatabaseWithGoodFixture();
        (string withoutDb, _, CommandExecutor withoutExtensions) = await CreateDatabase(Options);

        Assert.AreEqual(2L, (await Scalar(withExtensions, withDb, "SELECT fx_add(1, 1)")).LongValue);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Scalar(withoutExtensions, withoutDb, "SELECT fx_add(1, 1)"))!;
        StringAssert.Contains("Function not found", ex.Message);

        // And back again: entering the first engine restores its scope in the same flow.
        Assert.AreEqual(2L, (await Scalar(withExtensions, withDb, "SELECT fx_add(1, 1)")).LongValue);
    }

    [Test]
    public async Task Shippability_RefusesExtensionCallsEvenInsideTheScope()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        executor.EnterExtensionScope(database);

        try
        {
            Assert.IsFalse(CamusDB.Core.CommandsExecutor.Controllers.Queries.FragmentFilterShippability.IsShippable(
                SQLParserProcessor.ParseCondition("fx_add(num, 0) > 5")), "a peer may not load the extension");
            Assert.IsFalse(CamusDB.Core.CommandsExecutor.Controllers.Queries.FragmentFilterShippability.IsShippable(
                SQLParserProcessor.ParseCondition("abs(fx_add(num, 0)) > 5")), "nested inside a built-in too");
            Assert.IsTrue(CamusDB.Core.CommandsExecutor.Controllers.Queries.FragmentFilterShippability.IsShippable(
                SQLParserProcessor.ParseCondition("abs(num) > 5")), "built-ins still ship");
        }
        finally
        {
            ExtensionFunctionScope.Enter(null);
        }
    }

    [Test]
    public async Task VolatilityWalk_SeesExtensionFunctionsOnlyInsideTheScope()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        NodeAst ast = SQLParserProcessor.Parse("SELECT fx_counter()");

        ExtensionFunctionScope.Enter(null);
        Assert.IsFalse(ScalarFunctionEvaluator.ContainsVolatileFunction(ast), "an unknown name counts as non-volatile");

        executor.EnterExtensionScope(database);
        Assert.IsTrue(ScalarFunctionEvaluator.ContainsVolatileFunction(ast));
        Assert.IsTrue(ScalarFunctionEvaluator.IsRegisteredScalarFunction("fx_counter"));
        Assert.IsFalse(ScalarFunctionEvaluator.IsBuiltInScalarFunction("fx_counter"));
        Assert.AreEqual(ColumnType.Integer64, ScalarFunctionEvaluator.InferReturnType("fx_counter", []));

        ExtensionFunctionScope.Enter(null);
    }
}
