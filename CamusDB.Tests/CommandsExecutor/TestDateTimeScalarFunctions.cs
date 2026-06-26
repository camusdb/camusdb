
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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

[NonParallelizable]
public class TestDateTimeScalarFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        InsertTicket insertTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "robot") },
                }
            });

        await executor.Insert(insertTicket);
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableWithDatetime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new(
            databaseName: dbname,
            tableName: "events",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("created_at", ColumnType.DateTime),
                new("event_date", ColumnType.Date),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        KvTransaction txnState = await database.Transactions.BeginAsync();

        DateTimeOffset ts = new(2024, 6, 15, 10, 30, 0, TimeSpan.Zero);
        long tsTicks = ts.ToUniversalTime().Ticks;
        long dateTicks = ts.ToUniversalTime().Date.Ticks;

        InsertTicket insertTicket = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "events",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "created_at", new(ColumnType.DateTime, tsTicks) },
                    { "event_date", new(ColumnType.Date, dateTicks) },
                }
            });

        await executor.Insert(insertTicket);
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecuteSelect(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: sql,
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    private static async Task<CamusDBException> AssertSelectThrows(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: sql,
            parameters: null);

        return Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentTimestamp_ReturnsDateTimeType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT current_timestamp() FROM robots LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        // Wire format is still ISO-8601 via IsoValue
        string iso = val.IsoValue;
        Assert.IsTrue(DateTimeOffset.TryParse(iso, out _), $"IsoValue should be parseable: {iso}");
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentDate_ReturnsDateType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT current_date() FROM robots LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.Date, val.Type);
        string iso = val.IsoValue;
        Assert.IsTrue(DateTime.TryParseExact(iso, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _),
            $"IsoValue should be date-only: {iso}");
        Assert.AreEqual(10, iso.Length);
    }

    [Test]
    [NonParallelizable]
    public async Task DateAdd_DateOnlyInput_AddsDays()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_add(\"2024-06-15\", 1, \"day\") FROM robots LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-06-16T00:00:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateAdd_AcceptsPluralUnits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_add(\"2024-06-15\", 2, \"months\") FROM robots LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-08-15T00:00:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateAdd_TimestampInput_AddsHours()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_add(\"2024-06-15T10:30:00Z\", 2, \"hours\") FROM robots LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-06-15T12:30:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateAdd_TypedDatetimeColumn_AddsHours()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_add(created_at, 2, \"hours\") FROM events LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-06-15T12:30:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateAdd_TypedDateColumn_AddsDays()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_add(event_date, 1, \"day\") FROM events LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-06-16T00:00:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateDiff_ReturnsElapsedDays()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(\"2024-06-01\", \"2024-06-11\", \"days\") FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(10, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateDiff_ReturnsElapsedHours()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(\"2024-06-15T10:00:00Z\", \"2024-06-15T13:00:00Z\", \"hour\") FROM robots LIMIT 1");

        Assert.AreEqual(3, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateDiff_NegativeMonthWithinSameMonth_ReturnsZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(\"2024-06-15\", \"2024-06-14\", \"month\") FROM robots LIMIT 1");

        Assert.AreEqual(0, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateDiff_NegativeMonthAndYear_AdjustSymmetrically()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> month = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(\"2024-06-15\", \"2024-05-16\", \"month\") FROM robots LIMIT 1");
        List<QueryResultRow> year = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(\"2024-06-15\", \"2024-06-14\", \"year\") FROM robots LIMIT 1");
        List<QueryResultRow> positiveMonth = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(\"2024-01-15\", \"2024-03-14\", \"month\") FROM robots LIMIT 1");

        Assert.AreEqual(0, month[0].Row["0"].LongValue);
        Assert.AreEqual(0, year[0].Row["0"].LongValue);
        Assert.AreEqual(1, positiveMonth[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DatePart_ExtractsUtcComponents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> year = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"year\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> month = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"month\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> day = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"day\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> hour = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"hour\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> minute = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"minute\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> second = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"second\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> millisecond = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"millisecond\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");

        Assert.AreEqual(2024, year[0].Row["0"].LongValue);
        Assert.AreEqual(6, month[0].Row["0"].LongValue);
        Assert.AreEqual(15, day[0].Row["0"].LongValue);
        Assert.AreEqual(10, hour[0].Row["0"].LongValue);
        Assert.AreEqual(30, minute[0].Row["0"].LongValue);
        Assert.AreEqual(45, second[0].Row["0"].LongValue);
        Assert.AreEqual(123, millisecond[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DatePart_NormalizesOffsetInputToUtc()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"hour\", \"2024-06-15T10:30:00+05:00\") FROM robots LIMIT 1");

        Assert.AreEqual(5, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateTrunc_TruncatesToUnitStartInUtc()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> month = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_trunc(\"month\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> day = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_trunc(\"day\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");
        List<QueryResultRow> hour = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_trunc(\"hour\", \"2024-06-15T10:30:45.123Z\") FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.DateTime, month[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.DateTime, day[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.DateTime, hour[0].Row["0"].Type);
        Assert.AreEqual("2024-06-01T00:00:00.0000000Z", month[0].Row["0"].IsoValue);
        Assert.AreEqual("2024-06-15T00:00:00.0000000Z", day[0].Row["0"].IsoValue);
        Assert.AreEqual("2024-06-15T10:00:00.0000000Z", hour[0].Row["0"].IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateTrunc_TypedDatetimeColumn_Truncates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_trunc(\"hour\", created_at) FROM events LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-06-15T10:00:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DateFunctions_NullArgument_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> add = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_add(NULL, 1, \"day\") FROM robots LIMIT 1");
        List<QueryResultRow> diff = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(NULL, \"2024-06-01\", \"day\") FROM robots LIMIT 1");
        List<QueryResultRow> part = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"day\", NULL) FROM robots LIMIT 1");
        List<QueryResultRow> trunc = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_trunc(\"day\", NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, add[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, diff[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, part[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, trunc[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task DateFunctions_InvalidUnit_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT date_part(\"week\", \"2024-06-15\") FROM robots LIMIT 1");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'date_part' expects a supported date/time unit", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task DateFunctions_InvalidDate_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT date_add(\"not-a-date\", 1, \"day\") FROM robots LIMIT 1");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'date_add' could not parse date/time value", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task DateFunctions_AmbiguousLocalDateTime_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT date_add(\"2024-06-15T10:30:00\", 1, \"day\") FROM robots LIMIT 1");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("expects a UTC date/time string with an explicit offset", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task DateFunctions_SpaceSeparatedLocalDateTime_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT date_add(\"2024-06-15 10:30:00\", 1, \"day\") FROM robots LIMIT 1");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("expects a UTC date/time string with an explicit offset", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task UnixTimestamp_WithoutArgument_ReturnsCurrentUtcSeconds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT unix_timestamp() FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        long seconds = result[0].Row["0"].LongValue;
        long expected = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Assert.That(seconds, Is.EqualTo(expected).Within(2));
    }

    [Test]
    [NonParallelizable]
    public async Task UnixTimestamp_ConvertsDateTimeStringToUtcSeconds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> timestamp = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT unix_timestamp(\"2024-06-15T10:30:00Z\") FROM robots LIMIT 1");
        List<QueryResultRow> dateOnly = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT unix_timestamp(\"2024-06-15\") FROM robots LIMIT 1");

        Assert.AreEqual(1718447400, timestamp[0].Row["0"].LongValue);
        Assert.AreEqual(
            new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds(),
            dateOnly[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task FromUnixtime_ReturnsDateTimeType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> epoch = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT from_unixtime(0) FROM robots LIMIT 1");
        List<QueryResultRow> value = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT from_unixtime(1718447400) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.DateTime, epoch[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.DateTime, value[0].Row["0"].Type);
        Assert.AreEqual("1970-01-01T00:00:00.0000000Z", epoch[0].Row["0"].IsoValue);
        Assert.AreEqual("2024-06-15T10:30:00.0000000Z", value[0].Row["0"].IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task UnixTimestampAndFromUnixtime_RoundTrip()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT from_unixtime(unix_timestamp(\"2024-06-15T10:30:00+00:00\")) FROM robots LIMIT 1");

        ColumnValue val = result[0].Row["0"];
        Assert.AreEqual(ColumnType.DateTime, val.Type);
        Assert.AreEqual("2024-06-15T10:30:00.0000000Z", val.IsoValue);
    }

    [Test]
    [NonParallelizable]
    public async Task UnixTimestampFunctions_NullArgument_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> toSeconds = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT unix_timestamp(NULL) FROM robots LIMIT 1");
        List<QueryResultRow> fromSeconds = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT from_unixtime(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, toSeconds[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, fromSeconds[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task DateDiff_TypedDatetimeColumns_ReturnsDiff()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_diff(event_date, created_at, \"hours\") FROM events LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(10, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DatePart_TypedDatetimeColumn_ExtractsPart()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> hour = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"hour\", created_at) FROM events LIMIT 1");
        List<QueryResultRow> day = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT date_part(\"day\", event_date) FROM events LIMIT 1");

        Assert.AreEqual(10, hour[0].Row["0"].LongValue);
        Assert.AreEqual(15, day[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task UnixTimestamp_TypedDatetimeColumn_ReturnsSeconds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT unix_timestamp(created_at) FROM events LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(1718447400L, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task NowInsert_IntoDatetimeColumn_RoundTripsWithoutCast()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new(
            databaseName: dbname,
            tableName: "logs",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("created", ColumnType.DateTime),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket insertTicket = new(
            txnState: txn,
            database: dbname,
            sql: "INSERT INTO logs (id, created) VALUES (gen_id(), now())",
            parameters: null);
        await executor.ExecuteNonSQLQuery(insertTicket);
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT created FROM logs LIMIT 1");

        Assert.AreEqual(ColumnType.DateTime, result[0].Row["created"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task NowComparison_InWhereClause_WorksTypedToTyped()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithDatetime();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT created_at FROM events WHERE created_at < now() LIMIT 1");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.DateTime, result[0].Row["created_at"].Type);
    }
}
