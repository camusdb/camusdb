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
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Shared.Sequences;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end behavior of user sequences, driven through SQL rather than through the catalog:
/// the DDL, <c>nextval</c> on the write path, identity columns, and the lifecycle paths where a
/// counter is created or removed outside any transaction.
/// </summary>
/// <remarks>
/// Marked non-parallelizable because every test here boots the shared embedded node and draws from
/// counters whose partition leadership is process-wide state.
/// </remarks>
[NonParallelizable]
internal sealed class TestSequences : SharedNodeBaseTest
{
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

    private static async Task<int> ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(
        CommandExecutor executor, string dbname, string sql)
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

    /// <summary>
    /// Reads the counter straight out of Kahuna. Used where the assertion is about the counter's
    /// real presence rather than about what the catalog says, which is the only way to prove a
    /// lifecycle path actually removed one.
    /// </summary>
    private async Task<ReadOnlySequenceEntry?> ReadCounterAsync(string dbId, string sequenceId)
    {
        (SequenceResponseType type, ReadOnlySequenceEntry? entry) = await SharedKahuna.LocateAndGetSequence(
            MetaKeys.KahunaSequenceName(dbId, sequenceId), SequenceDurability.Persistent, CancellationToken.None);

        return type == SequenceResponseType.Success ? entry : null;
    }

    // -----------------------------------------------------------------------
    // DDL
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestCreateSequenceAndDraw()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE order_no");

        Assert.That(database.Schema.Sequences.ContainsKey("order_no"), Is.True);

        SequenceSchema sequence = database.Schema.Sequences["order_no"];
        Assert.That(sequence.StartValue, Is.EqualTo(1));
        Assert.That(sequence.Increment, Is.EqualTo(1));
        Assert.That(sequence.MinValue, Is.EqualTo(1));
        Assert.That(sequence.MaxValue, Is.Null);

        // The first value a sequence issues is its start value, not start + increment. That is the
        // one thing the counter's own semantics do not give for free: it hands out
        // current + increment, so a sequence has to be seeded one increment below its start.
        List<Dictionary<string, ColumnValue>> first = await Query(executor, dbname, "SELECT nextval('order_no') AS v");
        Assert.That(first[0]["v"].LongValue, Is.EqualTo(1));

        List<Dictionary<string, ColumnValue>> second = await Query(executor, dbname, "SELECT nextval('order_no') AS v");
        Assert.That(second[0]["v"].LongValue, Is.EqualTo(2));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateSequenceWithOptions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname,
            "CREATE SEQUENCE ticket_no START WITH 100 INCREMENT BY 5 MINVALUE 100 MAXVALUE 999 CACHE 1 NO CYCLE");

        SequenceSchema sequence = database.Schema.Sequences["ticket_no"];
        Assert.That(sequence.StartValue, Is.EqualTo(100));
        Assert.That(sequence.Increment, Is.EqualTo(5));
        Assert.That(sequence.MinValue, Is.EqualTo(100));
        Assert.That(sequence.MaxValue, Is.EqualTo(999));
        Assert.That(sequence.CacheSize, Is.EqualTo(1));

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT nextval('ticket_no') AS v");
        Assert.That(rows[0]["v"].LongValue, Is.EqualTo(100));

        rows = await Query(executor, dbname, "SELECT nextval('ticket_no') AS v");
        Assert.That(rows[0]["v"].LongValue, Is.EqualTo(105));
    }

    /// <summary>
    /// <c>CYCLE</c> parses and is refused with a message naming the reason, rather than being
    /// accepted and ignored. A user told their register wraps, and silently given one that does
    /// not, finds out from their data.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestCycleIsRejectedRatherThanIgnored()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        CamusDBException? exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE SEQUENCE wrapping MAXVALUE 3 CYCLE"));

        Assert.That(exception!.Code, Is.EqualTo(CamusDBErrorCodes.FeatureNotSupported));
        Assert.That(exception.Message, Does.Contain("CYCLE"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestInvalidDefinitionsAreRefused()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        CamusDBException? zeroIncrement = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE SEQUENCE bad_a INCREMENT BY 0"));
        Assert.That(zeroIncrement!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidSequenceDefinition));

        CamusDBException? negativeCache = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE SEQUENCE bad_b CACHE 0"));
        Assert.That(negativeCache!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidSequenceDefinition));

        CamusDBException? maxBelowMin = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE SEQUENCE bad_c MINVALUE 10 MAXVALUE 5"));
        Assert.That(maxBelowMin!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidSequenceDefinition));

        CamusDBException? startBelowMin = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE SEQUENCE bad_d MINVALUE 10 START WITH 1"));
        Assert.That(startBelowMin!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidSequenceDefinition));
    }

    [Test]
    [NonParallelizable]
    public async Task TestSequenceSharesTheRelationNamespace()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE clash");

        CamusDBException? asTable = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE TABLE clash (id oid PRIMARY KEY)"));
        Assert.That(asTable!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceAlreadyExists));

        await ExecuteDdl(executor, dbname, "CREATE TABLE occupied (id oid PRIMARY KEY)");

        CamusDBException? asSequence = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "CREATE SEQUENCE occupied"));
        Assert.That(asSequence!.Code, Is.EqualTo(CamusDBErrorCodes.TableAlreadyExists));
    }

    [Test]
    [NonParallelizable]
    public async Task TestDropSequenceRemovesTheCounter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE doomed");
        string sequenceId = database.Schema.Sequences["doomed"].Id!;

        await Query(executor, dbname, "SELECT nextval('doomed') AS v");
        Assert.That(await ReadCounterAsync(database.Id, sequenceId), Is.Not.Null);

        await ExecuteDdl(executor, dbname, "DROP SEQUENCE doomed");

        Assert.That(database.Schema.Sequences.ContainsKey("doomed"), Is.False);

        // Asserted against Kahuna rather than against the catalog: the catalog losing the record is
        // what the delta does, and the counter surviving it is exactly the leak this path exists to
        // prevent.
        Assert.That(await ReadCounterAsync(database.Id, sequenceId), Is.Null);
    }

    /// <summary>
    /// A rename moves the name and nothing else. The counter is named after the sequence's
    /// immutable id, so the values keep climbing from where they were rather than restarting.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestRenameKeepsTheCounter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE before_rename");
        string sequenceId = database.Schema.Sequences["before_rename"].Id!;

        await Query(executor, dbname, "SELECT nextval('before_rename') AS v");
        await Query(executor, dbname, "SELECT nextval('before_rename') AS v");

        await ExecuteDdl(executor, dbname, "ALTER SEQUENCE before_rename RENAME TO after_rename");

        Assert.That(database.Schema.Sequences.ContainsKey("before_rename"), Is.False);
        Assert.That(database.Schema.Sequences["after_rename"].Id, Is.EqualTo(sequenceId));

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT nextval('after_rename') AS v");
        Assert.That(rows[0]["v"].LongValue, Is.EqualTo(3));
    }

    // -----------------------------------------------------------------------
    // The write path
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestNextValInInsertValues()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE order_no");
        await ExecuteDdl(executor, dbname, "CREATE TABLE orders (id oid PRIMARY KEY, no int64)");

        int inserted = await ExecuteNonQuery(executor, dbname,
            "INSERT INTO orders (id, no) VALUES " +
            "(gen_id(), nextval('order_no')), (gen_id(), nextval('order_no')), (gen_id(), nextval('order_no'))");

        Assert.That(inserted, Is.EqualTo(3));

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT no FROM orders ORDER BY no");
        Assert.That(rows.Count, Is.EqualTo(3));
        Assert.That(rows[0]["no"].LongValue, Is.EqualTo(1));
        Assert.That(rows[1]["no"].LongValue, Is.EqualTo(2));
        Assert.That(rows[2]["no"].LongValue, Is.EqualTo(3));
    }

    [Test]
    [NonParallelizable]
    public async Task TestColumnDefaultDrawsFromSequence()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE invoice_no");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE invoices (id oid PRIMARY KEY, no int64 DEFAULT(nextval('invoice_no')), total float64)");

        await ExecuteNonQuery(executor, dbname,
            "INSERT INTO invoices (id, total) VALUES (gen_id(), 1.5), (gen_id(), 2.5)");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT no FROM invoices ORDER BY no");
        Assert.That(rows.Count, Is.EqualTo(2));
        Assert.That(rows[0]["no"].LongValue, Is.EqualTo(1));
        Assert.That(rows[1]["no"].LongValue, Is.EqualTo(2));
    }

    /// <summary>
    /// A default binds to the sequence's immutable id, so renaming the sequence does not break it.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestColumnDefaultSurvivesSequenceRename()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE old_name");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE bound (id oid PRIMARY KEY, n int64 DEFAULT(nextval('old_name')))");

        await ExecuteNonQuery(executor, dbname, "INSERT INTO bound (id) VALUES (gen_id())");
        await ExecuteDdl(executor, dbname, "ALTER SEQUENCE old_name RENAME TO new_name");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO bound (id) VALUES (gen_id())");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT n FROM bound ORDER BY n");
        Assert.That(rows.Count, Is.EqualTo(2));
        Assert.That(rows[0]["n"].LongValue, Is.EqualTo(1));
        Assert.That(rows[1]["n"].LongValue, Is.EqualTo(2));
    }

    // -----------------------------------------------------------------------
    // Identity columns
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestSerialColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE tickets (id oid PRIMARY KEY, no serial, subject string)");

        // SERIAL is a declaration shorthand, not a storage type: the column is int64 and carries an
        // owned sequence, and no ColumnType member was spent on spelling a default.
        Assert.That(database.Schema.Sequences.ContainsKey("tickets_no_seq"), Is.True);
        Assert.That(database.Schema.Sequences["tickets_no_seq"].OwnedByTableId,
            Is.EqualTo(database.Schema.Tables["tickets"].Id));

        await ExecuteNonQuery(executor, dbname,
            "INSERT INTO tickets (id, subject) VALUES (gen_id(), 'first'), (gen_id(), 'second')");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT no FROM tickets ORDER BY no");
        Assert.That(rows.Count, Is.EqualTo(2));
        Assert.That(rows[0]["no"].LongValue, Is.EqualTo(1));
        Assert.That(rows[1]["no"].LongValue, Is.EqualTo(2));
    }

    [Test]
    [NonParallelizable]
    public async Task TestGeneratedAlwaysRefusesASuppliedValue()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname,
            "CREATE TABLE strict_ids (id oid PRIMARY KEY, n int64 GENERATED ALWAYS AS IDENTITY, label string)");

        await ExecuteNonQuery(executor, dbname, "INSERT INTO strict_ids (id, label) VALUES (gen_id(), 'ok')");

        CamusDBException? supplied = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, "INSERT INTO strict_ids (id, n, label) VALUES (gen_id(), 99, 'no')"));

        Assert.That(supplied!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(supplied.Message, Does.Contain("GENERATED ALWAYS"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestGeneratedByDefaultAcceptsASuppliedValue()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname,
            "CREATE TABLE loose_ids (id oid PRIMARY KEY, n int64 GENERATED BY DEFAULT AS IDENTITY, label string)");

        await ExecuteNonQuery(executor, dbname, "INSERT INTO loose_ids (id, n, label) VALUES (gen_id(), 99, 'supplied')");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO loose_ids (id, label) VALUES (gen_id(), 'generated')");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT n FROM loose_ids ORDER BY n");
        Assert.That(rows.Count, Is.EqualTo(2));
        Assert.That(rows[0]["n"].LongValue, Is.EqualTo(1));
        Assert.That(rows[1]["n"].LongValue, Is.EqualTo(99));
    }

    /// <summary>
    /// An owned sequence belongs to its column and cannot be dropped on its own; the owner has to
    /// go instead. A free-standing sequence a column merely defaults from gets the other refusal,
    /// because the two mistakes need different corrections.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestSequenceInUseCannotBeDropped()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE owned_ids (id oid PRIMARY KEY, n serial)");

        CamusDBException? owned = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "DROP SEQUENCE owned_ids_n_seq"));
        Assert.That(owned!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceInUse));
        Assert.That(owned.Message, Does.Contain("identity column"));

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE shared_no");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE users_of_shared (id oid PRIMARY KEY, n int64 DEFAULT(nextval('shared_no')))");

        CamusDBException? defaulted = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "DROP SEQUENCE shared_no"));
        Assert.That(defaulted!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceInUse));
        Assert.That(defaulted.Message, Does.Contain("column defaults"));
    }

    // -----------------------------------------------------------------------
    // Positions a sequence call is refused in
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task TestNextValIsRefusedWhereTheCountIsUnbounded()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE bounded");
        await ExecuteDdl(executor, dbname, "CREATE TABLE rows_of (id oid PRIMARY KEY, n int64)");
        await ExecuteNonQuery(executor, dbname, "INSERT INTO rows_of (id, n) VALUES (gen_id(), 1)");

        // A projection over a relation is evaluated once per row, and the row count is not known
        // when the statement is bound.
        CamusDBException? inProjection = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT nextval('bounded') AS v FROM rows_of"));
        Assert.That(inProjection!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceCallNotAllowedHere));

        CamusDBException? inWhere = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT n FROM rows_of WHERE n = nextval('bounded')"));
        Assert.That(inWhere!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceCallNotAllowedHere));
    }

    [Test]
    [NonParallelizable]
    public async Task TestUnknownSequenceIsReported()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        CamusDBException? unknown = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT nextval('never_created') AS v"));

        Assert.That(unknown!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceDoesntExist));
    }

    // -----------------------------------------------------------------------
    // Reservation sizing and sharing
    // -----------------------------------------------------------------------

    /// <summary>
    /// Two columns defaulting from one sequence consume two values a row. A reservation sized from
    /// the distinct sequences rather than the defaulted columns bought one, and the second column
    /// of the first row then found the run already empty.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestTwoDefaultsOnOneSequenceEachDraw()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE shared_pair");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE pairs (id oid PRIMARY KEY, " +
            "a int64 DEFAULT(nextval('shared_pair')), b int64 DEFAULT(nextval('shared_pair')))");

        await ExecuteNonQuery(executor, dbname, "INSERT INTO pairs (id) VALUES (gen_id()), (gen_id())");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT a, b FROM pairs");

        List<long> values = [];
        foreach (Dictionary<string, ColumnValue> row in rows)
        {
            values.Add(row["a"].LongValue);
            values.Add(row["b"].LongValue);
        }

        values.Sort();
        Assert.That(values, Is.EqualTo(new List<long> { 1, 2, 3, 4 }));
    }

    /// <summary>
    /// An explicit call and a column default on the same sequence share one reservation. Reserving
    /// twice replaced the first cursor, so the explicit call's values were bought and then thrown
    /// away — and the statement drew past the end of the run that remained.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestExplicitCallAndDefaultShareOneReservation()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE mixed");
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE mixed_rows (id oid PRIMARY KEY, " +
            "a int64 DEFAULT(nextval('mixed')), b int64)");

        await ExecuteNonQuery(executor, dbname,
            "INSERT INTO mixed_rows (id, b) VALUES (gen_id(), nextval('mixed'))");

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT a, b FROM mixed_rows");

        Assert.That(rows.Count, Is.EqualTo(1));
        Assert.That(new[] { rows[0]["a"].LongValue, rows[0]["b"].LongValue }.OrderBy(v => v),
            Is.EqualTo(new long[] { 1, 2 }));
    }

    // -----------------------------------------------------------------------
    // setval accepts one shape, and refuses the ones it cannot honour
    // -----------------------------------------------------------------------

    /// <summary>
    /// The counter is moved before the statement is evaluated, so a call inside a branch the
    /// statement never takes would still reset the sequence — irrevocably, and invisibly. Refused
    /// rather than reinterpreted.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestSetValIsRefusedWhereItCannotBeHonoured()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE guarded");

        CamusDBException? nested = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT CASE WHEN false THEN setval('guarded', 1) ELSE 0 END AS n"));
        Assert.That(nested!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceCallNotAllowedHere));

        CamusDBException? twice = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, "SELECT setval('guarded', 10) AS a, setval('guarded', 20) AS b"));
        Assert.That(twice!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceCallNotAllowedHere));

        await ExecuteDdl(executor, dbname, "CREATE TABLE guarded_rows (id oid PRIMARY KEY, n int64)");

        CamusDBException? inInsert = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname,
                "INSERT INTO guarded_rows (id, n) VALUES (gen_id(), setval('guarded', 5))"));
        Assert.That(inInsert!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceCallNotAllowedHere));

        // None of the three moved the counter: the first value is still the sequence's start.
        List<Dictionary<string, ColumnValue>> drawn = await Query(executor, dbname, "SELECT nextval('guarded') AS v");
        Assert.That(drawn[0]["v"].LongValue, Is.EqualTo(1));
    }

    // -----------------------------------------------------------------------
    // ALTER is checked against the definition it produces
    // -----------------------------------------------------------------------

    /// <summary>
    /// An <c>ALTER</c> is validated against the folded result, not the fields it names. Both
    /// statements below leave a definition that contradicts itself while naming only one value.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestAlterIsRefusedWhenTheFoldedDefinitionCannotHold()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE folded");

        // A minimum above the recorded start. Accepting it advertised a bound the counter did not
        // hold: the sequence went on issuing 1.
        CamusDBException? raisedMinimum = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "ALTER SEQUENCE folded MINVALUE 100"));
        Assert.That(raisedMinimum!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidSequenceDefinition));

        await ExecuteDdl(executor, dbname, "CREATE SEQUENCE bounded MAXVALUE 100");

        CamusDBException? startAboveMaximum = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "ALTER SEQUENCE bounded START WITH 200"));
        Assert.That(startAboveMaximum!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidSequenceDefinition));
    }

    // -----------------------------------------------------------------------
    // Ownership boundaries
    // -----------------------------------------------------------------------

    /// <summary>
    /// A default in one relation cannot draw from a sequence owned by another's identity column:
    /// the owner takes that sequence with it when it is dropped, with no dependency check on the
    /// way out, so the reference would be left pointing at a counter that is gone.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestACrossOwnerDefaultIsRefused()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE owner_table (id oid PRIMARY KEY, n serial)");

        CamusDBException? borrowed = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname,
                "CREATE TABLE consumer (id oid PRIMARY KEY, n int64 DEFAULT(nextval('owner_table_n_seq')))"));

        Assert.That(borrowed!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceInUse));
    }

    /// <summary>
    /// A failed <c>CREATE TABLE</c> leaves nothing behind, including the sequences it had already
    /// created for identity columns declared before the one that failed.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestAFailedCreateTableLeavesNoSequence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        CamusDBException? failed = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname,
                "CREATE TABLE broken (id oid PRIMARY KEY, n serial, other int64 DEFAULT(nextval('missing_sequence')))"));

        Assert.That(failed!.Code, Is.EqualTo(CamusDBErrorCodes.SequenceDoesntExist));
        Assert.That(database.Schema.Tables.ContainsKey("broken"), Is.False);
        Assert.That(database.Schema.Sequences.ContainsKey("broken_n_seq"), Is.False,
            "the sequence created before the failure must not survive it");
    }

    /// <summary>
    /// Re-running <c>CREATE TABLE IF NOT EXISTS</c> on a table that already exists must mint
    /// nothing. It used to create one more sequence per identity column on every run and then
    /// report that it had done nothing.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestCreateTableIfNotExistsMintsNoExtraSequence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE IF NOT EXISTS idempotent (id oid PRIMARY KEY, n serial)");
        await ExecuteDdl(executor, dbname, "CREATE TABLE IF NOT EXISTS idempotent (id oid PRIMARY KEY, n serial)");
        await ExecuteDdl(executor, dbname, "CREATE TABLE IF NOT EXISTS idempotent (id oid PRIMARY KEY, n serial)");

        int sequences = 0;
        foreach (string name in database.Schema.Sequences.Keys)
        {
            if (name.StartsWith("idempotent_n_seq", StringComparison.OrdinalIgnoreCase))
                sequences++;
        }

        Assert.That(sequences, Is.EqualTo(1));
    }

    // -----------------------------------------------------------------------
    // Keyword regressions: every option word stays usable as an identifier
    // -----------------------------------------------------------------------

    /// <summary>
    /// Only SEQUENCE and SEQUENCES became reserved words. Every option word is matched as a plain
    /// identifier and validated in the parse action, so each must still work as a table name, a
    /// column name and an alias — a grammar change that quietly reserves a common word is the kind
    /// of break that surfaces months later in someone else's schema.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOptionWordsStayUsableAsIdentifiers()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await SetupDatabase();

        string[] words =
        [
            "increment", "minvalue", "maxvalue", "no", "cycle", "cache",
            "restart", "owned", "identity", "generated", "always", "serial", "continue"
        ];

        foreach (string word in words)
        {
            await ExecuteDdl(executor, dbname, $"CREATE TABLE {word} (id oid PRIMARY KEY, {word} int64)");
            await ExecuteNonQuery(executor, dbname, $"INSERT INTO {word} (id, {word}) VALUES (gen_id(), 7)");

            List<Dictionary<string, ColumnValue>> rows = await Query(
                executor, dbname, $"SELECT {word} AS {word} FROM {word}");

            Assert.That(rows.Count, Is.EqualTo(1), $"'{word}' stopped working as an identifier");
            Assert.That(rows[0][word].LongValue, Is.EqualTo(7));
        }
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupDatabase()
    {
        // A test database name used in SQL text needs a letter prefix: a raw GUID that begins with a
        // digit lexes as a number.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return (dbname, database, executor);
    }
}
