
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Nito.AsyncEx;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Configuration surface for row-level TTL: the storage parameters, their validation at
/// <c>ALTER TABLE … SET</c> time, <c>RESET</c>, the DROP/RENAME COLUMN interactions, and how the
/// configuration renders and survives a reopen.
///
/// <para>Everything here asserts that a misconfiguration is refused <em>in the user's session</em>. TTL
/// runs in the background, so a configuration error that is accepted here does not surface as an error
/// at all — it surfaces as a table that quietly never expires anything, which is far harder to notice
/// than a failed ALTER.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test, like every other node-booting fixture.
[NonParallelizable]
public sealed class TestTtlTableSettings : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupSessionsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "sessions",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("token", ColumnType.String, notNull: true),
                new("expires_at", ColumnType.DateTime),
                new("expires_epoch", ColumnType.Integer64),
                new("label", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    private static async Task ExecDdlAsync(CommandExecutor executor, string dbname, string sql)
        => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: sql, parameters: null));

    private static async Task<string> ShowCreateTableAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txn, dbname, "SHOW CREATE TABLE sessions", null));

        string rendered = "";
        await foreach (QueryResultRow row in cursor)
            rendered = row.Row["Create Table"].StrValue ?? "";

        await database.Transactions.CommitAsync(txn);
        return rendered;
    }

    // ── Settings are a schema change ──────────────────────────────────────────

    /// <summary>
    /// Changing table settings must advance the database schema version, and the descriptor's
    /// head-version fence must advance with it.
    ///
    /// <para>This is not bookkeeping for its own sake. Background sweeps decide whether a database's
    /// metadata is worth re-reading by comparing that version, so a settings change that left it
    /// untouched would be invisible to them — TTL could be switched on and the sweep would go on
    /// believing nothing had changed. The head fence is asserted alongside it because the two are read
    /// together as a stability signal: a version that moved without its fence reads as "a schema change
    /// is still in flight", which blocks branching from the database.</para>
    /// </summary>
    [Test]
    public async Task SettingsChangeAdvancesTheDatabaseSchemaVersionAndItsFence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();

        long versionBefore = database.Schema.SchemaVersion;
        Assert.AreEqual(
            versionBefore, database.HeadSchemaVersion,
            "Precondition: the schema is stable before the ALTER");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.Greater(
            database.Schema.SchemaVersion, versionBefore,
            "ALTER TABLE SET must advance the database schema version");
        Assert.AreEqual(
            database.Schema.SchemaVersion, database.HeadSchemaVersion,
            "The head-version fence must advance with the schema version, or the database reads as having in-flight DDL");

        long versionAfterSet = database.Schema.SchemaVersion;

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl_expiration_expression)");

        Assert.Greater(
            database.Schema.SchemaVersion, versionAfterSet,
            "RESET is the same delta in the opposite direction and must advance the version too");
        Assert.AreEqual(
            database.Schema.SchemaVersion, database.HeadSchemaVersion,
            "The head-version fence must advance on RESET as well");
    }

    /// <summary>
    /// The advanced version must also be durable: a sweep reads it from the persisted meta key, not
    /// from this node's memory, so an in-memory-only bump would be invisible to exactly the consumer
    /// that needs it.
    /// </summary>
    [Test]
    public async Task SettingsChangeAdvancesThePersistedSchemaVersionKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();

        long persistedBefore = await ReadPersistedSchemaVersionAsync(database);

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        long persistedAfter = await ReadPersistedSchemaVersionAsync(database);

        Assert.Greater(
            persistedAfter, persistedBefore,
            "The persisted {dbId}/meta/version key must advance, since background sweeps read it rather than in-memory state");
        Assert.AreEqual(
            database.Schema.SchemaVersion, persistedAfter,
            "The persisted version must match the in-memory one after the change commits");
    }

    private static async Task<long> ReadPersistedSchemaVersionAsync(DatabaseDescriptor database)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await database.Kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            $"{database.Id}/meta/version",
            -1,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            throw new InvalidOperationException($"No persisted schema version for database '{database.Name}'");

        return MetaJsonSerializer.DeserializeCompat(entry.Value, MetaJsonContext.Default.Int64);
    }

    // ── The happy path ────────────────────────────────────────────────────────

    [Test]
    public async Task SettingTtlParametersRoundTripsThroughSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly', " +
            "ttl_select_batch_size = 250, ttl_delete_batch_size = 50, ttl_delete_rate_limit = 20, ttl_grace_ms = 5000)");

        TtlSettings ttl = TtlSettings.Resolve(table.Schema.Settings, Options);

        Assert.AreEqual("expires_at", ttl.ExpirationColumn);
        Assert.AreEqual(3_600_000L, ttl.JobIntervalMs, "@hourly must resolve to one hour");
        Assert.AreEqual(250, ttl.SelectBatchSize);
        Assert.AreEqual(50, ttl.DeleteBatchSize);
        Assert.AreEqual(20, ttl.DeleteRateLimit);
        Assert.AreEqual(5000L, ttl.GraceMs);
        Assert.IsTrue(ttl.IsActive, "A configured, unpaused table is active");

        // Unset knobs must fall back to the node defaults, not to zero.
        Assert.AreEqual(Options.TtlDefaultSelectRateLimit, ttl.SelectRateLimit);
    }

    [Test]
    public async Task UnconfiguredTableResolvesToNodeDefaultsAndIsInactive()
    {
        (_, DatabaseDescriptor database, _) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        TtlSettings ttl = TtlSettings.Resolve(table.Schema.Settings, Options);

        Assert.IsNull(ttl.ExpirationColumn);
        Assert.IsFalse(ttl.IsActive, "TTL is off until a table names an expiration column");
        Assert.AreEqual(Options.TtlDefaultSelectBatchSize, ttl.SelectBatchSize);
        Assert.AreEqual(Options.TtlDefaultDeleteBatchSize, ttl.DeleteBatchSize);
        Assert.AreEqual(Options.TtlDefaultDeleteRateLimit, ttl.DeleteRateLimit);
        Assert.AreEqual(86_400_000L, ttl.JobIntervalMs, "@daily is the default cadence");
    }

    [Test]
    public async Task PauseKeepsTheConfigurationButStopsTheSweep()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_pause = true)");

        TtlSettings ttl = TtlSettings.Resolve(table.Schema.Settings, Options);
        Assert.AreEqual("expires_at", ttl.ExpirationColumn, "Pausing must not discard the configuration");
        Assert.IsTrue(ttl.Paused);
        Assert.IsFalse(ttl.IsActive);

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_pause = false)");
        Assert.IsTrue(TtlSettings.Resolve(table.Schema.Settings, Options).IsActive, "Unpausing resumes");
    }

    [Test]
    public async Task Integer64ColumnIsAcceptedAsAnEpochExpiry()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_epoch')");

        Assert.AreEqual("expires_epoch", TtlSettings.Resolve(table.Schema.Settings, Options).ExpirationColumn);
    }

    // ── Validation: every invalid form must be refused at ALTER time ──────────

    [Test]
    public async Task NonExistentColumnIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'nope')"))!;

        Assert.That(ex.Message, Does.Contain("nope"));
        Assert.That(ex.Message, Does.Contain("does not exist"));
    }

    [Test]
    public async Task WrongColumnTypeIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'label')"))!;

        Assert.That(ex.Message, Does.Contain("DateTime"), "The message must name the acceptable types");
    }

    [Test]
    public async Task PrimaryKeyColumnIsRejectedEvenWhenItsTypeWouldBeValid()
    {
        // The primary key must be rejected on its own merits, not incidentally because the usual
        // `Id`-typed key already fails the type check. So this table's key is an Integer64 epoch —
        // a perfectly good expiry type that the sweep still must not range over and delete by.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "readings",
            columns: new ColumnInfo[]
            {
                new("ts", ColumnType.Integer64, notNull: true),
                new("value", ColumnType.Float64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("ts", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: dbname,
                sql: "ALTER TABLE readings SET (ttl_expiration_expression = 'ts')", parameters: null)))!;

        Assert.That(ex.Message, Does.Contain("primary key"));
    }

    [Test]
    public async Task AnExpressionIsRejectedAsNotYetSupportedRatherThanInvalid()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        // A user arriving from CockroachDB will reasonably write an expression here. The message has to
        // tell them the parameter is right and the value grammar is narrower — not that they mistyped.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname,
                "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at + 30')"))!;

        Assert.That(ex.Message, Does.Contain("not supported yet"));
    }

    [Test]
    public async Task FullCronExpressionIsRejectedAsNotYetSupported()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_job_cron = '0 * * * *')"))!;

        Assert.That(ex.Message, Does.Contain("not supported yet"));
        Assert.That(ex.Message, Does.Contain("@daily"), "The message must list what IS accepted");
    }

    [Test]
    public async Task UnknownCronMacroIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_job_cron = '@fortnightly')"));
    }

    [Test]
    public async Task ZeroBatchSizeIsRejectedButZeroRateLimitMeansUnlimited()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        // A zero batch size would make the sweep do nothing forever; a zero rate limit is CockroachDB's
        // spelling of "unlimited". Same literal, opposite meanings — so they must validate differently.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_select_batch_size = 0)"));

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_delete_rate_limit = 0)");
        Assert.AreEqual(0, TtlSettings.Resolve(table.Schema.Settings, Options).DeleteRateLimit);
    }

    [Test]
    public async Task NegativeIntegerIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_grace_ms = -1)"));
    }

    [Test]
    public async Task NonIntegerValueForAnIntegerKeyIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_select_batch_size = 'lots')"));
    }

    [Test]
    public async Task EngineOwnedTtlMarkerCannotBeSetByAUser()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl = true)"))!;

        Assert.That(ex.Message, Does.Contain("set by the engine"));
    }

    [Test]
    public async Task UnknownSettingKeyIsStillRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expire_after = '3 months')"));
    }

    // ── RESET ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task ResetTtlClearsEveryTtlParameterAtOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly', ttl_grace_ms = 900)");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (sql_stats_automatic_collection_enabled = false)");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl)");

        TtlSettings ttl = TtlSettings.Resolve(table.Schema.Settings, Options);
        Assert.IsNull(ttl.ExpirationColumn, "RESET (ttl) turns TTL off");
        Assert.AreEqual(86_400_000L, ttl.JobIntervalMs, "Tuning must be cleared too, not orphaned");
        Assert.AreEqual(0L, ttl.GraceMs);

        // An unrelated setting must survive — RESET (ttl) is scoped to the TTL group.
        Assert.IsFalse(table.Schema.AutoStatsCollectionEnabled,
            "RESET (ttl) must not disturb settings outside the TTL group");
    }

    [Test]
    public async Task ResetOfASingleParameterLeavesTheRestConfigured()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@hourly')");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl_job_cron)");

        TtlSettings ttl = TtlSettings.Resolve(table.Schema.Settings, Options);
        Assert.AreEqual("expires_at", ttl.ExpirationColumn, "Only the named key is cleared");
        Assert.AreEqual(86_400_000L, ttl.JobIntervalMs, "The cleared key falls back to the node default");
    }

    [Test]
    public async Task ResetOfAnUnsetParameterIsANoOp()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        // The requested end state (key absent) already holds, so a defensive script must not fail.
        Assert.DoesNotThrowAsync(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl)"));
    }

    [Test]
    public async Task ResetOfAnUnknownKeyIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (not_a_setting)"));
    }

    // ── Column lifecycle ──────────────────────────────────────────────────────

    [Test]
    public async Task DroppingTheTtlColumnIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions DROP COLUMN expires_at"))!;

        Assert.That(ex.Message, Does.Contain("RESET (ttl)"), "The message must say how to proceed");
    }

    [Test]
    public async Task DroppingTheTtlColumnSucceedsAfterResettingTtl()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl)");

        Assert.DoesNotThrowAsync(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions DROP COLUMN expires_at"));
    }

    [Test]
    public async Task DroppingAnUnrelatedColumnIsUnaffected()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.DoesNotThrowAsync(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions DROP COLUMN label"));
    }

    [Test]
    public async Task RenamingTheTtlColumnCarriesTheSettingAcross()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RENAME COLUMN expires_at TO valid_until");

        Assert.AreEqual("valid_until", TtlSettings.Resolve(table.Schema.Settings, Options).ExpirationColumn,
            "A rename must not leave the TTL configuration pointing at a column that no longer exists");
    }

    [Test]
    public async Task RenamingAnUnrelatedColumnLeavesTheSettingAlone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");
        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RENAME COLUMN label TO tag");

        Assert.AreEqual("expires_at", TtlSettings.Resolve(table.Schema.Settings, Options).ExpirationColumn);
    }

    // ── Rendering and durability ──────────────────────────────────────────────

    // ── Bounds, the derived marker, and a genuine round-trip ──────────────────

    [Test]
    public async Task AValueTooLargeForTheRuntimeTypeIsRejectedRatherThanSilentlyIgnored()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        // Storing a value the resolver would silently discard is the worst outcome: the user sees the
        // setting accepted and the sweep quietly runs on the default instead.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdlAsync(executor, dbname,
                $"ALTER TABLE sessions SET (ttl_select_batch_size = {(long)int.MaxValue + 1})"))!;

        Assert.That(ex.Message, Does.Contain("must be <="));
    }

    [Test]
    public async Task ADeleteBatchThatCannotFitTheMutationBudgetIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        // Three writable indexes, so each row costs four mutations. A batch that needs more than the
        // per-transaction limit would make every delete transaction abort — a table that silently stops
        // expiring, diagnosable only from a background log.
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "wide",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("a", ColumnType.String),
                new("b", ColumnType.String),
                new("expires_at", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "a_idx", new ColumnIndexInfo[] { new("a", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "b_idx", new ColumnIndexInfo[] { new("b", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: dbname,
                sql: $"ALTER TABLE wide SET (ttl_delete_batch_size = {Options.MaxMutationsPerTransaction})",
                parameters: null)))!;

        Assert.That(ex.Message, Does.Contain("mutations per transaction"));
        Assert.That(ex.Message, Does.Contain("lower it to at most"), "The message must say what would work");
    }

    [Test]
    public async Task TheDerivedTtlMarkerFollowsTheExpirationSetting()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();
        TableDescriptor table = await OpenTableAsync(database, "sessions");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        Assert.IsTrue(table.Schema.Settings!.ContainsKey(TableSettings.TtlKey),
            "Configuring TTL must set the engine-owned marker");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions RESET (ttl_expiration_expression)");

        Assert.IsFalse(table.Schema.Settings!.ContainsKey(TableSettings.TtlKey),
            "Clearing the expiration column must clear the marker with it, or the two disagree");
    }

    [Test]
    public async Task SettingOnlyTuningStillValidatesAgainstTheTableItLandsOn()
    {
        (string dbname, _, CommandExecutor executor) = await SetupSessionsTable();

        await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at')");

        // The merged result is what must be valid, not the incoming fragment. A later ALTER that touches
        // only a batch size still has to be checked against the column already configured.
        Assert.DoesNotThrowAsync(async () =>
            await ExecDdlAsync(executor, dbname, "ALTER TABLE sessions SET (ttl_select_batch_size = 10)"));
    }

    [Test]
    public async Task ShowCreateTableOutputRecreatesTheSameTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "events",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("expiresAt", ColumnType.DateTime),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "ALTER TABLE events SET (ttl_expiration_expression = 'expiresAt', ttl_grace_ms = 250, ttl_job_cron = '@weekly')",
            parameters: null));

        KvTransaction readTxn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(readTxn, dbname, "SHOW CREATE TABLE events", null));

        string rendered = "";
        await foreach (QueryResultRow row in cursor)
            rendered = row.Row["Create Table"].StrValue ?? "";
        await database.Transactions.CommitAsync(readTxn);

        // Asserting on substrings only proves the renderer produced text. Executing the output is the
        // only thing that proves it is a faithful definition — and it is what catches the engine-owned
        // marker leaking into a statement the parser rejects.
        string replayed = rendered.Replace("`events`", "`events_copy`").TrimEnd(';');
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: replayed, parameters: null));

        TableDescriptor copy = await OpenTableAsync(database, "events_copy");
        TtlSettings ttl = TtlSettings.Resolve(copy.Schema.Settings, Options);

        Assert.AreEqual("expiresAt", ttl.ExpirationColumn, "Mixed-case column names must survive the round-trip");
        Assert.AreEqual(250L, ttl.GraceMs);
        Assert.AreEqual(604_800_000L, ttl.JobIntervalMs);
        Assert.IsTrue(ttl.IsActive);
    }

    [Test]
    public async Task CreateTableAcceptsInlineSettings()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "CREATE TABLE inline (id oid PRIMARY KEY, expires_at int64) WITH (ttl_expiration_expression = 'expires_at')",
            parameters: null));

        TableDescriptor table = await OpenTableAsync(database, "inline");
        Assert.AreEqual("expires_at", TtlSettings.Resolve(table.Schema.Settings, Options).ExpirationColumn);
    }

    [Test]
    public async Task ShowCreateTableRendersTheTtlConfiguration()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();

        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_grace_ms = 1000)");

        string rendered = await ShowCreateTableAsync(executor, database, dbname);

        Assert.That(rendered, Does.Contain("WITH ("), "A configuration nobody can see cannot be reviewed");
        Assert.That(rendered, Does.Contain("ttl_expiration_expression = 'expires_at'"));
        Assert.That(rendered, Does.Contain("ttl_grace_ms = 1000"), "Integers render bare, so the clause re-parses");
    }

    [Test]
    public async Task ShowCreateTableOmitsTheClauseWhenNoSettingsExist()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();

        string rendered = await ShowCreateTableAsync(executor, database, dbname);

        Assert.That(rendered, Does.Not.Contain("WITH ("));
    }

    [Test]
    public async Task TtlConfigurationSurvivesAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSessionsTable();

        await ExecDdlAsync(executor, dbname,
            "ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@weekly')");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        TableSchema? schema = reopened.Schema.Tables.GetValueOrDefault("sessions");
        Assert.IsNotNull(schema);

        TtlSettings ttl = TtlSettings.Resolve(schema!.Settings, Options);
        Assert.AreEqual("expires_at", ttl.ExpirationColumn, "The configuration must be persisted, not in-memory only");
        Assert.AreEqual(604_800_000L, ttl.JobIntervalMs);
    }

    [Test]
    public async Task ColumnNameCaseIsPreservedThroughPersistence()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "events",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("expiresAt", ColumnType.DateTime),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "ALTER TABLE events SET (ttl_expiration_expression = 'expiresAt')", parameters: null));

        TableDescriptor table = await OpenTableAsync(database, "events");

        // The settings bag lowercases KEYS; lowercasing VALUES too would silently break a camelCase
        // column name and leave the sweep looking for a column that does not exist.
        Assert.AreEqual("expiresAt", TtlSettings.Resolve(table.Schema.Settings, Options).ExpirationColumn);
    }
}
