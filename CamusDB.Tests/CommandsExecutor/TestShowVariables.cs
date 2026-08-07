/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>SHOW VARIABLES</c>, driven through <see cref="ExecuteSQLTicket"/> so the
/// statement is exercised the way a console session reaches it — parse, dispatch, and row projection
/// included.
///
/// <para>The central claim under test is that the rows describe the options the engine was
/// <em>constructed</em> with, not the shipped defaults and not a re-read of any file. Proving that
/// needs two engines built with different configurations, because an executor fixes its configuration
/// at construction: a test that mutates a setting after building one engine would compare a result
/// against itself and pass while verifying nothing.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowVariables : BaseTest
{
    /// <summary>
    /// Runs a statement that needs no transaction. <c>SHOW VARIABLES</c> is dispatched before any
    /// database is opened, so passing no transaction here is part of what the tests assert.
    /// </summary>
    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static string? Cell(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? value) && value.Type != ColumnType.Null ? value.StrValue : null;

    private static QueryResultRow Single(List<QueryResultRow> rows, string variable)
    {
        List<QueryResultRow> matches = rows.Where(r => Cell(r, "variable") == variable).ToList();
        Assert.AreEqual(1, matches.Count, $"expected exactly one row for '{variable}'");
        return matches[0];
    }

    // ── the statement runs at all, in the shape a transport uses ──────────────

    [Test]
    public async Task ShowVariables_NeedsNoDatabaseContext()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        // Configuration is per-process, not per-database: naming a database that does not exist must
        // not make the statement fail, because it never opens one.
        List<QueryResultRow> rows = await QueryAsync(executor, "", "SHOW VARIABLES");

        Assert.IsNotEmpty(rows);
        Assert.IsNotNull(Cell(Single(rows, "ttl_span_lease_ms"), "value"));
    }

    [Test]
    public async Task ShowVariables_ListsEverySettingExactlyOnce()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        List<string> names = (await QueryAsync(executor, db, "SHOW VARIABLES"))
            .Select(r => Cell(r, "variable")!)
            .ToList();

        Assert.AreEqual(names.Count, names.Distinct().Count(), "variable names must be unique");
        Assert.AreEqual(ConfigVariableCatalog.Describe(CamusDBOptions.Default).Count, names.Count);

        // Ordinal-sorted, so a scripted diff between two nodes lines up row by row.
        CollectionAssert.AreEqual(names.OrderBy(n => n, StringComparer.Ordinal).ToList(), names);

        // Both the flat options and the flattened nested section are represented.
        CollectionAssert.Contains(names, "data_dir");
        CollectionAssert.Contains(names, "kahuna.wal_sync_writes");
    }

    // ── effective values, not defaults ────────────────────────────────────────

    /// <summary>
    /// The point of the statement. Two engines, two configurations, one variable: the reported value
    /// has to follow the engine that served the statement, while <c>default</c> stays put on both.
    /// </summary>
    [Test]
    public async Task ShowVariables_ReportsTheConfigurationItsOwnEngineWasBuiltWith()
    {
        (string defaultDb, DatabaseDescriptor _, CommandExecutor defaultEngine) = await CreateDatabase();
        (string spillDb, DatabaseDescriptor _, CommandExecutor spillEngine) =
            await CreateDatabase(Options with { SpillEnabled = true, SpillThresholdRows = 1234 });

        QueryResultRow offRow = Single(await QueryAsync(defaultEngine, defaultDb, "SHOW VARIABLES"), "spill_enabled");
        List<QueryResultRow> onRows = await QueryAsync(spillEngine, spillDb, "SHOW VARIABLES");

        Assert.AreEqual("false", Cell(offRow, "value"));
        Assert.AreEqual("true", Cell(Single(onRows, "spill_enabled"), "value"));
        Assert.AreEqual("1234", Cell(Single(onRows, "spill_threshold_rows"), "value"));

        // `default` is the shipped value on both engines — it is what the setting would be if nothing
        // overrode it, so an overridden setting must show a value that differs from it.
        QueryResultRow overridden = Single(onRows, "spill_enabled");
        Assert.AreEqual("false", Cell(overridden, "default"));
        Assert.AreNotEqual(Cell(overridden, "default"), Cell(overridden, "value"));
    }

    // ── LIKE ──────────────────────────────────────────────────────────────────

    [Test]
    public async Task ShowVariables_LikeNarrowsToMatchingNames()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> all = await QueryAsync(executor, db, "SHOW VARIABLES");
        List<QueryResultRow> ttl = await QueryAsync(executor, db, "SHOW VARIABLES LIKE '%ttl%'");

        Assert.IsNotEmpty(ttl);
        Assert.Less(ttl.Count, all.Count);
        Assert.IsTrue(ttl.All(r => Cell(r, "variable")!.Contains("ttl", StringComparison.Ordinal)));

        // Anchored patterns and the single-character wildcard both work, and the match is on the name.
        List<QueryResultRow> prefixed = await QueryAsync(executor, db, "SHOW VARIABLES LIKE 'ttl_%'");
        Assert.IsTrue(prefixed.All(r => Cell(r, "variable")!.StartsWith("ttl_", StringComparison.Ordinal)));
        Assert.Less(prefixed.Count, ttl.Count, "'%ttl%' also matches query_result_cache_default_ttl_ms");
    }

    [Test]
    public async Task ShowVariables_LikeAcceptsEveryLiteralForm()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        int expected = (await QueryAsync(executor, db, "SHOW VARIABLES LIKE 'spill_%'")).Count;

        Assert.Greater(expected, 0);
        Assert.AreEqual(expected, (await QueryAsync(executor, db, "SHOW VARIABLES LIKE \"spill_%\"")).Count);
        Assert.AreEqual(expected, (await QueryAsync(executor, db, "SHOW VARIABLES LIKE E'spill_%'")).Count);
    }

    /// <summary>
    /// Pattern matching is ordinal, the same as <c>SHOW TABLES</c> / <c>SHOW DATABASES</c> /
    /// <c>SHOW ENGINE STATS</c> — they share one matcher. Pinned as a test because variable names are
    /// all lowercase, which makes an uppercase pattern silently match nothing; that is a consistency
    /// decision across the dialect rather than an oversight in this statement.
    /// </summary>
    [Test]
    public async Task ShowVariables_LikeIsCaseSensitiveLikeEveryOtherShowStatement()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        Assert.IsNotEmpty(await QueryAsync(executor, db, "SHOW VARIABLES LIKE 'spill_%'"));
        Assert.IsEmpty(await QueryAsync(executor, db, "SHOW VARIABLES LIKE 'SPILL_%'"));
    }

    /// <summary>A pattern nothing matches is an empty result, not an error.</summary>
    [Test]
    public async Task ShowVariables_LikeMatchingNothingReturnsNoRows()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        Assert.IsEmpty(await QueryAsync(executor, db, "SHOW VARIABLES LIKE 'no_such_setting_%'"));
    }

    // ── redaction ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The three settings that hold key material are listed — whether a node has a secret configured
    /// is an operational question — but never with their value. The assertion sweeps the whole result
    /// set rather than just the three rows, so a secret leaking through some other column fails too.
    /// </summary>
    [Test]
    public async Task ShowVariables_MasksSecretsButStillListsThem()
    {
        const string secret = "correct-horse-battery-staple";

        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(Options with
        {
            BootstrapSuperuserPassword = secret,
            AccessTokenServerKey = secret,
            NodeSecret = secret,
        });

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW VARIABLES");

        foreach (string name in new[] { "bootstrap_superuser_password", "access_token_server_key", "node_secret" })
            Assert.AreEqual("********", Cell(Single(rows, name), "value"), name);

        foreach (QueryResultRow row in rows)
            foreach (string column in new[] { "variable", "value", "type", "default", "source" })
                Assert.AreNotEqual(secret, Cell(row, column), $"{Cell(row, "variable")}.{column} leaked the secret");
    }

    /// <summary>An unconfigured secret reads as empty rather than as a mask, so the two are tellable apart.</summary>
    [Test]
    public async Task ShowVariables_UnsetSecretIsNotMasked()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual("", Cell(Single(await QueryAsync(executor, db, "SHOW VARIABLES"), "node_secret"), "value"));
    }

    // ── value rendering ───────────────────────────────────────────────────────

    /// <summary>
    /// A genuinely unset option is SQL NULL, which a caller can tell apart from the empty string that
    /// an unset <em>string</em> setting produces.
    /// </summary>
    [Test]
    public async Task ShowVariables_UnsetOptionIsNullNotEmpty()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW VARIABLES");

        Assert.AreEqual(ColumnType.Null, Single(rows, "force_spill_threshold_rows").Row["value"].Type);
        Assert.AreEqual(ColumnType.Null, Single(rows, "kahuna.storage").Row["value"].Type);
        Assert.AreEqual(ColumnType.String, Single(rows, "bootstrap_superuser").Row["value"].Type);
        Assert.AreEqual("", Cell(Single(rows, "bootstrap_superuser"), "value"));
    }

    /// <summary>
    /// Values are rendered the way <c>config.yml</c> spells them, so a value read here can be written
    /// back into a file unchanged. The enum assertions run the rendered text back through the
    /// configuration parser, which is the only check that actually proves round-tripping.
    /// </summary>
    [Test]
    public async Task ShowVariables_RendersValuesTheWayTheConfigFileSpellsThem()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(Options with
        {
            AutoAnalyzeFractionStaleRows = 0.25,
            DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted,
        });

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW VARIABLES");

        Assert.AreEqual("true", Cell(Single(rows, "ttl_enabled"), "value"));
        Assert.AreEqual("bool", Cell(Single(rows, "ttl_enabled"), "type"));

        Assert.AreEqual("0.25", Cell(Single(rows, "auto_analyze_fraction_stale_rows"), "value"));
        Assert.AreEqual("double", Cell(Single(rows, "auto_analyze_fraction_stale_rows"), "type"));

        // No digit separators and no unit suffix: 64 MiB renders as the integer a file would carry.
        Assert.AreEqual("67108864", Cell(Single(rows, "query_result_cache_max_bytes"), "value"));
        Assert.AreEqual("long", Cell(Single(rows, "query_result_cache_max_bytes"), "type"));

        // Durations are whole milliseconds like every other *_ms key, not 00:15:00.
        Assert.AreEqual("900000", Cell(Single(rows, "access_token_ttl"), "value"));
        Assert.AreEqual("duration_ms", Cell(Single(rows, "access_token_ttl"), "type"));

        QueryResultRow isolation = Single(rows, "default_isolation_level");
        Assert.AreEqual("enum", Cell(isolation, "type"));
        Assert.AreEqual(
            CamusIsolationLevel.ReadCommitted,
            new ConfigDefinition { DefaultIsolationLevel = Cell(isolation, "value")! }.ParseDefaultIsolationLevel());

        QueryResultRow locking = Single(rows, "default_transaction_locking");
        Assert.AreEqual(
            CamusDBOptions.Default.DefaultTransactionLocking,
            new ConfigDefinition { DefaultTransactionLocking = Cell(locking, "value")! }.ParseDefaultTransactionLocking());
    }

    // ── provenance ────────────────────────────────────────────────────────────

    /// <summary>
    /// Every setting an engine was handed directly reports as a default: nothing recorded a layer, and
    /// claiming a file or a flag supplied it would be a fabrication.
    /// </summary>
    [Test]
    public async Task ShowVariables_UnrecordedProvenanceReportsAsDefault()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW VARIABLES");

        Assert.IsTrue(rows.All(r => Cell(r, "source") == "default"));
    }

    /// <summary>
    /// The four layers are reported with the operator-facing spelling, and the layer that won the value
    /// is the layer that is reported.
    /// </summary>
    [Test]
    public async Task ShowVariables_ReportsTheLayerThatSuppliedEachValue()
    {
        CamusDBOptions options = Options with
        {
            ValueSources = new Dictionary<string, ConfigValueSource>(StringComparer.OrdinalIgnoreCase)
            {
                ["ttl_enabled"] = ConfigValueSource.ConfigFile,
                ["key_range_sharding"] = ConfigValueSource.Environment,
                ["data_dir"] = ConfigValueSource.CommandLine,
            },
        };

        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(options);

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW VARIABLES");

        Assert.AreEqual("config", Cell(Single(rows, "ttl_enabled"), "source"));
        Assert.AreEqual("env", Cell(Single(rows, "key_range_sharding"), "source"));
        Assert.AreEqual("cli", Cell(Single(rows, "data_dir"), "source"));
        Assert.AreEqual("default", Cell(Single(rows, "spill_enabled"), "source"));
    }
}
