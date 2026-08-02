
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
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Two engines, configured differently, running in one process — each honouring its own settings.
///
/// <para>This is the regression test for configuration being per-instance rather than process-wide.
/// It cannot be written against a global: with one shared value, the second engine's configuration
/// would overwrite the first's, and whichever ran last would decide the behaviour of both. If a knob
/// ever slips back to a static, a test here fails rather than some unrelated fixture failing
/// mysteriously months later.</para>
///
/// <para>The knobs chosen are deterministic — identifier and column ceilings — so a failure means
/// configuration leaked between engines, never a timing artefact.</para>
///
/// <para>Serial: one case deliberately replaces the ambient options to prove an already-built engine
/// ignores the change. That replacement is process-wide for the length of the case, so this is one of
/// the few fixtures whose isolation really is about configuration.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestIndependentlyConfiguredEngines : BaseTest
{
    /// <summary>
    /// An engine over this test's shared node with <paramref name="mutate"/> applied to its options.
    /// Each call produces an independent engine: nothing it configures is visible to any other.
    /// </summary>
    private CommandExecutor EngineWith(Func<CamusDBOptions, CamusDBOptions> mutate)
    {
        CamusDBOptions options = mutate(Options);

        return new CommandExecutor(
            new CommandValidator(options),
            new CatalogsManager(logger),
            logger,
            options,
            sharedNode: TestNode!,
            registry: sharedRegistry!,
            isClusterMode: false);
    }

    private static CreateTableTicket TableTicket(string dbname, string tableName) =>
        new(databaseName: dbname,
            tableName: tableName,
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false);

    /// <summary>
    /// The same identifier is rejected by a strict engine and accepted by a permissive one. Under a
    /// process-wide limit only one of these outcomes could exist at a time.
    /// </summary>
    [Test]
    public async Task TwoEnginesEnforceTheirOwnIdentifierLimit()
    {
        (string dbname, _, _) = await CreateDatabase();

        CommandExecutor strict     = EngineWith(o => o with { MaxIdentifierLength = 8 });
        CommandExecutor permissive = EngineWith(o => o with { MaxIdentifierLength = 64 });

        const string longName = "a_table_name_well_over_eight";

        CamusDBException rejected = Assert.ThrowsAsync<CamusDBException>(
            async () => await strict.CreateTable(TableTicket(dbname, longName)))!;
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, rejected.Code,
            "The strict engine must enforce its own 8-character ceiling");

        Assert.IsTrue((await permissive.CreateTable(TableTicket(dbname, longName))).Success,
            "The permissive engine must accept the same name — the strict engine's limit is not its own");
    }

    /// <summary>
    /// The same assertion under genuine concurrency: both engines validate at the same time. A knob
    /// that is still process-wide would let one engine's setting decide the other's outcome depending
    /// on interleaving, so this fails non-deterministically if configuration ever leaks.
    /// </summary>
    [Test]
    public async Task ConcurrentEnginesDoNotSeeEachOthersConfiguration()
    {
        (string dbname, _, _) = await CreateDatabase();

        CommandExecutor strict     = EngineWith(o => o with { MaxIdentifierLength = 8 });
        CommandExecutor permissive = EngineWith(o => o with { MaxIdentifierLength = 64 });

        // Enough rounds that any interleaving-dependent leak shows up rather than passing by luck.
        const int rounds = 25;

        Task<bool[]> strictRun = Task.Run(async () =>
        {
            bool[] rejections = new bool[rounds];
            for (int i = 0; i < rounds; i++)
            {
                try
                {
                    await strict.CreateTable(TableTicket(dbname, $"strict_long_name_{i}"));
                    rejections[i] = false;
                }
                catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaLimitExceeded)
                {
                    rejections[i] = true;
                }
            }
            return rejections;
        });

        Task<bool[]> permissiveRun = Task.Run(async () =>
        {
            bool[] accepted = new bool[rounds];
            for (int i = 0; i < rounds; i++)
                accepted[i] = (await permissive.CreateTable(TableTicket(dbname, $"permissive_long_name_{i}"))).Success;

            return accepted;
        });

        bool[] strictResults = await strictRun;
        bool[] permissiveResults = await permissiveRun;

        Assert.IsTrue(strictResults.All(rejected => rejected),
            "Every attempt on the strict engine must be rejected, whatever the other engine was doing");
        Assert.IsTrue(permissiveResults.All(ok => ok),
            "Every attempt on the permissive engine must succeed, whatever the other engine was doing");
    }

    /// <summary>
    /// A second knob, to show the first result is not specific to one code path: the per-table column
    /// ceiling is enforced by whichever engine the request went through.
    /// </summary>
    [Test]
    public async Task TwoEnginesEnforceTheirOwnColumnCeiling()
    {
        (string dbname, _, _) = await CreateDatabase();

        CommandExecutor strict     = EngineWith(o => o with { MaxColumnsPerTable = 1 });
        CommandExecutor permissive = EngineWith(o => o with { MaxColumnsPerTable = 64 });

        CamusDBException rejected = Assert.ThrowsAsync<CamusDBException>(
            async () => await strict.CreateTable(TableTicket(dbname, "cols_strict")))!;
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, rejected.Code,
            "The strict engine must enforce its own single-column ceiling");

        Assert.IsTrue((await permissive.CreateTable(TableTicket(dbname, "cols_permissive")).ConfigureAwait(false)).Success,
            "The permissive engine must accept the same two-column table");
    }

    /// <summary>
    /// Configuration is fixed when an engine is built. This pins the property that made so many tests
    /// need reordering during the migration, so the behaviour is now asserted rather than folklore.
    /// </summary>
    [Test]
    public async Task AnEnginesConfigurationDoesNotChangeAfterItIsBuilt()
    {
        (string dbname, _, _) = await CreateDatabase();

        CommandExecutor engine = EngineWith(o => o with { MaxIdentifierLength = 8 });

        // Replacing the process-wide value — the one a freshly started host would install — must not
        // reach an engine that already exists.
        CamusDBOptions saved = CamusDBConfig.Ambient;
        try
        {
            CamusDBConfig.SetAmbient(saved with { MaxIdentifierLength = 512 });

            CamusDBException stillRejected = Assert.ThrowsAsync<CamusDBException>(
                async () => await engine.CreateTable(TableTicket(dbname, "still_too_long_for_this_engine")))!;
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, stillRejected.Code,
                "An engine keeps the configuration it was constructed with");
        }
        finally
        {
            CamusDBConfig.SetAmbient(saved);
        }
    }
}
