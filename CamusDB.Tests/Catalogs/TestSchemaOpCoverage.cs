
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Replication;
using Kommander.Time;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// Guards the schema-delta switches against a newly added <see cref="SchemaOp"/> that nobody wires
/// up. The applier is split across seven classes, and the dispatch, the validator and the
/// idempotency predicate each switch on the operation independently. Adding an operation to one and
/// forgetting the others is the easy mistake, and only the dispatch fails loudly when it happens.
///
/// <para>These tests deliberately do not assert on payload validity. A minimal payload makes most
/// arms throw something — that is fine and expected. The only outcome under test is whether the
/// operation reached an arm at all.</para>
/// </summary>
[TestFixture]
public sealed class TestSchemaOpCoverage
{
    private static SchemaChangeLogEntry Entry(SchemaOp op, byte[]? payload = null)
    {
        return new()
        {
            Ts = new HLCTimestamp(1, 10, 2),
            Database = "db",
            FromVersion = 1,
            ToVersion = 2,
            Op = op,
            Payload = payload ?? []
        };
    }

    /// <summary>
    /// Every declared operation must reach a dispatch arm. The default arm throws
    /// "Unknown schema operation", so an unwired operation is detectable here — and only here,
    /// because the other two switches fall through silently instead of throwing.
    /// </summary>
    [Test]
    public void EverySchemaOpReachesADispatchArm()
    {
        List<SchemaOp> unhandled = [];

        foreach (SchemaOp op in Enum.GetValues<SchemaOp>())
        {
            Schema schema = new();

            try
            {
                SchemaDeltaApplier.ApplySchemaDelta(schema, Entry(op));
            }
            catch (CamusDBException ex) when (ex.Message.Contains("Unknown schema operation", StringComparison.Ordinal))
            {
                unhandled.Add(op);
            }
            catch (Exception)
            {
                // Reached an arm and failed on the empty payload. That is the pass condition.
            }
        }

        Assert.IsEmpty(
            unhandled,
            $"these SchemaOp values reach no dispatch arm: {string.Join(", ", unhandled)}"
        );
    }

    /// <summary>
    /// The idempotency predicate must answer TRUNCATE from the storage id, never from the version
    /// counter. A truncate leaves <c>TableSchema.Version</c> alone, so a version comparison would
    /// call any later unrelated DDL "the same truncate already landed" and the proposer's wait would
    /// return on the wrong evidence.
    /// </summary>
    [Test]
    public void TruncateIdempotencyIsAnsweredFromTheStorageIdNotTheVersion()
    {
        Schema schema = new();
        TableSchema table = new() { Id = "T1", Name = "robots", Version = 7, StorageId = "S1" };
        schema.Tables["robots"] = table;
        schema.SchemaVersion = 99;

        SchemaTruncateTablePayload payload = new()
        {
            TableId = "T1",
            ExpectedStorageId = "S1",
            NewStorageId = "S2"
        };

        SchemaChangeLogEntry entry = Entry(SchemaOp.TruncateTable, SchemaChangeLogEntryCodec.EncodePayload(payload));

        Assert.IsFalse(
            SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry),
            "the storage id has not moved, so this truncate has NOT been applied — the high schema " +
            "version must not be mistaken for evidence that it has"
        );

        table.StorageId = "S2";

        Assert.IsTrue(
            SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry),
            "the storage id now matches the payload, so the truncate has landed"
        );
    }

    /// <summary>
    /// View operations must be answered from the view map. Resolving a view through the table map
    /// finds nothing and reports "not applied" forever, which stalls the proposer's wait rather than
    /// failing it.
    /// </summary>
    [Test]
    public void ViewOperationsAreAnsweredFromTheViewMap()
    {
        Schema schema = new();
        schema.SchemaVersion = 1;

        SchemaViewPayload create = new() { ViewName = "v_robots", ViewId = "V1" };
        SchemaChangeLogEntry createEntry = Entry(SchemaOp.CreateView, SchemaChangeLogEntryCodec.EncodePayload(create));

        Assert.IsFalse(
            SchemaDeltaApplier.WasSchemaDeltaApplied(schema, createEntry),
            "the view is absent, so CreateView has not been applied"
        );

        schema.Views["v_robots"] = new ViewSchema { Id = "V1", Name = "v_robots" };

        Assert.IsTrue(
            SchemaDeltaApplier.WasSchemaDeltaApplied(schema, createEntry),
            "the view is present in Schema.Views, so CreateView has been applied"
        );

        SchemaDropViewPayload drop = new() { ViewName = "v_robots" };
        SchemaChangeLogEntry dropEntry = Entry(SchemaOp.DropView, SchemaChangeLogEntryCodec.EncodePayload(drop));

        Assert.IsFalse(
            SchemaDeltaApplier.WasSchemaDeltaApplied(schema, dropEntry),
            "the view is still present, so DropView has not been applied"
        );
    }

    /// <summary>
    /// Records which operations the idempotency predicate answers structurally rather than by
    /// falling back to the version comparison. This is documentation with teeth: if a later change
    /// drops an operation out of the structural set, this test says so, and the reader can see at a
    /// glance which operations rely on the weaker fallback.
    /// </summary>
    [Test]
    public void StructurallyCheckedOperationsAreTheExpectedSet()
    {
        HashSet<SchemaOp> expected =
        [
            SchemaOp.CreateTable, SchemaOp.RelinkTable, SchemaOp.DropTable,
            SchemaOp.AddColumn, SchemaOp.DropColumn, SchemaOp.SetElementState,
            SchemaOp.AddIndex, SchemaOp.DropIndex,
            SchemaOp.RenameTable, SchemaOp.RenameColumn, SchemaOp.RenameIndex,
            SchemaOp.CreateView, SchemaOp.ReplaceView, SchemaOp.DropView, SchemaOp.RenameView,
            SchemaOp.TruncateTable
        ];

        // A structurally checked op answers "not applied" against an empty schema even when the
        // schema version is far ahead. An op that only falls back to the version comparison answers
        // "applied" in that same situation.
        List<SchemaOp> structural = [];

        foreach (SchemaOp op in Enum.GetValues<SchemaOp>())
        {
            Schema schema = new() { SchemaVersion = 999 };

            try
            {
                if (!SchemaDeltaApplier.WasSchemaDeltaApplied(schema, Entry(op)))
                    structural.Add(op);
            }
            catch (Exception)
            {
                // Decoding an empty payload threw, which means the arm read the payload — that is
                // itself proof the op is checked structurally rather than by the version fallback.
                structural.Add(op);
            }
        }

        Assert.AreEqual(
            expected.OrderBy(x => x).ToArray(),
            structural.OrderBy(x => x).ToArray(),
            "the set of structurally checked schema operations changed; a new operation that only " +
            "falls back to the version comparison cannot be detected as applied or not"
        );
    }
}
