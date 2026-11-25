
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// A table schema shows its layout version and the columns of that version as one pair. A statement
/// reads the pair without the schema lock and encodes a row with the codec of the version from values
/// laid out by the columns, so a version read with the columns of another layout fails the statement.
/// </summary>
[TestFixture]
public sealed class TestTableSchemaLayout
{
    private const string Table = "robots";

    private const int BaseColumns = 2;

    private static Schema SchemaWithTable()
    {
        List<TableColumnSchema> columns =
        [
            new("id-col", "id", ColumnType.Id, true, null),
            new("name-col", "name", ColumnType.String, false, null),
        ];

        Schema schema = new();
        schema.Tables[Table] = new()
        {
            Id = "1",
            Name = Table,
            Version = 0,
            Columns = columns,
            SchemaHistory = [new() { Version = 0, Columns = columns }]
        };

        return schema;
    }

    private static SchemaAlterColumnPayload Column(string name)
        => new() { TableName = Table, Column = new() { Name = name, Type = ColumnType.Integer64 } };

    [Test]
    public void AnAddedColumnMovesTheVersionAndTheColumnsTogether()
    {
        Schema schema = SchemaWithTable();
        TableSchema table = schema.Tables[Table];

        ColumnDeltaApplier.ApplyAlterColumn(schema, Column("age"), SchemaOp.AddColumn);

        (int version, List<TableColumnSchema>? columns) = table.ReadLayout();

        Assert.That(version, Is.EqualTo(1));
        Assert.That(columns, Has.Count.EqualTo(BaseColumns + 1));
        Assert.That(table.Version, Is.EqualTo(1));
        Assert.That(table.Columns, Is.SameAs(columns));
        Assert.That(table.GetSchemaHistory(1).Columns, Is.SameAs(columns));
        Assert.That(table.GetSchemaHistory(0).Columns, Has.Count.EqualTo(BaseColumns));
    }

    [Test]
    public void ARefusedColumnChangeLeavesTheLayoutAsItWas()
    {
        Schema schema = SchemaWithTable();
        TableSchema table = schema.Tables[Table];
        (int version, List<TableColumnSchema>? columns) = table.ReadLayout();

        CamusDBException duplicate = Assert.Throws<CamusDBException>(
            () => ColumnDeltaApplier.ApplyAlterColumn(schema, Column("name"), SchemaOp.AddColumn))!;
        Assert.That(duplicate.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateColumn));

        CamusDBException unknown = Assert.Throws<CamusDBException>(
            () => ColumnDeltaApplier.ApplyAlterColumn(schema, Column("missing"), SchemaOp.DropColumn))!;
        Assert.That(unknown.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));

        (int versionAfter, List<TableColumnSchema>? columnsAfter) = table.ReadLayout();

        Assert.That(versionAfter, Is.EqualTo(version));
        Assert.That(columnsAfter, Is.SameAs(columns));
        Assert.That(table.SchemaHistory, Has.Count.EqualTo(1));
    }

    [Test]
    public void SettingOnePropertyKeepsTheOther()
    {
        TableSchema table = SchemaWithTable().Tables[Table];
        List<TableColumnSchema> replaced = [.. table.Columns!];

        table.Columns = replaced;
        Assert.That(table.Version, Is.EqualTo(0));

        table.Version = 7;
        Assert.That(table.Columns, Is.SameAs(replaced));
        Assert.That(table.ReadLayout(), Is.EqualTo((7, replaced)));
    }

    /// <summary>
    /// One thread adds and drops a column in turn, so an even version has the base columns and an odd
    /// version has one more. Readers that take the pair without a lock must never see a version with
    /// the column count of its neighbor.
    /// </summary>
    [Test]
    public async Task ReadersNeverSeeAVersionWithTheColumnsOfAnotherLayout()
    {
        Schema schema = SchemaWithTable();
        TableSchema table = schema.Tables[Table];

        const int changes = 20_000;
        long mixed = 0;
        long reads = 0;
        using CancellationTokenSource stop = new();

        Task[] readers = new Task[4];
        for (int r = 0; r < readers.Length; r++)
        {
            readers[r] = Task.Run(() =>
            {
                long seen = 0;

                while (!stop.IsCancellationRequested)
                {
                    (int version, List<TableColumnSchema>? columns) = table.ReadLayout();

                    if (columns!.Count != BaseColumns + (version & 1))
                        Interlocked.Increment(ref mixed);

                    seen++;
                }

                Interlocked.Add(ref reads, seen);
            });
        }

        for (int i = 0; i < changes; i++)
        {
            ColumnDeltaApplier.ApplyAlterColumn(schema, Column("age"), (i & 1) == 0 ? SchemaOp.AddColumn : SchemaOp.DropColumn);

            // The history list grows by one entry a change and no reader of this test uses it.
            table.SchemaHistory = [];
        }

        await stop.CancelAsync();
        await Task.WhenAll(readers);

        Assert.That(table.Version, Is.EqualTo(changes));
        Assert.That(Interlocked.Read(ref reads), Is.GreaterThan(0));
        Assert.That(Interlocked.Read(ref mixed), Is.EqualTo(0), "a reader saw a version with the columns of another layout");
    }

    [Test]
    public void AnAddedLayoutLeavesTheListAReaderHoldsAsItWas()
    {
        Schema schema = SchemaWithTable();
        TableSchema table = schema.Tables[Table];
        List<TableSchemaHistory> held = table.SchemaHistory!;

        ColumnDeltaApplier.ApplyAlterColumn(schema, Column("age"), SchemaOp.AddColumn);

        Assert.That(held, Has.Count.EqualTo(1));
        Assert.That(table.SchemaHistory, Is.Not.SameAs(held));
        Assert.That(table.SchemaHistory, Has.Count.EqualTo(2));
        Assert.That(table.SchemaHistory![0], Is.SameAs(held[0]));
        Assert.That(table.SchemaHistory[1].Version, Is.EqualTo(1));
    }

    /// <summary>
    /// One thread adds and drops a column in turn while readers look up the layout before the one they
    /// read. The lookup walks the history list without a lock, so the list a reader walks must not change.
    /// </summary>
    [Test]
    public async Task ReadersWalkTheHistoryWhileLayoutsAreAdded()
    {
        Schema schema = SchemaWithTable();
        TableSchema table = schema.Tables[Table];

        const int changes = 4_000;
        long wrong = 0;
        long lookups = 0;
        using CancellationTokenSource stop = new();

        Task[] readers = new Task[4];
        for (int r = 0; r < readers.Length; r++)
        {
            readers[r] = Task.Run(() =>
            {
                long done = 0;

                while (!stop.IsCancellationRequested)
                {
                    (int version, _) = table.ReadLayout();
                    if (version == 0)
                        continue;

                    TableSchemaHistory previous = table.GetSchemaHistory(version - 1);

                    if (previous.Version != version - 1 || previous.Columns!.Count != BaseColumns + ((version - 1) & 1))
                        Interlocked.Increment(ref wrong);

                    done++;
                }

                Interlocked.Add(ref lookups, done);
            });
        }

        for (int i = 0; i < changes; i++)
            ColumnDeltaApplier.ApplyAlterColumn(schema, Column("age"), (i & 1) == 0 ? SchemaOp.AddColumn : SchemaOp.DropColumn);

        await stop.CancelAsync();
        await Task.WhenAll(readers);

        Assert.That(table.SchemaHistory, Has.Count.EqualTo(changes + 1));
        Assert.That(Interlocked.Read(ref lookups), Is.GreaterThan(0));
        Assert.That(Interlocked.Read(ref wrong), Is.EqualTo(0), "a lookup returned the layout of another version");
    }

    /// <summary>
    /// A table loaded from storage holds no layouts and fetches a past one on demand. A schema change
    /// that adds its layout while that fetch runs must still be in the list after the fetch publishes.
    /// </summary>
    [Test]
    public async Task ALayoutAddedDuringALazyLoadIsKept([Values] bool startsWithAList)
    {
        Schema schema = SchemaWithTable();
        TableSchema table = schema.Tables[Table];
        List<TableColumnSchema> stored = [.. table.Columns!];

        table.PublishLayout(2, table.Columns);
        table.SchemaHistory = startsWithAList ? [new() { Version = 2, Columns = table.Columns }] : null;

        int loads = 0;
        table.SchemaHistoryLoader = (_, version) =>
        {
            loads++;
            ColumnDeltaApplier.ApplyAlterColumn(schema, Column("age"), SchemaOp.AddColumn);
            return new(new TableSchemaHistory { Version = version, Columns = stored });
        };

        TableSchemaHistory loaded = await table.GetSchemaHistoryAsync(default, 0);

        Assert.That(loaded.Version, Is.EqualTo(0));
        Assert.That(table.Version, Is.EqualTo(3));

        List<int> versions = table.SchemaHistory!.ConvertAll(history => history.Version);
        Assert.That(versions, Is.EqualTo(startsWithAList ? new[] { 0, 2, 3 } : new[] { 0, 3 }));

        Assert.That(await table.GetSchemaHistoryAsync(default, 0), Is.SameAs(loaded));
        Assert.That(loads, Is.EqualTo(1));
    }
}
