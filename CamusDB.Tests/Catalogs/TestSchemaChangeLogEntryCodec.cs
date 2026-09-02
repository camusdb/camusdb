/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.Serializer;

using Kommander.Time;

using NUnit.Framework;

namespace CamusDB.Tests.Catalogs;

[TestFixture]
public sealed class TestSchemaChangeLogEntryCodec
{
    /// <summary>
    /// One representative entry per <see cref="SchemaOp"/>, each carrying the payload type its op
    /// really uses. Round-tripping all of them proves two things at once: the frame preserves the
    /// entry, and every payload type is registered for source-generated serialization — an
    /// unregistered one throws instead of silently falling back to reflection.
    /// </summary>
    private static IEnumerable<TestCaseData> EveryOp()
    {
        foreach ((SchemaOp op, object payload) in Payloads())
            yield return new TestCaseData(op, payload).SetName($"RoundTrip_{op}");
    }

    private static IEnumerable<(SchemaOp, object)> Payloads()
    {
        yield return (SchemaOp.CreateTable, CreateTablePayload());
        yield return (SchemaOp.DropTable, new SchemaDropTablePayload { TableName = "robots", Deferred = true });
        yield return (SchemaOp.AddColumn, AlterColumnPayload());
        yield return (SchemaOp.DropColumn, AlterColumnPayload());
        yield return (SchemaOp.AddIndex, IndexPayload());
        yield return (SchemaOp.DropIndex, IndexPayload());
        yield return (SchemaOp.SetElementState, new SchemaElementStatePayload
        {
            TableName = "robots",
            ElementName = "name",
            ElementKind = SchemaElementKind.Column,
            State = SchemaElementState.Public
        });
        yield return (SchemaOp.RenameTable, RenamePayload(SchemaRenameKind.Table));
        yield return (SchemaOp.RenameColumn, RenamePayload(SchemaRenameKind.Column));
        yield return (SchemaOp.RenameIndex, RenamePayload(SchemaRenameKind.Index));
        yield return (SchemaOp.AddCheckConstraint, new SchemaCheckConstraintPayload
        {
            TableName = "robots",
            ConstraintName = "robots_age_positive",
            Expression = "age > 0",
            ReferencedColumns = ["age"]
        });
        yield return (SchemaOp.DropCheckConstraint, new SchemaCheckConstraintPayload
        {
            TableName = "robots",
            ConstraintName = "robots_age_positive"
        });
        yield return (SchemaOp.SetColumnNotNull, new SchemaSetColumnNotNullPayload
        {
            TableName = "robots",
            ColumnName = "name",
            NotNull = true,
            ConstraintName = "robots_name_not_null"
        });
        yield return (SchemaOp.RelinkTable, new SchemaRelinkTablePayload
        {
            TableId = "A0",
            TableName = "robots_recovered",
            Version = 3,
            Kind = RelationKind.Table,
            Columns = [ColumnPayload("id", ColumnType.Id)]
        });
        yield return (SchemaOp.SetTableSettings, new SchemaSetTableSettingsPayload
        {
            TableName = "robots",
            Settings = new Dictionary<string, string> { ["ttl_column"] = "created_at" },
            RemovedKeys = ["ttl_interval"]
        });
        yield return (SchemaOp.SetComment, new SchemaSetCommentPayload
        {
            TableName = "robots",
            Target = CommentTarget.Column,
            ElementName = "name",
            Comment = "the robot's name"
        });
        yield return (SchemaOp.CreateView, ViewPayload());
        yield return (SchemaOp.ReplaceView, ViewPayload());
        yield return (SchemaOp.DropView, new SchemaDropViewPayload { ViewName = "active_robots" });
        yield return (SchemaOp.RenameView, RenamePayload(SchemaRenameKind.Table));
        yield return (SchemaOp.SetViewDefinition, new SchemaSetViewDefinitionPayload
        {
            ViewName = "active_robots",
            Definition = new ViewDefinition { Sql = "SELECT * FROM machines" }
        });
        yield return (SchemaOp.SetMaterializedViewState, new SchemaSetMatViewStatePayload
        {
            TableId = "A0",
            IsPopulated = true,
            RefreshedAt = new HLCTimestamp(1, 500, 7),
            SwapToTableId = "A1",
            ExpectedMetadataGeneration = 4
        });
        yield return (SchemaOp.TruncateTable, new SchemaTruncateTablePayload
        {
            TableId = "A0",
            TableName = "robots",
            ExpectedStorageId = "A0",
            ExpectedContentsGeneration = 1
        });
    }

    [Test]
    public void Payloads_CoverEverySchemaOp()
    {
        HashSet<SchemaOp> covered = [.. Payloads().Select(entry => entry.Item1)];

        Assert.That(covered, Is.EquivalentTo(Enum.GetValues<SchemaOp>()));
    }

    [TestCaseSource(nameof(EveryOp))]
    public void Encode_RoundTripsEveryOpThroughTheFramedFormat(SchemaOp op, object payload)
    {
        SchemaChangeLogEntry entry = Entry(op, EncodePayload(payload));

        SchemaChangeLogEntry roundTrip = SchemaChangeLogEntryCodec.Decode(SchemaChangeLogEntryCodec.Encode(entry));

        AssertSameEntry(entry, roundTrip);
        Assert.AreEqual(SchemaPayloadFormat.Utf8, roundTrip.PayloadFormat);
        AssertPayloadDecodes(roundTrip, payload);
    }

    [TestCaseSource(nameof(EveryOp))]
    public void Decode_ReadsPreFramingBytesForEveryOp(SchemaOp op, object payload)
    {
        // Built exactly as this engine wrote entries before the frame existed: the entry as UTF-16
        // JSON, with a payload that is itself UTF-16 JSON. Both are still sitting in Raft logs.
        SchemaChangeLogEntry entry = Entry(op, Serializator.Serialize(payload));
        entry.PayloadFormat = SchemaPayloadFormat.Utf16Legacy;

        SchemaChangeLogEntry roundTrip = SchemaChangeLogEntryCodec.Decode(Serializator.Serialize(entry));

        AssertSameEntry(entry, roundTrip);
        Assert.AreEqual(SchemaPayloadFormat.Utf16Legacy, roundTrip.PayloadFormat);
        AssertPayloadDecodes(roundTrip, payload);
    }

    [Test]
    public void Encode_ProducesSmallerBytesThanThePreFramingFormat()
    {
        SchemaChangeLogEntry framed = Entry(SchemaOp.CreateTable, EncodePayload(CreateTablePayload()));
        SchemaChangeLogEntry legacy = Entry(SchemaOp.CreateTable, Serializator.Serialize(CreateTablePayload()));

        Assert.Less(SchemaChangeLogEntryCodec.Encode(framed).Length, Serializator.Serialize(legacy).Length);
    }

    [Test]
    public void TryReadHeader_ReadsDatabaseIdAndVersionsFromAFramedEntry()
    {
        SchemaChangeLogEntry entry = Entry(SchemaOp.DropTable, EncodePayload(new SchemaDropTablePayload { TableName = "robots" }));
        entry.Database = "some_database_id";
        entry.FromVersion = 41;
        entry.ToVersion = 42;

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(entry);

        Assert.True(SchemaChangeLogEntryCodec.TryReadHeader(bytes, out SchemaEntryHeader header));
        Assert.AreEqual("some_database_id", System.Text.Encoding.UTF8.GetString(header.DatabaseId));
        Assert.AreEqual(41, header.FromVersion);
        Assert.AreEqual(42, header.ToVersion);
    }

    [Test]
    public void TryReadHeader_RejectsPreFramingBytes()
    {
        byte[] legacy = Serializator.Serialize(Entry(SchemaOp.DropTable, []));

        Assert.False(SchemaChangeLogEntryCodec.TryReadHeader(legacy, out _));
    }

    [Test]
    public void TryReadHeader_RejectsATruncatedHeader()
    {
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(Entry(SchemaOp.DropTable, []));

        // Cut inside the header the frame announces: the database id and the two versions no longer
        // fit, so there is nothing to read even though the magic byte is intact.
        Assert.False(SchemaChangeLogEntryCodec.TryReadHeader(bytes.AsSpan(0, 6), out _));
        Assert.False(SchemaChangeLogEntryCodec.TryReadHeader([], out _));
    }

    [Test]
    public void TryReadHeader_AllocatesNothing()
    {
        byte[] framed = SchemaChangeLogEntryCodec.Encode(Entry(SchemaOp.DropTable, []));
        byte[] legacy = Serializator.Serialize(Entry(SchemaOp.DropTable, []));
        byte[] truncated = framed.AsSpan(0, 6).ToArray();

        // Warm up so the measurement below covers the reads and not the one-time JIT of this method.
        ReadHeaders(framed, legacy, truncated);

        long before = GC.GetAllocatedBytesForCurrentThread();
        ReadHeaders(framed, legacy, truncated);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.AreEqual(0, allocated, "reading an entry's frame must not allocate");
    }

    private static void ReadHeaders(byte[] framed, byte[] legacy, byte[] truncated)
    {
        for (int i = 0; i < 1_000; i++)
        {
            SchemaChangeLogEntryCodec.TryReadHeader(framed, out _);
            SchemaChangeLogEntryCodec.TryReadHeader(legacy, out _);
            SchemaChangeLogEntryCodec.TryReadHeader(truncated, out _);
        }
    }

    [Test]
    public void Decode_RejectsBytesThatAreNeitherFramedNorJson()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => SchemaChangeLogEntryCodec.Decode([0x42, 0x00, 0x00]));

        Assert.NotNull(ex);
        Assert.That(ex!.Message, Does.Contain("0x42"));
    }

    [Test]
    public void Decode_RejectsEmptyBytes()
    {
        Assert.Throws<CamusDBException>(() => SchemaChangeLogEntryCodec.Decode([]));
    }

    [Test]
    public void Decode_RejectsAFramedEntryWhoseBodyIsTruncated()
    {
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(Entry(SchemaOp.CreateTable, EncodePayload(CreateTablePayload())));

        // Keep the header intact and cut the body in half: the frame still says "framed entry", so
        // the failure has to surface from the body parse rather than be mistaken for another format.
        byte[] truncated = bytes.AsSpan(0, bytes.Length / 2).ToArray();

        Assert.Throws<CamusDBException>(() => SchemaChangeLogEntryCodec.Decode(truncated));
    }

    [Test]
    public void Decode_RejectsAFramedEntryTruncatedInsideItsHeader()
    {
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(Entry(SchemaOp.DropTable, []));

        Assert.Throws<CamusDBException>(() => SchemaChangeLogEntryCodec.Decode(bytes.AsSpan(0, 6).ToArray()));
    }

    [Test]
    public void Encode_RejectsADatabaseIdTooLongForTheFrame()
    {
        SchemaChangeLogEntry entry = Entry(SchemaOp.DropTable, []);
        entry.Database = new string('d', 256);

        Assert.Throws<CamusDBException>(() => SchemaChangeLogEntryCodec.Encode(entry));
    }

    [Test]
    public void Encode_CarriesADatabaseIdOfExactlyTheMaximumFrameLength()
    {
        SchemaChangeLogEntry entry = Entry(SchemaOp.DropTable, []);
        entry.Database = new string('d', 255);

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(entry);

        Assert.True(SchemaChangeLogEntryCodec.TryReadHeader(bytes, out SchemaEntryHeader header));
        Assert.AreEqual(255, header.DatabaseId.Length);
        Assert.AreEqual(entry.Database, SchemaChangeLogEntryCodec.Decode(bytes).Database);
    }

    [Test]
    public void Fingerprint_SeparatesTwoDeltasThatClaimTheSameVersion()
    {
        // The pair the digest exists for: same database, same from/to versions, different change.
        byte[] first = SchemaChangeLogEntryCodec.Encode(
            Entry(SchemaOp.CreateTable, EncodePayload(new SchemaCreateTablePayload { TableName = "robots_a" })));
        byte[] second = SchemaChangeLogEntryCodec.Encode(
            Entry(SchemaOp.CreateTable, EncodePayload(new SchemaCreateTablePayload { TableName = "robots_b" })));

        Assert.AreEqual(SchemaChangeLogEntryCodec.Fingerprint(first), SchemaChangeLogEntryCodec.Fingerprint(first));
        Assert.AreNotEqual(SchemaChangeLogEntryCodec.Fingerprint(first), SchemaChangeLogEntryCodec.Fingerprint(second));
    }

    private static void AssertSameEntry(SchemaChangeLogEntry expected, SchemaChangeLogEntry actual)
    {
        Assert.AreEqual(expected.Ts, actual.Ts);
        Assert.AreEqual(expected.Database, actual.Database);
        Assert.AreEqual(expected.FromVersion, actual.FromVersion);
        Assert.AreEqual(expected.ToVersion, actual.ToVersion);
        Assert.AreEqual(expected.Op, actual.Op);
        Assert.AreEqual(expected.Payload, actual.Payload);
    }

    /// <summary>
    /// Re-decodes the payload out of the round-tripped entry and compares it to the object the test
    /// built, field by field, through the JSON the engine itself would write. Comparing the decoded
    /// object rather than the raw bytes is what makes this a test of the payload format instead of
    /// a second copy of the byte-equality assertion above.
    /// </summary>
    private static void AssertPayloadDecodes(SchemaChangeLogEntry entry, object expected)
    {
        object decoded = typeof(SchemaChangeLogEntry)
            .GetMethod(nameof(SchemaChangeLogEntry.GetPayload))!
            .MakeGenericMethod(expected.GetType())
            .Invoke(entry, null)!;

        Assert.AreEqual(
            System.Text.Json.JsonSerializer.Serialize(expected),
            System.Text.Json.JsonSerializer.Serialize(decoded)
        );
    }

    private static byte[] EncodePayload(object payload)
    {
        return (byte[])typeof(SchemaChangeLogEntryCodec)
            .GetMethod(nameof(SchemaChangeLogEntryCodec.EncodePayload), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(payload.GetType())
            .Invoke(null, [payload])!;
    }

    private static SchemaChangeLogEntry Entry(SchemaOp op, byte[] payload) => new()
    {
        Ts = new HLCTimestamp(1, 10, 2),
        Database = "db",
        FromVersion = 1,
        ToVersion = 2,
        Op = op,
        Payload = payload
    };

    private static SchemaCreateTablePayload CreateTablePayload() => new()
    {
        TableId = "A0",
        TableName = "robots",
        Columns = [ColumnPayload("id", ColumnType.Id), ColumnPayload("name", ColumnType.String)],
        Comment = "robot inventory",
        Settings = new Dictionary<string, string> { ["ttl_column"] = "created_at" }
    };

    private static SchemaColumnPayload ColumnPayload(string name, ColumnType type) => new()
    {
        Id = "000000000000000000000101",
        Name = name,
        Type = type,
        NotNull = type == ColumnType.Id
    };

    private static SchemaAlterColumnPayload AlterColumnPayload() => new()
    {
        TableName = "robots",
        Column = ColumnPayload("name", ColumnType.String)
    };

    private static SchemaIndexPayload IndexPayload() => new()
    {
        TableName = "robots",
        IndexName = "name_idx"
    };

    private static SchemaRenamePayload RenamePayload(SchemaRenameKind kind) => new()
    {
        TableName = "robots",
        Kind = kind,
        ElementName = kind == SchemaRenameKind.Table ? null : "name",
        NewName = "machines"
    };

    private static SchemaViewPayload ViewPayload() => new()
    {
        ViewId = "A1",
        ViewName = "active_robots",
        Definition = new ViewDefinition { Sql = "SELECT * FROM robots" },
        Comment = "only the live ones"
    };
}
