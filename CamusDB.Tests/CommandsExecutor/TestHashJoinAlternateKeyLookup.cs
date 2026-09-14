/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Differential tests for the hash-join key comparer's span alternate lookup.
/// The join hash tables now probe with a <see cref="ReadOnlySpan{T}"/> over a reused scratch
/// array instead of an owned <see cref="CompositeColumnValue"/> per row. These tests assert
/// that the span form and the object form agree on hit and miss for every eligible key type,
/// that a key inserted through the alternate lookup is an owned copy (scratch reuse cannot
/// corrupt it), and that type mismatches never match in either form.
/// </summary>
internal sealed class TestHashJoinAlternateKeyLookup
{
    private static CompositeColumnValueComparer Comparer =>
        CompositeColumnValueComparer.Instance;

    /// <summary>
    /// Asserts that the object lookup and the span lookup return the same hit/miss result and,
    /// on a hit, the same stored value. Returns the shared hit/miss verdict.
    /// </summary>
    private static bool AssertLookupParity(
        Dictionary<CompositeColumnValue, int> table,
        ColumnValue[] probeKey)
    {
        bool objectHit = table.TryGetValue(new CompositeColumnValue(probeKey), out int objectValue);

        Dictionary<CompositeColumnValue, int>.AlternateLookup<ReadOnlySpan<ColumnValue>> lookup =
            table.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        bool spanHit = lookup.TryGetValue(probeKey.AsSpan(), out int spanValue);

        Assert.That(spanHit, Is.EqualTo(objectHit),
            "Span lookup and object lookup disagree on hit/miss for the same key values");

        if (objectHit)
            Assert.That(spanValue, Is.EqualTo(objectValue),
                "Span lookup and object lookup found different buckets for the same key values");

        return objectHit;
    }

    private static Dictionary<CompositeColumnValue, int> BuildTable(params ColumnValue[][] keys)
    {
        Dictionary<CompositeColumnValue, int> table = new(Comparer);
        for (int i = 0; i < keys.Length; i++)
            table[new CompositeColumnValue(keys[i])] = i;
        return table;
    }

    [Test]
    public void SpanAndObjectLookup_AgreeForEveryEligibleKeyType()
    {
        // One (stored key, equal probe with distinct instances, unequal probe) triple per type.
        (ColumnValue Stored, ColumnValue EqualProbe, ColumnValue UnequalProbe)[] cases =
        [
            (new ColumnValue(ColumnType.String, "abc"),
             new ColumnValue(ColumnType.String, "abc"),
             new ColumnValue(ColumnType.String, "abd")),

            (new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d3"),
             new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d3"),
             new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d4")),

            (new ColumnValue(ColumnType.Integer64, 42L),
             new ColumnValue(ColumnType.Integer64, 42L),
             new ColumnValue(ColumnType.Integer64, -42L)),

            (new ColumnValue(ColumnType.Bool, true),
             new ColumnValue(ColumnType.Bool, true),
             new ColumnValue(ColumnType.Bool, false)),

            (new ColumnValue(ColumnType.Float64, 1.5d),
             new ColumnValue(ColumnType.Float64, 1.5d),
             new ColumnValue(ColumnType.Float64, 2.5d)),

            (new ColumnValue(ColumnType.Float32, 1.25d),
             new ColumnValue(ColumnType.Float32, 1.25d),
             new ColumnValue(ColumnType.Float32, 1.75d)),

            (new ColumnValue(ColumnType.Date, 20260913L),
             new ColumnValue(ColumnType.Date, 20260913L),
             new ColumnValue(ColumnType.Date, 20260914L)),

            (new ColumnValue(ColumnType.DateTime, 638600000000000000L),
             new ColumnValue(ColumnType.DateTime, 638600000000000000L),
             new ColumnValue(ColumnType.DateTime, 638600000000000001L)),

            // Equal byte content in DISTINCT arrays must hit; a one-byte difference must miss.
            (new ColumnValue([1, 2, 3]),
             new ColumnValue([1, 2, 3]),
             new ColumnValue([1, 2, 4])),

            // UUIDs that differ only in the high half must miss: the hash and the equality
            // must both consume both halves.
            (new ColumnValue(ColumnType.Uuid, uuidHigh: 111L, uuidLow: 999L),
             new ColumnValue(ColumnType.Uuid, uuidHigh: 111L, uuidLow: 999L),
             new ColumnValue(ColumnType.Uuid, uuidHigh: 222L, uuidLow: 999L)),
        ];

        foreach ((ColumnValue stored, ColumnValue equalProbe, ColumnValue unequalProbe) in cases)
        {
            Dictionary<CompositeColumnValue, int> table = BuildTable([stored]);

            Assert.That(AssertLookupParity(table, [equalProbe]), Is.True,
                $"Equal probe missed for {stored.Type}");
            Assert.That(AssertLookupParity(table, [unequalProbe]), Is.False,
                $"Unequal probe hit for {stored.Type}");
        }
    }

    [Test]
    public void TypeMismatch_NeverMatches_InEitherLookupForm()
    {
        Dictionary<CompositeColumnValue, int> table =
            BuildTable([new ColumnValue(ColumnType.Integer64, 42L)]);

        ColumnValue[][] mismatchedProbes =
        [
            [new ColumnValue(ColumnType.String, "42")],
            [new ColumnValue(ColumnType.Float64, 42d)],
            [new ColumnValue(ColumnType.Date, 42L)],
            [new ColumnValue(ColumnType.Bool, true)],
        ];

        foreach (ColumnValue[] probe in mismatchedProbes)
            Assert.That(AssertLookupParity(table, probe), Is.False,
                $"A {probe[0].Type} probe matched an Integer64 key");
    }

    [Test]
    public void FloatEdgeCases_SpanAndObjectLookupAgree()
    {
        // Semantics come from ColumnValue.CompareTo (NaN equals NaN; -0.0 orders below 0.0,
        // so they are distinct keys). The assertion here is parity: whatever the object form
        // decides, the span form must decide identically.
        ColumnValue[] edgeValues =
        [
            new ColumnValue(ColumnType.Float64, double.NaN),
            new ColumnValue(ColumnType.Float64, double.PositiveInfinity),
            new ColumnValue(ColumnType.Float64, double.NegativeInfinity),
            new ColumnValue(ColumnType.Float64, 0.0d),
            new ColumnValue(ColumnType.Float64, -0.0d),
        ];

        foreach (ColumnValue stored in edgeValues)
        {
            Dictionary<CompositeColumnValue, int> table = BuildTable([stored]);

            foreach (ColumnValue probeValue in edgeValues)
            {
                // A fresh instance with the same payload, so reference identity cannot help.
                ColumnValue probe = new(ColumnType.Float64, probeValue.FloatValue);
                bool hit = AssertLookupParity(table, [probe]);

                bool expected = Comparer.Equals(
                    new CompositeColumnValue([stored]),
                    new CompositeColumnValue([probe]));
                Assert.That(hit, Is.EqualTo(expected),
                    $"Lookup of {probe.FloatValue} against stored {stored.FloatValue} diverged from comparer equality");
            }
        }
    }

    [Test]
    public void MultiColumnKeys_SharedPrefixesDisambiguate_InBothForms()
    {
        ColumnValue[] key1 = [new(ColumnType.Integer64, 1L), new(ColumnType.String, "a"), new(ColumnType.Bool, true)];
        ColumnValue[] key2 = [new(ColumnType.Integer64, 1L), new(ColumnType.String, "a"), new(ColumnType.Bool, false)];
        ColumnValue[] key3 = [new(ColumnType.Integer64, 1L), new(ColumnType.String, "b"), new(ColumnType.Bool, true)];

        Dictionary<CompositeColumnValue, int> table = BuildTable(key1, key2, key3);

        Assert.That(AssertLookupParity(table,
            [new(ColumnType.Integer64, 1L), new(ColumnType.String, "a"), new(ColumnType.Bool, true)]), Is.True);
        Assert.That(AssertLookupParity(table,
            [new(ColumnType.Integer64, 1L), new(ColumnType.String, "a"), new(ColumnType.Bool, false)]), Is.True);
        Assert.That(AssertLookupParity(table,
            [new(ColumnType.Integer64, 1L), new(ColumnType.String, "b"), new(ColumnType.Bool, false)]), Is.False);
        Assert.That(AssertLookupParity(table,
            [new(ColumnType.Integer64, 2L), new(ColumnType.String, "a"), new(ColumnType.Bool, true)]), Is.False);
    }

    [Test]
    public void AlternateInsert_StoresOwnedCopy_ScratchReuseCannotCorruptIt()
    {
        Dictionary<CompositeColumnValue, int> table = new(Comparer);
        Dictionary<CompositeColumnValue, int>.AlternateLookup<ReadOnlySpan<ColumnValue>> lookup =
            table.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();

        // Insert through the alternate indexer, exactly as the build loops do.
        ColumnValue[] scratch = [new(ColumnType.Integer64, 7L), new(ColumnType.String, "x")];
        lookup[scratch.AsSpan()] = 1;

        // Overwrite the scratch array, as the next build row would.
        scratch[0] = new ColumnValue(ColumnType.Integer64, 8L);
        scratch[1] = new ColumnValue(ColumnType.String, "y");

        // The original key must still be found: the stored key is a copy, not the scratch array.
        Assert.That(AssertLookupParity(table,
            [new(ColumnType.Integer64, 7L), new(ColumnType.String, "x")]), Is.True);

        // The overwritten scratch contents were never inserted, so they must miss.
        Assert.That(AssertLookupParity(table,
            [new(ColumnType.Integer64, 8L), new(ColumnType.String, "y")]), Is.False);
        Assert.That(table.Count, Is.EqualTo(1));
    }

    [Test]
    public void RandomizedMixedTypeSweep_SpanAndObjectLookupNeverDiverge()
    {
        // Deterministic seed: the sweep must reproduce.
        Random random = new(20260914);

        ColumnValue RandomValue() => random.Next(6) switch
        {
            0 => new ColumnValue(ColumnType.Integer64, random.Next(50)),
            1 => new ColumnValue(ColumnType.String, ((char)('a' + random.Next(4))).ToString()),
            2 => new ColumnValue(ColumnType.Float64, random.Next(20) / 4.0),
            3 => new ColumnValue(ColumnType.Bool, random.Next(2) == 0),
            4 => new ColumnValue([(byte)random.Next(8)]),
            _ => new ColumnValue(ColumnType.Uuid, uuidHigh: random.Next(3), uuidLow: random.Next(3)),
        };

        ColumnValue[] RandomKey() => [RandomValue(), RandomValue()];

        Dictionary<CompositeColumnValue, int> table = new(Comparer);
        for (int i = 0; i < 300; i++)
            table[new CompositeColumnValue(RandomKey())] = i;

        int hits = 0;
        for (int i = 0; i < 1000; i++)
        {
            if (AssertLookupParity(table, RandomKey()))
                hits++;
        }

        // The narrow value domains make hits and misses both plentiful; the counts only prove
        // the sweep exercised both outcomes.
        Assert.That(hits, Is.GreaterThan(0), "The sweep produced no hits — the domains are wrong");
        Assert.That(hits, Is.LessThan(1000), "The sweep produced no misses — the domains are wrong");
    }
}
