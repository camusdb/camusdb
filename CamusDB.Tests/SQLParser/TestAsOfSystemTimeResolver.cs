/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using NUnit.Framework;
using Kommander.Time;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Unit coverage for <see cref="AsOfSystemTimeResolver"/>: relative offsets, absolute timestamps,
/// epoch integers, placeholders, and every rejection path. "Now" is a fixed far-future HLC so absolute
/// past instants are unambiguously in the past.
/// </summary>
public sealed class TestAsOfSystemTimeResolver
{
    // ~ year 2033 in Unix ms, so any real-world absolute timestamp in a test is comfortably in the past.
    private static readonly HLCTimestamp Now = new(7, 2_000_000_000_000L, 0);

    private static NodeAst StringNode(string quotedYytext) =>
        new(NodeType.String, null, null, null, null, null, null, null, quotedYytext);

    private static NodeAst IntNode(string yytext) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, yytext);

    private static NodeAst PlaceholderNode(string yytext) =>
        new(NodeType.Placeholder, null, null, null, null, null, null, null, yytext);

    private static HLCTimestamp Resolve(NodeAst node, Dictionary<string, ColumnValue>? parameters = null) =>
        AsOfSystemTimeResolver.Resolve(node, parameters, Now);

    [Test]
    public void RelativeSeconds_SubtractsFromNow()
    {
        HLCTimestamp t = Resolve(StringNode("'-10s'"));
        Assert.AreEqual(Now.L - 10_000, t.L);
    }

    [Test]
    public void RelativeUnits_AllSupported()
    {
        Assert.AreEqual(Now.L - 500, Resolve(StringNode("'-500ms'")).L);
        Assert.AreEqual(Now.L - 2 * 60_000, Resolve(StringNode("'-2m'")).L);
        Assert.AreEqual(Now.L - 60 * 60_000, Resolve(StringNode("'-1h'")).L);
        Assert.AreEqual(Now.L - 24 * 60 * 60_000L, Resolve(StringNode("'-1d'")).L);
    }

    [Test]
    public void AbsoluteTimestamp_ResolvesToEpochMillisWithInclusiveBound()
    {
        long expected = DateTimeOffset.Parse("2026-07-19 20:00:00+00:00").ToUnixTimeMilliseconds();

        HLCTimestamp t = Resolve(StringNode("'2026-07-19 20:00:00+00:00'"));

        Assert.AreEqual(expected, t.L);
        Assert.AreEqual(uint.MaxValue, t.C); // inclusive of every revision in that millisecond
        Assert.AreEqual(int.MaxValue, t.N);
    }

    [Test]
    public void IsoAbsoluteTimestamp_Resolves()
    {
        long expected = DateTimeOffset.Parse("2026-07-19T20:00:00Z").ToUnixTimeMilliseconds();
        Assert.AreEqual(expected, Resolve(StringNode("'2026-07-19T20:00:00Z'")).L);
    }

    [Test]
    public void IntegerLiteral_IsEpochMillis()
    {
        HLCTimestamp t = Resolve(IntNode("1721420000000"));
        Assert.AreEqual(1721420000000L, t.L);
        Assert.AreEqual(uint.MaxValue, t.C);
    }

    [Test]
    public void Placeholder_StringOffset_Resolves()
    {
        Dictionary<string, ColumnValue> p = new() { ["@ts"] = new ColumnValue(ColumnType.String, "-10s") };
        Assert.AreEqual(Now.L - 10_000, Resolve(PlaceholderNode("@ts"), p).L);
    }

    [Test]
    public void Placeholder_IntegerEpoch_Resolves()
    {
        Dictionary<string, ColumnValue> p = new() { ["@ts"] = new ColumnValue(ColumnType.Integer64, 1721420000000L) };
        Assert.AreEqual(1721420000000L, Resolve(PlaceholderNode("@ts"), p).L);
    }

    [Test]
    public void FutureRelativeOffset_Rejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Resolve(StringNode("'+10s'")))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
    }

    [Test]
    public void FutureAbsoluteTimestamp_Rejected()
    {
        // Year 2099 is after the fixed "now" (~2033).
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Resolve(StringNode("'2099-01-01T00:00:00Z'")))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
    }

    [Test]
    public void MalformedDuration_Rejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Resolve(StringNode("'-10x'")))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
    }

    [Test]
    public void EmptyString_Rejected()
    {
        Assert.Throws<CamusDBException>(() => Resolve(StringNode("''")));
    }

    [Test]
    public void PreEpochInteger_Rejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Resolve(IntNode("0")))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex.Code);
    }

    [Test]
    public void UnboundPlaceholder_Rejected()
    {
        Assert.Throws<CamusDBException>(() => Resolve(PlaceholderNode("@missing"), new Dictionary<string, ColumnValue>()));
    }
}
