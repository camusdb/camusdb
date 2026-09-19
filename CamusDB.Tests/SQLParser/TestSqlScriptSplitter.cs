/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using NUnit.Framework;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// A semicolon ends a statement only in code. Each case puts one inside a construct where the lexer
/// reads it as text, and checks that the split leaves it there.
/// </summary>
public sealed class TestSqlScriptSplitter
{
    [Test]
    public void SplitsAtTopLevelSemicolons()
    {
        AssertSplit("SELECT 1; SELECT 2;SELECT 3", "SELECT 1", "SELECT 2", "SELECT 3");
    }

    [Test]
    public void DropsEmptyAndCommentOnlyPieces()
    {
        AssertSplit(";; SELECT 1 ;\n  ; -- the end\n/* nothing */;", "SELECT 1");
        AssertSplit("", []);
        AssertSplit("  -- only a comment", []);
    }

    [Test]
    public void KeepsSemicolonsInsideStrings()
    {
        AssertSplit("INSERT INTO t VALUES ('a;b'); SELECT \"x;y\"", "INSERT INTO t VALUES ('a;b')", "SELECT \"x;y\"");
    }

    [Test]
    public void DoubledQuotesDoNotCloseAString()
    {
        AssertSplit("SELECT 'it''s; fine'; SELECT 2", "SELECT 'it''s; fine'", "SELECT 2");
        AssertSplit("SELECT \"a\"\"; b\"; SELECT 2", "SELECT \"a\"\"; b\"", "SELECT 2");
    }

    [Test]
    public void BackslashIsOrdinaryInAPlainString()
    {
        // In a plain string '\' is one character, so the quote after it closes the string.
        AssertSplit(@"SELECT 'C:\'; SELECT 2", @"SELECT 'C:\'", "SELECT 2");
    }

    [Test]
    public void BackslashEscapesAQuoteInAnEscapeString()
    {
        AssertSplit(@"SELECT E'it\'s; fine'; SELECT 2", @"SELECT E'it\'s; fine'", "SELECT 2");
        AssertSplit(@"SELECT e'\\'; SELECT 2", @"SELECT e'\\'", "SELECT 2");
    }

    [Test]
    public void AnEAtTheEndOfAnIdentifierIsNotAPrefix()
    {
        // "name" ends in e, but the quote after it is not an escape string, so '\' is ordinary.
        AssertSplit(@"SELECT name'\'; SELECT 2", @"SELECT name'\'", "SELECT 2");
    }

    [Test]
    public void KeepsSemicolonsInsideQuotedIdentifiersAndComments()
    {
        AssertSplit("SELECT `a` -- x; y\nFROM t; SELECT /* ; */ 2", "SELECT `a` -- x; y\nFROM t", "SELECT /* ; */ 2");
    }

    [Test]
    public void AnUnterminatedStringKeepsTheRestOfTheScript()
    {
        AssertSplit("SELECT 1; SELECT 'open; SELECT 2", "SELECT 1", "SELECT 'open; SELECT 2");
    }

    private static void AssertSplit(string script, params string[] expected)
    {
        List<string> actual = SqlScriptSplitter.Split(script);
        CollectionAssert.AreEqual(expected, actual);
    }
}
