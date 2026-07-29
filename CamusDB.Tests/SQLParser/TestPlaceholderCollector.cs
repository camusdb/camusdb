/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Coverage for <see cref="PlaceholderCollector"/>. The published order is a wire contract for
/// positional binding, so these tests assert the exact sequence — not just set membership — and pin
/// the two properties clients depend on: names keep their leading '@' (the key the executor binds
/// by), and a repeated name occupies exactly one slot.
/// </summary>
public sealed class TestPlaceholderCollector
{
    private static string[] Collect(string sql) => PlaceholderCollector.Collect(SQLParserProcessor.Parse(sql));

    [Test]
    public void CollectsWhereClausePlaceholdersInTextOrder()
    {
        Assert.That(
            Collect("SELECT * FROM robots WHERE year = @year AND name = @name"),
            Is.EqualTo(new[] { "@year", "@name" }));
    }

    [Test]
    public void KeepsTheAtSignBecauseTheEngineBindsByIt()
    {
        foreach (string name in Collect("SELECT * FROM robots WHERE id = @id"))
            Assert.That(name, Does.StartWith("@"));
    }

    [Test]
    public void CollectsInsertValuePlaceholders()
    {
        Assert.That(
            Collect("INSERT INTO robots (id, name, year) VALUES (@id, @name, @year)"),
            Is.EqualTo(new[] { "@id", "@name", "@year" }));
    }

    [Test]
    public void CollectsUpdateSetPlaceholdersBeforeWherePlaceholders()
    {
        Assert.That(
            Collect("UPDATE robots SET name = @name, year = @year WHERE id = @id"),
            Is.EqualTo(new[] { "@name", "@year", "@id" }));
    }

    [Test]
    public void CollapsesRepeatedNameToItsFirstOccurrence()
    {
        // Two occurrences, one binding slot: both resolve to the single supplied value.
        Assert.That(
            Collect("SELECT * FROM robots WHERE year = @y OR year = @y + 1"),
            Is.EqualTo(new[] { "@y" }));
    }

    [Test]
    public void CollectsPlaceholdersInsideSubqueries()
    {
        string[] names = Collect(
            "SELECT * FROM robots WHERE year = @outer AND id IN (SELECT robot_id FROM parts WHERE kind = @inner)");

        Assert.That(names, Is.EqualTo(new[] { "@outer", "@inner" }));
    }

    [Test]
    public void CollectsAsOfSystemTimePlaceholderFromTheSeventhChildSlot()
    {
        // AS OF SYSTEM TIME hangs off extendedSeven; a walk that stops at extendedSix misses it and
        // publishes a parameter list the statement cannot be executed with.
        Assert.That(
            Collect("SELECT * FROM robots AS OF SYSTEM TIME @t WHERE id = @id"),
            Does.Contain("@t"));
    }

    [Test]
    public void ReturnsEmptyForAStatementWithoutPlaceholders()
    {
        Assert.That(Collect("SELECT * FROM robots"), Is.Empty);
    }

    [Test]
    public void ReturnsEmptyForANullTree()
    {
        Assert.That(PlaceholderCollector.Collect(null), Is.Empty);
    }
}
