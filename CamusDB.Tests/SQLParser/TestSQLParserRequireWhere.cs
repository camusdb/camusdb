/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// UPDATE and DELETE without a WHERE clause are refused on purpose, so that one statement cannot
/// change or delete every row by accident. The parser names that rule in the error and points to
/// <c>WHERE TRUE</c>, instead of a bare "expecting TWHERE" syntax error. The ON CONFLICT DO UPDATE
/// form keeps its optional WHERE, because it changes only the conflicting row.
/// </summary>
public sealed class TestSQLParserRequireWhere
{
    [TestCase("UPDATE t SET a = 1")]
    [TestCase("UPDATE t SET a = 1, b = 2")]
    [TestCase("update t set a = a + 1")]
    [TestCase("UPDATE t SET a = 1 LIMIT 5")]
    [TestCase("UPDATE t SET a = 1 RETURNING a")]
    [TestCase("UPDATE t SET a = 1 LIMIT 5 RETURNING a")]
    public void UpdateWithoutWhere_IsRefusedWithTheRuleNamed(string sql)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, sql);
        StringAssert.StartsWith("UPDATE requires a WHERE clause", ex.Message, sql);
        StringAssert.Contains("WHERE TRUE", ex.Message, sql);
    }

    [TestCase("DELETE FROM t")]
    [TestCase("delete from t")]
    [TestCase("DELETE FROM t LIMIT 5")]
    [TestCase("DELETE FROM t RETURNING a")]
    [TestCase("DELETE FROM t LIMIT 5 RETURNING a")]
    public void DeleteWithoutWhere_IsRefusedWithTheRuleNamed(string sql)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, sql);
        StringAssert.StartsWith("DELETE requires a WHERE clause", ex.Message, sql);
        StringAssert.Contains("WHERE TRUE", ex.Message, sql);
    }

    [TestCase("UPDATE t SET a = 1 WHERE TRUE", NodeType.Update)]
    [TestCase("UPDATE t SET a = 1 WHERE b = 2 LIMIT 5 RETURNING a", NodeType.Update)]
    [TestCase("DELETE FROM t WHERE TRUE", NodeType.Delete)]
    [TestCase("DELETE FROM t WHERE b = 2 LIMIT 5 RETURNING a", NodeType.Delete)]
    public void WithWhere_StillParses(string sql, NodeType expected)
    {
        Assert.AreEqual(expected, SQLParserProcessor.Parse(sql).nodeType, sql);
    }

    [Test]
    public void OnConflictDoUpdate_KeepsItsOptionalWhere()
    {
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("INSERT INTO t (k) VALUES (1) ON CONFLICT (k) DO UPDATE SET k = 2"));
    }

    [Test]
    public void TheRefusalIsNotCached_EachParseRefusesAgain()
    {
        // The parser keeps a cache of parsed statements. A refused statement must not enter it as a
        // value, so a repeated statement still gets the same clear error.
        for (int i = 0; i < 3; i++)
            Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("DELETE FROM t"));
    }
}
