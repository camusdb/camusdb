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
/// A host that takes free-form SQL uses <see cref="StatementScope.ReturnsRows"/> to choose between the
/// query and the non-query entry points. A statement on the wrong side is refused as unknown, so each
/// family is checked here from real SQL through the parser.
/// </summary>
public sealed class TestStatementScopeReturnsRows
{
    [TestCase("SELECT 1")]
    [TestCase("SELECT a FROM t WHERE a > 1 ORDER BY a LIMIT 3")]
    [TestCase("SHOW TABLES")]
    [TestCase("SHOW COLUMNS FROM t")]
    [TestCase("SHOW INDEXES FROM t")]
    [TestCase("SHOW CREATE TABLE t")]
    [TestCase("SHOW DATABASES")]
    [TestCase("SHOW SEQUENCES")]
    [TestCase("SHOW CREATE SEQUENCE s")]
    [TestCase("SHOW VARIABLES")]
    [TestCase("EXPLAIN SELECT a FROM t")]
    [TestCase("EXPLAIN (ANALYZE) SELECT a FROM t")]
    [TestCase("ANALYZE t")]
    public void RowReturningStatements(string sql)
    {
        Assert.IsTrue(StatementScope.ReturnsRows(SQLParserProcessor.Parse(sql).nodeType), sql);
    }

    [TestCase("INSERT INTO t (a) VALUES (1)")]
    [TestCase("UPDATE t SET a = 2 WHERE a = 1")]
    [TestCase("DELETE FROM t WHERE a = 1")]
    [TestCase("CREATE TABLE t (id OID PRIMARY KEY NOT NULL, a INT64)")]
    [TestCase("DROP TABLE t")]
    [TestCase("CREATE DATABASE d")]
    [TestCase("TRUNCATE TABLE t")]
    public void StatementsWithoutRows(string sql)
    {
        Assert.IsFalse(StatementScope.ReturnsRows(SQLParserProcessor.Parse(sql).nodeType), sql);
    }
}
