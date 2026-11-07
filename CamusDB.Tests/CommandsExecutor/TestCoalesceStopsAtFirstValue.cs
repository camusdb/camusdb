
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// COALESCE, IFNULL and NVL stop at the first argument that is not NULL, through the whole engine. Each
/// expected result in a table was measured on PostgreSQL 16.11 in the C locale with the same rows, so the tables
/// hold 205 statements that the two engines agree on. A NULL renders as an empty cell, cells join with
/// <c>|</c> and rows join with <c> ; </c>. An expected value that starts with <c>CADB</c> is the error code of
/// CamusDB, where PostgreSQL also gave an error.
///
/// <para>One test runs a whole table in one database, and a failure names each statement that differs. A test
/// for each statement costs minutes of suite time, because each test builds a database.</para>
///
/// <para><see cref="DecidedRows"/> pins the statements that differ from PostgreSQL by decision, so a change of
/// one is seen. <c>docs/postgresql-differences.md</c> gives the reasons.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCoalesceStopsAtFirstValue : BaseTest
{
    private static readonly (string Sql, string Expected)[] Constants =
    [
        ("SELECT COALESCE(1, 1 / 0)",
            "1"),
        ("SELECT COALESCE(NULL, 1, 1 / 0)",
            "1"),
        ("SELECT COALESCE(1 / 0, 1)",
            "CADB0400"),
        ("SELECT COALESCE(NULL, 1 / 0)",
            "CADB0400"),
        ("SELECT COALESCE(1, 2 / 1, 1 / 0)",
            "1"),
        ("SELECT NULLIF(1, 1 / 0)",
            "CADB0400"),
        ("SELECT NULLIF(1 / 0, 1)",
            "CADB0400"),
        ("SELECT COALESCE(COALESCE(1, 1 / 0), 1 / 0)",
            "1"),
        ("SELECT COALESCE(1, COALESCE(1 / 0, 2))",
            "1"),
        ("SELECT COALESCE(CASE WHEN TRUE THEN 1 ELSE 1 / 0 END, 1 / 0)",
            "1"),
        ("SELECT COALESCE(1, 1 % 0)",
            "1"),
        ("SELECT COALESCE(1, 9223372036854775807 + 1)",
            "1"),
        ("SELECT COALESCE(1, 1 / 0) FROM em",
            ""),
        ("SELECT COALESCE(NULL, 1, 1 / 0) FROM em",
            ""),
        ("SELECT COALESCE(1 / 0, 1) FROM em",
            "CADB0400"),
        ("SELECT COALESCE(NULL, 1 / 0) FROM em",
            "CADB0400"),
        ("SELECT COALESCE(1, 2 / 1, 1 / 0) FROM em",
            ""),
        ("SELECT NULLIF(1, 1 / 0) FROM em",
            "CADB0400"),
        ("SELECT NULLIF(1 / 0, 1) FROM em",
            "CADB0400"),
        ("SELECT COALESCE(COALESCE(1, 1 / 0), 1 / 0) FROM em",
            ""),
        ("SELECT COALESCE(1, COALESCE(1 / 0, 2)) FROM em",
            ""),
        ("SELECT COALESCE(CASE WHEN TRUE THEN 1 ELSE 1 / 0 END, 1 / 0) FROM em",
            ""),
        ("SELECT COALESCE(1, 1 % 0) FROM em",
            ""),
        ("SELECT COALESCE(1, 9223372036854775807 + 1) FROM em",
            ""),
        ("SELECT COALESCE(1, 1 / 0) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(NULL, 1, 1 / 0) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(1 / 0, 1) FROM t",
            "CADB0400"),
        ("SELECT COALESCE(NULL, 1 / 0) FROM t",
            "CADB0400"),
        ("SELECT COALESCE(1, 2 / 1, 1 / 0) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT NULLIF(1, 1 / 0) FROM t",
            "CADB0400"),
        ("SELECT NULLIF(1 / 0, 1) FROM t",
            "CADB0400"),
        ("SELECT COALESCE(COALESCE(1, 1 / 0), 1 / 0) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(1, COALESCE(1 / 0, 2)) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(CASE WHEN TRUE THEN 1 ELSE 1 / 0 END, 1 / 0) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(1, 1 % 0) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(1, 9223372036854775807 + 1) FROM t",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(1, 1 / 0) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(NULL, 1, 1 / 0) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(1 / 0, 1) FROM t WHERE id = 1",
            "CADB0400"),
        ("SELECT COALESCE(NULL, 1 / 0) FROM t WHERE id = 1",
            "CADB0400"),
        ("SELECT COALESCE(1, 2 / 1, 1 / 0) FROM t WHERE id = 1",
            "1"),
        ("SELECT NULLIF(1, 1 / 0) FROM t WHERE id = 1",
            "CADB0400"),
        ("SELECT NULLIF(1 / 0, 1) FROM t WHERE id = 1",
            "CADB0400"),
        ("SELECT COALESCE(COALESCE(1, 1 / 0), 1 / 0) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(1, COALESCE(1 / 0, 2)) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(CASE WHEN TRUE THEN 1 ELSE 1 / 0 END, 1 / 0) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(1, 1 % 0) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(1, 9223372036854775807 + 1) FROM t WHERE id = 1",
            "1"),
        ("SELECT COALESCE(1, 1 / 0) FROM t WHERE FALSE",
            ""),
        ("SELECT COALESCE(NULL, 1, 1 / 0) FROM t WHERE FALSE",
            ""),
        ("SELECT COALESCE(1 / 0, 1) FROM t WHERE FALSE",
            "CADB0400"),
        ("SELECT COALESCE(NULL, 1 / 0) FROM t WHERE FALSE",
            "CADB0400"),
        ("SELECT COALESCE(1, 2 / 1, 1 / 0) FROM t WHERE FALSE",
            ""),
        ("SELECT NULLIF(1, 1 / 0) FROM t WHERE FALSE",
            "CADB0400"),
        ("SELECT NULLIF(1 / 0, 1) FROM t WHERE FALSE",
            "CADB0400"),
        ("SELECT COALESCE(COALESCE(1, 1 / 0), 1 / 0) FROM t WHERE FALSE",
            ""),
        ("SELECT COALESCE(1, COALESCE(1 / 0, 2)) FROM t WHERE FALSE",
            ""),
        ("SELECT COALESCE(CASE WHEN TRUE THEN 1 ELSE 1 / 0 END, 1 / 0) FROM t WHERE FALSE",
            ""),
        ("SELECT COALESCE(1, 1 % 0) FROM t WHERE FALSE",
            ""),
        ("SELECT COALESCE(1, 9223372036854775807 + 1) FROM t WHERE FALSE",
            ""),
    ];

    private static readonly (string Sql, string Expected)[] RowValues =
    [
        ("SELECT id, COALESCE(1, 1 / v) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(v, 1 / v) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(v, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(n, 1 / v) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(n, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(n, 1, 1 / v) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|5"),
        ("SELECT id, COALESCE(n, 1, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1 / v, 1) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(1 / v, 1) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(n, v, 1 / v) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|5"),
        ("SELECT id, COALESCE(n, v, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(v, 1 / 0) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(v, 1 / 0) FROM em ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(n, 1 / 0) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(n, 1 / 0) FROM em ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(COALESCE(n, 1), 1 / v) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|5"),
        ("SELECT id, COALESCE(COALESCE(n, 1), 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1, COALESCE(n, 1 / v)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, COALESCE(n, 1 / v)) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1, abs(1 / v)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, abs(1 / v)) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(v, abs(1 / v)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(v, abs(1 / v)) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1, 1 / v + 1) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, 1 / v + 1) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1, sqrt(-1.0 - f)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, sqrt(-1.0 - f)) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1, CAST(s AS BIGINT)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, CAST(s AS BIGINT)) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(v, CAST(s AS BIGINT)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(v, CAST(s AS BIGINT)) FROM em ORDER BY id",
            ""),
        ("SELECT id, NULLIF(v, 1 / v) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, NULLIF(v, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, NULLIF(1 / v, v) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, NULLIF(1 / v, v) FROM em ORDER BY id",
            ""),
        ("SELECT id, NULLIF(1, 1 / v) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, NULLIF(1, 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(NULLIF(v, 0), 7) FROM t ORDER BY id",
            "1|7 ; 2|2 ; 3|7"),
        ("SELECT id, COALESCE(NULLIF(v, 0), 7) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(NULLIF(v, 0), 1 / v) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(NULLIF(v, 0), 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(1 / NULLIF(v, 0), -1) FROM t ORDER BY id",
            "1|-1 ; 2|0 ; 3|-1"),
        ("SELECT id, COALESCE(1 / NULLIF(v, 0), -1) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(NULLIF(1, 1), 1 / v) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(NULLIF(1, 1), 1 / v) FROM em ORDER BY id",
            ""),
        ("SELECT id, COALESCE(n, 9223372036854775807 + 1) FROM t ORDER BY id",
            "CADB0420"),
        ("SELECT id, COALESCE(n, CAST('x' AS BIGINT)) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(n, sqrt(-1.0)) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(n, 1 % 0) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(v, abs(1 / 0)) FROM t ORDER BY id",
            "CADB0400"),
    ];

    private static readonly (string Sql, string Expected)[] Positions =
    [
        ("SELECT id FROM t WHERE COALESCE(1, 1 / v) = 1 ORDER BY id",
            "1 ; 2 ; 3"),
        ("SELECT id FROM t WHERE COALESCE(v, 1 / v) = 0 ORDER BY id",
            "1 ; 3"),
        ("SELECT id FROM t WHERE COALESCE(n, 1 / v) = 5 ORDER BY id",
            "CADB0400"),
        ("SELECT id FROM t WHERE COALESCE(n, 1) = 1 AND COALESCE(v, 1 / v) = 0 ORDER BY id",
            "1"),
        ("SELECT id FROM t WHERE COALESCE(1, 1 / 0) = 1 ORDER BY id",
            "1 ; 2 ; 3"),
        ("SELECT id FROM t ORDER BY COALESCE(id, 1 / v)",
            "1 ; 2 ; 3"),
        ("SELECT id FROM t ORDER BY COALESCE(id, 1 / 0)",
            "CADB0400"),
        ("SELECT COALESCE(id, 1 / v) AS k FROM t GROUP BY COALESCE(id, 1 / v) ORDER BY k",
            "1 ; 2 ; 3"),
        ("SELECT DISTINCT COALESCE(id, 1 / v) AS k FROM t ORDER BY k",
            "1 ; 2 ; 3"),
        ("SELECT SUM(COALESCE(v, 1 / v)) FROM t",
            "2"),
        ("SELECT SUM(COALESCE(1, 1 / v)) FROM t",
            "3"),
        ("SELECT COUNT(COALESCE(1, 1 / v)) FROM t",
            "3"),
        ("SELECT MAX(COALESCE(n, 1 / v)) FROM t",
            "CADB0400"),
        ("SELECT COALESCE(SUM(v), 1 / COUNT(*)) FROM t",
            "2"),
        ("SELECT COALESCE(SUM(v), 1 / COUNT(*)) FROM em",
            "CADB0400"),
        ("SELECT COALESCE(SUM(v), 1 / COUNT(id)) FROM em",
            "CADB0400"),
        ("SELECT COALESCE(COUNT(*), 1 / 0) FROM t",
            "CADB0400"),
        ("SELECT COALESCE(1, MAX(v)) FROM t",
            "1"),
        ("SELECT COALESCE(1, MAX(v)) FROM em",
            "1"),
        ("SELECT v, COALESCE(SUM(id), 1 / v) FROM t GROUP BY v ORDER BY v",
            "0|4 ; 2|2"),
        ("SELECT v, COUNT(*) FROM t GROUP BY v HAVING COALESCE(MAX(id), 1 / v) > 0 ORDER BY v",
            "0|2 ; 2|1"),
        ("SELECT v, COUNT(*) FROM t GROUP BY v HAVING COALESCE(1, 1 / v) > 0 ORDER BY v",
            "0|2 ; 2|1"),
        ("SELECT a.id FROM t a JOIN t b ON COALESCE(a.id, 1 / a.v) = b.id ORDER BY a.id",
            "1 ; 2 ; 3"),
        ("SELECT a.id FROM t a LEFT JOIN t b ON COALESCE(b.id, 1 / b.v) = a.id ORDER BY a.id",
            "1 ; 2 ; 3"),
        ("SELECT x FROM (SELECT COALESCE(1, 1 / v) AS x FROM t) d ORDER BY x",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(x, 1 / y) FROM (SELECT v + 1 AS x, v AS y FROM t) d ORDER BY 1",
            "1 ; 1 ; 3"),
        ("SELECT (SELECT COALESCE(1, 1 / v) FROM t WHERE id = 1)",
            "1"),
        ("SELECT id FROM t WHERE id IN (SELECT COALESCE(id, 1 / v) FROM t) ORDER BY id",
            "1 ; 2 ; 3"),
        ("SELECT id FROM t a WHERE EXISTS (SELECT 1 FROM t b WHERE COALESCE(b.id, 1 / b.v) = a.id) ORDER BY id",
            "1 ; 2 ; 3"),
        ("SELECT id, CASE WHEN COALESCE(1, 1 / v) = 1 THEN 'a' ELSE 'b' END FROM t ORDER BY id",
            "1|a ; 2|a ; 3|a"),
        ("SELECT id, COALESCE(n, 1 / v) IS NULL FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, 1 + COALESCE(1, 1 / v) FROM t ORDER BY id",
            "1|2 ; 2|2 ; 3|2"),
        ("SELECT id, SUM(COALESCE(1, 1 / v)) OVER (ORDER BY id) FROM t ORDER BY id",
            "1|1 ; 2|2 ; 3|3"),
        ("SELECT id, COALESCE(n, (SELECT 1 / v FROM t WHERE id = 1)) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(n, (SELECT b.id FROM t b)) FROM t ORDER BY id",
            "CADB0400"),
    ];

    private static readonly (string Sql, string Expected)[] Writes =
    [
        ("UPDATE t SET n = COALESCE(1, 1 / v) WHERE TRUE",
            "3"),
        ("UPDATE t SET n = COALESCE(v, 1 / v) WHERE TRUE",
            "3"),
        ("UPDATE t SET n = COALESCE(n, 1 / v) WHERE id = 3",
            "1"),
        ("UPDATE t SET n = COALESCE(n, 1 / v) WHERE TRUE",
            "CADB0400"),
        ("UPDATE t SET n = 1 WHERE COALESCE(v, 1 / v) = 0",
            "2"),
        ("DELETE FROM t WHERE COALESCE(v, 1 / v) = 0",
            "2"),
        ("DELETE FROM t WHERE COALESCE(1, 1 / v) = 1",
            "3"),
        ("INSERT INTO w (id, c) VALUES (1, COALESCE(1, 1 / 0))",
            "1"),
        ("INSERT INTO w (id, c) VALUES (COALESCE(2, 1 / 0), 1)",
            "1"),
        ("INSERT INTO w (id, c) SELECT id, COALESCE(1, 1 / v) FROM t",
            "3"),
        ("INSERT INTO w (id, c) SELECT id, COALESCE(n, 1 / v) FROM t WHERE id = 3",
            "1"),
        ("INSERT INTO w (id, c) SELECT id, COALESCE(n, 1 / v) FROM t",
            "CADB0400"),
    ];

    private static readonly (string Sql, string Expected)[] Types =
    [
        ("SELECT id, COALESCE(1, 2.5) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(v, f) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(f, v) FROM t ORDER BY id",
            "1|2.5 ; 2|2 ; 3|0.5"),
        ("SELECT id, COALESCE(v, 2.5) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(n, 2.5) FROM t ORDER BY id",
            "1|2.5 ; 2|2.5 ; 3|5"),
        ("SELECT id, COALESCE(1, f) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(v, sqrt(f)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(1, sqrt(f)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, sqrt(4.0)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(v, abs(f)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(1, abs(2.5)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(v, power(2.0, 2.0)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(v, length(s)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(1, length(s)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(s, upper(s)) FROM t ORDER BY id",
            "1|x ; 2|y ; 3|"),
        ("SELECT id, COALESCE('a', upper(s)) FROM t ORDER BY id",
            "1|a ; 2|a ; 3|a"),
        ("SELECT id, COALESCE(1, 'x') FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(v, 'x') FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(v, s) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(1, s) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(NULL, upper('a')) FROM t ORDER BY id",
            "1|A ; 2|A ; 3|A"),
        ("SELECT id, COALESCE(n, upper(s)) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(1, TRUE) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(TRUE, 1) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(1, CAST(2.5 AS DOUBLE PRECISION)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, 1.0 + 1) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, v + 0.5) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(NULL, NULL) FROM t ORDER BY id",
            "1| ; 2| ; 3|"),
        ("SELECT id, COALESCE(NULL) FROM t ORDER BY id",
            "1| ; 2| ; 3|"),
        ("SELECT id, COALESCE(n) FROM t ORDER BY id",
            "1| ; 2| ; 3|5"),
        ("SELECT id, COALESCE(n, NULL) FROM t ORDER BY id",
            "1| ; 2| ; 3|5"),
        ("SELECT id, COALESCE(v, NULL) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(NULL, v) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0"),
        ("SELECT id, COALESCE(1, NULL, 2.5) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(n, n, 2.5) FROM t ORDER BY id",
            "1|2.5 ; 2|2.5 ; 3|5"),
        ("SELECT id, COALESCE(COALESCE(1, 2.5), 3) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, COALESCE(2, 2.5)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, CASE WHEN TRUE THEN 2.5 ELSE 3.5 END) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, CASE WHEN TRUE THEN 'a' ELSE 'b' END) FROM t ORDER BY id",
            "CADB0400"),
        ("SELECT id, COALESCE(1, abs(v)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1.5, abs(v)) FROM t ORDER BY id",
            "1|1.5 ; 2|1.5 ; 3|1.5"),
        ("SELECT id, COALESCE(1, round(f)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, COALESCE(1, floor(f)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
    ];

    private static readonly (string Sql, string Expected)[] Case =
    [
        ("SELECT id, CASE WHEN TRUE THEN 1 ELSE sqrt(f) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE sqrt(f) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE 2.5 END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, CASE WHEN TRUE THEN 1 ELSE (SELECT b.id FROM t b) END FROM t ORDER BY id",
            "CADB0400"),
    ];

    private static readonly (string Sql, string CamusDb, string PostgreSql)[] DecidedRows =
    [
        ("SELECT COALESCE(1, CAST('x' AS BIGINT))",
            "1",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM em",
            "",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM t",
            "1 ; 1 ; 1",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM t WHERE id = 1",
            "1",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT COALESCE(1, CAST('x' AS BIGINT)) FROM t WHERE FALSE",
            "",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT COALESCE(1, SUM(1 / v)) FROM t",
            "CADB0400",
            "1"),
        ("SELECT id, COALESCE(1, SUM(1 / v) OVER (ORDER BY id)) FROM t ORDER BY id",
            "CADB0400",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT COALESCE(1, (SELECT 1 / v FROM t WHERE id = 1))",
            "CADB0400",
            "1"),
        ("SELECT COALESCE(1, (SELECT 1 / 0))",
            "CADB0400",
            "1"),
        ("SELECT COALESCE(1, (SELECT 1 / 0 FROM t WHERE id = 1))",
            "CADB0400",
            "1"),
        ("SELECT COALESCE(1, (SELECT 1 / v FROM t WHERE id = 1)) FROM t ORDER BY id",
            "CADB0400",
            "1 ; 1 ; 1"),
        ("SELECT COALESCE(1, (SELECT id FROM t)) ",
            "CADB0400",
            "1"),
        ("SELECT COALESCE(1, (SELECT id FROM t)) FROM t ORDER BY id",
            "CADB0400",
            "1 ; 1 ; 1"),
        ("SELECT id, COALESCE(id, (SELECT b.id FROM t b WHERE b.id = a.id + 100)) FROM t a ORDER BY id",
            "CADB0400",
            "1|1 ; 2|2 ; 3|3"),
        ("SELECT id, COALESCE(n, sqrt(4.0)) FROM t ORDER BY id",
            "1|2 ; 2|2 ; 3|5",
            "1|2.000000000000000 ; 2|2.000000000000000 ; 3|5"),
        ("SELECT id, COALESCE(1, upper(s)) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR COALESCE types integer and text cannot be matched"),
        ("SELECT id, COALESCE(v, upper(s)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0",
            "ERROR COALESCE types bigint and text cannot be matched"),
        ("SELECT id, COALESCE(1, upper('a')) FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR COALESCE types integer and text cannot be matched"),
        ("SELECT id, CASE WHEN TRUE THEN 1 ELSE upper('a') END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR CASE types text and integer cannot be matched"),
        ("SELECT id, CASE WHEN TRUE THEN 1 ELSE upper(s) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR CASE types text and integer cannot be matched"),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE upper(s) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR CASE types text and integer cannot be matched"),
        ("SELECT id, CASE WHEN TRUE THEN 1 ELSE (SELECT 1 / b.v FROM t b WHERE b.id = 1) END FROM t ORDER BY id",
            "CADB0400",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE (SELECT 1 / b.v FROM t b WHERE b.id = 1) END FROM t ORDER BY id",
            "CADB0400",
            "1|1 ; 2|1 ; 3|1"),
        ("SELECT id, CASE WHEN TRUE THEN 1 ELSE CAST('x' AS BIGINT) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE CAST('x' AS BIGINT) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE 9223372036854775807 + 1 END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR bigint out of range"),
        ("SELECT id, CASE WHEN id > 0 THEN 1 ELSE sqrt(-1.0) END FROM t ORDER BY id",
            "1|1 ; 2|1 ; 3|1",
            "ERROR cannot take square root of a negative number"),
        ("SELECT id, COALESCE(v, 9223372036854775807 + 1) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0",
            "ERROR bigint out of range"),
        ("SELECT id, COALESCE(v, CAST('x' AS BIGINT)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0",
            "ERROR invalid input syntax for type bigint: \"x\""),
        ("SELECT id, COALESCE(v, sqrt(-1.0)) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0",
            "ERROR cannot take square root of a negative number"),
        ("SELECT id, COALESCE(v, 1 % 0) FROM t ORDER BY id",
            "1|0 ; 2|2 ; 3|0",
            "ERROR division by zero"),
        ("SELECT id, COALESCE(id, (SELECT b.id FROM t b)) FROM t ORDER BY id",
            "CADB0400",
            "1|1 ; 2|2 ; 3|3"),
        ("SELECT id, COALESCE(id, (SELECT 1 / b.v FROM t b WHERE b.id = 1)) FROM t ORDER BY id",
            "CADB0400",
            "1|1 ; 2|2 ; 3|3"),
    ];

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString("G17", CultureInfo.InvariantCulture),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static bool IsWrite(string sql) =>
        sql.StartsWith("UPDATE", StringComparison.Ordinal)
        || sql.StartsWith("DELETE", StringComparison.Ordinal)
        || sql.StartsWith("INSERT", StringComparison.Ordinal);

    /// <summary>The rendered rows, or the error code. A write renders the count of the rows that it changed.</summary>
    private static async Task<string> Run(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool commit = false,
        Dictionary<string, ColumnValue>? parameters = null, List<ColumnType>? lastCellTypes = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        bool succeeded = false;

        try
        {
            string result;

            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            {
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, parameters));
                result = "";
            }
            else if (IsWrite(sql) || sql.StartsWith("REFRESH", StringComparison.Ordinal))
            {
                result = (await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters))).ModifiedRows.ToString(CultureInfo.InvariantCulture);
            }
            else
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
                List<QueryResultRow> rows = await cursor.ToListAsync();
                lastCellTypes?.AddRange(rows.Select(row => row.Row.Values.Last().Type));
                result = string.Join(" ; ", rows.Select(row => string.Join("|", row.Row.Values.Select(Render))));
            }

            succeeded = true;
            return result;
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            if (succeeded && commit)
                await database.Transactions.CommitAsync(tx);
            else
                await database.Transactions.RollbackAsync(tx);
        }
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        string result = await Run(database, executor, dbname, sql, commit: true);
        Assert.That(result, Does.Not.StartWith("CADB"), sql);
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, n INT64, f FLOAT64, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64, n INT64, f FLOAT64, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE w (id INT64 PRIMARY KEY, c INT64)");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, n, f, s) VALUES (1, 0, NULL, 2.5, 'x')");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, n, f, s) VALUES (2, 2, NULL, NULL, 'y')");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, n, f, s) VALUES (3, 0, 5, 0.5, NULL)");

        return (dbname, database, executor);
    }

    private async Task RunTable((string Sql, string Expected)[] table)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<string> failures = [];

        foreach ((string sql, string expected) in table)
        {
            string actual = await Run(database, executor, dbname, sql);

            if (actual != expected)
                failures.Add($"{sql}\n    expected: {expected}\n    actual:   {actual}");
        }

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    [Test]
    public Task ConstantArguments_InEveryFromForm() => RunTable(Constants);

    [Test]
    public Task RowValues_WithRowsAndOnNoRows() => RunTable(RowValues);

    [Test]
    public Task EveryPositionOfAQuery() => RunTable(Positions);

    [Test]
    public Task EveryWritePosition() => RunTable(Writes);

    [Test]
    public Task ResultValues() => RunTable(Types);

    [Test]
    public Task CaseForms_ThatAgree() => RunTable(Case);

    [Test]
    public async Task DecidedRows_HaveTheOutcomeOnRecord()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<string> failures = [];

        foreach ((string sql, string camusDb, string postgreSql) in DecidedRows)
        {
            string actual = await Run(database, executor, dbname, sql);

            if (actual != camusDb)
                failures.Add($"{sql}\n    expected: {camusDb}\n    actual:   {actual}\n    PostgreSQL: {postgreSql}");
        }

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    // PostgreSQL has no IFNULL or NVL, so these follow the rule of COALESCE, which they share.
    [TestCase("SELECT id, IFNULL(1, 1 / v) FROM t ORDER BY id", "1|1 ; 2|1 ; 3|1")]
    [TestCase("SELECT id, NVL(1, 1 / v) FROM t ORDER BY id", "1|1 ; 2|1 ; 3|1")]
    [TestCase("SELECT id, IFNULL(v, 1 / v) FROM t ORDER BY id", "1|0 ; 2|2 ; 3|0")]
    [TestCase("SELECT id, NVL(v, 1 / v) FROM t ORDER BY id", "1|0 ; 2|2 ; 3|0")]
    [TestCase("SELECT id, IFNULL(n, 1 / v) FROM t ORDER BY id", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT id, NVL(n, 1 / v) FROM t ORDER BY id", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT IFNULL(1, 2, 3) FROM t", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT IFNULL(1) FROM t", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT IFNULL(1) FROM em", CamusDBErrorCodes.InvalidInput)]
    [TestCase("SELECT IFNULL(1, 1 / 0) FROM t WHERE id = 1", "1")]
    public async Task IfNullAndNvl_StopLikeCoalesce(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    /// <summary>
    /// The arguments run in order. A failure in an earlier argument is a failure, and a later NULL does not
    /// hide the value that won.
    /// </summary>
    [TestCase("SELECT id, COALESCE(n, NULL, v, 1 / v) FROM t ORDER BY id", "1|0 ; 2|2 ; 3|5")]
    [TestCase("SELECT id, COALESCE(1 / v, 7) FROM t WHERE id = 2", "2|0")]
    [TestCase("SELECT id, COALESCE(1 / v, 7) FROM t WHERE id = 1", CamusDBErrorCodes.InvalidInput)]
    public async Task Arguments_RunInOrder(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    /// <summary>The evaluator must walk the list with no recursion for each row, also when most of it does not run.</summary>
    [Test]
    public async Task ALongArgumentList_StopsAtItsFirstValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string dead = string.Join(", ", Enumerable.Repeat("1 / v", 300));

        Assert.That(await Run(database, executor, dbname, $"SELECT COALESCE(n, id, {dead}) FROM t ORDER BY id"), Is.EqualTo("1 ; 2 ; 5"));
    }

    // PostgreSQL 16.11: the insert of (1, 1, 0) succeeds, (2, NULL, 0) fails with division by zero, and (3, NULL, 1) succeeds.
    [Test]
    public async Task Check_DividesOnlyWhenItReachesTheDivision()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE ck (id INT64 PRIMARY KEY, a INT64, b INT64, CHECK (COALESCE(a, 1 / b) > 0))");

        Assert.Multiple(async () =>
        {
            Assert.That(await Run(database, executor, dbname, "INSERT INTO ck (id, a, b) VALUES (1, 1, 0)", commit: true), Is.EqualTo("1"));
            Assert.That(await Run(database, executor, dbname, "INSERT INTO ck (id, a, b) VALUES (2, NULL, 0)", commit: true), Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(await Run(database, executor, dbname, "INSERT INTO ck (id, a, b) VALUES (3, NULL, 1)", commit: true), Is.EqualTo("1"));
            Assert.That(await Run(database, executor, dbname, "SELECT id FROM ck ORDER BY id"), Is.EqualTo("1 ; 3"));
        });
    }

    /// <summary>
    /// A CHECK has no recorded result type, so the column that does not win must still widen the value. An integer
    /// division gives 0 here and refuses the row. PostgreSQL 16.11 accepts it.
    /// </summary>
    [Test]
    public async Task Check_WidensByAColumnThatDoesNotWin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE ck (id INT64 PRIMARY KEY, a INT64, f FLOAT64, CHECK (COALESCE(a, f) / 2 > 0))");

        Assert.That(await Run(database, executor, dbname, "INSERT INTO ck (id, a, f) VALUES (1, 1, 2.5)", commit: true), Is.EqualTo("1"));
    }

    /// <summary>A quoted literal takes the date type of a later argument that does not run. PostgreSQL 16.11 gives the same dates.</summary>
    [TestCase("SELECT CAST(COALESCE('2026-01-01', CAST('2026-01-02' AS DATE)) AS STRING)", "2026-01-01")]
    [TestCase("SELECT CAST(COALESCE(CAST('2026-01-02' AS DATE), '2026-01-01') AS STRING)", "2026-01-02")]
    public async Task StringLiteral_TakesTheDateTypeOfALaterArgument(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    // PostgreSQL 16.11: the view of id 3 gives 5, and the view of every row fails with division by zero.
    [Test]
    public async Task View_DividesOnlyWhenARowReachesTheDivision()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW one AS SELECT id, COALESCE(n, 1 / v) AS x FROM t WHERE id = 3");
        await Commit(database, executor, dbname, "CREATE VIEW every AS SELECT id, COALESCE(n, 1 / v) AS x FROM t");

        Assert.Multiple(async () =>
        {
            Assert.That(await Run(database, executor, dbname, "SELECT x FROM one"), Is.EqualTo("5"));
            Assert.That(await Run(database, executor, dbname, "SELECT x FROM every ORDER BY id"), Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        });
    }

    // PostgreSQL 16.11: both give 0, 2 and 0.
    [TestCase("CREATE TABLE ct AS SELECT id, COALESCE(v, 1 / v) AS x FROM t", "ct")]
    [TestCase("CREATE MATERIALIZED VIEW mv AS SELECT id, COALESCE(v, 1 / v) AS x FROM t", "mv")]
    public async Task StoredResults_DoNotRunALaterArgument(string create, string name)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, create);

        Assert.That(await Run(database, executor, dbname, $"SELECT x FROM {name} ORDER BY id"), Is.EqualTo("0 ; 2 ; 0"));
    }

    /// <summary>
    /// A stored column has one declared type, so the row where the INT64 argument wins still stores a FLOAT64.
    /// PostgreSQL 16.11 gives one type in each row, with the values 2.5, 2.5 and 5.
    /// </summary>
    [Test]
    public async Task StoredColumn_HasItsDeclaredTypeWhenAnEarlierArgumentWins()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE ctd AS SELECT id, COALESCE(n, 2.5) AS x FROM t");

        List<ColumnType> types = [];
        string rows = await Run(database, executor, dbname, "SELECT x FROM ctd ORDER BY id", lastCellTypes: types);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo("2.5 ; 2.5 ; 5"));
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Float64, ColumnType.Float64, ColumnType.Float64 }));
        });
    }

    /// <summary>
    /// A function result does not widen the row where an earlier argument wins, so the write converts it. PostgreSQL
    /// 16.11 gives double precision in each row, with the values 1.58..., NULL and 5.
    /// </summary>
    [Test]
    public async Task InsertSelect_ConvertsTheValueThatWon()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE target (id INT64 PRIMARY KEY, x FLOAT64)");

        Assert.That(await Run(database, executor, dbname, "INSERT INTO target (id, x) SELECT id, COALESCE(n, sqrt(f)) FROM t", commit: true), Is.EqualTo("3"));

        List<ColumnType> types = [];
        string rows = await Run(database, executor, dbname, "SELECT x FROM target ORDER BY id", lastCellTypes: types);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo("1.5811388300841898 ;  ; 5"));
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Float64, ColumnType.Null, ColumnType.Float64 }));
        });
    }

    /// <summary>A type that the statement shows widens the value, also when the argument that shows it does not run.</summary>
    [TestCase("COALESCE(1, 2.5)", ColumnType.Float64)]
    [TestCase("COALESCE(v, f)", ColumnType.Float64)]
    [TestCase("COALESCE(v, 2.5)", ColumnType.Float64)]
    [TestCase("COALESCE(1, NULL, 2.5)", ColumnType.Float64)]
    [TestCase("COALESCE(1, 1.0 + 1)", ColumnType.Float64)]
    [TestCase("COALESCE(1, v + 0.5)", ColumnType.Float64)]
    [TestCase("COALESCE(1, CAST(2.5 AS DOUBLE PRECISION))", ColumnType.Float64)]
    [TestCase("COALESCE(1, COALESCE(2, 2.5))", ColumnType.Float64)]
    [TestCase("COALESCE(1, CASE WHEN TRUE THEN 2.5 ELSE 3.5 END)", ColumnType.Float64)]
    [TestCase("COALESCE(1.5, abs(v))", ColumnType.Float64)]
    [TestCase("COALESCE(1, 1 / v)", ColumnType.Integer64)]
    [TestCase("COALESCE(1, abs(v))", ColumnType.Integer64)]
    [TestCase("COALESCE('a', upper(s))", ColumnType.String)]
    public async Task ResultType_ComesFromEveryArgumentThatShowsIt(string expression, ColumnType expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        await Run(database, executor, dbname, $"SELECT id, {expression} FROM t ORDER BY id", lastCellTypes: types);

        Assert.That(types, Is.EqualTo(new[] { expected, expected, expected }));
    }

    /// <summary>
    /// A function result shows its type only when it runs, by decision, as a CASE branch does. PostgreSQL gives
    /// double precision in each row here, and the values are equal.
    /// </summary>
    [TestCase("COALESCE(1, sqrt(f))", ColumnType.Integer64, ColumnType.Integer64, ColumnType.Integer64)]
    [TestCase("COALESCE(v, abs(f))", ColumnType.Integer64, ColumnType.Integer64, ColumnType.Integer64)]
    [TestCase("COALESCE(n, sqrt(4.0))", ColumnType.Float64, ColumnType.Float64, ColumnType.Integer64)]
    [TestCase("CASE WHEN TRUE THEN 1 ELSE sqrt(f) END", ColumnType.Integer64, ColumnType.Integer64, ColumnType.Integer64)]
    public async Task ResultType_OfAFunctionResultThatDoesNotRun_IsNotRead(string expression, ColumnType first, ColumnType second, ColumnType third)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        await Run(database, executor, dbname, $"SELECT id, {expression} FROM t ORDER BY id", lastCellTypes: types);

        Assert.That(types, Is.EqualTo(new[] { first, second, third }));
    }

    // A pair that the statement shows has no common type before any row is read, as before this rule.
    [TestCase("SELECT COALESCE(1, 'x') FROM t")]
    [TestCase("SELECT COALESCE(1, 'x') FROM em")]
    [TestCase("SELECT COALESCE(v, s) FROM t")]
    [TestCase("SELECT COALESCE(v, s) FROM em")]
    [TestCase("SELECT COALESCE(1, TRUE) FROM em")]
    [TestCase("SELECT COALESCE(1, CASE WHEN TRUE THEN 'a' ELSE 'b' END) FROM em")]
    public async Task ResultType_ThatTheStatementShows_IsCheckedBeforeAnyRow(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(CamusDBErrorCodes.InvalidInput), sql);
    }

    /// <summary>
    /// HAVING evaluates with no recorded result type, so the literal that does not run must still widen the
    /// value. PostgreSQL 16.11 gives 0.5, and an integer division here gives 0.
    /// </summary>
    [TestCase("SELECT COALESCE(COUNT(*), 2.5) / 2 FROM t WHERE id = 1", "0.5")]
    [TestCase("SELECT COUNT(*) FROM t WHERE id = 1 HAVING COALESCE(COUNT(*), 2.5) / 2 > 0.4", "1")]
    public async Task Aggregate_WidensByALiteralThatDoesNotRun(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    // PostgreSQL 16.11, prepared: id 2 gives 5 and then 0; id 1 gives 5 and then division by zero.
    [Test]
    public async Task Parameter_DecidesForEachExecution()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> five = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 5L) };
        Dictionary<string, ColumnValue> none = new() { ["@p"] = ColumnValue.Null };

        const string sql = "SELECT COALESCE(@p, 1 / v) FROM t WHERE id = ";

        Assert.Multiple(async () =>
        {
            Assert.That(await Run(database, executor, dbname, sql + "2", parameters: five), Is.EqualTo("5"));
            Assert.That(await Run(database, executor, dbname, sql + "2", parameters: none), Is.EqualTo("0"));
            Assert.That(await Run(database, executor, dbname, sql + "1", parameters: five), Is.EqualTo("5"));
            Assert.That(await Run(database, executor, dbname, sql + "1", parameters: none), Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(await Run(database, executor, dbname, sql + "2", parameters: five), Is.EqualTo("5"));
        });
    }

    /// <summary>
    /// Reading a parameter has no effect, so its type stays part of the rule when it does not run, as before this
    /// change. A parameter in an unselected CASE branch is not read: that is the older rule of CASE.
    /// </summary>
    [Test]
    public async Task Parameter_ThatDoesNotWin_StillShowsItsType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> text = new() { ["@p"] = new ColumnValue(ColumnType.String, "x") };
        Dictionary<string, ColumnValue> real = new() { ["@p"] = new ColumnValue(ColumnType.Float64, 2.5) };
        Dictionary<string, ColumnValue> none = new() { ["@p"] = ColumnValue.Null };

        List<ColumnType> widened = [];
        List<ColumnType> plain = [];

        Assert.Multiple(async () =>
        {
            Assert.That(await Run(database, executor, dbname, "SELECT COALESCE(id, @p) FROM t WHERE id = 1", parameters: text), Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(await Run(database, executor, dbname, "SELECT COALESCE(id, @p) FROM t WHERE id = 1", parameters: real, lastCellTypes: widened), Is.EqualTo("1"));
            Assert.That(await Run(database, executor, dbname, "SELECT COALESCE(id, @p) FROM t WHERE id = 1", parameters: none, lastCellTypes: plain), Is.EqualTo("1"));
            Assert.That(widened, Is.EqualTo(new[] { ColumnType.Float64 }));
            Assert.That(plain, Is.EqualTo(new[] { ColumnType.Integer64 }));
        });
    }

    private static async Task<(string Result, string Next)> Draw(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sequence, string statement,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        string result = await Run(database, executor, dbname, statement.Replace("{s}", sequence), commit: true, parameters: parameters);
        string next = await Run(database, executor, dbname, $"SELECT nextval('{sequence}')", commit: true);

        return (result, next);
    }

    /// <summary>
    /// A fresh sequence for each statement. The next value shows how many values the statement used. Each
    /// expected pair was measured on PostgreSQL 16.11.
    /// </summary>
    [TestCase("SELECT COALESCE(1, nextval('{s}'))", "1", "1")]
    [TestCase("SELECT COALESCE(NULL, nextval('{s}'))", "1", "2")]
    [TestCase("SELECT COALESCE(NULL, 1, nextval('{s}'))", "1", "1")]
    [TestCase("SELECT COALESCE(1, nextval('{s}')), nextval('{s}')", "1|1", "2")]
    [TestCase("SELECT nextval('{s}'), COALESCE(1, nextval('{s}'))", "1|1", "2")]
    [TestCase("SELECT COALESCE(1, nextval('{s}')), COALESCE(NULL, nextval('{s}'))", "1|1", "2")]
    [TestCase("SELECT COALESCE(1, abs(nextval('{s}')))", "1", "1")]
    [TestCase("SELECT COALESCE(COALESCE(1, nextval('{s}')), nextval('{s}'))", "1", "1")]
    [TestCase("SELECT COALESCE(abs(nextval('{s}')), 1, nextval('{s}'))", "1", "2")]
    [TestCase("SELECT COALESCE(COALESCE(nextval('{s}'), 1), nextval('{s}'))", "1", "2")]
    [TestCase("SELECT COALESCE(NULLIF(1, 1), nextval('{s}'))", "1", "2")]
    [TestCase("SELECT COALESCE(CAST(NULL AS BIGINT), nextval('{s}'))", "1", "2")]
    [TestCase("SELECT NULLIF(1, nextval('{s}'))", "", "2")]
    [TestCase("SELECT COALESCE(nextval('{s}'), currval('{s}'))", "1", "2")]
    public async Task Sequence_DrawsOnlyForACallThatRuns(string statement, string result, string next)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        (string actualResult, string actualNext) = await Draw(database, executor, dbname, "sq", statement);

        Assert.Multiple(() =>
        {
            Assert.That(actualResult, Is.EqualTo(result));
            Assert.That(actualNext, Is.EqualTo(next));
        });
    }

    /// <summary>
    /// An exhausted reservation is an engine defect, so a call that runs must be counted. Two NULL literals are
    /// known to be NULL here. PostgreSQL 16.11 refuses this form, because it reads two NULL literals as text.
    /// </summary>
    [Test]
    public async Task Sequence_ACallAfterKnownNulls_IsCounted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        Assert.That(await Draw(database, executor, dbname, "sq", "SELECT COALESCE(COALESCE(NULL, NULL), nextval('{s}'))"), Is.EqualTo(("1", "2")));
    }

    /// <summary>
    /// The argument before the call shows its value only when it runs, so the value is reserved and lost. By
    /// decision a gap is allowed here. The result stays correct. PostgreSQL 16.11 gives the next value 2, 1, 2 and 2.
    /// </summary>
    [TestCase("SELECT COALESCE(nextval('{s}'), nextval('{s}'))", "1", "3")]
    [TestCase("SELECT COALESCE(abs(-1), nextval('{s}'))", "1", "2")]
    [TestCase("SELECT COALESCE(COALESCE(NULL, nextval('{s}')), nextval('{s}'))", "1", "3")]
    [TestCase("INSERT INTO w (id, c) VALUES (1, COALESCE(nextval('{s}'), nextval('{s}')))", "1", "3")]
    public async Task Sequence_ACallAfterAnArgumentThatMustRun_LeavesAGap(string statement, string result, string next)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        (string actualResult, string actualNext) = await Draw(database, executor, dbname, "sq", statement);

        Assert.Multiple(() =>
        {
            Assert.That(actualResult, Is.EqualTo(result));
            Assert.That(actualNext, Is.EqualTo(next));
        });
    }

    // PostgreSQL 16.11: c is 1, 1 and 1,1, and the next value is 1, 2 and 2.
    [TestCase("INSERT INTO w (id, c) VALUES (1, COALESCE(1, nextval('{s}')))", "1", "1")]
    [TestCase("INSERT INTO w (id, c) VALUES (1, COALESCE(NULL, nextval('{s}')))", "1", "2")]
    [TestCase("INSERT INTO w (id, c) VALUES (1, COALESCE(1, nextval('{s}'))), (2, COALESCE(NULL, nextval('{s}')))", "1 ; 1", "2")]
    public async Task Sequence_InAnInsert_DrawsOnlyForACallThatRuns(string statement, string rows, string next)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        (_, string actualNext) = await Draw(database, executor, dbname, "sq", statement);

        Assert.Multiple(async () =>
        {
            Assert.That(await Run(database, executor, dbname, "SELECT c FROM w ORDER BY id"), Is.EqualTo(rows));
            Assert.That(actualNext, Is.EqualTo(next));
        });
    }

    /// <summary>
    /// One statement text with a parameter that changes: the reservation must read each execution, and must never
    /// give fewer values than the evaluator draws. PostgreSQL 16.11: 5 leaves the next value at 1, NULL draws.
    /// </summary>
    [Test]
    public async Task Sequence_ParameterDecidesForEachExecution()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        const string statement = "SELECT COALESCE(@p, nextval('sq'))";
        const string insert = "INSERT INTO w (id, c) VALUES (@id, COALESCE(@p, nextval('sq')))";

        Dictionary<string, ColumnValue> five = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 5L) };
        Dictionary<string, ColumnValue> none = new() { ["@p"] = ColumnValue.Null };

        Assert.Multiple(async () =>
        {
            Assert.That(await Draw(database, executor, dbname, "sq", statement, five), Is.EqualTo(("5", "1")));
            Assert.That(await Draw(database, executor, dbname, "sq", statement, none), Is.EqualTo(("2", "3")));
            Assert.That(await Draw(database, executor, dbname, "sq", statement, five), Is.EqualTo(("5", "4")));

            Dictionary<string, ColumnValue> firstRow = new(five) { ["@id"] = new ColumnValue(ColumnType.Integer64, 1L) };
            Dictionary<string, ColumnValue> secondRow = new(none) { ["@id"] = new ColumnValue(ColumnType.Integer64, 2L) };

            Assert.That((await Draw(database, executor, dbname, "sq", insert, firstRow)).Next, Is.EqualTo("5"));
            Assert.That((await Draw(database, executor, dbname, "sq", insert, secondRow)).Next, Is.EqualTo("7"));
            Assert.That(await Run(database, executor, dbname, "SELECT c FROM w ORDER BY id"), Is.EqualTo("5 ; 6"));
        });
    }

    /// <summary>
    /// Over a relation no value is reserved, so the call is refused when a row reaches it, and only then. PostgreSQL
    /// gives 1, 2 and 5 for the refused statement, and draws 2 values.
    /// </summary>
    [TestCase("SELECT id, COALESCE(1, nextval('sq')) FROM t ORDER BY id", "1|1 ; 2|1 ; 3|1")]
    [TestCase("SELECT id, COALESCE(v, nextval('sq')) FROM t ORDER BY id", "1|0 ; 2|2 ; 3|0")]
    [TestCase("SELECT id, COALESCE(n, nextval('sq')) FROM t ORDER BY id", CamusDBErrorCodes.SequenceCallNotAllowedHere)]
    [TestCase("UPDATE t SET n = COALESCE(1, nextval('sq')) WHERE id = 1", "1")]
    [TestCase("UPDATE t SET n = COALESCE(n, nextval('sq')) WHERE id = 1", CamusDBErrorCodes.SequenceCallNotAllowedHere)]
    public async Task Sequence_OverARelation_IsRefusedWhenARowReachesIt(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }
}
