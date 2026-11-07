/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// A call of an extension function shows the type its C# signature declares to COALESCE, IFNULL, NVL, CASE
/// and IF, as a built-in call does. The adapter converts the return value to that one type, so the
/// declaration is exact.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestExtensionFunctionResultTypes : ExtensionTestBase
{
    [Test]
    public async Task ACallWithNoCommonType_IsRefusedBeforeAnyRow()
    {
        (string db, _, var executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE em (id INT64 PRIMARY KEY, n INT64, s STRING)");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT COALESCE(1, fx_slugify(s)) FROM em"));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(error.Message, Does.Contain("Integer64 and String"));
    }

    [Test]
    public async Task ACall_ShowsItsDeclaredType_InEveryRow()
    {
        (string db, _, var executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, n INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id, n) VALUES (1, NULL), (2, 5)");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT id, COALESCE(n, fx_half(2.0)) AS x FROM t ORDER BY id");

        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(r => r["x"].Type), Is.All.EqualTo(ColumnType.Float64));
            Assert.That(rows.Select(r => r["x"].FloatValue), Is.EqualTo(new[] { 1.0, 5.0 }));
        });
    }
}
