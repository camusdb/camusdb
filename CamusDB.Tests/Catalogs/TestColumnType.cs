
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// Locks the persisted integer values for ColumnType.
/// These values are stored in schema JSON — changing them would corrupt existing databases.
/// </summary>
[TestFixture]
public class TestColumnType
{
    [Test]
    public void ExistingColumnTypeValues_AreUnchanged()
    {
        Assert.That((int)ColumnType.Null, Is.EqualTo(0));
        Assert.That((int)ColumnType.Id, Is.EqualTo(1));
        Assert.That((int)ColumnType.Integer64, Is.EqualTo(2));
        Assert.That((int)ColumnType.String, Is.EqualTo(3));
        Assert.That((int)ColumnType.Bool, Is.EqualTo(4));
        Assert.That((int)ColumnType.Float64, Is.EqualTo(5));
    }

    [Test]
    public void NewColumnTypeValues_AreCorrect()
    {
        Assert.That((int)ColumnType.Float32, Is.EqualTo(6));
        Assert.That((int)ColumnType.Bytes, Is.EqualTo(7));
        Assert.That((int)ColumnType.Date, Is.EqualTo(8));
        Assert.That((int)ColumnType.DateTime, Is.EqualTo(9));
        Assert.That((int)ColumnType.Array, Is.EqualTo(10));
    }
}
