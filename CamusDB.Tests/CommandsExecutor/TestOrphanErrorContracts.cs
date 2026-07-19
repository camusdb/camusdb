
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The recovery error codes must map to the right HTTP status so SQL <c>RELINK</c> surfaces the correct
/// contract: a missing orphan is a 404, a taken target name is a 409 (both permanent caller mistakes,
/// not server errors). The DDL/query/non-query controller paths all route <c>CamusDBException.Code</c>
/// through <see cref="CamusDBErrorCodes.GetHttpStatus"/>.
/// </summary>
[TestFixture]
internal sealed class TestOrphanErrorContracts
{
    [Test]
    public void OrphanNotFound_MapsTo404()
        => Assert.AreEqual(404, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.OrphanNotFound));

    [Test]
    public void AlreadyExists_MapTo409()
    {
        Assert.AreEqual(409, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.DatabaseAlreadyExists));
        Assert.AreEqual(409, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.TableAlreadyExists));
    }

    [Test]
    public void UnmappedCode_DefaultsTo500()
        => Assert.AreEqual(500, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.SystemSpaceCorrupt));
}
