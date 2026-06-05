/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// Pure unit tests for <see cref="SchemaChangeCoordinator.ComputeTransitionPath"/> — no cluster
/// needed, so they stay in the fast suite. The cluster-driven coordinator tests live in
/// CamusDB.Cluster.Tests (TestSchemaChangeCoordinatorCluster).
/// </summary>
[TestFixture]
public sealed class TestSchemaChangeCoordinatorPath
{
    [Test]
    public void ComputePath_AbsentToPublic_ReturnsFullAddSequence()
    {
        SchemaElementState[] path = SchemaChangeCoordinator.ComputeTransitionPath(
            SchemaElementState.Absent, SchemaElementState.Public);

        Assert.AreEqual(3, path.Length);
        Assert.AreEqual(SchemaElementState.DeleteOnly, path[0]);
        Assert.AreEqual(SchemaElementState.WriteOnly, path[1]);
        Assert.AreEqual(SchemaElementState.Public, path[2]);
    }

    [Test]
    public void ComputePath_DeleteOnlyToPublic_SkipsAbsent()
    {
        SchemaElementState[] path = SchemaChangeCoordinator.ComputeTransitionPath(
            SchemaElementState.DeleteOnly, SchemaElementState.Public);

        Assert.AreEqual(2, path.Length);
        Assert.AreEqual(SchemaElementState.WriteOnly, path[0]);
        Assert.AreEqual(SchemaElementState.Public, path[1]);
    }

    [Test]
    public void ComputePath_WriteOnlyToPublic_SingleStep()
    {
        SchemaElementState[] path = SchemaChangeCoordinator.ComputeTransitionPath(
            SchemaElementState.WriteOnly, SchemaElementState.Public);

        Assert.AreEqual(1, path.Length);
        Assert.AreEqual(SchemaElementState.Public, path[0]);
    }

    [Test]
    public void ComputePath_PublicToAbsent_ReturnsFullDropSequence()
    {
        SchemaElementState[] path = SchemaChangeCoordinator.ComputeTransitionPath(
            SchemaElementState.Public, SchemaElementState.Absent);

        Assert.AreEqual(3, path.Length);
        Assert.AreEqual(SchemaElementState.WriteOnly, path[0]);
        Assert.AreEqual(SchemaElementState.DeleteOnly, path[1]);
        Assert.AreEqual(SchemaElementState.Absent, path[2]);
    }

    [Test]
    public void ComputePath_SameState_ReturnsEmpty()
    {
        SchemaElementState[] path = SchemaChangeCoordinator.ComputeTransitionPath(
            SchemaElementState.Public, SchemaElementState.Public);

        Assert.AreEqual(0, path.Length);
    }
}
