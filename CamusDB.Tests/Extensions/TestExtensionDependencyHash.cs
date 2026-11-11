/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// The hash of the files an UNSAFE extension can load from its folder: it is taken at boot, pinned by
/// the install record next to the assembly hash, compared on every node, and checked again when a
/// dependency loads, so a swapped dependency file is refused instead of run under an unchanged
/// <c>sha256</c>. Each test works on its own copy of the <c>Fixture.UsesHelper</c> folder.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestExtensionDependencyHash : ExtensionTestBase
{
    private readonly List<string> copies = [];

    [TearDown]
    public void DeleteCopies()
    {
        foreach (string copy in copies)
        {
            try
            {
                Directory.Delete(copy, recursive: true);
            }
            catch (IOException)
            {
                // A loaded assembly can keep its file open on some platforms; the temp folder is cleaned later.
            }
        }

        copies.Clear();
    }

    /// <summary>A fresh copy of the folder of <c>Fixture.UsesHelper</c>; returns the path of its assembly.</summary>
    private string CopyUsesHelper()
    {
        string source = Path.GetDirectoryName(FixturePath("Fixture.UsesHelper"))!;
        string target = Path.Combine(Path.GetTempPath(), "camusdb-deps-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(target);
        copies.Add(target);

        foreach (string file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(target, Path.GetFileName(file)));

        return Path.Combine(target, "Fixture.UsesHelper.dll");
    }

    private CamusDBOptions Unsafe(string assembly) => Options with { UnsafeExtensionAssemblies = [assembly] };

    [Test]
    public void TheEmptySet_IsTheHashOfTheEmptyText()
    {
        Assert.AreEqual(Convert.ToHexStringLower(SHA256.HashData(Array.Empty<byte>())), ExtensionBuild.EmptyDependencySet);
    }

    [Test]
    public async Task ASafeExtension_HasTheEmptySet_AndAnUnsafeOne_HashesItsFolder()
    {
        string assembly = CopyUsesHelper();
        (string db, _, CommandExecutor executor) = await CreateDatabase(
            WithExtensions("Fixture.Good") with { UnsafeExtensionAssemblies = [assembly] });

        LoadedExtension good = executor.Extensions.Extensions.Single(e => e.Name == "good");
        LoadedExtension usesHelper = executor.Extensions.Extensions.Single(e => e.Name == "uses_helper");

        Assert.AreEqual(ExtensionBuild.EmptyDependencySet, good.DependenciesSha256, "a SAFE extension can load no private file");
        Assert.AreNotEqual(ExtensionBuild.EmptyDependencySet, usesHelper.DependenciesSha256);
        Assert.AreEqual(ExtensionDependencyHash.Compute(assembly, ExtensionTrust.Unsafe).Sha256, usesHelper.DependenciesSha256);

        // The same files under another folder give the same hash: the text holds relative paths only.
        Assert.AreEqual(usesHelper.DependenciesSha256, ExtensionDependencyHash.Compute(CopyUsesHelper(), ExtensionTrust.Unsafe).Sha256);

        await ExecDdl(executor, db, "CREATE EXTENSION uses_helper");
        Assert.AreEqual(usesHelper.DependenciesSha256, (await executor.OpenDatabase(db)).Schema.Extensions["uses_helper"].DependenciesSha256,
            "the install record pins the hash");

        Dictionary<string, ColumnValue> row = (await ExecQuery(executor, db, "SHOW EXTENSIONS")).Single(r => r["name"].StrValue == "uses_helper");
        Assert.AreEqual(usesHelper.DependenciesSha256, row["dependencies_sha256"].StrValue);
    }

    [Test]
    public async Task ADependencyChangedAfterTheBoot_IsRefusedAtItsFirstLoad()
    {
        string assembly = CopyUsesHelper();
        (string db, _, CommandExecutor executor) = await CreateDatabase(Unsafe(assembly));
        await ExecDdl(executor, db, "CREATE EXTENSION uses_helper");

        // The helper has not loaded yet: it loads when the function first runs.
        string helper = Path.Combine(Path.GetDirectoryName(assembly)!, "Fixture.Helper.dll");
        File.AppendAllText(helper, "changed");

        Exception failed = Assert.CatchAsync(() => Scalar(executor, db, "SELECT fx_helper_hello('world')"))!;
        StringAssert.Contains("changed after the server started", failed.ToString());
    }

    [Test]
    public async Task ANodeWithOtherDependencyFiles_DoesNotBindTheInstalledBuild_UntilAnUpdatePinsItsFiles()
    {
        (string db, _, CommandExecutor first) = await CreateDatabase(Unsafe(CopyUsesHelper()));
        await ExecDdl(first, db, "CREATE EXTENSION uses_helper");
        Assert.AreEqual("hello world", (await Scalar(first, db, "SELECT fx_helper_hello('world')")).StrValue);

        // Another engine on the same storage, like a node whose folder holds one more file.
        string other = CopyUsesHelper();
        File.WriteAllText(Path.Combine(Path.GetDirectoryName(other)!, "extra.dll"), "not an assembly");
        CommandExecutor second = CreateCommandExecutor(Unsafe(other));

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => Scalar(second, db, "SELECT fx_helper_hello('world')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotLoadedOnNode, refused.Code);
        StringAssert.Contains("different dependency files", refused.Message);

        Dictionary<string, ColumnValue> missing = (await ExecQuery(second, db, "SHOW EXTENSIONS")).Single(r => r["status"].StrValue == "missing");
        Assert.AreEqual("uses_helper", missing["name"].StrValue);

        // An update to the same version pins the files of the node that runs it.
        await ExecDdl(second, db, "ALTER EXTENSION uses_helper UPDATE TO '1.0.0'");
        Assert.AreEqual("hello world", (await Scalar(second, db, "SELECT fx_helper_hello('world')")).StrValue);

        // Now a fresh engine with the first set of files is the one that does not match.
        CommandExecutor third = CreateCommandExecutor(Unsafe(CopyUsesHelper()));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionNotLoadedOnNode, await Code(() => Scalar(third, db, "SELECT fx_helper_hello('world')")));
    }

    [Test]
    public void ARecordOrAProbeAnswerFromBeforeTheHash_DoesNotCompareDependencies()
    {
        string pinned = ExtensionDependencyHash.Compute(CopyUsesHelper(), ExtensionTrust.Unsafe).Sha256;

        Assert.IsTrue(ExtensionBuild.SameDependencies("", pinned), "a record installed before the field pins nothing");
        Assert.IsTrue(ExtensionBuild.SameDependencies(pinned, ""), "a node that predates the field reports nothing");
        Assert.IsTrue(ExtensionBuild.SameDependencies(pinned, pinned.ToUpperInvariant()));
        Assert.IsFalse(ExtensionBuild.SameDependencies(pinned, ExtensionBuild.EmptyDependencySet));
    }

    private static async Task<string> Code(Func<Task> action) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await action())!.Code;
}
