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
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Extensions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Startup loading of extension assemblies: each configuration or file problem fails the engine's
/// construction with its own error code, and a good fixture registers its members. The SAFE check
/// is covered here too, because it runs inside the load.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionLoading : ExtensionTestBase
{
    private CamusDBException AssertBootFails(CamusDB.Core.CamusDBOptions options)
    {
        TargetInvocationException? wrapped = null;
        CamusDBException? error = null;

        try
        {
            CreateCommandExecutor(options);
        }
        catch (CamusDBException ex)
        {
            error = ex;
        }
        catch (TargetInvocationException ex)
        {
            wrapped = ex;
        }

        error ??= wrapped?.InnerException as CamusDBException;
        Assert.IsNotNull(error, "the engine must refuse to construct");
        return error!;
    }

    [Test]
    public void NoExtensions_LoadsNothing()
    {
        CommandExecutor executor = CreateCommandExecutor(Options);
        Assert.IsTrue(executor.Extensions.IsEmpty);
    }

    [Test]
    public void GoodFixture_RegistersFunctionsTriggersAndProcedures()
    {
        CommandExecutor executor = CreateCommandExecutor(WithExtensions("Fixture.Good"));

        LoadedExtension extension = executor.Extensions.Extensions.Single();
        Assert.AreEqual("good", extension.Name, "the manifest names the extension");
        Assert.AreEqual("1.0.0", extension.Version);
        Assert.AreEqual("Fixture.Good", extension.AssemblyName);
        Assert.AreEqual(ExtensionTrust.Safe, extension.Trust);
        Assert.AreEqual(64, extension.Sha256.Length);
        Assert.AreEqual(FixturePath("Fixture.Good"), extension.Path);

        CollectionAssert.Contains(extension.Functions, "fx_slugify");
        CollectionAssert.Contains(extension.Functions, "fx_tag_alias");
        CollectionAssert.Contains(extension.Triggers, "CamusDB.Fixtures.Good.AuditTrigger");
        CollectionAssert.Contains(extension.Procedures, "fx_transfer");
        CollectionAssert.DoesNotContain(extension.Triggers, "CamusDB.Fixtures.Good.NotATrigger");

        Assert.IsTrue(extension.FunctionTable.ContainsKey("FX_SLUGIFY"), "function names are case-insensitive");
        Assert.AreSame(extension, executor.Extensions.Find("GOOD", "1.0.0"), "extension names are case-insensitive");
    }

    [Test]
    public async Task ShowExtensions_ReportsTheLoadedAssembly()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(
            Options with
            {
                ExtensionAssemblies = [FixturePath("Fixture.Good")],
                UnsafeExtensionAssemblies = [FixturePath("Fixture.BannedApis")],
            });

        var rows = await ExecQuery(executor, db, "SHOW EXTENSIONS");
        Assert.AreEqual(2, rows.Count);

        var good = rows.Single(r => r["name"].StrValue == "good");
        Assert.AreEqual("1.0.0", good["version"].StrValue);
        Assert.AreEqual("available", good["status"].StrValue, "loaded but not installed in this database");
        Assert.AreEqual("safe", good["trust"].StrValue);
        Assert.AreEqual(executor.Extensions.Extensions[0].Sha256, good["sha256"].StrValue);
        Assert.AreEqual(FixturePath("Fixture.Good"), good["path"].StrValue);
        StringAssert.Contains("fx_slugify", good["functions"].StrValue);
        StringAssert.Contains("CamusDB.Fixtures.Good.AuditTrigger", good["triggers"].StrValue);
        StringAssert.Contains("fx_transfer", good["procedures"].StrValue);

        Assert.AreEqual("unsafe", rows.Single(r => r["name"].StrValue == "banned_apis")["trust"].StrValue);
    }

    [Test]
    public async Task ShowExtensions_NoExtensions_ReturnsNoRows()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options);
        Assert.IsEmpty(await ExecQuery(executor, db, "SHOW EXTENSIONS"));
    }

    [Test]
    public void Sha256_IsTheHashOfTheFileBytes()
    {
        CommandExecutor executor = CreateCommandExecutor(WithExtensions("Fixture.Good"));

        byte[] bytes = File.ReadAllBytes(FixturePath("Fixture.Good"));
        string expected = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(bytes));

        Assert.AreEqual(expected, executor.Extensions.Extensions.Single().Sha256);
    }

    [Test]
    public void RelativePath_FailsBoot()
    {
        CamusDBException ex = AssertBootFails(Options with { ExtensionAssemblies = ["Fixture.Good.dll"] });
        Assert.AreEqual(CamusDBErrorCodes.ExtensionPathInvalid, ex.Code);
    }

    [Test]
    public void PathInBothLists_FailsBoot()
    {
        string path = FixturePath("Fixture.Good");
        CamusDBException ex = AssertBootFails(Options with { ExtensionAssemblies = [path], UnsafeExtensionAssemblies = [path] });
        Assert.AreEqual(CamusDBErrorCodes.ExtensionPathInvalid, ex.Code);
    }

    [Test]
    public void MissingFile_FailsBoot()
    {
        string path = Path.Combine(Path.GetTempPath(), "camusdb-missing-" + Guid.NewGuid().ToString("n") + ".dll");
        CamusDBException ex = AssertBootFails(Options with { ExtensionAssemblies = [path] });
        Assert.AreEqual(CamusDBErrorCodes.ExtensionFileNotFound, ex.Code);
        StringAssert.Contains(path, ex.Message);
    }

    [Test]
    public void NotAnAssembly_FailsBoot()
    {
        string path = Path.Combine(Path.GetTempPath(), "camusdb-garbage-" + Guid.NewGuid().ToString("n") + ".dll");
        File.WriteAllBytes(path, [1, 2, 3, 4, 5]);

        try
        {
            CamusDBException ex = AssertBootFails(Options with { ExtensionAssemblies = [path] });
            Assert.AreEqual(CamusDBErrorCodes.ExtensionNotAnAssembly, ex.Code);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void CollisionWithBuiltIn_FailsBoot()
    {
        CamusDBException ex = AssertBootFails(WithExtensions("Fixture.Collision"));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionDuplicateMember, ex.Code);
        StringAssert.Contains("upper", ex.Message);
    }

    [Test]
    public void SharedMemberNameBetweenExtensions_BothLoad()
    {
        // Both define fx_slugify. Loading makes them available only; installing both in one database
        // is what is refused (see TestExtensionPackaging).
        CommandExecutor executor = CreateCommandExecutor(WithExtensions("Fixture.Good", "Fixture.Duplicate"));
        Assert.AreEqual(2, executor.Extensions.Extensions.Count);
    }

    [Test]
    public void MissingManifest_FailsBoot()
    {
        CamusDBException ex = AssertBootFails(WithExtensions("Fixture.NoManifest"));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionManifestInvalid, ex.Code);
        StringAssert.Contains("CamusExtension", ex.Message);
    }

    [Test]
    public void TwoVersionsOfOneExtension_LoadSideBySide()
    {
        CommandExecutor executor = CreateCommandExecutor(WithExtensions("Fixture.Good", "Fixture.GoodV2"));

        IReadOnlyList<LoadedExtension> versions = executor.Extensions.GetVersions("good");
        Assert.AreEqual(2, versions.Count);
        Assert.AreEqual("2.0.0", versions[0].Version, "highest version first");
        Assert.AreEqual("1.0.0", versions[1].Version);
        Assert.AreNotEqual(versions[0].Sha256, versions[1].Sha256);
        Assert.IsTrue(versions[1].FunctionTable.ContainsKey("fx_v1_only"));
        Assert.IsFalse(versions[0].FunctionTable.ContainsKey("fx_v1_only"));
    }

    [Test]
    public void Manifest_IsReadFromMetadata()
    {
        ExtensionAssemblyIdentity identity = ExtensionAssemblyVerifier.ReadIdentity(File.ReadAllBytes(FixturePath("Fixture.Requires")), "x");

        Assert.AreEqual("needs_good", identity.Manifest!.Name);
        Assert.AreEqual("1.0.0", identity.Manifest.Version);
        CollectionAssert.AreEqual(new[] { "good" }, identity.Manifest.Requires);

        Assert.IsNull(ExtensionAssemblyVerifier.ReadIdentity(File.ReadAllBytes(FixturePath("Fixture.NoManifest")), "x").Manifest);
    }

    [Test]
    public void ExtensionVersion_FollowsSemVerPrecedence()
    {
        static ExtensionVersion V(string text)
        {
            Assert.IsTrue(ExtensionVersion.TryParse(text, out ExtensionVersion version), text);
            return version;
        }

        string[] ordered = ["1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-rc.1", "1.0.0", "1.0.1", "1.2.0", "2.0.0", "10.0.0"];

        for (int i = 1; i < ordered.Length; i++)
            Assert.Less(V(ordered[i - 1]).CompareTo(V(ordered[i])), 0, $"{ordered[i - 1]} < {ordered[i]}");

        foreach (string invalid in new[] { "", "1", "1.0", "1.0.0.0", "01.0.0", "1.0.0+build", "1.0.0-", "1.0.0-a..b", "a.b.c" })
            Assert.IsFalse(ExtensionVersion.TryParse(invalid, out _), invalid);
    }

    [Test]
    public void DuplicateExtensionVersion_FailsBoot()
    {
        // The same assembly copied to a second path is a second configured file with the same
        // extension name and version.
        string copyDirectory = Path.Combine(Path.GetTempPath(), "camusdb-ext-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(copyDirectory);
        string copy = Path.Combine(copyDirectory, "Fixture.Good.dll");
        File.Copy(FixturePath("Fixture.Good"), copy);

        try
        {
            CamusDBException ex = AssertBootFails(Options with { ExtensionAssemblies = [FixturePath("Fixture.Good"), copy] });
            Assert.AreEqual(CamusDBErrorCodes.ExtensionDuplicateAssembly, ex.Code);
        }
        finally
        {
            Directory.Delete(copyDirectory, recursive: true);
        }
    }

    [Test]
    public void ContractVersion_NewerKeyRefused_OlderOrEqualAccepted()
    {
        void Refused(Version contract, Version host) =>
            Assert.AreEqual(CamusDBErrorCodes.ExtensionContractMismatch,
                Assert.Throws<CamusDBException>(() => ExtensionLoader.CheckContractVersion(contract, host, "/x/ext.dll"))!.Code,
                $"{contract} on a {host} server");

        void Accepted(Version? contract, Version host) =>
            Assert.DoesNotThrow(() => ExtensionLoader.CheckContractVersion(contract, host, "/x/ext.dll"), $"{contract} on a {host} server");

        // While the major is 0, the minor is part of the key: a 0.x minor release may break the contract.
        Refused(new Version(0, 17, 0, 0), new Version(0, 16, 0, 0));
        Accepted(new Version(0, 16, 3, 0), new Version(0, 16, 0, 0));
        Accepted(new Version(0, 15, 0, 0), new Version(0, 16, 0, 0));
        Refused(new Version(1, 0, 0, 0), new Version(0, 16, 0, 0));

        // From 1.0 on, the major alone counts.
        Refused(new Version(2, 0, 0, 0), new Version(1, 4, 0, 0));
        Accepted(new Version(1, 9, 0, 0), new Version(1, 4, 0, 0));
        Accepted(new Version(0, 16, 0, 0), new Version(1, 0, 0, 0));
        Accepted(null, new Version(1, 0, 0, 0));
    }

    [Test]
    public void ContractVersion_FollowsTheServerVersion()
    {
        // The contract is versioned with the server, so its csproj <Version> must move with
        // CamusDB/CamusDB.csproj; this fails the build in CI when one is bumped without the other.
        System.Version contract = typeof(CamusDB.Extensions.ICamusTrigger).Assembly.GetName().Version!;
        System.Version server = typeof(CamusDB.App.Controllers.ExecuteSQLController).Assembly.GetName().Version!;

        Assert.AreEqual(server, contract, "CamusDB.Extensions.Abstractions <Version> must equal the server's <Version>");
    }

    // ── SAFE check ────────────────────────────────────────────────────────────

    [Test]
    public void Safe_BannedApis_ListsEveryFinding()
    {
        CamusDBException ex = AssertBootFails(WithExtensions("Fixture.BannedApis"));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionVerificationFailed, ex.Code);

        StringAssert.Contains("type System.IO.File", ex.Message, "a type ban");
        StringAssert.Contains("member System.Environment.Exit", ex.Message, "a member ban");
        StringAssert.Contains("(namespace System.Net)", ex.Message, "a namespace ban");
    }

    [Test]
    public void Safe_PInvoke_Refused()
    {
        CamusDBException ex = AssertBootFails(WithExtensions("Fixture.PInvoke"));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionVerificationFailed, ex.Code);
        StringAssert.Contains("P/Invoke", ex.Message);
    }

    [Test]
    public void Safe_UnsafeCode_Refused()
    {
        CamusDBException ex = AssertBootFails(WithExtensions("Fixture.Unsafe"));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionVerificationFailed, ex.Code);
        StringAssert.Contains("unsafe code", ex.Message);
    }

    [Test]
    public void Safe_PrivateDependency_Refused()
    {
        CamusDBException ex = AssertBootFails(WithExtensions("Fixture.UsesHelper"));
        Assert.AreEqual(CamusDBErrorCodes.ExtensionVerificationFailed, ex.Code);
        StringAssert.Contains("assembly reference Fixture.Helper", ex.Message);
    }

    [Test]
    public void Safe_GoodFixture_HasNoFindings()
    {
        Assert.IsEmpty(ExtensionAssemblyVerifier.Verify(File.ReadAllBytes(FixturePath("Fixture.Good"))));
    }

    [Test]
    public async Task Unsafe_PrivateDependency_LoadsFromTheExtensionFolder()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(
            Options with { UnsafeExtensionAssemblies = [FixturePath("Fixture.UsesHelper")] });

        Assert.AreEqual(ExtensionTrust.Unsafe, executor.Extensions.Extensions.Single().Trust);

        await ExecDdl(executor, dbname, "CREATE EXTENSION uses_helper");
        Assert.AreEqual("hello world", (await Scalar(executor, dbname, "SELECT fx_helper_hello('world')")).StrValue);
    }

    [Test]
    public void Unsafe_BannedApis_Load()
    {
        CommandExecutor executor = CreateCommandExecutor(
            Options with { UnsafeExtensionAssemblies = [FixturePath("Fixture.BannedApis")] });

        Assert.IsTrue(executor.Extensions.Extensions.Single().FunctionTable.ContainsKey("fx_banned_file"));
    }

    [Test]
    public void SafeLoadContext_RefusesAssemblyOutsideTheAllowList()
    {
        // The runtime half of the SAFE check: a dynamic load that the static scan cannot see.
        ExtensionLoadContext context = new("probe", FixturePath("Fixture.Good"), ExtensionTrust.Safe);

        // The runtime wraps an exception from Load in a FileLoadException; the refusal is its inner
        // exception, so the extension's own catch sees a failed load, never a loaded assembly.
        FileLoadException ex = Assert.Throws<FileLoadException>(
            () => context.LoadFromAssemblyName(new AssemblyName("System.Net.Http")))!;
        Assert.IsInstanceOf<CamusDBException>(ex.InnerException);
        Assert.AreEqual(CamusDBErrorCodes.ExtensionAssemblyRefused, ((CamusDBException)ex.InnerException!).Code);

        // An allowed framework facade still resolves (from the default context).
        Assert.IsNotNull(context.LoadFromAssemblyName(new AssemblyName("netstandard")));
    }

    [Test]
    public void LoadContext_ReturnsTheServerContract()
    {
        ExtensionLoadContext context = new("probe", FixturePath("Fixture.Good"), ExtensionTrust.Safe);

        Assembly contract = context.LoadFromAssemblyName(new AssemblyName(ExtensionAssemblyVerifier.ContractAssemblyName));
        Assert.AreSame(typeof(CamusDB.Extensions.ICamusTrigger).Assembly, contract);
    }
}
