/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Collections.Frozen;
using System.Security.Cryptography;
using System.Text;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The hash of the files an extension can load besides its own assembly, taken once at boot, and the
/// per-file hashes the load context checks each dependency against before it loads it.
///
/// <para><b>Why.</b> The SHA-256 of an install record covers the extension's own assembly. An UNSAFE
/// extension also loads private dependencies from its folder (<see cref="ExtensionLoadContext"/>),
/// and without this hash a swapped dependency file would run different code under an unchanged
/// <c>sha256</c>, and two nodes with different dependency files would pass every build check.</para>
///
/// <para><b>Which files.</b> For UNSAFE, every file under the folder of the extension's assembly and
/// its subfolders, except that assembly: the <c>.deps.json</c>, the private assemblies, the native
/// libraries of <c>runtimes/</c>, and anything else there. The dependency resolver can load any
/// assembly of the folder when there is no <c>.deps.json</c>, so a narrower set would miss a file that
/// can load. A file that never loads (a <c>.pdb</c>, another extension in the same folder) still
/// changes the hash; deploy the folder as one unit. For SAFE, the set is empty, because the SAFE load
/// context refuses every assembly that is not part of the framework.</para>
///
/// <para><b>The text that is hashed.</b> One line per file, sorted by ordinal comparison of the path
/// relative to the folder with <c>/</c> separators: the path, a tab, the lowercase hex SHA-256 of the
/// bytes, and a line feed. An empty set hashes the empty text, so a record of an extension without
/// dependencies still pins a value, and a file added later is a different build. The text does not
/// hold the absolute folder, so two nodes that deploy the same files under different paths agree.</para>
///
/// <para><b>Load time.</b> A dependency loads at its first use, long after the boot. The load context
/// therefore reads the bytes of each dependency, compares their hash with <see cref="Files"/>, and
/// loads those same bytes, so a file changed after the boot is refused instead of loaded. A native
/// library is loaded from its path after the check, because the runtime loads native code only from a
/// file.</para>
/// </summary>
internal sealed class ExtensionDependencyHash
{
    private static readonly ExtensionDependencyHash None = new(ExtensionBuild.EmptyDependencySet, FrozenDictionary<string, string>.Empty);

    private ExtensionDependencyHash(string sha256, FrozenDictionary<string, string> files)
    {
        Sha256 = sha256;
        Files = files;
    }

    /// <summary>The lowercase hex SHA-256 of the file list; see the class summary.</summary>
    public string Sha256 { get; }

    /// <summary>The lowercase hex SHA-256 of each file at boot, keyed by its full path.</summary>
    public FrozenDictionary<string, string> Files { get; }

    /// <summary>Hashes the dependency set of the extension whose assembly is <paramref name="assemblyPath"/>.</summary>
    public static ExtensionDependencyHash Compute(string assemblyPath, ExtensionTrust trust)
    {
        if (trust == ExtensionTrust.Safe)
            return None;

        string main = Path.GetFullPath(assemblyPath);
        string folder = Path.GetDirectoryName(main) ?? main;

        List<(string Relative, string Full)> found = [];

        foreach (string file in Directory.EnumerateFiles(folder, "*", SearchOption.AllDirectories))
        {
            string full = Path.GetFullPath(file);

            if (string.Equals(full, main, StringComparison.Ordinal))
                continue;

            found.Add((Path.GetRelativePath(folder, full).Replace(Path.DirectorySeparatorChar, '/'), full));
        }

        if (found.Count == 0)
            return None;

        found.Sort((a, b) => string.CompareOrdinal(a.Relative, b.Relative));

        StringBuilder listing = new(found.Count * 96);
        Dictionary<string, string> files = new(found.Count, StringComparer.Ordinal);

        foreach ((string relative, string full) in found)
        {
            string sha256 = HashFile(full);
            files[full] = sha256;
            listing.Append(relative).Append('\t').Append(sha256).Append('\n');
        }

        string total = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(listing.ToString())));
        return new ExtensionDependencyHash(total, files.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>
    /// Reads <paramref name="path"/> and returns its bytes when they are the bytes hashed at boot.
    /// Throws <see cref="CamusDBErrorCodes.ExtensionAssemblyRefused"/> for a file that was not there at
    /// boot or that changed since.
    /// </summary>
    public byte[] ReadVerified(string path, string extensionName)
    {
        string full = Path.GetFullPath(path);
        byte[] bytes = File.ReadAllBytes(full);
        Verify(full, Convert.ToHexStringLower(SHA256.HashData(bytes)), extensionName);
        return bytes;
    }

    /// <summary>Checks the file at <paramref name="path"/> against its hash at boot; see <see cref="ReadVerified"/>.</summary>
    public void VerifyFile(string path, string extensionName)
    {
        string full = Path.GetFullPath(path);
        Verify(full, HashFile(full), extensionName);
    }

    private void Verify(string full, string sha256, string extensionName)
    {
        if (!Files.TryGetValue(full, out string? expected))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionAssemblyRefused,
                $"Extension '{extensionName}' tried to load '{full}', which was not in its folder when the server started; restart the node to load a new dependency");

        if (!string.Equals(expected, sha256, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionAssemblyRefused,
                $"Extension '{extensionName}' tried to load '{full}', which changed after the server started (sha256 {sha256}, at startup {expected}); restart the node to load a new dependency");
    }

    private static string HashFile(string path)
    {
        using FileStream stream = File.OpenRead(path);
        return Convert.ToHexStringLower(SHA256.HashData(stream));
    }
}

#endif
