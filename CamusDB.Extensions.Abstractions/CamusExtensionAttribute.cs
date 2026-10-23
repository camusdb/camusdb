/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions;

/// <summary>
/// The manifest of an extension assembly: the name and the version that SQL refers to, and the other
/// extensions it needs. Every extension assembly must carry exactly one, at assembly level:
///
/// <code>[assembly: CamusExtension("geo", "1.2.0", Requires = new[] { "units" })]</code>
///
/// <para><b>Available and installed.</b> Listing the assembly in the server configuration makes the
/// extension <i>available</i> on that node. <c>CREATE EXTENSION geo</c> makes it <i>installed</i> in
/// one database, and its functions, aggregates, table functions, triggers and procedures resolve only in a database where it is
/// installed. A node can load two versions of one extension side by side; each database's installed
/// version selects which one it uses, and <c>ALTER EXTENSION geo UPDATE TO '2.0.0'</c> moves a
/// database to another version in one step.</para>
///
/// <para><b>Read without running code.</b> The server reads this attribute from the assembly's
/// metadata before it loads the assembly, so the constructor arguments must be constants, as the C#
/// compiler already requires for an attribute. An assembly without the attribute fails the boot.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false, Inherited = false)]
public sealed class CamusExtensionAttribute : Attribute
{
    /// <summary>Creates the manifest.</summary>
    /// <param name="name">The SQL name of the extension: a letter or underscore followed by letters,
    /// digits and underscores. Case-insensitive. It names the extension in <c>CREATE EXTENSION</c>
    /// and qualifies a function call (<c>geo.distance(a, b)</c>).</param>
    /// <param name="version">The version, as <c>major.minor.patch</c> with an optional
    /// <c>-prerelease</c> suffix (SemVer). <c>CREATE EXTENSION</c> without a version installs the
    /// highest version the node loads.</param>
    public CamusExtensionAttribute(string name, string version)
    {
        Name = name;
        Version = version;
    }

    /// <summary>The SQL name of the extension.</summary>
    public string Name { get; }

    /// <summary>The SemVer version of this build of the extension.</summary>
    public string Version { get; }

    /// <summary>The names of the extensions that must be installed in a database before this one
    /// can be installed there. Any installed version satisfies an entry.</summary>
    public string[] Requires { get; set; } = Array.Empty<string>();
}
