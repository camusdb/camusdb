/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// Which layer of the configuration chain supplied a value, recorded per key so an operator can be
/// told not just what a setting is but why it is that.
///
/// <para>The layers are the ones <see cref="ConfigResolver"/> already merges — command line beats
/// environment beats YAML beats the built-in default — and each writer records itself as it applies
/// its layer, so the last writer wins for the same reason its value does. A key nobody recorded is
/// <see cref="Default"/> by absence; there is deliberately no explicit "default" entry to keep in
/// sync with the option record.</para>
///
/// <para>This is a diagnostic, not an input: nothing in the engine branches on it. It exists so that
/// "which configuration is this node running?" is answerable from a SQL prompt instead of by
/// reconstructing the merge by hand from a file, a unit file, and a command line.</para>
/// </summary>
public enum ConfigValueSource
{
    /// <summary>Nobody overrode the setting; it holds the value compiled into <see cref="CamusDBOptions"/>.</summary>
    Default = 0,

    /// <summary>The key was present in the YAML document that was loaded.</summary>
    ConfigFile = 1,

    /// <summary>An environment variable supplied the value, overriding any file value.</summary>
    Environment = 2,

    /// <summary>A command-line flag supplied the value, overriding both file and environment.</summary>
    CommandLine = 3,

    /// <summary>
    /// A replicated cluster setting supplied the value, overriding file, environment, and command
    /// line for that key. Sits above <see cref="CommandLine"/> because the cluster layer is applied
    /// last: a fleet-wide change that silently lost to one node's local YAML would be exactly the
    /// invisible drift runtime cluster settings exist to eliminate. Restart-class keys are never
    /// cluster-overridable, so node-defining values keep their local provenance.
    /// </summary>
    Cluster = 4,
}
