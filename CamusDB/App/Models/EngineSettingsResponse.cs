/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// The engine configuration this node is actually running, resolved.
///
/// <para>It exists because <c>SHOW VARIABLES</c> cannot answer the question a benchmark needs to ask.
/// That statement reports the configuration layer, so an engine key the operator never set comes back
/// unset — accurate, but silent about the mode-specific baseline CamusDB applies underneath it. A run
/// manifest built from it therefore cannot state whether synchronous WAL was on, which is exactly the
/// setting a throughput comparison must never let drift.</para>
///
/// <para><see cref="Settings"/> is keyed by the option's own property name, not by a YAML key. The two
/// vocabularies do not correspond one to one (<c>kahuna.wal_group_commit_linger_ms</c> configures
/// <c>RaftWalGroupCommitLingerMs</c>), and inventing a mapping would assert a correspondence that does
/// not hold.</para>
/// </summary>
/// <param name="Settings">Resolved option name to value, ordered by name.</param>
public sealed record EngineSettingsResponse(IReadOnlyDictionary<string, string> Settings);
