
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.App.Services;

/// <summary>
/// One statement registered by a PREPARE, shared by both transports.
///
/// <para>It holds the exact <see cref="Database"/> and <see cref="Sql"/> string instances the client
/// sent once. Every later execution reuses them, which buys two things: the transport layer never
/// re-parses the SQL (<see cref="RootNodeType"/> already answers the routing question that otherwise
/// forces an uncached parse per request), and the executor's parser cache is probed with an
/// instance-identical key instead of a freshly deserialized one.</para>
///
/// <para><see cref="ParameterNames"/> is the published binding order: a prepared execution's value
/// at index <c>i</c> binds to the name at index <c>i</c>. Names carry their leading <c>@</c> because
/// that is the exact key the executor resolves placeholders by — see
/// <see cref="PlaceholderCollector"/>.</para>
///
/// <para>Instances are immutable and are shared across concurrent executions without
/// synchronization; nothing in this record may become mutable without revisiting that.</para>
/// </summary>
public sealed record PreparedStatement(
    string Database,
    string Sql,
    NodeType RootNodeType,
    string[] ParameterNames)
{
    /// <summary>Number of values a valid execution of this statement must supply.</summary>
    public int ParameterCount => ParameterNames.Length;
}
