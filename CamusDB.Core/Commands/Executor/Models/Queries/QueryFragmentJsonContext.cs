/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Source-generated serialization for <see cref="QueryFragmentRequest"/>, used by the in-process
/// fragment transport.
///
/// <para>It exists because that transport ships in the browser build, and a trimmed application
/// turns reflection-based <c>System.Text.Json</c> off: a reflection call there compiles with an
/// IL2026 warning and fails at run time. The property names match the reflection defaults, so the
/// bytes are the same ones the HTTP fragment controller reads.</para>
/// </summary>
[JsonSerializable(typeof(QueryFragmentRequest))]
internal sealed partial class QueryFragmentJsonContext : JsonSerializerContext;
