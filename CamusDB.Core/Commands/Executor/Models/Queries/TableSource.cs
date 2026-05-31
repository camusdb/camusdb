
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A base-table reference in the <c>FROM</c> clause.
/// </summary>
/// <param name="TableName">Catalog table name.</param>
/// <param name="Alias">Optional source alias (<c>FROM users u</c>).</param>
/// <param name="ForcedIndexName">
/// Optional index hint from <c>@{FORCE_INDEX=name}</c>. Primary-key hints use the internal
/// <c>~pk</c> name after ticket creation.
/// </param>
public sealed record TableSource(
    string TableName,
    string? Alias = null,
    string? ForcedIndexName = null) : QuerySource;
