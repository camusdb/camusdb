
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Collects the ordered output column schema for a row-producing query so the presentation layer
/// can encode rows positionally without relying on per-row column-name dictionaries.
/// Populated by <see cref="CommandExecutor.ExecuteSQLQuery"/> before the cursor is returned.
/// </summary>
public sealed class QuerySchemaHolder
{
    public IReadOnlyList<DerivedColumnSchema> Schema { get; set; } = [];
}
