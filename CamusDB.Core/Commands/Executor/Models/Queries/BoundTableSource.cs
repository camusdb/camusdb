
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A table source from a logical query after catalog lookup and descriptor open.
/// </summary>
/// <param name="Source">Logical table reference from the parsed query.</param>
/// <param name="Table">Opened table descriptor for execution.</param>
/// <param name="Alias">
/// Effective source alias used for qualified column keys. When the query omits an explicit alias,
/// this is the table name.
/// </param>
public sealed record BoundTableSource(TableSource Source, TableDescriptor Table, string Alias);
