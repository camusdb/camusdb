
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Output column metadata for a bound derived table source (QP5.5).
/// </summary>
public sealed record DerivedColumnSchema(string Name, ColumnType Type);
