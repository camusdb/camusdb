
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// One entry in the <c>SELECT</c> projection list.
/// </summary>
/// <param name="Expression">Parsed projection expression (column, aggregate, or expression tree).</param>
/// <param name="OutputName">
/// Result column name after alias resolution (explicit alias or generated name).
/// </param>
public sealed record ProjectionItem(NodeAst Expression, string? OutputName);
