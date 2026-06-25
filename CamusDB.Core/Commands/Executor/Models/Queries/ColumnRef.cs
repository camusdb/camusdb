
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A bound column reference after name resolution.
/// </summary>
/// <param name="SourceAlias">
/// Table or derived-table alias, or <see langword="null"/> when the reference is unqualified.
/// </param>
/// <param name="ColumnName">Resolved column name in the source schema.</param>
public sealed record ColumnRef(string? SourceAlias, string ColumnName);
