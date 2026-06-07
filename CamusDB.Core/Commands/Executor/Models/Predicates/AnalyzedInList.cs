
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Predicates;

/// <summary>
/// An indexable IN-list predicate: <c>column IN (v1, v2, ...)</c> where every list item
/// is a constant or resolved parameter (no columns, no expressions, no subqueries).
/// NULL list values are already filtered out — a NULL list item matches nothing in SQL.
/// </summary>
public sealed record AnalyzedInList(
    string ColumnName,
    IReadOnlyList<ColumnValue> Values,
    NodeAst Conjunct);
