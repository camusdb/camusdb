/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Where the statement check (<see cref="Controllers.Queries.ConditionalResultTypes.Check"/>) records the
/// types it decides from the column types, for the evaluator to read while the statement runs. A SELECT
/// records on its binding (<see cref="QueryRowNameResolver"/>). An UPDATE, a DELETE and the DO UPDATE of
/// an upsert have no binding and record on a <see cref="Controllers.Queries.StatementExpressionTypes"/>.
/// Both key a type by node identity, and neither stores it on the node, because the parse cache shares
/// one tree between databases.
/// </summary>
internal interface IExpressionTypeRecorder
{
    /// <summary>Records the result type of a CASE, COALESCE, IFNULL, IF, GREATEST or LEAST node.</summary>
    void RecordConditionalResultType(NodeAst node, ExpressionType resultType);

    /// <summary>
    /// Records the element type of an <c>ARRAY[…]</c> node. The type can be partial: an element whose type
    /// only its value shows is combined in when the array is evaluated.
    /// </summary>
    void RecordArrayElementType(NodeAst node, ExpressionType elementType);
}
