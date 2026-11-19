
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Resolves <c>UPDATE … SET column = DEFAULT</c> to the column's default before the statement binds.
/// </summary>
/// <remarks>
/// <para>A constant default becomes a statement parameter holding the value. A function default
/// becomes a call of that function, so it is evaluated for each row, as an INSERT evaluates it, and a
/// clock function reads the statement's time. A column with no default is set to NULL, as in
/// PostgreSQL.</para>
///
/// <para>A default that draws from a sequence, including an identity column's, is refused: an UPDATE
/// cannot draw sequence values, because the values are reserved before the rows are known and an
/// UPDATE does not know how many rows it changes.</para>
///
/// <para>Runs after a write through a view is rewritten onto its table, so the default is the
/// table's.</para>
/// </remarks>
internal static class UpdateDefaults
{
    private const string ParameterPrefix = "@__set_default_";

    internal static (NodeAst Ast, ExecuteSQLTicket Ticket) Resolve(Schema schema, NodeAst ast, ExecuteSQLTicket ticket)
    {
        if (ast.nodeType != NodeType.Update || ast.rightAst is null || !HasDefault(ast.rightAst))
            return (ast, ticket);

        // An unknown table is reported when the statement opens it; the DEFAULT becomes a NULL meanwhile.
        schema.Tables.TryGetValue(ast.leftAst?.yytext ?? "", out TableSchema? table);

        Dictionary<string, ColumnValue>? parameters = null;
        bool readsClock = false;

        NodeAst Replace(NodeAst list)
        {
            if (list.nodeType == NodeType.UpdateList)
            {
                NodeAst? left = list.leftAst is null ? null : Replace(list.leftAst);
                NodeAst? right = list.rightAst is null ? null : Replace(list.rightAst);

                return ReferenceEquals(left, list.leftAst) && ReferenceEquals(right, list.rightAst)
                    ? list
                    : new NodeAst(NodeType.UpdateList, left, right, null, null, null, null, null, list.yytext);
            }

            if (list.nodeType != NodeType.UpdateItem || list.rightAst?.nodeType != NodeType.ExprDefault)
                return list;

            string target = list.leftAst?.yytext ?? "";
            string columnName = target.Contains('.') ? target[(target.LastIndexOf('.') + 1)..] : target;
            TableColumnSchema? column = table?.Columns?.Find(
                c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));

            return new NodeAst(NodeType.UpdateItem, list.leftAst, DefaultExpression(column), null, null, null, null, null, list.yytext);
        }

        NodeAst DefaultExpression(TableColumnSchema? column)
        {
            if (column is null)
                return NodeAst.Null;

            if (column.DefaultSequenceId is { Length: > 0 })
                throw new CamusDBException(
                    CamusDBErrorCodes.FeatureNotSupported,
                    $"UPDATE ... SET {column.Name} = DEFAULT is not supported: the default of column '{column.Name}' " +
                    "draws from a sequence, and an UPDATE cannot draw sequence values. Assign a value instead.");

            if (column.DefaultFunction is { } function)
            {
                readsClock |= ScalarFunctionEvaluator.IsStatementClockFunction(function);
                return new NodeAst(
                    NodeType.ExprFuncCall,
                    new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, function),
                    null, null, null, null, null, null, null);
            }

            if (column.DefaultValue is not { } constant)
                return NodeAst.Null;

            parameters ??= ticket.Parameters is null
                ? new Dictionary<string, ColumnValue>()
                : new Dictionary<string, ColumnValue>(ticket.Parameters, ticket.Parameters.Comparer);

            string name = ParameterPrefix + column.Name;
            parameters[name] = constant;

            return new NodeAst(NodeType.Placeholder, null, null, null, null, null, null, null, name);
        }

        NodeAst updated = new(
            ast.nodeType, ast.leftAst, Replace(ast.rightAst), ast.extendedOne, ast.extendedTwo, ast.extendedThree,
            ast.extendedFour, ast.extendedFive, ast.yytext, ast.extendedSix, ast.extendedSeven);

        if (parameters is not null)
            ticket = WithParameters(ticket, parameters);

        if (readsClock)
            ticket = ClockScalarFunctions.AttachStatementClock(ticket);

        return (updated, ticket);
    }

    private static bool HasDefault(NodeAst list) => list.nodeType switch
    {
        NodeType.UpdateList => (list.leftAst is not null && HasDefault(list.leftAst))
                               || (list.rightAst is not null && HasDefault(list.rightAst)),
        NodeType.UpdateItem => list.rightAst?.nodeType == NodeType.ExprDefault,
        _ => false,
    };

    /// <summary>
    /// <paramref name="ticket"/> with <paramref name="parameters"/>. Every other per-statement value is
    /// carried forward; the caller's dictionary is never changed.
    /// </summary>
    private static ExecuteSQLTicket WithParameters(ExecuteSQLTicket ticket, Dictionary<string, ColumnValue> parameters)
        => new(
            ticket.TxnState,
            ticket.DatabaseName,
            ticket.Sql,
            parameters,
            ticket.Principal,
            ticket.CancellationToken,
            ticket.Probe,
            ticket.Routing,
            ticket.RetryableAborts,
            ticket.DiscardReturningRows);
}
