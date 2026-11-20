
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
/// Resolves <c>column = DEFAULT</c> in a SET list to the column's default before the statement binds.
/// The SET list is the one of <c>UPDATE … SET</c>, or the one of <c>INSERT … ON CONFLICT … DO UPDATE
/// SET</c> (VALUES and SELECT forms).
/// </summary>
/// <remarks>
/// <para>A constant default becomes a statement parameter holding the value. A function default
/// becomes a call of that function, so it is evaluated for each row, as an INSERT evaluates it, and a
/// clock function reads the statement's time. A column with no default is set to NULL, as in
/// PostgreSQL.</para>
///
/// <para>A default that draws from a sequence, including an identity column's, is refused: an update
/// cannot draw sequence values, because the values are reserved before the rows are known and an
/// update does not know how many rows it changes. <c>nextval</c> written in the same SET list is
/// refused for the same reason (<see cref="SequenceStatementBinder"/>).</para>
///
/// <para>Only a whole SET value is resolved. A <c>DEFAULT</c> inside a larger expression stays in the
/// tree, and the binder refuses it.</para>
///
/// <para>Runs after a write through a view is rewritten onto its table, so the default is the
/// table's. An INSERT with ON CONFLICT cannot target a view.</para>
/// </remarks>
internal static class UpdateDefaults
{
    private const string ParameterPrefix = "@__set_default_";

    /// <summary>
    /// Returns <paramref name="ast"/> with every whole-value <c>DEFAULT</c> of its SET list replaced,
    /// and <paramref name="ticket"/> with the parameters and statement times the replacements read. A
    /// statement with no such <c>DEFAULT</c> is returned unchanged, with no allocation. The caller's
    /// parameter dictionary is never changed.
    /// </summary>
    internal static (NodeAst Ast, ExecuteSQLTicket Ticket) Resolve(Schema schema, NodeAst ast, ExecuteSQLTicket ticket)
    {
        if (SetListOf(ast) is not { } setList || !HasDefault(setList))
            return (ast, ticket);

        string statementForm = ast.nodeType == NodeType.Update ? "UPDATE ... SET" : "ON CONFLICT DO UPDATE SET";

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
                    $"{statementForm} {column.Name} = DEFAULT is not supported: the default of column '{column.Name}' " +
                    "draws from a sequence, and an update cannot draw sequence values. Assign a value instead.");

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

        NodeAst resolved = Replace(setList);
        NodeAst updated;

        if (ast.nodeType == NodeType.Update)
        {
            updated = new(
                ast.nodeType, ast.leftAst, resolved, ast.extendedOne, ast.extendedTwo, ast.extendedThree,
                ast.extendedFour, ast.extendedFive, ast.yytext, ast.extendedSix, ast.extendedSeven);
        }
        else
        {
            // The ON CONFLICT node keeps its target (leftAst), WHERE (extendedOne) and action (yytext).
            NodeAst onConflict = ast.extendedThree!;
            NodeAst resolvedOnConflict = new(
                onConflict.nodeType, onConflict.leftAst, resolved, onConflict.extendedOne, onConflict.extendedTwo,
                onConflict.extendedThree, onConflict.extendedFour, onConflict.extendedFive, onConflict.yytext,
                onConflict.extendedSix, onConflict.extendedSeven);

            updated = new(
                ast.nodeType, ast.leftAst, ast.rightAst, ast.extendedOne, ast.extendedTwo, resolvedOnConflict,
                ast.extendedFour, ast.extendedFive, ast.yytext, ast.extendedSix, ast.extendedSeven);
        }

        if (parameters is not null)
            ticket = WithParameters(ticket, parameters);

        if (readsClock)
            ticket = ClockScalarFunctions.AttachStatementClock(ticket);

        return (updated, ticket);
    }

    /// <summary>
    /// The SET list of an UPDATE, or the DO UPDATE SET list of an INSERT's ON CONFLICT clause (the
    /// grammar puts it in the ON CONFLICT node's rightAst). Null for any other statement, and for DO
    /// NOTHING and UPSERT, which have no SET list.
    /// </summary>
    private static NodeAst? SetListOf(NodeAst ast) => ast.nodeType switch
    {
        NodeType.Update => ast.rightAst,
        NodeType.Insert or NodeType.InsertSelect when ast.extendedThree is { nodeType: NodeType.OnConflict } onConflict => onConflict.rightAst,
        _ => null,
    };

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
