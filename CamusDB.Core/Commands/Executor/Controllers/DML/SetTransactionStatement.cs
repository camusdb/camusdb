
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// The <c>SET TRANSACTION</c> family — isolation level, locking mode, and priority.
///
/// <para>Shared by every SQL entry point rather than owned by one, because a client may route these
/// to whichever endpoint it uses for non-row statements. They are also the one statement family
/// exempt from the "mark the transaction as having executed a statement" gate: standard SQL requires
/// them to come first in a transaction, so counting them would make them retroactively illegal.</para>
/// </summary>
internal static class SetTransactionStatement
{
    /// <summary>
    /// Applies a <c>SET TRANSACTION</c> / <c>SET TRANSACTION LOCKING</c> statement to the in-flight
    /// transaction state. Shared by both SQL entry points: these statements return no rows, so a
    /// client may route them to either <see cref="ExecuteSQLQuery"/> (the row-returning endpoint) or
    /// <see cref="ExecuteNonSQLQuery"/> (the "rows affected" endpoint). Keeping the parse-and-apply
    /// logic in one place means the two dispatchers cannot drift — a missing case here is why
    /// <c>SET TRANSACTION LOCKING</c> previously threw "Unknown non-query AST stmt" when a client
    /// sent it to the non-query endpoint. Callers must exempt these node types from the
    /// "must be the first statement" gate before invoking this.
    /// </summary>
    internal static void Apply(NodeAst ast, ExecuteSQLTicket ticket)
    {
        switch (ast.nodeType)
        {
            case NodeType.SetTransaction:
                {
                    // yytext holds the isolation level ("Serializable"); leftAst.yytext holds the mode
                    // ("ReadOnly" or "ReadWrite"). Both are set by the grammar.
                    if (!Enum.TryParse(ast.yytext, out CamusIsolationLevel level))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown isolation level '{ast.yytext}' in SET TRANSACTION");

                    string modeStr = ast.leftAst?.yytext ?? "ReadWrite";
                    if (!Enum.TryParse(modeStr, out CamusTransactionMode mode))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown transaction mode '{modeStr}' in SET TRANSACTION");

                    // ApplyIsolationLevel throws if locks are already held, ensuring the level
                    // change cannot silently skip required read-locks already missed.
                    ticket.TxnState.ApplyIsolationLevel(level, mode);
                    return;
                }

            case NodeType.SetTransactionLocking:
                {
                    // yytext carries the resolved enum name ("Pessimistic" or "Optimistic") set by the grammar.
                    if (!Enum.TryParse(ast.yytext, out Kahuna.Shared.KeyValue.KeyValueTransactionLocking locking))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown locking mode '{ast.yytext}' in SET TRANSACTION LOCKING");

                    ticket.TxnState.ApplyLocking(locking);
                    return;
                }

            case NodeType.SetTransactionPriority:
                {
                    // yytext carries the resolved enum name ("Background" … "Critical") set by the grammar.
                    if (!Enum.TryParse(ast.yytext, out Kahuna.Shared.KeyValue.TransactionPriority priority))
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                            $"Unknown transaction priority '{ast.yytext}' in SET TRANSACTION PRIORITY");

                    ticket.TxnState.ApplyPriority(priority);
                    return;
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt,
                    "SetTransactionStatement.Apply called with non-SET node: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Whether the node is one of the <c>SET TRANSACTION</c> family. These configure the in-flight
    /// transaction rather than reading or writing data, so they are exempt from the
    /// "first statement" latch — a <c>SET TRANSACTION</c> must be able to follow another one.
    ///
    /// <para>Both SQL dispatchers gate on this. Adding a new <c>SET TRANSACTION</c> node type without
    /// adding it here silently makes that statement count as a data statement, which then rejects any
    /// <c>SET TRANSACTION</c> issued after it.</para>
    /// </summary>
    internal static bool IsSetTransactionStatement(NodeType nodeType) =>
        nodeType is NodeType.SetTransaction
                 or NodeType.SetTransactionLocking
                 or NodeType.SetTransactionPriority;}
