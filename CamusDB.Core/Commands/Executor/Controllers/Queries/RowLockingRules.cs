
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Decides whether a <c>SELECT … FOR UPDATE</c> may run, before it takes any lock. Every refusal is a
/// statement error that leaves the transaction as it was, except that the transports roll back a
/// transaction whose statement failed.
///
/// <para><b>The transaction rules.</b></para>
/// <list type="bullet">
/// <item><description>A read-only transaction, and the identity-less snapshot an autocommit read runs
/// in, cannot own a lock: refused with <see cref="CamusDBErrorCodes.InvalidInput"/>. The transports
/// begin a writable autocommit transaction for this statement
/// (<see cref="StatementScope.BeginsWritableTransaction(NodeAst)"/>), so a caller meets this only in
/// an explicit read-only transaction or through an internal entry point.</description></item>
/// <item><description>An optimistic transaction takes no lock at all, so a FOR UPDATE there would tell
/// the caller it holds a lock that does not exist: refused with
/// <see cref="CamusDBErrorCodes.InvalidInput"/>.</description></item>
/// <item><description>SKIP LOCKED at Serializable is refused with
/// <see cref="CamusDBErrorCodes.FeatureNotSupported"/>. Two queue workers that scan the same range both
/// hold a shared predicate lock over it, so the second one's later UPDATE of its own row still meets the
/// first one's lock and fails, which defeats SKIP LOCKED. FOR UPDATE and NOWAIT work at both
/// levels.</description></item>
/// </list>
///
/// <para><b>The shape rules.</b> The statement must read one base table, with no DISTINCT, GROUP BY,
/// HAVING or aggregate: those produce rows that are not table rows, so there is no row to lock. A join,
/// a view and a set-returning function in FROM are refused for the same reason in this version (a
/// joined row does not carry the row id of each table it came from). These are PostgreSQL's rules plus
/// the join limit.</para>
/// </summary>
internal static class RowLockingRules
{
    /// <summary>Maps the parsed wait policy of a <see cref="NodeType.LockingClause"/>.</summary>
    internal static RowLockWaitPolicy PolicyOf(NodeAst lockingClause) => RowLockingAst.PolicyOf(lockingClause) switch
    {
        RowLockingAst.NoWait => RowLockWaitPolicy.NoWait,
        RowLockingAst.SkipLocked => RowLockWaitPolicy.SkipLocked,
        _ => RowLockWaitPolicy.Wait,
    };

    /// <summary>
    /// The checks that need only the statement and its transaction: the query checks of
    /// <see cref="RequireLockableSelect"/>, then the transaction rules of the class summary
    /// (<see cref="RequireLockingTransaction"/>).
    /// </summary>
    internal static void RequireLockableStatement(NodeAst select, KvTransaction tx, RowLockWaitPolicy policy)
    {
        RequireLockableSelect(select);
        RequireLockingTransaction(tx, policy);
    }

    /// <summary>
    /// The checks that need only the parsed query: a FROM clause and no <c>AS OF SYSTEM TIME</c>. Plain
    /// EXPLAIN of a locking read runs these and not the transaction rules: it takes no lock, so, as in
    /// PostgreSQL, it works in a read-only or optimistic transaction too.
    /// </summary>
    internal static void RequireLockableSelect(NodeAst select)
    {
        if (select.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE needs a FROM clause that names a table");

        if (select.extendedSeven is not null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE cannot be used with AS OF SYSTEM TIME: a historical row cannot be locked");
    }

    /// <summary>The transaction rules of the class summary, for a statement that takes the row locks.</summary>
    internal static void RequireLockingTransaction(KvTransaction tx, RowLockWaitPolicy policy)
    {
        // Read-only covers the identity-less autocommit snapshot too. A writable transaction may not
        // have its identity yet (the Kahuna session starts with its first lock), so the identity is not
        // what is checked here.
        if (tx.IsReadOnly)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE is not allowed in a read-only transaction");

        if (tx.Locking == KeyValueTransactionLocking.Optimistic)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE requires LOCKING PESSIMISTIC");

        if (policy == RowLockWaitPolicy.SkipLocked && tx.IsolationLevel == CamusIsolationLevel.Serializable)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                "FOR UPDATE SKIP LOCKED is not supported at SERIALIZABLE; use SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    }

    /// <summary>
    /// The checks that need the bound query: one base table, no grouping, DISTINCT or aggregate, and an
    /// <c>OF</c> name that names that table (by its name or its alias).
    /// </summary>
    internal static void RequireLockableQuery(BoundSelectQuery bound, QueryTicket ticket, NodeAst lockingClause)
    {
        if (bound.IsMultiSource || bound.Query.Source is not TableSource source)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "FOR UPDATE supports a query over one table; joins, views, derived tables and table functions are not supported");

        if (ticket.IsDistinct)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE is not allowed with DISTINCT");

        if (ticket.GroupBy is { Count: > 0 })
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE is not allowed with GROUP BY");

        if (ticket.Having is not null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE is not allowed with HAVING");

        if (ticket.Projection is { Count: > 0 } && QueryPostScanPipeline.HasAggregation(ticket.Projection, ticket))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE is not allowed with aggregate functions");

        TableDescriptor table = bound.PrimaryTable;

        if (table.Schema.IsMaterializedView)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"FOR UPDATE cannot lock rows of materialized view '{table.Name}'");

        if (lockingClause.leftAst?.yytext is { } ofName
            && !string.Equals(ofName, source.TableName, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(ofName, source.Alias, StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"FOR UPDATE OF '{ofName}' does not name a table in the FROM clause");
    }

    /// <summary>
    /// Checks UPDATE on the locked table. SELECT rides the ambient scope and the table open already
    /// checked it; only one privilege can ride that scope, so the second one is checked here, as
    /// TRUNCATE checks DROP. An absent scope is an internal caller or a forwarded statement that was
    /// authorized where it entered, and passes, as at the per-table chokepoint.
    /// </summary>
    internal static void RequireUpdatePrivilege(DatabaseDescriptor database, TableDescriptor table)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        AuthorizationScope scope = AuthorizationContext.Current;
        if (scope.Principal is not { } principal || scope.RequiredPrivilege is null)
            return;

        if (!principal.HasPrivilege(Privilege.Update, database.Id, table.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"Missing {Privilege.Update} privilege on table '{database.Name}.{table.Name}'");
    }
}
