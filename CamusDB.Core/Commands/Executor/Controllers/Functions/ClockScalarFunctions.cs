/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The functions that report the current time, and the rule that decides which time each one reports:
/// <list type="bullet">
/// <item><description><c>now()</c>, <c>current_timestamp()</c>, <c>transaction_timestamp()</c> and
/// <c>current_date()</c> report the start of the transaction (<see cref="KvTransaction.StartTime"/>),
/// as in PostgreSQL. Every row and every statement of one transaction sees the same value, so
/// <c>INSERT … SELECT now() FROM t</c> writes one time and two statements of a <c>BEGIN … COMMIT</c>
/// agree. A no-argument <c>unix_timestamp()</c> reports the same instant
/// (<see cref="DateTimeScalarFunctions"/>).</description></item>
/// <item><description><c>statement_timestamp()</c> reports the start of the statement: one value for
/// the whole statement, a new value for the next one.</description></item>
/// <item><description><c>clock_timestamp()</c>, and MySQL's <c>sysdate()</c>, read the clock on each
/// call.</description></item>
/// </list>
///
/// <para><b>How the times reach the evaluator.</b> Through the statement's parameter dictionary, the
/// channel <see cref="SessionScalarFunctions"/> uses for the session, and for the same reason: rows are
/// produced lazily, after the entry method returned, and the dictionary travels with the ticket into
/// every subquery, spill and cursor. The SQL entry points call <see cref="AttachStatementClock(ExecuteSQLTicket, NodeAst)"/>
/// through <see cref="StatementScopedValues.Attach"/>, together with the session snapshot, and again
/// where a view body is expanded into the statement. The keys start with a doubled <c>@</c>, which no
/// SQL placeholder can produce.</para>
///
/// <para><b>Attached only when the statement calls a clock function.</b> The result-cache fingerprint
/// hashes every parameter, so a time attached to every statement would make no two statements share a
/// cache entry. A statement that calls one bypasses the result cache anyway: these functions are
/// volatile.</para>
///
/// <para><b>Attaching is idempotent.</b> A ticket that already carries the times keeps them, so the
/// second attach after a view expansion cannot move the statement time of a statement that already
/// started.</para>
///
/// <para><b>A column default reads the same times.</b> A <c>DEFAULT now()</c> is not in the INSERT's
/// AST, so the insert paths attach the times when the table has such a default
/// (<see cref="AttachStatementClock(ExecuteSQLTicket)"/>), and the default is evaluated with the
/// statement's parameters (<see cref="ScalarFunctionEvaluator.EvaluateNullary"/>). One multi-row INSERT
/// therefore writes one default time, which also equals an explicit <c>now()</c> in its VALUES.</para>
///
/// <para><b>A distributed query computes no time remotely.</b> A volatile expression is never shipped
/// in a query fragment (<see cref="Queries.FragmentFilterShippability"/>), so the coordinator node
/// evaluates every clock call with its own statement's parameters.</para>
/// </summary>
internal static class ClockScalarFunctions
{
    /// <summary>Reserved parameter key carrying the start of the transaction, as a <c>DateTime</c>.</summary>
    internal const string TransactionTimestampKey = "@@transaction_timestamp";

    /// <summary>Reserved parameter key carrying the start of the statement, as a <c>DateTime</c>.</summary>
    internal const string StatementTimestampKey = "@@statement_timestamp";

    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "current_timestamp",
            Aliases = ["now", "transaction_timestamp"],
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            ReadsStatementClock = true,
            StatementEvaluator = static (calledName, _, parameters) =>
                new ColumnValue(ColumnType.DateTime, ReadTransactionTime(calledName, parameters).Ticks),
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.DateTime,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "current_date",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            ReadsStatementClock = true,
            StatementEvaluator = static (calledName, _, parameters) =>
                new ColumnValue(ColumnType.Date, ReadTransactionTime(calledName, parameters).Date.Ticks),
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.Date,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "statement_timestamp",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            ReadsStatementClock = true,
            StatementEvaluator = static (calledName, _, parameters) =>
                new ColumnValue(ColumnType.DateTime, Read(calledName, StatementTimestampKey, parameters).Ticks),
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.DateTime,
        });

        // Reads the clock on each call, so it needs no statement and stays on the plain evaluator.
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "clock_timestamp",
            Aliases = ["sysdate"],
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = static (_, _) => new ColumnValue(ColumnType.DateTime, DateTime.UtcNow.Ticks),
            InferReturnType = static _ => ColumnType.DateTime,
        });
    }

    /// <summary>
    /// Returns <paramref name="ticket"/> with the transaction and statement times added to its
    /// parameters, or the ticket unchanged when <paramref name="ast"/> calls no function that reads
    /// them. Called by the SQL entry points before any sub-ticket is built, and again wherever an AST
    /// grows a call the entry point could not see (a view body expanded into the statement).
    /// </summary>
    public static ExecuteSQLTicket AttachStatementClock(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (!ScalarFunctionEvaluator.ContainsStatementClockFunction(ast))
            return ticket;

        return AttachStatementClock(ticket);
    }

    /// <summary>
    /// Returns <paramref name="ticket"/> with the transaction and statement times added to its
    /// parameters, whatever its AST calls. The insert paths call it when a column default reads the
    /// clock, because such a default is not in the statement's AST.
    ///
    /// <para>A ticket that already carries the times is returned unchanged, so the statement time
    /// is fixed by the first attach. The caller's dictionary is never changed: it belongs to the
    /// transport and may be used again for the next statement, so a copy with the same key comparer
    /// gets the times.</para>
    ///
    /// <para>The statement time is never earlier than the transaction time, so
    /// <c>statement_timestamp() &gt;= transaction_timestamp()</c> holds even when the wall clock is
    /// behind the HLC the transaction started from. A server-level statement has no transaction;
    /// both times are then the start of the statement.</para>
    /// </summary>
    public static ExecuteSQLTicket AttachStatementClock(ExecuteSQLTicket ticket)
    {
        if (ticket.Parameters is not null && ticket.Parameters.ContainsKey(TransactionTimestampKey))
            return ticket;

        Dictionary<string, ColumnValue> parameters = ticket.Parameters is null
            ? new Dictionary<string, ColumnValue>(2)
            : new Dictionary<string, ColumnValue>(ticket.Parameters, ticket.Parameters.Comparer);

        long statementTicks = DateTime.UtcNow.Ticks;

        // Null only for a server-level statement (SHOW DATABASES and similar), which the transports
        // run without a transaction.
        KvTransaction? transaction = ticket.TxnState;
        long transactionTicks = transaction is null ? statementTicks : transaction.StartTime.Ticks;

        parameters[TransactionTimestampKey] = new ColumnValue(ColumnType.DateTime, transactionTicks);
        parameters[StatementTimestampKey] = new ColumnValue(ColumnType.DateTime, Math.Max(statementTicks, transactionTicks));

        // Every per-statement value the incoming ticket carried is carried forward: this rebuild
        // exists to add the times, and a value dropped here is lost for the rest of the statement.
        return new ExecuteSQLTicket(
            ticket.TxnState,
            ticket.DatabaseName,
            ticket.Sql,
            parameters,
            ticket.Principal,
            ticket.CancellationToken,
            ticket.Probe,
            ticket.Routing,
            ticket.RetryableAborts,
            ticket.DiscardReturningRows
        );
    }

    /// <summary>
    /// A new parameter dictionary that carries <paramref name="instant"/> as both the transaction
    /// and the statement time. For work that writes rows without a SQL statement of its own, such as
    /// the fill of a column added with <c>DEFAULT now()</c>, which must give every row one time.
    /// </summary>
    internal static Dictionary<string, ColumnValue> CreateFixedClock(DateTime instant)
    {
        ColumnValue value = new(ColumnType.DateTime, instant.ToUniversalTime().Ticks);

        return new Dictionary<string, ColumnValue>(2)
        {
            [TransactionTimestampKey] = value,
            [StatementTimestampKey] = value,
        };
    }

    /// <summary>
    /// The transaction and statement times of <paramref name="parameters"/>, in a new dictionary
    /// without the statement's other parameters; or null when the statement carries no times.
    ///
    /// <para>For a planner path that takes index bounds from a filter but must not resolve a
    /// <c>@name</c> placeholder, such as a join leaf. With these values a clock call becomes a bound
    /// at the statement's time, the time the row filter reads. Without them the call refuses to
    /// evaluate, the bound is dropped, and the leaf scans the whole table. A bound read from the wall
    /// clock instead would be later than the filter's time, and the scan would miss rows that the
    /// filter accepts.</para>
    /// </summary>
    internal static Dictionary<string, ColumnValue>? ClockValuesOnly(IReadOnlyDictionary<string, ColumnValue>? parameters)
    {
        if (parameters is null
            || !parameters.TryGetValue(TransactionTimestampKey, out ColumnValue? transactionTime)
            || !parameters.TryGetValue(StatementTimestampKey, out ColumnValue? statementTime))
            return null;

        return new Dictionary<string, ColumnValue>(2)
        {
            [TransactionTimestampKey] = transactionTime,
            [StatementTimestampKey] = statementTime,
        };
    }

    /// <summary>
    /// The transaction start the statement carries. Also read by the no-argument
    /// <c>unix_timestamp()</c>.
    /// </summary>
    internal static DateTime ReadTransactionTime(string calledName, IReadOnlyDictionary<string, ColumnValue>? parameters)
        => Read(calledName, TransactionTimestampKey, parameters);

    private static DateTime Read(string calledName, string key, IReadOnlyDictionary<string, ColumnValue>? parameters)
    {
        if (parameters is null || !parameters.TryGetValue(key, out ColumnValue? value) || value.Type != ColumnType.DateTime)
            throw OutsideStatement(calledName);

        return new DateTime(value.LongValue, DateTimeKind.Utc);
    }

    /// <summary>
    /// Raised where a clock function is evaluated without the times of a statement. Every SQL entry
    /// point and every insert path that applies a clock default attaches them, so reaching this means
    /// a new evaluation path was added without carrying them. Answering with the wall clock there
    /// instead would bring back, silently, the per-call time this class exists to remove.
    /// </summary>
    private static CamusDBException OutsideStatement(string calledName)
    {
        return new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' is only available in a statement executed within a transaction");
    }
}
