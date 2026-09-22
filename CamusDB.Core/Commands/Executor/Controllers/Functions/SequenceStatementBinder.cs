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
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Prepares a statement that calls a sequence function: it decides how many values the statement
/// will draw, reserves them in one round trip per sequence, and puts the reservation where the
/// expression evaluator can reach it.
///
/// <para><b>Why the count has to be known first.</b> Drawing a value is an asynchronous, routed
/// call and expression evaluation is synchronous, so the only way to avoid one network round trip
/// per row is to reserve the whole run up front. Everything else here follows from that: which
/// positions <c>nextval</c> is allowed in, why it is refused elsewhere, and why an exhausted run
/// is reported as an engine defect rather than quietly refilled.</para>
///
/// <para><b>Where a sequence call is allowed</b> — the positions whose count the engine can bound
/// before the statement runs:</para>
/// <list type="bullet">
/// <item><description>an <c>INSERT … VALUES</c> expression, and a column default the target list
/// omits: the row count is in the statement;</description></item>
/// <item><description>an <c>INSERT … SELECT</c> column default: the source rows are all read
/// before the first one is written, so the count is exact by the time it is needed;</description></item>
/// <item><description>a <c>SELECT</c> with no <c>FROM</c> clause: one value per call
/// site.</description></item>
/// </list>
///
/// <para><b><c>UPDATE … SET c = nextval(…)</c> is refused today.</b> The matched-row count is not
/// known when the statement is bound, and the update path drains its matched rows in chunks, so
/// supporting it means reserving per chunk rather than once. That is a real gap rather than a
/// design position — it is refused with the same message as every other unbounded position, so
/// nobody discovers it from their data.</para>
///
/// <para><b>Everywhere else it is refused</b>, with the error naming the rule — a <c>WHERE</c>
/// clause, an aggregate argument, a subquery, an <c>ORDER BY</c>, a stored view body, a
/// <c>CHECK</c> condition, or a <c>SELECT</c> over a relation. In each of those the expression is
/// evaluated an unpredictable number of times, and a counter is a surprising thing to consume an
/// unpredictable number of values from. Refusing is the honest answer; silently evaluating it
/// per row would put the round trip back.</para>
///
/// <para><b>Allocation happens before the mutation begins.</b> A sequence failure must not be able
/// to fail a transaction late, after rows have already been written.</para>
/// </summary>
internal sealed class SequenceStatementBinder
{
    private readonly SequenceAllocator allocator;

    internal SequenceStatementBinder(SequenceAllocator allocator)
    {
        ArgumentNullException.ThrowIfNull(allocator);

        this.allocator = allocator;
    }

    // setval's value arguments are literals or bind parameters, never row columns — the binder
    // runs before any row exists — so one shared empty row is reused instead of allocating one.
    private static readonly Dictionary<string, ColumnValue> EmptyRow = new();

    /// <summary>The sequence functions, by the name they are called under.</summary>
    private static readonly HashSet<string> FunctionNames =
        new(StringComparer.OrdinalIgnoreCase) { "nextval", "currval", "lastval", "setval" };

    /// <summary>True when <paramref name="ast"/> or any descendant calls a sequence function.</summary>
    internal static bool ContainsSequenceCall(NodeAst? ast) => NodeAstWalk.Any(ast, IsSequenceCall);

    private static bool IsSequenceCall(NodeAst node)
        => node.nodeType == NodeType.ExprFuncCall
           && node.leftAst?.yytext is { Length: > 0 } name
           && FunctionNames.Contains(name);

    /// <summary>
    /// Binds a statement whose sequence calls all sit inside <paramref name="allowedRegion"/>.
    ///
    /// <para>Returns the ticket unchanged when the statement calls no sequence function, which is
    /// what keeps the reserved parameter keys out of the result-cache fingerprint for every query
    /// that does not need them.</para>
    /// </summary>
    /// <param name="statementAst">
    /// The whole statement, scanned to find calls outside the allowed region. Null where the
    /// caller has nothing to position-check — a path whose statement was already bound elsewhere
    /// and that is here only to reserve for column defaults.
    /// </param>
    /// <param name="allowedRegion">The part of the statement a sequence call may appear in, or null when none may.</param>
    /// <param name="rowCount">
    /// How many times the allowed region will be evaluated. One for a single-row statement; the
    /// VALUES row count for an INSERT; the chunk size for a pumped statement.
    /// </param>
    /// <param name="defaultSequenceIds">
    /// One entry per column whose default draws from a sequence — with duplicates, because two
    /// columns defaulting from one sequence consume two values a row. Counted separately from the
    /// call sites because a default carries no call site at all: the column is simply absent from
    /// the statement.
    /// </param>
    internal async Task<ExecuteSQLTicket> BindAsync(
        DatabaseDescriptor database,
        ExecuteSQLTicket ticket,
        NodeAst? statementAst,
        NodeAst? allowedRegion,
        int rowCount,
        IReadOnlyCollection<string>? defaultSequenceIds,
        string statementKind,
        CancellationToken cancellationToken,
        bool regionIsProjectionList = false)
    {
        List<NodeAst> callsInStatement = [];
        Collect(statementAst, callsInStatement);

        bool hasDefaults = defaultSequenceIds is { Count: > 0 };

        if (callsInStatement.Count == 0 && !hasDefaults)
            return ticket;

        List<NodeAst> callsInRegion = [];
        Collect(allowedRegion, callsInRegion);

        // A call the statement makes but the allowed region does not hold is a call the engine
        // cannot count. Compared by count rather than by identity because the two walks visit the
        // same node instances: the region is a subtree of the statement.
        if (callsInStatement.Count != callsInRegion.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceCallNotAllowedHere,
                $"A sequence function was used somewhere {statementKind} cannot bound how many values it " +
                "would draw. nextval, currval, lastval and setval are accepted in an INSERT ... VALUES list, " +
                "a column DEFAULT, and a SELECT with no FROM clause — not in a WHERE clause, an aggregate, " +
                "a subquery, an ORDER BY, or an UPDATE assignment.");

        Dictionary<string, ColumnValue> parameters = ticket.Parameters is null
            ? new Dictionary<string, ColumnValue>(8)
            : new Dictionary<string, ColumnValue>(ticket.Parameters, ticket.Parameters.Comparer);

        // Demand is aggregated by the sequence's **immutable id**, not by the name the statement
        // wrote. Two names cannot reach one sequence today, but an explicit call and a column
        // default routinely do — and a second reservation for the same sequence would replace the
        // first one's cursor, losing the values it had already bought. One id, one reservation,
        // one cursor.
        Dictionary<string, SequenceSchema> resolvedById = new(StringComparer.Ordinal);
        Dictionary<string, int> demandById = new(StringComparer.Ordinal);
        List<SetValRequest> setValRequests = [];

        HashSet<NodeAst> standalonePositions = regionIsProjectionList
            ? CollectStandalonePositions(allowedRegion)
            : [];

        foreach (NodeAst call in callsInRegion)
        {
            string functionName = call.leftAst!.yytext!.ToLowerInvariant();

            if (functionName == "lastval")
                continue;

            string sequenceName = ReadSequenceNameArgument(functionName, call);
            SequenceSchema sequence = RequireSequence(database, sequenceName);

            resolvedById[sequence.Id!] = sequence;
            demandById.TryAdd(sequence.Id!, 0);

            parameters[SequenceScalarFunctions.IdKeyPrefix + sequenceName.ToLowerInvariant()] =
                new ColumnValue(ColumnType.String, sequence.Id!);

            if (functionName == "nextval")
            {
                // One value per call site, not one per row. A VALUES list holds its own copy of the
                // call for every row it writes, so the call sites already carry the row count;
                // multiplying by it again over-reserves, and every value over-reserved is a gap.
                demandById[sequence.Id!] += 1;
                continue;
            }

            if (functionName == "setval")
            {
                RequireStandaloneSetVal(call, standalonePositions, setValRequests.Count);
                setValRequests.Add(ReadSetValRequest(call, sequence, ticket.Parameters));
            }

            // currval draws nothing. The name still had to resolve, so an unknown sequence is
            // reported here rather than as a missing parameter key deep in the evaluator.
        }

        if (hasDefaults)
        {
            // Counted per defaulted column, not per distinct sequence: two omitted columns that
            // default from one sequence consume two values a row, and a set would have bought one.
            foreach (string sequenceId in defaultSequenceIds!)
            {
                SequenceSchema? sequence = database.Schema.FindSequenceById(sequenceId);

                if (sequence is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.SequenceDoesntExist,
                        $"A column default draws from sequence '{sequenceId}', which no longer exists");

                resolvedById[sequenceId] = sequence;
                demandById.TryGetValue(sequenceId, out int already);
                demandById[sequenceId] = already + Math.Max(rowCount, 1);
            }
        }

        // Seeded for every sequence the statement names, before anything is reserved. A statement
        // that both reads currval and draws from the same sequence needs the prior value present:
        // seeding only the no-demand case made `SELECT currval('s'), nextval('s')` report the
        // sequence as undefined.
        foreach (SequenceSchema sequence in resolvedById.Values)
            SeedRecordedValue(parameters, sequence, ticket.TxnState);

        // setval runs before anything is reserved: it moves the counter, so a run reserved from the
        // old incarnation would be voided by it anyway.
        foreach (SetValRequest request in setValRequests)
            await ApplySetValAsync(database, request, parameters, ticket.TxnState, cancellationToken).ConfigureAwait(false);

        foreach ((string sequenceId, int needed) in demandById)
        {
            if (needed == 0)
                continue;

            await ReserveIntoAsync(database, resolvedById[sequenceId], needed, parameters, ticket.TxnState, cancellationToken)
                .ConfigureAwait(false);
        }

        SeedLastValue(parameters, ticket.TxnState);

        // Every per-statement value the incoming ticket carried is carried forward; a value dropped
        // here is silently lost for the rest of the statement.
        return new ExecuteSQLTicket(
            ticket.TxnState,
            ticket.DatabaseName,
            ticket.Sql,
            parameters,
            ticket.Principal,
            ticket.CancellationToken,
            ticket.Probe,
            ticket.Routing);
    }

    /// <summary>
    /// Reserves a run and installs it as the statement's cursor for one sequence. Called again for
    /// the same sequence when a pumped statement needs a second chunk; the later reservation
    /// replaces the cursor, and whatever was left of the earlier run is abandoned — a gap, which a
    /// sequence is allowed to have.
    /// </summary>
    internal async Task ReserveIntoAsync(
        DatabaseDescriptor database,
        SequenceSchema sequence,
        int count,
        Dictionary<string, ColumnValue> parameters,
        KvTransaction transaction,
        CancellationToken cancellationToken)
    {
        // The key ties the reservation to this transaction and this allocation, so a retry after a
        // timeout replays the same allocation instead of consuming a second run. The ordinal comes
        // from the transaction, not the statement: a per-statement counter restarts at one every
        // statement, and two statements of one transaction would then reserve under the same key —
        // which Kahuna answers by replaying the first allocation, silently handing the second
        // statement values it already had. Replay is guaranteed only inside Kahuna's retention
        // window; past it a retry draws fresh values, which is a gap and therefore allowed.
        string idempotencyKey =
            $"{transaction.UniqueId}:{sequence.Id}:{transaction.NextSequenceAllocationOrdinal()}:{count}";

        SequenceRun run = await allocator.ReserveAsync(
            database.Kahuna.Kahuna, database.Id, sequence, count, idempotencyKey, cancellationToken).ConfigureAwait(false);

        lock (parameters)
        {
            parameters[SequenceScalarFunctions.NextKeyPrefix + sequence.Id] = new ColumnValue(ColumnType.Integer64, run.Start);
            parameters[SequenceScalarFunctions.RemainingKeyPrefix + sequence.Id] = new ColumnValue(ColumnType.Integer64, run.Count);
            parameters[SequenceScalarFunctions.StepKeyPrefix + sequence.Id] = new ColumnValue(ColumnType.Integer64, StepOf(run));
        }
    }

    /// <summary>
    /// The step between the values of <paramref name="run"/>, derived from the run itself.
    /// </summary>
    /// <remarks>
    /// <para><b>Not the catalog's increment.</b> The counter and the catalog record are changed by
    /// two separate writes — <c>ALTER SEQUENCE</c> moves the counter first and publishes the record
    /// afterwards — so a reservation can be allocated with one increment while the record still
    /// describes another. Stepping a run by the record's number then either overshoots its end
    /// (raising the exhausted-run error on a reservation that was perfectly good) or emits values
    /// between the ones Kahuna allocated. The run's own endpoints cannot disagree with themselves.</para>
    ///
    /// <para>A single-value run has no step to derive and never needs one: the cursor hands its one
    /// value out and stops on the remaining count. Widened arithmetic because a run that spans most
    /// of the 64-bit range would overflow the subtraction.</para>
    /// </remarks>
    private static long StepOf(SequenceRun run)
        => run.Count > 1 ? (long)(((Int128)run.End - run.Start) / (run.Count - 1)) : 0;

    /// <summary>
    /// Copies the values the statement actually drew into the transaction, so <c>currval</c> and
    /// <c>lastval</c> report what was issued.
    /// </summary>
    /// <remarks>
    /// <para>Called once the statement's expressions have been evaluated, and deliberately not at
    /// reservation time. A reservation is an upper bound on what a statement will use: a value
    /// inside a branch the evaluator does not take is never drawn, and recording the run's end
    /// would report a number no row holds. A statement that draws nothing leaves the transaction's
    /// previous value alone, which is also correct.</para>
    ///
    /// <para>The draws write the dictionary in evaluation order, so the last write per sequence is
    /// the last value that sequence issued — which is what <c>lastval</c> means.</para>
    /// </remarks>
    internal static void RecordDrawnValues(Dictionary<string, ColumnValue>? parameters, KvTransaction transaction)
    {
        if (parameters is null)
            return;

        lock (parameters)
        {
            // Only a sequence the statement really drew from carries a drawn marker. A value that
            // was merely seeded here so currval could read it must not be re-recorded: doing so
            // would make it the transaction's most recent draw and move lastval onto a sequence
            // this statement never advanced.
            List<string> drawnIds = [];

            foreach (string key in parameters.Keys)
            {
                if (key.StartsWith(SequenceScalarFunctions.DrawnKeyPrefix, StringComparison.Ordinal))
                    drawnIds.Add(key[SequenceScalarFunctions.DrawnKeyPrefix.Length..]);
            }

            foreach (string sequenceId in drawnIds)
            {
                if (parameters.TryGetValue(SequenceScalarFunctions.CurrentKeyPrefix + sequenceId, out ColumnValue? value))
                    transaction.RecordSequenceValue(sequenceId, value.LongValue);
            }

            // Recorded last so the transaction's "most recent" is the sequence the statement drew
            // from last, which is what lastval means. Without it the order would be whatever the
            // dictionary happened to enumerate.
            if (parameters.TryGetValue(SequenceScalarFunctions.LastDrawnIdKey, out ColumnValue? lastId)
                && lastId.StrValue is { Length: > 0 } lastDrawnId
                && parameters.TryGetValue(SequenceScalarFunctions.CurrentKeyPrefix + lastDrawnId, out ColumnValue? lastValue))
                transaction.RecordSequenceValue(lastDrawnId, lastValue.LongValue);
        }
    }

    /// <summary>Puts the value this transaction already drew from a sequence where <c>currval</c> can read it.</summary>
    private static void SeedRecordedValue(
        Dictionary<string, ColumnValue> parameters, SequenceSchema sequence, KvTransaction transaction)
    {
        if (transaction.TryGetSequenceValue(sequence.Id!, out long recorded))
            parameters[SequenceScalarFunctions.CurrentKeyPrefix + sequence.Id] = new ColumnValue(ColumnType.Integer64, recorded);
    }

    private static void SeedLastValue(Dictionary<string, ColumnValue> parameters, KvTransaction transaction)
    {
        if (parameters.ContainsKey(SequenceScalarFunctions.LastValueKey))
            return;

        if (transaction.TryGetLastSequenceValue(out _, out long value))
            parameters[SequenceScalarFunctions.LastValueKey] = new ColumnValue(ColumnType.Integer64, value);
    }

    /// <summary>
    /// The top-level items of a projection list, with any alias unwrapped: the only positions a
    /// <c>setval</c> call may occupy.
    /// </summary>
    private static HashSet<NodeAst> CollectStandalonePositions(NodeAst? projectionList)
    {
        HashSet<NodeAst> positions = [];

        if (projectionList is null)
            return positions;

        Stack<NodeAst> pending = new();
        pending.Push(projectionList);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.IdentifierList)
            {
                if (node.leftAst is not null)
                    pending.Push(node.leftAst);
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);
                continue;
            }

            positions.Add(node.nodeType == NodeType.ExprAlias && node.leftAst is not null ? node.leftAst : node);
        }

        return positions;
    }

    /// <summary>
    /// Refuses every <c>setval</c> shape the engine cannot honour, leaving exactly one it can:
    /// <c>SELECT setval('s', n)</c>, on its own, as a whole projection.
    /// </summary>
    /// <remarks>
    /// <para>Three shapes are refused, and each is refused because accepting it would do something
    /// other than what it reads as.</para>
    ///
    /// <list type="bullet">
    /// <item><description><b>Nested in a larger expression</b>, such as inside a <c>CASE</c>. The
    /// counter is moved here, before the statement is evaluated, so a branch the evaluator never
    /// takes would still have reset the sequence — irrevocably, and invisibly.</description></item>
    /// <item><description><b>More than one in a statement.</b> Each call reports its own installed
    /// value, and the statement carries one slot per sequence, so two calls would both report the
    /// last one's argument. The same shape also pays one block-lease wait per call before a single
    /// row is produced.</description></item>
    /// <item><description><b>Anywhere but a projection list</b> — an <c>INSERT … VALUES</c> row,
    /// for instance. A statement that writes rows and moves a counter in the same breath has two
    /// outcomes and can only roll one of them back.</description></item>
    /// </list>
    ///
    /// <para>Refused rather than reinterpreted: a caller who writes one of these means something,
    /// and doing something else quietly is worse than saying no.</para>
    /// </remarks>
    private static void RequireStandaloneSetVal(NodeAst call, HashSet<NodeAst> standalonePositions, int alreadySeen)
    {
        if (alreadySeen > 0)
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceCallNotAllowedHere,
                "Only one setval may appear in a statement. Each call reports its own installed value " +
                "and each waits out a block lease, so several in one statement can neither report " +
                "correctly nor run promptly. Issue them as separate statements.");

        if (!standalonePositions.Contains(call))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceCallNotAllowedHere,
                "setval may only be written as a whole projection of a SELECT with no FROM clause, as in " +
                "SELECT setval('order_no', 5000). It moves the counter before the statement is evaluated, " +
                "so nesting it in a larger expression would reset the sequence even along a branch the " +
                "statement does not take.");
    }

    /// <summary>The resolved target and arguments of one <c>setval</c> call, read at bind time.</summary>
    private readonly record struct SetValRequest(SequenceSchema Sequence, long Value, bool IsCalled);

    /// <summary>
    /// Moves the counter so the next value it issues follows <paramref name="request"/>, then
    /// records the installed value where the evaluator will report it.
    /// </summary>
    /// <remarks>
    /// <c>is_called = true</c> (the default) means the value is treated as already issued, so the
    /// next <c>nextval</c> returns it plus the increment. <c>false</c> means the next
    /// <c>nextval</c> returns the value itself. Getting that off by one wrong is the classic
    /// <c>setval</c> mistake, which is why the two spellings are computed here rather than at the
    /// call site.
    ///
    /// <para>Moving a counter <b>downwards</b> makes the sequence reissue values that committed
    /// rows may already hold, and a unique index will then reject the insert. PostgreSQL permits
    /// it and so does this; the value is not clamped, because a clamp nobody asked for is its own
    /// surprise.</para>
    /// </remarks>
    private async Task ApplySetValAsync(
        DatabaseDescriptor database,
        SetValRequest request,
        Dictionary<string, ColumnValue> parameters,
        KvTransaction transaction,
        CancellationToken cancellationToken)
    {
        SequenceSchema sequence = request.Sequence;

        if (request.Value < sequence.MinValue)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"setval('{sequence.Name}', {request.Value}) is below the sequence's MINVALUE ({sequence.MinValue})");

        if (sequence.MaxValue is { } ceiling && request.Value > ceiling)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"setval('{sequence.Name}', {request.Value}) is above the sequence's MAXVALUE ({ceiling})");

        long nextIssued = request.IsCalled ? request.Value + sequence.Increment : request.Value;

        await allocator.UpdateAsync(
            database.Kahuna.Kahuna,
            database.Id,
            sequence,
            // The counter holds the reserved ceiling, so seeding it one increment below the value
            // that should come next makes that value the one issued.
            currentValue: nextIssued - sequence.Increment,
            increment: null,
            initialValue: null,
            maxValue: null,
            removeMaxValue: false,
            blockSize: null,
            removeBlockSize: false,
            cancellationToken).ConfigureAwait(false);

        // PostgreSQL's setval returns the value it was given, not the value that comes next.
        parameters[SequenceScalarFunctions.CurrentKeyPrefix + sequence.Id] = new ColumnValue(ColumnType.Integer64, request.Value);
        parameters[SequenceScalarFunctions.LastValueKey] = new ColumnValue(ColumnType.Integer64, request.Value);
        parameters[SequenceScalarFunctions.DrawnKeyPrefix + sequence.Id] = ColumnValue.True;
        parameters[SequenceScalarFunctions.LastDrawnIdKey] = new ColumnValue(ColumnType.String, sequence.Id!);
        transaction.RecordSequenceValue(sequence.Id!, request.Value);
    }

    private static SetValRequest ReadSetValRequest(NodeAst call, SequenceSchema sequence, Dictionary<string, ColumnValue>? parameters)
    {
        string sequenceName = sequence.Name ?? sequence.Id!;

        List<NodeAst> arguments = [];
        FlattenArguments(call.rightAst, arguments);

        if (arguments.Count is < 2 or > 3)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "setval takes the sequence name, the value, and optionally is_called");

        ColumnValue value = SqlExecutor.EvalExpr(arguments[1], EmptyRow, parameters);

        if (value.Type != ColumnType.Integer64)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"setval('{sequenceName}', …) needs an integer value");

        bool isCalled = true;

        if (arguments.Count == 3)
        {
            ColumnValue flag = SqlExecutor.EvalExpr(arguments[2], EmptyRow, parameters);

            if (flag.Type != ColumnType.Bool)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"setval('{sequenceName}', …) third argument must be true or false");

            isCalled = flag.BoolValue;
        }

        return new SetValRequest(sequence, value.LongValue, isCalled);
    }

    /// <summary>
    /// Reads the sequence name out of a call's first argument. It must be a string literal: the
    /// binder has to know which sequence the statement touches before the statement runs, and a
    /// name computed per row could name a different sequence each time.
    /// </summary>
    private static string ReadSequenceNameArgument(string functionName, NodeAst call)
    {
        List<NodeAst> arguments = [];
        FlattenArguments(call.rightAst, arguments);

        if (arguments.Count > 0 && arguments[0].nodeType == NodeType.String && arguments[0].yytext is { Length: > 0 } literal)
            return SqlStringLiteral.Decode(literal);

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"{functionName}() takes the sequence's name as a string literal, for example {functionName}('order_no')");
    }

    /// <summary>
    /// Resolves a sequence the statement named, and refuses one that cannot issue.
    /// </summary>
    /// <remarks>
    /// <para><b>An owned sequence whose owner is not in the live schema is refused.</b> That is a
    /// relation which was deferred-dropped, or one whose orphan the garbage collector has already
    /// reclaimed — and in the second case the counter is gone while this node's cached catalog
    /// still names it, so a draw would find nothing, recreate the counter at its start value, and
    /// begin reissuing numbers the reclaimed relation's rows held. Nothing legitimate draws here
    /// either: the only column that could is on the relation that is no longer there.</para>
    /// </remarks>
    private static SequenceSchema RequireSequence(DatabaseDescriptor database, string sequenceName)
    {
        if (!database.Schema.Sequences.TryGetValue(sequenceName, out SequenceSchema? sequence))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceDoesntExist, $"Sequence '{sequenceName}' does not exist");

        // Answered from the published relation index, not by walking the table map: this runs on
        // the insert binding path, once per named sequence, and a walk there makes every identity
        // table's inserts slower in proportion to how many unrelated relations the database holds.
        if (sequence.OwnedByTableId is { Length: > 0 } ownerTableId
            && !database.Schema.TryGetRelationNameById(ownerTableId, out _))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceInUse,
                $"Sequence '{sequenceName}' belongs to an identity column of a relation that is no longer in " +
                "the schema, so it cannot issue. Relink the relation to bring it back, or create a " +
                "free-standing sequence.");

        return sequence;
    }


    /// <summary>
    /// Flattens a call's argument list into source order.
    /// </summary>
    /// <remarks>
    /// The grammar builds a call's arguments as a left-deep <see cref="NodeType.ExprArgumentList"/>
    /// chain, and a single-argument call as the bare expression with no chain at all — which is why
    /// a one-argument function appeared to work while a two-argument one read its second argument
    /// as its first.
    /// </remarks>
    private static void FlattenArguments(NodeAst? node, List<NodeAst> into)
    {
        if (node is null)
            return;

        if (node.nodeType is NodeType.ExprArgumentList or NodeType.ExprList)
        {
            FlattenArguments(node.leftAst, into);
            FlattenArguments(node.rightAst, into);
            return;
        }

        into.Add(node);
    }

    /// <summary>
    /// Gathers every sequence call under <paramref name="ast"/>.
    /// </summary>
    /// <remarks>
    /// <para>Iterative, like every other walk over a parse tree here: the depth of an expression is
    /// bounded only by what the user wrote, and a recursive walk puts that on the stack of the
    /// thread serving the request.</para>
    ///
    /// <para><b>The order the calls come back in is not the order they will be evaluated</b>, and
    /// nothing here depends on it: demand is summed per sequence, <c>setval</c> is limited to one
    /// call per statement, and which value <c>lastval</c> reports is decided by the draws
    /// themselves rather than by this walk.</para>
    /// </remarks>
    private static void Collect(NodeAst? ast, List<NodeAst> into)
    {
        if (ast is null)
            return;

        Stack<NodeAst> pending = new();
        pending.Push(ast);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (IsSequenceCall(node))
                into.Add(node);

            Push(pending, node.leftAst);
            Push(pending, node.rightAst);
            Push(pending, node.extendedOne);
            Push(pending, node.extendedTwo);
            Push(pending, node.extendedThree);
            Push(pending, node.extendedFour);
            Push(pending, node.extendedFive);
            Push(pending, node.extendedSix);
            Push(pending, node.extendedSeven);
        }
    }

    private static void Push(Stack<NodeAst> pending, NodeAst? child)
    {
        if (child is not null)
            pending.Push(child);
    }
}
