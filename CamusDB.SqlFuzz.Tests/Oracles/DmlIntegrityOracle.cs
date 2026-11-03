/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Oracles;

/// <summary>
/// The <see cref="ISqlFuzzSession"/> of the shared rules on a fuzzer engine. A refusal reaches the rule
/// as its error code. An outcome that is a finding of the error-classification rule (an internal code,
/// an unclassified exception, a hang, a wire failure) is kept in <see cref="Bad"/>, and the check
/// reports it before anything the rule says.
/// </summary>
internal sealed class EngineSession(IStatementExecutor engine) : ISqlFuzzSession
{
    public StatementOutcome? Bad { get; private set; }

    public async Task<(SqlFuzzRows? Rows, string? Error)> QueryAsync(string sql, KvTransaction? transaction)
    {
        StatementOutcome outcome = await engine.ExecuteAsync(StatementRoute.Query, sql, transaction).ConfigureAwait(false);
        Note(outcome);
        return outcome.Kind == OutcomeKind.Rows ? (ToRows(outcome), null) : (null, Code(outcome));
    }

    public async Task<(int Affected, string? Error)> WriteAsync(string sql, KvTransaction? transaction)
    {
        StatementOutcome outcome = await engine.ExecuteAsync(StatementRoute.NonQuery, sql, transaction).ConfigureAwait(false);
        Note(outcome);
        return outcome.IsSuccess ? (outcome.AffectedRows, null) : (0, Code(outcome));
    }

    public Task<KvTransaction> BeginAsync() => engine.BeginAsync();

    public async Task<string?> CommitAsync(KvTransaction transaction)
    {
        StatementOutcome outcome = await engine.CommitAsync(transaction).ConfigureAwait(false);
        Note(outcome);
        return outcome.IsSuccess ? null : Code(outcome);
    }

    public Task RollbackAsync(KvTransaction transaction) => engine.RollbackAsync(transaction);

    public static SqlFuzzRows ToRows(StatementOutcome outcome) =>
        new(outcome.Columns.Select(c => c.Name).ToList(), outcome.Rows.Select(r => (IReadOnlyList<ColumnValue>)r).ToList())
        {
            Types = outcome.Columns.Select(c => c.Type).ToList(),
            WireError = outcome.WireError,
        };

    private static string Code(StatementOutcome outcome) => outcome.ErrorCode ?? outcome.Kind.ToString().ToLowerInvariant();

    private void Note(StatementOutcome outcome)
    {
        if (Bad is null && ErrorClassifier.Classify(outcome) == ErrorVerdict.Finding)
            Bad = outcome;
    }
}

/// <summary>
/// One write on its own, checked two ways: the effect rule of <see cref="WriteEffect"/> (the
/// affected-row count and the full expected table after the write, cell by cell, and no change at all
/// after a failure), then <see cref="TableIntegrityAudit"/> on the table (twins, orphan index entries,
/// rows missing from an index, stale covering payloads, orphan or lost large-value cells).
/// </summary>
internal sealed class WriteCheck : OracleCheck
{
    public WriteCheck(Statement write, SchemaState schema)
    {
        Write = write;
        Schema = schema;
    }

    public Statement Write { get; }

    /// <summary>The schema of the round, for the key column and the column types of the table of the write.</summary>
    public SchemaState Schema { get; }

    public override OracleKind Oracle => OracleKind.DmlIntegrity;

    public override string Variant => "dml:" + Kind;

    private string Kind => Write switch
    {
        InsertStmt { OnConflict: not null } => "upsert",
        InsertStmt => "insert",
        InsertSelectStmt => "insert-select",
        UpdateStmt => "update",
        DeleteStmt => "delete",
        _ => "other",
    };

    public string Table => Write switch
    {
        InsertStmt i => i.Table,
        InsertSelectStmt s => s.Table,
        UpdateStmt u => u.Table,
        DeleteStmt d => d.Table,
        _ => throw new InvalidOperationException(),
    };

    /// <summary>
    /// The <see cref="WriteSpec"/> of this write: its SQL, the prediction query, the proposed rows and
    /// the named keys. Every prediction reads the database before the write, through the same engine.
    /// </summary>
    public WriteSpec Spec(ISqlPrinter printer)
    {
        TableDef table = Schema.Table(Table);
        ColumnExpr key = new(null, table.PrimaryKey.Name, table.PrimaryKey.Type);
        string? prediction = null;
        List<string> assigned = new();
        List<string> keys = new();
        List<string>? proposed = null;
        IReadOnlyList<string>? proposedColumns = null;

        switch (Write)
        {
            case UpdateStmt update:
                assigned = update.Set.Select(a => a.Column).ToList();
                prediction = printer.Print(new SelectStmt(
                    update.Set.Select(a => new SelectItem(a.Value)).Prepend(new SelectItem(key)).ToList(),
                    new TableRef(update.Table, "d"), update.Where));
                break;

            case DeleteStmt delete:
                prediction = printer.Print(new SelectStmt([new SelectItem(key)], new TableRef(delete.Table, "d"), delete.Where));
                break;

            case InsertSelectStmt insertSelect:
                proposed = [printer.Print(insertSelect.Source)];
                proposedColumns = insertSelect.Columns;
                break;

            case InsertStmt insert:
            {
                int keyAt = insert.Columns.ToList().IndexOf(table.PrimaryKey.Name);
                keys = insert.Rows.Select(r => keyAt < 0 ? "NULL" : RenderKey(r[keyAt])).ToList();
                proposed = insert.Rows.Select(r => ProposedRow(printer, table, insert.Columns, r)).ToList();
                proposedColumns = insert.Columns;

                if (insert.OnConflict?.DoUpdate is { Count: > 0 } set && keyAt >= 0)
                {
                    // For each proposed row whose key exists, the new assigned values: the assignment on
                    // the stored row, with excluded.c replaced by the proposed value of c.
                    assigned = set.Select(a => a.Column).ToList();
                    prediction = string.Join(" UNION ALL ", insert.Rows.Select(row => printer.Print(new SelectStmt(
                        set.Select(a => new SelectItem(Excluded(a.Value, insert.Columns, row))).Prepend(new SelectItem(key)).ToList(),
                        new TableRef(insert.Table, "d"),
                        new BinaryExpr(BinaryOp.Eq, key, row[keyAt], FuzzType.Bool)))));
                }
                break;
            }
        }

        return new WriteSpec(Table, Kind, printer.Print(Write), prediction, assigned, keys, proposed, proposedColumns);
    }

    /// <summary>
    /// A one-row SELECT with no FROM that gives the values of one VALUES row, each cast to the type of
    /// its column, as the INSERT stores them.
    /// </summary>
    private static string ProposedRow(ISqlPrinter printer, TableDef table, IReadOnlyList<string> columns, IReadOnlyList<Expr> row) =>
        "SELECT " + string.Join(", ", columns.Select((c, i) =>
            $"CAST({printer.Print(row[i])} AS {printer.TypeName(table.Column(c).Type)}) AS {c}"));

    private static Expr Excluded(Expr value, IReadOnlyList<string> columns, IReadOnlyList<Expr> row) =>
        ExprWalker.Rewrite(value, e => e is ColumnExpr { Source: "excluded" } excluded
            ? row[columns.ToList().IndexOf(excluded.Column)]
            : null);

    /// <summary>The canonical text (<see cref="SqlFuzzValues.Render"/>) of a literal primary key.</summary>
    private static string RenderKey(Expr expression) => expression is LiteralExpr { Value: { IsNull: false } value }
        ? value.Type switch
        {
            FuzzType.Int64 => "i:" + value.Int.ToString(CultureInfo.InvariantCulture),
            FuzzType.String => "s:" + value.Text,
            _ => "?",
        }
        : "NULL";

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        EngineSession session = new(context.Engine);
        WriteResult result = await WriteEffect.CheckAsync(Spec(context.Printer),
            async sql => (await session.QueryAsync(sql, null).ConfigureAwait(false)).Rows,
            sql => session.WriteAsync(sql, null)).ConfigureAwait(false);

        if (session.Bad is not null)
            return CheckResult.FromBadOutcome(session.Bad);
        if (result.Problem is not null)
            return CheckResult.Fail("dml:effect:" + Kind, result.Problem);
        if (!result.Checked)
            return CheckResult.Skip("the reads the effect rule needs did not run");

        return await IntegrityAsync(context.Engine, [Table]).ConfigureAwait(false);
    }

    /// <summary>
    /// The integrity result of <paramref name="tables"/>. The problems of every table are collected
    /// before the result is given, so a later table cannot hide an earlier one's problem. A scan that
    /// cannot run is classified, not skipped: an internal code (corrupt storage, an invalid index layout)
    /// or any exception that is not a <see cref="CamusDBException"/> is a finding, and so is any other
    /// refusal of a scan of a table that exists. Only a missing table (one a shrink step removed) is
    /// passed over.
    /// </summary>
    public static async Task<CheckResult> IntegrityAsync(IStatementExecutor engine, IReadOnlyList<string> tables)
    {
        List<string> problems = new();
        int missing = 0;
        foreach (string table in tables)
        {
            try
            {
                problems.AddRange(await engine.CheckIntegrityAsync(table).ConfigureAwait(false));
            }
            catch (StatementHangException)
            {
                throw;
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TableDoesntExist)
            {
                missing++;
            }
            catch (CamusDBException ex) when (ErrorClassifier.InternalCodes.Contains(ex.Code))
            {
                return CheckResult.Fail($"error:integrity-scan:{ex.Code}:{SqlFuzzErrorRules.MessageShape(ex.Message, 60)}",
                    $"the integrity scan of {table} failed with {ex.Code}: {ex.Message}" + Earlier(problems));
            }
            catch (CamusDBException ex)
            {
                return CheckResult.Fail("dml:integrity-scan-refused:" + ex.Code,
                    $"the integrity scan of {table}, which exists, was refused with {ex.Code}: {ex.Message}" + Earlier(problems));
            }
            catch (Exception ex)
            {
                return CheckResult.Fail("error:integrity-scan:" + ex.GetType().Name,
                    $"the integrity scan of {table} threw {ex.GetType().Name}: {ex.Message}" + Earlier(problems));
            }
        }
        if (problems.Count > 0)
            return CheckResult.Fail("dml:integrity", string.Join("\n", problems));
        return missing == tables.Count ? CheckResult.Skip("no table of the check exists") : CheckResult.Pass;

        static string Earlier(List<string> found) => found.Count == 0 ? "" : "\nproblems of the earlier tables:\n" + string.Join("\n", found);
    }

    public override IEnumerable<OracleCheck> Simplify()
    {
        switch (Write)
        {
            case UpdateStmt update:
                foreach (Expr p in ExprShrinker.Candidates(update.Where))
                    yield return new WriteCheck(update with { Where = p }, Schema);
                if (update.Set.Count > 1)
                    for (int i = 0; i < update.Set.Count; i++)
                        yield return new WriteCheck(update with { Set = update.Set.Where((_, j) => j != i).ToList() }, Schema);
                break;
            case DeleteStmt delete:
                foreach (Expr p in ExprShrinker.Candidates(delete.Where))
                    yield return new WriteCheck(delete with { Where = p }, Schema);
                break;
            case InsertStmt { Rows.Count: > 1 } insert:
                for (int i = 0; i < insert.Rows.Count; i++)
                    yield return new WriteCheck(insert with { Rows = insert.Rows.Where((_, j) => j != i).ToList() }, Schema);
                break;
            case InsertSelectStmt insertSelect when insertSelect.Source.Where is not null:
                foreach (Expr p in ExprShrinker.Candidates(insertSelect.Source.Where))
                    yield return new WriteCheck(insertSelect with { Source = insertSelect.Source with { Where = p } }, Schema);
                break;
        }
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        WriteSpec spec = Spec(printer);
        yield return spec.WriteSql;
        if (spec.PredictionSql is not null)
            yield return "-- prediction: " + spec.PredictionSql;
        foreach (string proposed in spec.ProposedRowsSql ?? [])
            yield return "-- proposed rows: " + proposed;
    }

    public override void WriteTest(TestWriter writer, ISqlPrinter printer)
    {
        WriteSpec spec = Spec(printer);
        writer.Line($"await db.AssertWriteEffect({TestWriter.WriteSpec(spec)});");
        writer.Line($"await db.AssertIntegrity({TestWriter.Literal(spec.Table)});");
    }
}

/// <summary>How a transaction check makes its statement fail on purpose.</summary>
internal enum DeliberateFailure
{
    /// <summary>No deliberate failure: the transaction commits, unless a generated write fails by itself.</summary>
    None,

    /// <summary>The same INSERT twice: the second must fail with a duplicate key.</summary>
    DuplicateKey,

    /// <summary>An INSERT with a NULL primary key.</summary>
    NullKey,

    /// <summary>An INSERT whose value divides by zero.</summary>
    DivisionByZero,
}

/// <summary>
/// One explicit transaction of generated writes, checked with <see cref="TransactionEffect"/>: every
/// write has its effect inside the transaction; when a statement fails, the PostgreSQL rule applies
/// (the next statement is refused with <c>CADB0824</c>, COMMIT rolls back and reports <c>CADB0824</c>,
/// and the committed state equals the state before the transaction); when none fails, COMMIT succeeds
/// and keeps exactly the state the transaction saw at its end. After either, every table passes
/// <see cref="TableIntegrityAudit"/>.
/// </summary>
internal sealed class TransactionCheck : OracleCheck
{
    public TransactionCheck(IReadOnlyList<Statement> writes, Statement? failing, DeliberateFailure failure, Statement after, IReadOnlyList<string> tables, SchemaState schema)
    {
        Writes = writes;
        Failing = failing;
        Failure = failure;
        After = after;
        Tables = tables;
        Schema = schema;
    }

    public IReadOnlyList<Statement> Writes { get; }

    public Statement? Failing { get; }

    public DeliberateFailure Failure { get; }

    /// <summary>The statement after the failure, which must be refused: a SELECT or a write.</summary>
    public Statement After { get; }

    public IReadOnlyList<string> Tables { get; }

    public SchemaState Schema { get; }

    /// <summary>True when the last run committed the transaction, so its writes are part of the history.</summary>
    public bool Committed { get; private set; }

    public override OracleKind Oracle => OracleKind.DmlIntegrity;

    public override string Variant => Failure == DeliberateFailure.None ? "dml:transaction" : "dml:failed-statement:" + Failure.ToString().ToLowerInvariant();

    private IReadOnlyList<WriteSpec> Specs(ISqlPrinter printer) => Writes.Select(w => new WriteCheck(w, Schema).Spec(printer)).ToList();

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        Committed = false;
        EngineSession session = new(context.Engine);
        ISqlPrinter printer = context.Printer;
        (SqlFuzzVerdict verdict, bool committed) = await TransactionEffect.CheckAsync(
            Specs(printer),
            Failing is null ? null : printer.Print(Failing),
            Failure == DeliberateFailure.DuplicateKey,
            printer.Print(After),
            After is SelectStmt,
            Tables,
            session).ConfigureAwait(false);
        Committed = committed;

        if (session.Bad is not null)
            return CheckResult.FromBadOutcome(session.Bad);
        switch (verdict.Kind)
        {
            case SqlFuzzVerdictKind.Fail:
                return CheckResult.Fail(verdict.Signature, verdict.Detail);
            case SqlFuzzVerdictKind.Skip:
                return CheckResult.Skip(verdict.Detail);
        }

        return await WriteCheck.IntegrityAsync(context.Engine, Tables).ConfigureAwait(false);
    }

    public override IEnumerable<OracleCheck> Simplify()
    {
        for (int i = 0; i < Writes.Count; i++)
        {
            // The INSERT that a duplicate-key failure repeats must stay.
            if (Failure == DeliberateFailure.DuplicateKey && Failing is not null && Writes[i] == Failing)
                continue;
            yield return new TransactionCheck(Writes.Where((_, j) => j != i).ToList(), Failing, Failure, After, Tables, Schema);
        }
        if (Tables.Count > 1)
            for (int i = 0; i < Tables.Count; i++)
                yield return new TransactionCheck(Writes, Failing, Failure, After, Tables.Where((_, j) => j != i).ToList(), Schema);
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        yield return "BEGIN";
        foreach (Statement write in Writes)
            yield return printer.Print(write);
        if (Failing is not null)
            yield return printer.Print(Failing) + "  -- fails on purpose";
        yield return printer.Print(After) + "  -- must be refused after a failure";
        yield return "COMMIT";
    }

    public override void WriteTest(TestWriter writer, ISqlPrinter printer)
    {
        writer.Line("await db.AssertTransaction(");
        writer.Line("    new WriteSpec[]");
        writer.Line("    {");
        foreach (WriteSpec spec in Specs(printer))
            writer.Line($"        {TestWriter.WriteSpec(spec)},");
        writer.Line("    },");
        writer.Line($"    {(Failing is null ? "null" : TestWriter.Literal(printer.Print(Failing)))}, failureIsDuplicateKey: {(Failure == DeliberateFailure.DuplicateKey ? "true" : "false")},");
        writer.Line($"    {TestWriter.Literal(printer.Print(After))}, afterIsQuery: {(After is SelectStmt ? "true" : "false")},");
        writer.Line($"    {string.Join(", ", Tables.Select(TestWriter.Literal))});");
        writer.Line($"await db.AssertIntegrity({string.Join(", ", Tables.Select(TestWriter.Literal))});");
    }
}

/// <summary>
/// Makes write batches. Most are autocommit writes, one <see cref="WriteCheck"/> each; about one in
/// three is an explicit transaction (<see cref="TransactionCheck"/>), half of them with a statement
/// that fails on purpose. Every autocommit write joins the setup after it runs, whatever its outcome,
/// and a transaction joins it (as BEGIN, its writes, COMMIT) when it committed, so a later finding
/// replays the full history of the table.
/// </summary>
internal sealed class DmlIntegrityOracle : IOracle
{
    public OracleKind Kind => OracleKind.DmlIntegrity;

    public async Task RunRoundAsync(RoundContext round)
    {
        StatementGenerator generator = round.Generator;
        FuzzRandom random = generator.Random;
        List<string> tables = generator.State.Tables.Select(t => t.Name).ToList();

        for (int i = 0; i < round.CheckCount && !round.ShouldStop; i++)
        {
            if (random.Chance(0.35))
            {
                TransactionCheck check = GenerateTransaction(generator, tables);
                await round.RunAsync(check).ConfigureAwait(false);
                if (check.Committed)
                {
                    round.Setup.Add(SetupStatement.Of(new BeginStmt()));
                    round.Setup.AddRange(check.Writes.Select(SetupStatement.Of));
                    round.Setup.Add(SetupStatement.Of(new CommitStmt()));
                }
                continue;
            }

            int writes = random.Between(1, 4);
            for (int w = 0; w < writes && !round.ShouldStop; w++)
            {
                Statement write = generator.GenerateWrite();
                await round.RunAsync(new WriteCheck(write, generator.State)).ConfigureAwait(false);
                round.Setup.Add(SetupStatement.Of(write));
            }
        }
    }

    private static TransactionCheck GenerateTransaction(StatementGenerator generator, List<string> tables)
    {
        FuzzRandom random = generator.Random;
        List<Statement> writes = Enumerable.Range(0, random.Between(1, 4)).Select(_ => generator.GenerateWrite()).ToList();
        Statement after = random.NextBool()
            ? new SelectStmt([], new TableRef(random.Pick(tables), "a0"))
            : generator.GenerateInsert();

        if (random.NextBool())
            return new TransactionCheck(writes, null, DeliberateFailure.None, after, tables, generator.State);

        TableDef table = random.Pick(generator.State.Tables);
        DeliberateFailure failure = random.Pick(new[] { DeliberateFailure.DuplicateKey, DeliberateFailure.NullKey, DeliberateFailure.DivisionByZero });
        InsertStmt insert = new(table.Name, table.Columns.Select(c => c.Name).ToList(), generator.GenerateRows(table, 1, boundaries: false));

        Statement failing;
        switch (failure)
        {
            case DeliberateFailure.DuplicateKey:
                writes.Add(insert);
                failing = insert;
                break;

            case DeliberateFailure.NullKey:
            {
                List<Expr> row = insert.Rows[0].ToList();
                row[0] = new LiteralExpr(FuzzValue.Null(table.PrimaryKey.Type));
                failing = insert with { Rows = [row] };
                break;
            }

            default:
            {
                int column = table.Columns.ToList().FindIndex(c => c.Type == FuzzType.Int64);
                List<Expr> row = insert.Rows[0].ToList();
                if (column < 0)
                {
                    // No INT64 column: a NULL key fails as surely.
                    row[0] = new LiteralExpr(FuzzValue.Null(table.PrimaryKey.Type));
                    failure = DeliberateFailure.NullKey;
                }
                else
                    row[column] = new BinaryExpr(BinaryOp.Div, new LiteralExpr(FuzzValue.Of(1L)), new LiteralExpr(FuzzValue.Of(0L)), FuzzType.Int64);
                failing = insert with { Rows = [row] };
                break;
            }
        }

        return new TransactionCheck(writes, failing, failure, after, tables, generator.State);
    }
}
