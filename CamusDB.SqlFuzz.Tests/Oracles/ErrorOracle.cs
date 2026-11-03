/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests.Oracles;

/// <summary>
/// One error-classification check: run one generated statement and classify its outcome with
/// <see cref="ErrorClassifier"/>. An unclassified exception, an internal-error code, a hang, or rows
/// that do not serialize to the HTTP JSON format are findings.
/// </summary>
internal sealed class ErrorCheck : OracleCheck
{
    public ErrorCheck(Statement statement)
    {
        Statement = statement;
    }

    public Statement Statement { get; }

    /// <summary>The outcome of the last run, for the acceptance counts.</summary>
    public StatementOutcome? LastOutcome { get; private set; }

    public override OracleKind Oracle => OracleKind.ErrorClassification;

    public override string Variant => "error:" + Statement switch
    {
        SelectStmt => "select",
        InsertStmt { OnConflict: not null } => "upsert",
        InsertStmt => "insert",
        InsertSelectStmt => "insert-select",
        UpdateStmt => "update",
        DeleteStmt => "delete",
        AnalyzeStmt => "analyze",
        _ => "other",
    };

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        StatementOutcome outcome = await context.Engine.ExecuteAsync(SetupStatement.RouteOf(Statement), context.Printer.Print(Statement)).ConfigureAwait(false);
        LastOutcome = outcome;
        return ErrorClassifier.Classify(outcome) == ErrorVerdict.Finding
            ? CheckResult.Fail("error:" + ErrorClassifier.Signature(outcome), outcome.ToString())
            : CheckResult.Pass;
    }

    public override IEnumerable<OracleCheck> Simplify()
    {
        switch (Statement)
        {
            case SelectStmt select:
                if (select.SetOp is not null)
                    yield return new ErrorCheck(select with { SetOp = null });
                if (select.OrderBy is not null || select.Limit is not null)
                    yield return new ErrorCheck(select with { OrderBy = null, Limit = null, Offset = null });
                if (select.Distinct)
                    yield return new ErrorCheck(select with { Distinct = false });
                if (select.Having is not null)
                    yield return new ErrorCheck(select with { Having = null });
                if (select.Where is not null)
                {
                    yield return new ErrorCheck(select with { Where = null });
                    foreach (Expr p in ExprShrinker.Candidates(select.Where))
                        yield return new ErrorCheck(select with { Where = p });
                }
                if (select.Items.Count > 1)
                    for (int i = 0; i < select.Items.Count; i++)
                        yield return new ErrorCheck(select with { Items = select.Items.Where((_, j) => j != i).ToList() });
                for (int i = 0; i < select.Items.Count; i++)
                    foreach (Expr e in ExprShrinker.Candidates(select.Items[i].Expr))
                    {
                        List<SelectItem> items = select.Items.ToList();
                        items[i] = items[i] with { Expr = e };
                        yield return new ErrorCheck(select with { Items = items });
                    }
                foreach (FromItem from in FromShrinker.Candidates(select.From, keepJoin: false))
                    yield return new ErrorCheck(select with { From = from });
                break;

            case UpdateStmt update:
                foreach (Expr p in ExprShrinker.Candidates(update.Where))
                    yield return new ErrorCheck(update with { Where = p });
                for (int i = 0; i < update.Set.Count; i++)
                    foreach (Expr e in ExprShrinker.Candidates(update.Set[i].Value))
                    {
                        List<Assignment> set = update.Set.ToList();
                        set[i] = set[i] with { Value = e };
                        yield return new ErrorCheck(update with { Set = set });
                    }
                break;

            case DeleteStmt delete:
                foreach (Expr p in ExprShrinker.Candidates(delete.Where))
                    yield return new ErrorCheck(delete with { Where = p });
                break;

            case InsertStmt { Rows.Count: > 1 } insert:
                for (int i = 0; i < insert.Rows.Count; i++)
                    yield return new ErrorCheck(insert with { Rows = insert.Rows.Where((_, j) => j != i).ToList() });
                break;
        }
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        yield return printer.Print(Statement);
    }

    /// <summary>The printed test runs the statement on the entry point the campaign used: a read with the wire check, ANALYZE in a read-write transaction, or a write.</summary>
    public override void WriteTest(TestWriter writer, ISqlPrinter printer)
    {
        string route = SetupStatement.RouteOf(Statement) switch
        {
            StatementRoute.Query => "query",
            StatementRoute.Analyze => "analyze",
            _ => "write",
        };
        writer.Line($"await db.AssertClassified({TestWriter.Literal(printer.Print(Statement))}, {TestWriter.Literal(route)});");
    }
}

/// <summary>
/// Runs the free-form statement stream of the generator: SELECT in every form, INSERT, multi-row
/// INSERT, INSERT … SELECT, UPDATE, DELETE, UPSERT and ANALYZE. Each statement is one check. Every
/// statement joins the setup after it runs, a SELECT too: a read can change what a later statement
/// does through the bound-query, plan and probe caches, so a later finding replays the reads as well.
/// The shrinker removes the ones a finding does not need.
/// </summary>
internal sealed class ErrorOracle : IOracle
{
    public OracleKind Kind => OracleKind.ErrorClassification;

    public async Task RunRoundAsync(RoundContext round)
    {
        for (int i = 0; i < round.CheckCount && !round.ShouldStop; i++)
        {
            Statement statement = round.Generator.GenerateStatement();
            ErrorCheck check = new(statement);
            await round.RunAsync(check).ConfigureAwait(false);

            if (check.LastOutcome is { } outcome)
            {
                ErrorVerdict verdict = ErrorClassifier.Classify(outcome);
                if (verdict == ErrorVerdict.UnlistedCode)
                {
                    round.Stats.Unlisted++;
                    string key = outcome.ErrorCode + ": " + Shorten(outcome.ErrorMessage);
                    round.Stats.UnlistedCodes[key] = round.Stats.UnlistedCodes.GetValueOrDefault(key) + 1;
                }
                else if (verdict == ErrorVerdict.Accepted)
                    round.Stats.Accepted++;
                else if (verdict == ErrorVerdict.KnownIssue)
                {
                    round.Stats.Known++;
                    string key = "known: " + Shorten(outcome.ErrorMessage);
                    round.Stats.UnlistedCodes[key] = round.Stats.UnlistedCodes.GetValueOrDefault(key) + 1;
                }
            }

            round.Setup.Add(SetupStatement.Of(statement));
        }
    }

    // Keep the shape of the message, not its values, so equal causes count together.
    private static string Shorten(string? message) => CamusDB.Tests.SqlFuzz.SqlFuzzErrorRules.MessageShape(message, 110);
}

/// <summary>
/// A CREATE TABLE or CREATE INDEX of a round's generated schema, which must succeed. The generator
/// writes only DDL that CamusDB supports, and the checks of the round assume the schema, so a refusal is
/// a finding: an engine regression, or a generator rule that is wrong. Without this check the round's
/// relations would run on a missing table and could all pass.
/// </summary>
internal sealed class SetupDdlCheck(Statement statement) : OracleCheck
{
    public Statement Statement { get; } = statement;

    public override OracleKind Oracle => OracleKind.ErrorClassification;

    public override string Variant => "setup:ddl";

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        StatementOutcome outcome = await context.Engine.ExecuteAsync(StatementRoute.Ddl, context.Printer.Print(Statement)).ConfigureAwait(false);
        if (ErrorClassifier.Classify(outcome) == ErrorVerdict.Finding)
            return CheckResult.FromBadOutcome(outcome);
        return outcome.IsSuccess
            ? CheckResult.Pass
            : CheckResult.Fail("setup:ddl-refused:" + outcome.ErrorCode, $"{outcome}\n  in: {outcome.Sql}");
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        yield return printer.Print(Statement);
    }

    public override void WriteTest(TestWriter writer, ISqlPrinter printer) =>
        writer.Line($"await db.Ddl({TestWriter.Literal(printer.Print(Statement))});");
}
