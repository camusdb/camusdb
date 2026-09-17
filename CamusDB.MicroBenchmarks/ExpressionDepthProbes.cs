/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Finds the size at which a long expression in a WHERE clause stops working: an OR chain, an AND
/// chain, an IN list, an arithmetic chain, a NOT chain and a CASE WHEN list, through SELECT, UPDATE
/// and DELETE. Each of these parses into a tree whose depth grows with the number of terms, and a
/// recursive walk over a deep enough tree overflows the stack and ends the process.
///
/// <para>Every size runs in a child process, because a stack overflow cannot be caught in .NET. The
/// parent reads the exit code and, for a child that died, the repeated frame from its stderr. Run with
/// <c>dotnet run -c Release -- probe exprscan [shape...]</c>.</para>
/// </summary>
internal static class ExpressionDepthProbes
{
    private static readonly string[] AllShapes =
    [
        "parse-or", "select-or", "select-and", "update-or", "delete-or",
        "select-in", "select-add", "select-not", "select-case",
        "select-func", "select-paren", "select-proj", "select-subquery",
        "update-subquery-add", "select-exists-not",
    ];

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length > 1 && args[1] == "exprchild")
            return await ChildAsync(args[2], int.Parse(args[3]));

        string[] shapes = args.Length > 2 ? args[2..] : AllShapes;

        foreach (string shape in shapes)
            Scan(shape);

        return 0;
    }

    private static string BuildSql(string shape, int terms)
    {
        StringBuilder sb = new(terms * 24);

        switch (shape)
        {
            case "parse-or":
            case "select-or":
                sb.Append("SELECT id FROM bench WHERE ");
                AppendChain(sb, terms, " OR ", i => $"value = {i}");
                break;

            case "select-and":
                sb.Append("SELECT id FROM bench WHERE ");
                AppendChain(sb, terms, " AND ", i => $"value <> {i + 1_000_000}");
                break;

            case "update-or":
                sb.Append("UPDATE bench SET name = 'u' WHERE ");
                AppendChain(sb, terms, " OR ", i => $"value = {i}");
                break;

            case "delete-or":
                sb.Append("DELETE FROM bench WHERE ");
                AppendChain(sb, terms, " OR ", i => $"value = {i}");
                break;

            case "select-in":
                sb.Append("SELECT id FROM bench WHERE value IN (");
                AppendChain(sb, terms, ", ", i => i.ToString());
                sb.Append(')');
                break;

            case "select-add":
                sb.Append("SELECT id FROM bench WHERE value + ");
                AppendChain(sb, terms, " + ", _ => "0");
                sb.Append(" >= 0");
                break;

            case "select-not":
                sb.Append("SELECT id FROM bench WHERE ");
                // An even count keeps the predicate true, so the row loop runs to completion.
                for (int i = 0; i < terms * 2; i++)
                    sb.Append("NOT ");
                sb.Append("value >= 0");
                break;

            case "select-case":
                sb.Append("SELECT id FROM bench WHERE CASE ");
                for (int i = 0; i < terms; i++)
                    sb.Append("WHEN value = ").Append(i + 1_000_000).Append(" THEN 0 ");
                sb.Append("ELSE 1 END = 1");
                break;

            case "select-func":
                sb.Append("SELECT id FROM bench WHERE ");
                for (int i = 0; i < terms; i++)
                    sb.Append("abs(");
                sb.Append("value");
                sb.Append(')', terms);
                sb.Append(" >= 0");
                break;

            case "select-paren":
                sb.Append("SELECT id FROM bench WHERE value");
                for (int i = 0; i < terms; i++)
                    sb.Append(" + (0");
                sb.Append(')', terms);
                sb.Append(" >= 0");
                break;

            case "select-proj":
                sb.Append("SELECT ");
                AppendChain(sb, terms, ", ", i => $"value + {i}");
                sb.Append(" FROM bench");
                break;

            case "select-subquery":
                sb.Append("SELECT id FROM bench WHERE value IN (");
                for (int i = 1; i < terms; i++)
                    sb.Append("SELECT value FROM bench WHERE value IN (");
                sb.Append("SELECT value FROM bench");
                sb.Append(')', terms);
                break;

            case "update-subquery-add":
                // The IN subquery sends the statement through the asynchronous subquery rewrite, which
                // recurses over the rest of the WHERE clause as well.
                sb.Append("UPDATE bench SET name = 'u' WHERE value IN (SELECT value FROM bench) AND value");
                for (int i = 0; i < terms; i++)
                    sb.Append(" + 0");
                sb.Append(" >= 0");
                break;

            case "select-exists-not":
                // A correlated EXISTS makes the filter use the asynchronous predicate walker, which
                // recurses once per NOT. An even count keeps the predicate true.
                sb.Append("SELECT id FROM bench b WHERE ");
                for (int i = 0; i < terms * 2; i++)
                    sb.Append("NOT ");
                sb.Append("EXISTS (SELECT 1 FROM bench c WHERE c.value = b.value)");
                break;

            default:
                throw new ArgumentException($"unknown shape '{shape}'");
        }

        return sb.ToString();
    }

    private static void AppendChain(StringBuilder sb, int terms, string separator, Func<int, string> term)
    {
        for (int i = 0; i < terms; i++)
        {
            if (i > 0)
                sb.Append(separator);
            sb.Append(term(i));
        }
    }

    /// <summary>
    /// Runs one statement through the real executor against a table that holds rows, so the per-row
    /// evaluator runs and not only parse and analysis. Prints the stage reached, so a child that dies
    /// says whether parse alone survived. Exit code 0 means the statement completed without an
    /// overflow; an exception is reported as exit 5 with its message, which is also a survival.
    /// </summary>
    private static async Task<int> ChildAsync(string shape, int terms)
    {
        string sql = BuildSql(shape, terms);

        SQLParserProcessor.Parse(sql);
        Console.WriteLine("stage=parsed");
        Console.Out.Flush();

        if (shape == "parse-or")
        {
            Environment.Exit(0);
            return 0;
        }

        InsertBenchHarness.Context ctx = await InsertBenchHarness.StartAsync(
            "probe-expr", InsertBenchIndexShape.PrimaryKeyOnly, wideRows: false, CamusDBOptions.Default);

        KvTransaction seed = await ctx.Db.Transactions.BeginAsync();
        await ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            seed, ctx.DbName, InsertBenchHarness.BuildInsertSql(5, wideRows: false), null));
        await ctx.Db.Transactions.CommitAsync(seed);

        Console.WriteLine("stage=seeded");
        Console.Out.Flush();

        int code = 0;

        try
        {
            KvTransaction tx = await ctx.Db.Transactions.BeginAsync();
            long rows = 0;

            if (shape.StartsWith("select", StringComparison.Ordinal))
            {
                (DatabaseDescriptor ___, IAsyncEnumerable<QueryResultRow> cursor) = await ctx.Executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(tx, ctx.DbName, sql, null));

                await foreach (QueryResultRow __ in cursor)
                    rows++;
            }
            else
            {
                rows = (await ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, ctx.DbName, sql, null))).ModifiedRows;
            }

            Console.WriteLine($"stage=executed rows={rows}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"stage=exception {ex.GetType().Name}: {Truncate(ex.Message, 160)}");
            code = 5;
        }

        Console.Out.Flush();
        Environment.Exit(code);
        return code;
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s[..max] + "…";

    private enum Outcome { Survived, Refused, Died, TimedOut }

    private sealed record ChildResult(Outcome Outcome, string Stage, string Frame);

    /// <summary>
    /// Doubles the term count until a child dies, then bisects to within 5 %. Reports the stage a
    /// surviving child reached and the repeated frame of a child that died.
    /// </summary>
    private static void Scan(string shape)
    {
        Console.WriteLine($"── {shape} ──────────────────────────────");

        int lastGood = 0;
        int firstBad = 0;

        for (int terms = StartTerms(shape); terms <= 256_000; terms *= 2)
        {
            ChildResult r = RunChild(shape, terms);
            Report(shape, terms, r);

            if (r.Outcome == Outcome.TimedOut)
                return;

            if (r.Outcome is Outcome.Survived or Outcome.Refused)
            {
                lastGood = terms;
                continue;
            }

            firstBad = terms;
            break;
        }

        if (firstBad == 0)
        {
            Console.WriteLine($"no failure up to {lastGood:N0} terms");
            Console.WriteLine();
            return;
        }

        while (firstBad - lastGood > Math.Max(shape == "select-subquery" ? 1 : 50, firstBad / 20))
        {
            int mid = lastGood + ((firstBad - lastGood) / 2);
            ChildResult r = RunChild(shape, mid);
            Report(shape, mid, r);

            if (r.Outcome == Outcome.TimedOut)
                break;

            if (r.Outcome is Outcome.Survived or Outcome.Refused)
                lastGood = mid;
            else
                firstBad = mid;
        }

        Console.WriteLine(
            $"{shape}: survives {lastGood:N0} terms, dies by {firstBad:N0} " +
            $"(statement {BuildSql(shape, firstBad).Length / 1024.0:N0} KiB)");
        Console.WriteLine();
    }

    /// <summary>Subquery nesting is expensive per level, so its scan starts small.</summary>
    private static int StartTerms(string shape) => shape == "select-subquery" ? 8 : 250;

    private static void Report(string shape, int terms, ChildResult r)
    {
        Console.WriteLine($"{terms,9:N0} terms → {r.Outcome,-9} {r.Stage} {r.Frame}");
        Console.Out.Flush();
    }

    private static ChildResult RunChild(string shape, int terms)
    {
        string exe = Environment.ProcessPath!;
        string dll = typeof(ExpressionDepthProbes).Assembly.Location;

        ProcessStartInfo psi = new()
        {
            FileName = exe,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        if (Path.GetFileNameWithoutExtension(exe).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            psi.ArgumentList.Add(dll);

        psi.ArgumentList.Add("probe");
        psi.ArgumentList.Add("exprchild");
        psi.ArgumentList.Add(shape);
        psi.ArgumentList.Add(terms.ToString());

        using Process child = Process.Start(psi)!;

        Task<string> stdout = child.StandardOutput.ReadToEndAsync();
        Task<string> stderr = child.StandardError.ReadToEndAsync();

        if (!child.WaitForExit(180_000))
        {
            try { child.Kill(entireProcessTree: true); } catch { /* already gone */ }
            return new ChildResult(Outcome.TimedOut, "", "");
        }

        string output = stdout.GetAwaiter().GetResult();
        string error = stderr.GetAwaiter().GetResult();

        string stage = output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .LastOrDefault(l => l.StartsWith("stage=", StringComparison.Ordinal)) ?? "stage=none";

        Outcome outcome = child.ExitCode switch
        {
            0 => Outcome.Survived,
            5 => Outcome.Refused,
            _ => Outcome.Died,
        };

        return new ChildResult(outcome, stage, outcome == Outcome.Died ? RepeatedFrame(error) : "");
    }

    /// <summary>
    /// Pulls the "Repeated N times" block out of a .NET stack-overflow report, which names the frame
    /// the recursion went through and how deep it went.
    /// </summary>
    private static string RepeatedFrame(string stderr)
    {
        string[] lines = stderr.Split('\n');

        for (int i = 0; i < lines.Length; i++)
        {
            if (!lines[i].Contains("Repeat", StringComparison.Ordinal))
                continue;

            StringBuilder sb = new();
            sb.Append(lines[i].Trim());

            for (int j = i + 1; j < Math.Min(lines.Length, i + 4); j++)
                sb.Append(" | ").Append(lines[j].Trim());

            return sb.ToString();
        }

        return lines.Length > 0 ? "stderr: " + Truncate(string.Join(" | ", lines.Take(3)), 200) : "";
    }
}
