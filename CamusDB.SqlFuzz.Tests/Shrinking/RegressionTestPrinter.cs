/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Security.Cryptography;
using System.Text;

using CamusDB.SqlFuzz.Tests.Campaign;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;

namespace CamusDB.SqlFuzz.Tests.Shrinking;

/// <summary>
/// Prints a finding as a ready-to-paste NUnit test for <c>CamusDB.Tests</c>. The test derives from
/// <c>BaseTest</c>, replays the shrunk setup through <c>ExecuteSQLTicket</c> with
/// <c>CamusDB.Tests.SqlFuzz.SqlFuzzReplay</c>, and asserts the relation of the oracle, so it fails on
/// the code that gave the finding and passes once the bug is fixed. The seed and the replay command
/// are in a comment.
/// </summary>
internal static class RegressionTestPrinter
{
    /// <summary>The test class name: the oracle and a hash of the signature and the check. An original (unshrunk) finding ends in <c>Original</c>, so both files can sit in one folder.</summary>
    public static string ClassName(Finding finding, bool original = false)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(finding.Signature + "\n" + string.Join("\n", finding.Check.Describe(CamusSqlPrinter.Instance))));
        return "TestSqlFuzz" + Pascal(finding.Oracle.ToString()) + Convert.ToHexString(hash, 0, 4) + (original ? "Original" : "");
    }

    public static string Print(Finding finding, ISqlPrinter? printer = null, bool original = false)
    {
        printer ??= CamusSqlPrinter.Instance;
        string className = ClassName(finding, original);

        TestWriter writer = new();
        writer.Line($"(string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with {{ {TestWriter.BaseOptions} }});");
        writer.Line("SqlFuzzReplay db = new(executor, database, dbname);");
        writer.Line("");

        // The setup replays as SetupRunner ran it: a refused write or read is history, a transaction
        // goes on after a refused statement, and a recorded read runs on the arm it first ran on.
        List<string>? transaction = null;
        foreach (SetupStatement statement in finding.Setup)
        {
            switch (statement.Statement)
            {
                case BeginStmt:
                    transaction = new List<string>();
                    continue;
                case CommitStmt:
                case RollbackStmt:
                    if (transaction is not null)
                        writer.Line($"await db.Transaction({(statement.Statement is CommitStmt ? "true" : "false")}, {string.Join(", ", transaction.Select(TestWriter.Literal))});");
                    transaction = null;
                    continue;
            }

            string sql = printer.Print(statement.Statement);
            if (transaction is not null && statement.Route is StatementRoute.NonQuery or StatementRoute.Query && statement.Arm is null)
            {
                transaction.Add(sql);
                continue;
            }

            string target = statement.Arm is { } arm && PlanEquivalenceOracle.ArmByName(arm) is { } spec ? writer.Arm(spec) : "db";
            writer.Line(statement.Route switch
            {
                StatementRoute.Ddl => $"await db.Ddl({TestWriter.Literal(sql)});",
                StatementRoute.Analyze => $"await db.Analyze({TestWriter.Literal(((AnalyzeStmt)statement.Statement).Table)});",
                StatementRoute.Query => $"await {target}.Read({TestWriter.Literal(sql)});",
                _ => $"await db.Write({TestWriter.Literal(sql)});",
            });
        }

        writer.Line("");
        finding.Check.WriteTest(writer, printer);

        string firstLine = finding.Detail.Split('\n')[0];
        if (firstLine.Length > 300)
            firstLine = firstLine[..300] + " …";

        StringBuilder sb = new();
        sb.Append("/**\n * This file is part of CamusDB\n *\n * For the full copyright and license information, please view the LICENSE.txt\n * file that was distributed with this source code.\n */\n\n");
        sb.Append("using System.Threading.Tasks;\n\nusing NUnit.Framework;\n\n");
        sb.Append("using CamusDB.Core.CommandsExecutor;\nusing CamusDB.Core.CommandsExecutor.Models;\nusing CamusDB.Tests.CommandsExecutor;\nusing CamusDB.Tests.SqlFuzz;\n\n");
        sb.Append("namespace CamusDB.Tests.SqlFuzz;\n\n");
        sb.Append("/// <summary>\n");
        sb.Append(CultureInfo.InvariantCulture, $"/// Regression test printed by the SQL fuzzer. The {finding.Oracle} oracle found <c>{Xml(finding.Signature)}</c>:\n");
        sb.Append(CultureInfo.InvariantCulture, $"/// {Xml(firstLine)}\n");
        if (finding.Reproduced == false)
            sb.Append("/// The finding did not reproduce when the fuzzer replayed it on a new database, so these are the\n/// original statements, not a reduction. The cause may be state that the history does not hold.\n");
        sb.Append("/// Replace this summary with the cause once it is known.\n");
        sb.Append("/// </summary>\n");
        sb.Append("[TestFixture]\n");
        sb.Append(CultureInfo.InvariantCulture, $"internal sealed class {className} : BaseTest\n{{\n");
        sb.Append(CultureInfo.InvariantCulture, $"    // Seed {finding.BaseSeed}, round {finding.Round} (round seed {finding.RoundSeed}), profile {finding.ProfileName}.\n");
        sb.Append(CultureInfo.InvariantCulture, $"    // Shrunk from {finding.OriginalSetupCount} setup statements to {finding.Setup.Count}. Replay in the fuzzer:\n");
        sb.Append(CultureInfo.InvariantCulture, $"    //   SQLFUZZ_SEED={finding.BaseSeed} SQLFUZZ_ROUND={finding.Round} SQLFUZZ_ORACLE={FuzzSettings.OracleName(finding.Oracle)} \\\n");
        sb.Append("    //   dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter \"FullyQualifiedName~.FuzzCampaignTests.\"\n");
        sb.Append("    [Test]\n    [NonParallelizable]\n");
        sb.Append("    public async Task Reproduces()\n    {\n");
        sb.Append(writer.Body);
        sb.Append("    }\n}\n");
        return sb.ToString();
    }

    private static string Pascal(string text) => char.ToUpperInvariant(text[0]) + text[1..];

    private static string Xml(string text) => text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");
}

/// <summary>
/// Writes each finding to the output folder: a report with the seed, the disagreement, the plans and
/// the statements, and the printed regression test. The CI job keeps the folder as an artifact. The
/// campaign writes an <paramref name="original"/> finding as soon as it finds it, before shrinking, and
/// the reduced one after.
/// </summary>
internal static class FindingWriter
{
    public static string Write(string directory, Finding finding, ISqlPrinter? printer = null, bool original = false)
    {
        printer ??= CamusSqlPrinter.Instance;
        Directory.CreateDirectory(directory);
        string className = RegressionTestPrinter.ClassName(finding, original);

        File.WriteAllText(Path.Combine(directory, className + ".cs"), RegressionTestPrinter.Print(finding, printer, original));
        File.WriteAllText(Path.Combine(directory, className + ".md"), Report(finding, printer, original));
        return Path.Combine(directory, className + ".cs");
    }

    public static string Report(Finding finding, ISqlPrinter printer, bool original = false)
    {
        StringBuilder sb = new();
        sb.AppendLine(CultureInfo.InvariantCulture, $"# {finding.Oracle}: {finding.Signature}");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- base seed: {finding.BaseSeed}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- round: {finding.Round} (round seed {finding.RoundSeed})");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- profile: {finding.ProfileName}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- variant: {finding.Check.Variant}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- setup: {finding.Setup.Count} statements (from {finding.OriginalSetupCount})");
        string reproduced = original
            ? "not checked yet: this is the original finding, written before shrinking"
            : finding.Reproduced is { } r ? (r ? "yes" : "NO (the statements are the original ones, not a reduction)") : "not replayed (a hang is not replayed)";
        sb.AppendLine(CultureInfo.InvariantCulture, $"- reproduced on a new database: {reproduced}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- replay: `SQLFUZZ_SEED={finding.BaseSeed} SQLFUZZ_ROUND={finding.Round} SQLFUZZ_ORACLE={FuzzSettings.OracleName(finding.Oracle)} dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter \"FullyQualifiedName~.FuzzCampaignTests.\"`");
        sb.AppendLine();
        sb.AppendLine("## Disagreement");
        sb.AppendLine();
        sb.AppendLine("```text");
        sb.AppendLine(finding.Detail);
        sb.AppendLine("```");
        sb.AppendLine();
        sb.AppendLine("## Setup");
        sb.AppendLine();
        sb.AppendLine("```sql");
        foreach (SetupStatement statement in finding.Setup)
            sb.AppendLine((statement.Arm is null ? "" : $"-- on arm {statement.Arm}: ") + printer.Print(statement.Statement) + ";");
        sb.AppendLine("```");
        sb.AppendLine();
        sb.AppendLine("## Check");
        sb.AppendLine();
        sb.AppendLine("```sql");
        foreach (string line in finding.Check.Describe(printer))
            sb.AppendLine(line + ";");
        sb.AppendLine("```");
        if (finding.Plans.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("## Plans");
            sb.AppendLine();
            sb.AppendLine("```text");
            foreach (string plan in finding.Plans)
                sb.AppendLine(plan);
            sb.AppendLine("```");
        }
        return sb.ToString();
    }
}
