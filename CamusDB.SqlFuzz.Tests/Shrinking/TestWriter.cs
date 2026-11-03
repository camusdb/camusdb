/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Shrinking;

/// <summary>
/// Collects the C# lines of a printed regression test body. The body runs inside a <c>BaseTest</c>
/// method of <c>CamusDB.Tests</c> and has <c>db</c> in scope: a <c>CamusDB.Tests.SqlFuzz.SqlFuzzReplay</c>
/// on the test database. A setup read or a check that needs another engine configuration calls
/// <see cref="Arm"/>, which declares one replay variable per configuration.
/// </summary>
internal sealed class TestWriter
{
    /// <summary>The options every fuzzer engine is built with, as a C# <c>with</c> body (see <c>FuzzEngine.FuzzOptions</c>).</summary>
    public const string BaseOptions = "AutoAnalyzeEnabled = false, TtlEnabled = false, QueryResultCacheEnabled = false";

    private readonly StringBuilder body = new();

    private readonly Dictionary<string, string> arms = new(StringComparer.Ordinal);

    public string Body => body.ToString();

    public void Line(string line)
    {
        if (line.Length > 0)
            body.Append("        ").Append(line);
        body.Append('\n');
    }

    /// <summary>
    /// The replay variable of the configuration <paramref name="spec"/>: on first use, a line declares it
    /// on a second executor built with the arm's options, and a line sets the arm's test seam when it has
    /// one. Later calls return the same variable.
    /// </summary>
    public string Arm(ArmSpec spec)
    {
        if (arms.TryGetValue(spec.Name, out string? name))
            return name;
        name = "arm" + string.Concat(spec.Name.Split('-').Select(p => p.Length == 0 ? p : char.ToUpperInvariant(p[0]) + p[1..]));
        arms[spec.Name] = name;
        string options = spec.OptionsText.Length == 0 ? BaseOptions : BaseOptions + ", " + spec.OptionsText;
        Line($"SqlFuzzReplay {name} = await db.On(CreateCommandExecutor(Options with {{ {options} }}));");
        if (spec.PrepareText is not null)
            Line(string.Format(System.Globalization.CultureInfo.InvariantCulture, spec.PrepareText, name + ".Executor") + ";");
        return name;
    }

    /// <summary>A C# expression that builds <paramref name="spec"/>.</summary>
    public static string WriteSpec(WriteSpec spec) =>
        $"new WriteSpec({Literal(spec.Table)}, {Literal(spec.Kind)}, {Literal(spec.WriteSql)}, "
        + $"{(spec.PredictionSql is null ? "null" : Literal(spec.PredictionSql))}, {Array(spec.AssignedColumns)}, {Array(spec.InsertedKeys)}, "
        + $"{(spec.ProposedRowsSql is null ? "null" : Array(spec.ProposedRowsSql))}, {(spec.ProposedColumns is null ? "null" : Array(spec.ProposedColumns))})";

    /// <summary>A C# string literal for <paramref name="text"/>: a regular literal with escapes, on one line.</summary>
    public static string Literal(string text)
    {
        StringBuilder sb = new(text.Length + 2);
        sb.Append('"');
        foreach (char c in text)
        {
            switch (c)
            {
                case '\\': sb.Append("\\\\"); break;
                case '"': sb.Append("\\\""); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < 0x20 || (c >= 0xD800 && c <= 0xDFFF) || c > 0x7E)
                        sb.Append("\\u").Append(((int)c).ToString("x4"));
                    else
                        sb.Append(c);
                    break;
            }
        }
        sb.Append('"');
        return sb.ToString();
    }

    /// <summary>A C# array of string literals.</summary>
    public static string Array(IEnumerable<string> items) => "new string[] { " + string.Join(", ", items.Select(Literal)) + " }";
}
