/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Security.Cryptography;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Microsoft.CodeAnalysis.CSharp;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// What the compiler needs to turn one body into one loaded unit, for either kind. A plan carries the
/// body, the two server-written wrappers the body is grafted into (one for the check compile and one
/// for the final compile), the names the body sees as locals, and the identity of the unit.
/// </summary>
internal sealed class InlineUnitPlan
{
    public required InlineUnitKind Kind { get; init; }

    /// <summary>"Function" or "Trigger", the first word of every message.</summary>
    public string Label => Kind == InlineUnitKind.Trigger ? "Trigger" : "Function";

    /// <summary>The SQL name of the function or trigger, for messages.</summary>
    public required string Name { get; init; }

    public required string Source { get; init; }

    /// <summary>The first sixteen hex digits of the unit key: the suffix of the wrapper type name and of the assembly name.</summary>
    public required string Unit { get; init; }

    /// <summary>The full name of the wrapper type in the loaded assembly.</summary>
    public required string TypeName { get; init; }

    /// <summary>The wrapper source of the check compile: no budget code, and for a function no reference to the budget assembly.</summary>
    public required string CheckWrapper { get; init; }

    /// <summary>The wrapper source of the final compile, with the public <c>Run</c> method.</summary>
    public required string FinalWrapper { get; init; }

    /// <summary>Whether the check compile needs the budget assembly: a trigger wrapper holds a budget object in a field.</summary>
    public bool CheckNeedsBudget => Kind == InlineUnitKind.Trigger;

    /// <summary>Names the body sees as values with small types, for the bound of the type growth check.</summary>
    public required IReadOnlyList<string> ParameterNames { get; init; }
}

/// <summary>
/// Writes the class that holds a trigger body. The body is an instance method; the instance holds
/// what the body sees by name:
/// <list type="bullet">
/// <item><c>NEW</c> and <c>OLD</c>: the row after and before the change, as classes generated from the
/// table's columns, one property per column with the CLR type of its SQL type. <c>NEW</c> is null for
/// DELETE, <c>OLD</c> for INSERT. A setter writes through the engine's row, which accepts writes in a
/// BEFORE trigger only.</item>
/// <item><c>Sql</c>: the typed session. <c>Context</c>: the contract's context, for a column whose
/// name is no C# identifier. <c>Operation</c>, <c>Timing</c>, <c>TableName</c>, <c>TriggerName</c>.</item>
/// <item>One private method per inline function of the database, named as the function, which calls
/// it through a delegate the engine passes in. The body calls <c>slugify(title)</c>.</item>
/// </list>
/// <para>The budget object and the delegate array sit in fields with the reserved prefix, which the
/// body cannot name. The symbol check allows the generated members because the body cannot declare
/// members, so every member of the class apart from <c>Body</c> and <c>Run</c> is the server's.</para>
/// </summary>
internal static class InlineTriggerWrapper
{
    public const string TypePrefix = "Tg_";

    private const string Functions = InlineFunctionSyntaxChecker.ReservedPrefix + "_functions";

    private const string RowField = InlineFunctionSyntaxChecker.ReservedPrefix + "_row";

    private const string WritableField = InlineFunctionSyntaxChecker.ReservedPrefix + "_writable";

    private const string Writable = InlineFunctionSyntaxChecker.ReservedPrefix + "_Writable";

    /// <summary>
    /// The C# spelling of a property or stub name: a reserved keyword (<c>class</c>, <c>event</c>)
    /// gets a verbatim <c>@</c>, so the body writes <c>NEW.@class</c>. A contextual keyword
    /// (<c>value</c>, <c>var</c>) is a valid name as it is.
    /// </summary>
    private static string MemberName(string name) =>
        SyntaxFacts.GetKeywordKind(name) != Microsoft.CodeAnalysis.CSharp.SyntaxKind.None ? "@" + name : name;

    /// <summary>
    /// The identity of a trigger unit: the body, the language version, the rule revision, the columns
    /// the row classes expose and the signatures of the function stubs. Two triggers on tables with the
    /// same columns in a database with the same functions share a unit.
    /// </summary>
    public static string UnitKey(InlineTriggerSource source)
    {
        StringBuilder text = new(source.Source.Length + 256);
        text.Append("trigger\n").Append(InlineFunctionCompiler.LanguageName).Append('\n');
        text.Append(InlineFunctionAllowList.Revision.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append('\n');

        foreach (InlineTriggerColumn column in source.Columns)
            text.Append(column.Name).Append(':').Append((int)column.Type).Append(':').Append(column.ElementType is { } e ? (int)e : -1).Append('\n');

        text.Append("--\n");

        foreach (InlineFunctionDefinition function in source.Functions)
        {
            text.Append(function.Name).Append('(');
            InlineFunctionSignature.AppendParameters(text, function);
            text.Append("):").Append((int)function.ReturnType).Append(':').Append(function.ReturnElementType is { } r ? (int)r : -1)
                .Append(':').Append(function.NullInput == InlineFunctionNullInput.Called ? "called" : "strict").Append('\n');
        }

        text.Append("--\n").Append(source.Source);

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())));
    }

    /// <summary>True when a column name can be a property name in the generated row classes.</summary>
    public static bool IsPropertyName(string name) =>
        SyntaxFacts.IsValidIdentifier(name) && !name.StartsWith(InlineFunctionSyntaxChecker.ReservedPrefix, StringComparison.Ordinal) &&
        name is not ("NEW" or "OLD" or "Sql" or "Context" or "Operation" or "Timing" or "TableName" or "TriggerName" or "Body" or "Run" or "Row" or "Values");

    /// <summary>The columns that the row classes do not expose, for the notice <c>CREATE TRIGGER</c> gives.</summary>
    public static List<string> UnexposedColumns(IReadOnlyList<InlineTriggerColumn> columns)
    {
        List<string> names = [];

        foreach (InlineTriggerColumn column in columns)
        {
            if (!IsPropertyName(column.Name) || !InlineFunctionSignature.IsAccepted(column.Type, column.ElementType))
                names.Add(column.Name);
        }

        return names;
    }

    public static string Source(InlineTriggerSource source, string unit)
    {
        string type = TypePrefix + unit;
        StringBuilder text = new(2048);

        text.Append("using System;\nusing System.Collections.Generic;\nusing System.Globalization;\nusing System.Linq;\n");
        text.Append("using System.Text;\nusing System.Text.RegularExpressions;\nusing System.Threading.Tasks;\n");
        text.Append("using CamusDB.Extensions;\nusing CamusDB.Extensions.Inline;\n\n");
        text.Append("namespace CamusDB.Inline\n{\n    public sealed class ").Append(type).Append("\n    {\n");

        text.Append("        private readonly ITriggerContext Context;\n");
        text.Append("        private readonly Row? NEW;\n");
        text.Append("        private readonly Row? OLD;\n");
        text.Append("        private readonly InlineSqlSession Sql;\n");
        text.Append("        private readonly TriggerBudget ").Append(InlineFunctionBudgetRewriter.TriggerFrame).Append(";\n");
        text.Append("        private readonly Func<object?[], object?>[] ").Append(Functions).Append(";\n\n");
        text.Append("        private TriggerOperation Operation => Context.Operation;\n");
        text.Append("        private TriggerTiming Timing => Context.Timing;\n");
        text.Append("        private string TableName => Context.TableName;\n");
        text.Append("        private string TriggerName => Context.TriggerName;\n\n");

        text.Append("        private ").Append(type).Append("(ITriggerContext context, TriggerBudget frame, InlineSqlSession sql, Func<object?[], object?>[] functions)\n        {\n");
        text.Append("            Context = context;\n            Sql = sql;\n");
        text.Append("            ").Append(InlineFunctionBudgetRewriter.TriggerFrame).Append(" = frame;\n");
        text.Append("            ").Append(Functions).Append(" = functions;\n");
        text.Append("            NEW = context.New is null ? null : new Row(context.New, context.New);\n");
        text.Append("            OLD = context.Old is null ? null : new Row(context.Old, null);\n        }\n\n");

        text.Append("        public static Task Run(ITriggerContext context, TriggerBudget frame, InlineSqlSession sql, Func<object?[], object?>[] functions) =>\n");
        text.Append("            new ").Append(type).Append("(context, frame, sql, functions).Body();\n\n");

        text.Append("        private async Task Body()\n        {\n        }\n");

        // A function whose name is a C# keyword gets no stub; the body reaches it through SQL.
        for (int i = 0; i < source.Functions.Count; i++)
        {
            if (IsPropertyName(source.Functions[i].Name))
                AppendStub(text, source.Functions[i], i);
        }


        AppendRowClass(text, source.Columns);

        text.Append("    }\n}\n");
        return text.ToString();
    }

    /// <summary>
    /// <c>private string? slugify(string? s) => (string?)functions[0](new object?[] { s });</c>. The
    /// parameters have the nullable CLR types of the function's SQL types; the engine's delegate maps
    /// the values and applies the function's own NULL rule and budget.
    /// </summary>
    private static void AppendStub(StringBuilder text, InlineFunctionDefinition function, int index)
    {
        string result = InlineFunctionSignature.CSharpTypeName(function.ReturnType, function.ReturnElementType, nullable: true);

        text.Append("\n        private ").Append(result).Append(' ').Append(MemberName(function.Name)).Append('(');

        for (int i = 0; i < function.Parameters.Count; i++)
        {
            InlineFunctionParameter parameter = function.Parameters[i];
            text.Append(i > 0 ? ", " : "")
                .Append(InlineFunctionSignature.CSharpTypeName(parameter.Type, parameter.ElementType, nullable: true))
                .Append(" p").Append(i);
        }

        text.Append(") => (").Append(result).Append(')').Append(Functions).Append('[').Append(index).Append("](new object?[] { ");

        for (int i = 0; i < function.Parameters.Count; i++)
            text.Append(i > 0 ? ", " : "").Append('p').Append(i);

        text.Append(" });\n");
    }

    /// <summary>
    /// One row class for NEW and OLD, so that <c>OLD ?? NEW</c> has a type. A setter writes through the
    /// engine's writable row when there is one and it accepts writes (NEW in a BEFORE trigger), and
    /// throws for OLD and for NEW in an AFTER trigger.
    /// </summary>
    private static void AppendRowClass(StringBuilder text, IReadOnlyList<InlineTriggerColumn> columns)
    {
        // The members of the class itself carry the reserved prefix, which no column property and no
        // body can name, so a column called "row" or "writable" is still a property.
        text.Append("\n        public sealed class Row\n        {\n");
        text.Append("            private readonly IRow ").Append(RowField).Append(";\n\n");
        text.Append("            private readonly IWritableRow? ").Append(WritableField).Append(";\n\n");
        text.Append("            public Row(IRow row, IWritableRow? writable) { ").Append(RowField).Append(" = row; ").Append(WritableField).Append(" = writable; }\n\n");
        text.Append("            public IRow Values => ").Append(RowField).Append(";\n\n");
        text.Append("            private IWritableRow ").Append(Writable).Append(" => ").Append(WritableField)
            .Append(" is { IsWritable: true } w ? w : throw new InvalidOperationException(\"only NEW in a BEFORE trigger can be changed\");\n");

        foreach (InlineTriggerColumn column in columns)
        {
            if (!IsPropertyName(column.Name) || !InlineFunctionSignature.IsAccepted(column.Type, column.ElementType))
                continue;

            string clr = InlineFunctionSignature.CSharpTypeName(column.Type, column.ElementType, nullable: true);
            string literal = SymbolDisplay.FormatLiteral(column.Name, quote: true);

            text.Append("\n            public ").Append(clr).Append(' ').Append(MemberName(column.Name)).Append("\n            {\n");
            text.Append("                get => ").Append(Getter(column, literal)).Append(";\n");
            text.Append("                set { ").Append(Setter(column, literal)).Append(" }\n");
            text.Append("            }\n");
        }

        text.Append("        }\n");
    }

    private static string Getter(InlineTriggerColumn column, string literal) => column.Type switch
    {
        ColumnType.Integer64 => RowField + ".GetInt64(" + literal + ")",
        ColumnType.Float64 => RowField + ".GetDouble(" + literal + ")",
        ColumnType.Float32 => RowField + ".GetSingle(" + literal + ")",
        ColumnType.Bool => RowField + ".GetBoolean(" + literal + ")",
        ColumnType.String => RowField + ".GetString(" + literal + ")",
        ColumnType.Uuid => RowField + ".GetGuid(" + literal + ")",
        ColumnType.Bytes => RowField + ".GetBytes(" + literal + ")",
        ColumnType.DateTime => RowField + ".GetDateTime(" + literal + ")",
        ColumnType.Numeric => RowField + ".GetDecimal(" + literal + ")",
        ColumnType.Array => RowField + ".GetArray<" + InlineFunctionSignature.CSharpTypeName(column.ElementType!.Value, null, nullable: true) + ">(" + literal + ")",
        _ => "null",
    };

    private static string Setter(InlineTriggerColumn column, string literal) => column.Type switch
    {
        ColumnType.String => Writable + ".Set(" + literal + ", value);",
        ColumnType.Bytes => Writable + ".Set(" + literal + ", value);",
        ColumnType.Array => Writable + ".SetArray(" + literal + ", value);",
        _ => "if (value is null) " + Writable + ".SetNull(" + literal + "); else " + Writable + ".Set(" + literal + ", value.Value);",
    };
}

#endif
