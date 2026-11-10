/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// The type rules of an inline function's signature: which SQL types a parameter or a result may
/// have, how each is written in SQL, and which C# type the server-written method declares for it.
///
/// <para>The set is the scalar set of <see cref="ExtensionValueMapping"/> (<c>INT64</c>,
/// <c>FLOAT64</c>, <c>FLOAT32</c>, <c>BOOL</c>, <c>STRING</c>, <c>UUID</c>, <c>BYTES</c>,
/// <c>DATETIME</c>, <c>NUMERIC</c>) and <c>ARRAY(E)</c> over it, except <c>ARRAY(BYTES)</c>, which
/// the mapping has no form for. An array is <c>IReadOnlyList&lt;T&gt;</c> on the C# side.</para>
///
/// <para>This class holds no compiler type, so the browser build, the catalog and the SHOW statements
/// can use it.</para>
/// </summary>
internal static class InlineFunctionSignature
{
    /// <summary>The SQL types an inline function accepts, for error messages.</summary>
    public const string AcceptedTypes = "INT64, FLOAT64, FLOAT32, BOOL, STRING, UUID, BYTES, DATETIME, NUMERIC and ARRAY of one of them except BYTES";

    /// <summary>True when <paramref name="name"/> is a plain SQL identifier: the shape a function name must have.</summary>
    public static bool IsPlainName(string? name)
    {
        if (string.IsNullOrEmpty(name) || !(char.IsAsciiLetter(name[0]) || name[0] == '_'))
            return false;

        foreach (char c in name)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '_')
                return false;
        }

        return true;
    }

    /// <summary>True when a parameter or a result can have this SQL type.</summary>

    public static bool IsAccepted(ColumnType type, ColumnType? elementType)
    {
        if (type == ColumnType.Array)
            return elementType is { } element && element != ColumnType.Bytes && IsScalar(element);

        return elementType is null && IsScalar(type);
    }

    private static bool IsScalar(ColumnType type) => type is
        ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32 or ColumnType.Bool or ColumnType.String or
        ColumnType.Uuid or ColumnType.Bytes or ColumnType.DateTime or ColumnType.Numeric;

    /// <summary>The SQL spelling of a type: <c>INT64</c>, <c>ARRAY(STRING)</c>.</summary>
    public static string SqlTypeName(ColumnType type, ColumnType? elementType)
    {
        if (type == ColumnType.Array)
            return "ARRAY(" + SqlTypeName(elementType ?? ColumnType.Null, null) + ")";

        return type switch
        {
            ColumnType.Integer64 => "INT64",
            ColumnType.Float64 => "FLOAT64",
            ColumnType.Float32 => "FLOAT32",
            ColumnType.Bool => "BOOL",
            ColumnType.String => "STRING",
            ColumnType.Uuid => "UUID",
            ColumnType.Bytes => "BYTES",
            ColumnType.DateTime => "DATETIME",
            ColumnType.Numeric => "NUMERIC",
            _ => type.ToString().ToUpperInvariant(),
        };
    }

    /// <summary>
    /// The C# type the server-written method declares for a SQL type. <paramref name="nullable"/>
    /// gives the form that accepts <c>null</c>: <c>long?</c>, <c>string?</c>. An array element is
    /// always nullable, because any element of a SQL array can be NULL.
    /// </summary>
    public static string CSharpTypeName(ColumnType type, ColumnType? elementType, bool nullable)
    {
        string name = type == ColumnType.Array
            ? "global::System.Collections.Generic.IReadOnlyList<" + CSharpTypeName(elementType ?? ColumnType.Null, null, nullable: true) + ">"
            : type switch
            {
                ColumnType.Integer64 => "long",
                ColumnType.Float64 => "double",
                ColumnType.Float32 => "float",
                ColumnType.Bool => "bool",
                ColumnType.String => "string",
                ColumnType.Uuid => "global::System.Guid",
                ColumnType.Bytes => "byte[]",
                ColumnType.DateTime => "global::System.DateTime",
                ColumnType.Numeric => "decimal",
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Type {type} has no C# form for an inline function"),
            };

        return nullable ? name + "?" : name;
    }

    /// <summary>The SQL signature of a function: <c>slugify(s STRING) RETURNS STRING</c>.</summary>
    public static string Describe(InlineFunctionDefinition function)
    {
        StringBuilder text = new(function.Name.Length + 24 * (function.Parameters.Count + 1));
        text.Append(function.Name).Append('(');
        AppendParameters(text, function);
        return text.Append(") RETURNS ").Append(SqlTypeName(function.ReturnType, function.ReturnElementType)).ToString();
    }

    /// <summary>
    /// Appends <c>a INT64, b STRING</c>. With <paramref name="quoteNames"/>, a parameter name that the
    /// SQL lexer does not read as a plain word (a name such as <c>text</c>, which is a type word) is
    /// written between backquotes, so the text parses again.
    /// </summary>
    public static void AppendParameters(StringBuilder text, InlineFunctionDefinition function, bool quoteNames = false)
    {
        for (int i = 0; i < function.Parameters.Count; i++)
        {
            InlineFunctionParameter parameter = function.Parameters[i];

            if (i > 0)
                text.Append(", ");

            text.Append(quoteNames ? SqlName(parameter.Name) : parameter.Name).Append(' ').Append(SqlTypeName(parameter.Type, parameter.ElementType));
        }
    }

    /// <summary>A name as SQL text: as it is when the lexer reads it as one plain word, and between backquotes when it does not.</summary>
    private static string SqlName(string name) => SQLParser.ExtractCall.IsBareWord(name) ? name : "`" + name + "`";

    /// <summary>
    /// The <c>CREATE FUNCTION</c> statement that creates <paramref name="function"/> again, for
    /// <c>SHOW CREATE FUNCTION</c> and a logical dump. The body goes between dollar quotes; when the
    /// body holds <c>$$</c>, the quotes carry a tag that the body does not hold, so the statement
    /// parses to the same body.
    /// </summary>
    public static string RenderCreate(InlineFunctionDefinition function)
    {
        string quote = DollarQuoteFor(function.Source);

        StringBuilder text = new(function.Source.Length + 128);
        text.Append("CREATE FUNCTION ").Append(SqlName(function.Name)).Append('(');
        AppendParameters(text, function, quoteNames: true);

        text.Append(") RETURNS ").Append(SqlTypeName(function.ReturnType, function.ReturnElementType));
        text.Append(function.NullInput == InlineFunctionNullInput.Called ? " CALLED ON NULL INPUT" : " RETURNS NULL ON NULL INPUT");
        text.Append(" LANGUAGE ").Append(function.Language).Append(" AS ").Append(quote).Append(function.Source).Append(quote);
        return text.ToString();
    }

    /// <summary>
    /// The dollar quote that can delimit <paramref name="body"/>: <c>$$</c>, or <c>$fn$</c>,
    /// <c>$fn1$</c>, ... for a body that holds the plainer ones.
    /// </summary>
    public static string DollarQuoteFor(string body)
    {
        for (int i = -1; ; i++)
        {
            string quote = i < 0 ? "$$" : i == 0 ? "$fn$" : "$fn" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) + "$";

            // The lexer ends the body at the first place where the text reads as the opening quote.
            // That place must be the closing quote itself: a body that holds the quote, or that ends
            // with the first characters of it, would end early.
            if ((body + quote).IndexOf(quote, StringComparison.Ordinal) == body.Length)
                return quote;
        }
    }
}
