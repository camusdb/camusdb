
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Validates a SQL statement ticket before any of the three SQL entry points (query, non-query,
/// DDL) parses it. Every transport that binds parameters (gRPC, REST, prepared statements, the
/// batch stream) reaches the engine through those entry points, so this is the one place that
/// checks a bound parameter value for all of them.
/// </summary>
internal sealed class ExecuteSQLValidator : ValidatorBase
{
    public ExecuteSQLValidator(CamusDBOptions options) : base(options) { }

    public void Validate(ExecuteSQLTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.Sql))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "SQL statement is required"
            );

        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
        {
            // Statements that operate at the server level (not tied to a specific database)
            // are valid without a DatabaseName. Parse to check before rejecting.
            NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

            if (!StatementScope.AllowsEmptyContextDatabase(ast.nodeType))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Database name is required"
                );
        }

        CanonicalizeIdParameters(ticket.Parameters);
    }

    /// <summary>
    /// Checks every parameter typed <see cref="ColumnType.Id"/>, and every element of an Id array
    /// parameter. Text that is not an object id (24 hex characters) is refused with
    /// <see cref="CamusDBErrorCodes.InvalidInput"/>. A valid object id with upper-case digits is
    /// replaced in <paramref name="parameters"/> by its lower-case form.
    ///
    /// <para>A transport decodes a wire Id value as <c>new ColumnValue(ColumnType.Id, text)</c> and
    /// does not check the text. The usual invalid value is GUID text, from a client that binds a
    /// <see cref="Guid"/> as an Id parameter; the message then says to bind it as a uuid parameter.
    /// Without this check such a value equals no stored value, so <c>IN</c> returned no rows and
    /// <c>NOT IN</c> returned every row, with no error.</para>
    ///
    /// <para>The engine stores and compares an object id as lower-case text, but the row and index
    /// encoders parse upper-case hex too. Without the lower-case step, an upper-case parameter inserts
    /// correctly and an index seek finds its row, while a table scan compares the text as-is and does
    /// not. The planner picks between the seek and the scan by selectivity, so the result depended on
    /// the size of the table.</para>
    ///
    /// <para>Every transport builds a new dictionary for each statement, so the replacement changes
    /// no caller state. A retry of the same ticket sees values that are already canonical.</para>
    /// </summary>
    private static void CanonicalizeIdParameters(Dictionary<string, ColumnValue>? parameters)
    {
        if (parameters is null)
            return;

        List<KeyValuePair<string, ColumnValue>>? replaced = null;

        foreach (KeyValuePair<string, ColumnValue> parameter in parameters)
        {
            ColumnValue? canonical = CanonicalIdParameter(parameter.Key, parameter.Value);
            if (canonical is not null)
                (replaced ??= []).Add(new(parameter.Key, canonical));
        }

        // A dictionary must not change while it is enumerated, so the replacements go in after.
        if (replaced is null)
            return;

        foreach (KeyValuePair<string, ColumnValue> parameter in replaced)
            parameters[parameter.Key] = parameter.Value;
    }

    /// <summary>
    /// The lower-case form of an Id parameter, or null when the value is not an Id (or an Id array)
    /// or is already canonical. Throws when an Id value is not an object id.
    /// </summary>
    private static ColumnValue? CanonicalIdParameter(string name, ColumnValue value)
    {
        if (value.Type == ColumnType.Id)
        {
            string? lower = CanonicalObjectId(name, value.StrValue);
            return lower is null ? null : new ColumnValue(ColumnType.Id, lower);
        }

        if (value is not { Type: ColumnType.Array, ArrayElementType: ColumnType.Id, ArrayValues: { } elements })
            return null;

        ColumnValue[]? rebuilt = null;

        for (int i = 0; i < elements.Count; i++)
        {
            if (elements[i].Type != ColumnType.Id)
                continue;

            string? lower = CanonicalObjectId(name, elements[i].StrValue);
            if (lower is null)
                continue;

            if (rebuilt is null)
            {
                rebuilt = new ColumnValue[elements.Count];
                for (int j = 0; j < elements.Count; j++)
                    rebuilt[j] = elements[j];
            }

            rebuilt[i] = new ColumnValue(ColumnType.Id, lower);
        }

        return rebuilt is null ? null : ColumnValue.FromArray(ColumnType.Id, rebuilt);
    }

    private const int MaxShownValueLength = 64;

    /// <summary>
    /// Returns null when <paramref name="text"/> is already a lower-case object id, the lower-case
    /// form when it is an object id with upper-case digits, and throws when it is not an object id.
    /// </summary>
    private static string? CanonicalObjectId(string name, string? text)
    {
        if (text is null || CastScalarFunctions.IsValidLowerHexObjectId(text))
            return null;

        if (IsHexObjectId(text))
            return text.ToLowerInvariant();

        string hint = Guid.TryParse(text, out _)
            ? " The value is a UUID: bind it as a uuid parameter, not as an object id."
            : "";

        string shown = text.Length <= MaxShownValueLength ? text : string.Concat(text.AsSpan(0, MaxShownValueLength), "...");

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Parameter '{name}' is typed as an object id, but its value '{shown}' is not a 24-character hex object id.{hint}");
    }

    private static bool IsHexObjectId(string text)
    {
        if (text.Length != 24)
            return false;

        foreach (char c in text)
        {
            if (!char.IsAsciiHexDigit(c))
                return false;
        }

        return true;
    }
}
