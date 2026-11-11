/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// What an inline function does when an argument is NULL. Persisted by value: never renumber.
/// </summary>
public enum InlineFunctionNullInput
{
    /// <summary>
    /// <c>RETURNS NULL ON NULL INPUT</c>, the default: a NULL argument makes the result NULL and the
    /// body does not run. The parameters have their plain CLR types (<c>string</c>, <c>long</c>).
    /// </summary>
    ReturnsNull = 0,

    /// <summary>
    /// <c>CALLED ON NULL INPUT</c>: the body runs and sees <c>null</c>. Every parameter has its
    /// nullable CLR type (<c>string?</c>, <c>long?</c>).
    /// </summary>
    Called = 1
}

/// <summary>One parameter of an inline function, as the statement declared it.</summary>
public sealed class InlineFunctionParameter
{
    /// <summary>
    /// The parameter name, in the case the statement wrote. The body names it in this exact case,
    /// because C# identifiers are case-sensitive.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>The SQL type; <see cref="ColumnType.Array"/> for an array.</summary>
    public ColumnType Type { get; set; }

    /// <summary>The element type of an array parameter, and null for a scalar.</summary>
    public ColumnType? ElementType { get; set; }
}

/// <summary>
/// One SQL scalar function whose body is C# source text, created by <c>CREATE FUNCTION ... LANGUAGE
/// csharp AS $$ ... $$</c>. Stored at <c>{dbId}/meta/function:{name}</c>, in the <c>{dbId}/meta</c>
/// bucket, so the schema load reads it and a database drop purges it with the rest of the bucket.
/// Changes replicate through the schema log (<see cref="SchemaOp.CreateFunction"/>,
/// <see cref="SchemaOp.ReplaceFunction"/>, <see cref="SchemaOp.DropFunction"/>,
/// <see cref="SchemaOp.SetFunctionOwner"/>).
///
/// <para><b>Source, never code.</b> The record carries the source text and its SHA-256. Each node
/// compiles the source itself, at the first call, and keeps the compiled code in memory only. The
/// compiler is part of the server build, so every node of one build compiles one source to the same
/// behavior. <see cref="LanguageVersion"/> and <see cref="RuleRevision"/> record what the proposer
/// compiled with; a node whose rules refuse the source fails the calls of this function on that node
/// only.</para>
///
/// <para><b>Deterministic by construction.</b> The server compiles a body only when every member it
/// names is on a list of members whose result depends on their inputs alone. There is therefore no
/// volatility flag: a view, a CHECK constraint, a column default and the result cache can all use an
/// inline function.</para>
///
/// <para><b>Ownership.</b> <see cref="OwnerId"/> is the account that may replace or drop the function
/// besides a superuser. A function does not run with its owner's privileges: it receives its arguments
/// only, and can read no table.</para>
///
/// <para><b>Immutable once published.</b> A change replaces the record (and the map that holds it), so
/// a statement that resolved the name against the old record keeps a consistent view, and the code a
/// node compiled for a record goes away with it.</para>
/// </summary>
public sealed class InlineFunctionDefinition
{
    /// <summary>The only language an inline function can be written in.</summary>
    public const string CSharp = "csharp";

    /// <summary>The SQL name, in the case the statement wrote. Names compare case-insensitively.</summary>
    public string Name { get; set; } = "";

    /// <summary>The parameters, in call order.</summary>
    public List<InlineFunctionParameter> Parameters { get; set; } = [];

    /// <summary>The SQL type of the result; <see cref="ColumnType.Array"/> for an array.</summary>
    public ColumnType ReturnType { get; set; }

    /// <summary>The element type of an array result, and null for a scalar.</summary>
    public ColumnType? ReturnElementType { get; set; }

    /// <summary>What a NULL argument does.</summary>
    public InlineFunctionNullInput NullInput { get; set; }

    /// <summary>The language of <see cref="Source"/>. Always <see cref="CSharp"/> today.</summary>
    public string Language { get; set; } = CSharp;

    /// <summary>The body of the function: the statements of a C# method, exactly as the statement gave them.</summary>
    public string Source { get; set; } = "";

    /// <summary>The lowercase hex SHA-256 of the UTF-8 bytes of <see cref="Source"/>.</summary>
    public string SourceSha256 { get; set; } = "";

    /// <summary>The C# language version the proposer compiled the body with, as text (for example <c>CSharp13</c>).</summary>
    public string LanguageVersion { get; set; } = "";

    /// <summary>The revision of the allowed-member list the proposer checked the body against.</summary>
    public int RuleRevision { get; set; }

    /// <summary>The name of the owning account when the function was created or last given away. For display.</summary>
    public string? Owner { get; set; }

    /// <summary>
    /// The immutable id of the owning account, which the ownership check compares. Null when the
    /// function was created with authentication off; then only a superuser passes the check once
    /// authentication is on.
    /// </summary>
    public string? OwnerId { get; set; }

    /// <summary>When the function was created, on the proposing node's clock. Tells two functions of one name apart.</summary>
    public HLCTimestamp CreatedAt { get; set; }

    /// <summary>When the record last changed (equal to <see cref="CreatedAt"/> before any change).</summary>
    public HLCTimestamp UpdatedAt { get; set; }

    /// <summary>
    /// How many times <c>CREATE OR REPLACE</c> replaced the body: 0 for a new function. With
    /// <see cref="CreatedAt"/> it identifies one exact record, which is how a re-delivered delta is
    /// recognized.
    /// </summary>
    public long Generation { get; set; }

    /// <summary>
    /// True when <paramref name="other"/> declares the same parameter types, in order, and the same
    /// result type. Parameter names are not part of the signature, because a call passes arguments by
    /// position, and neither is the NULL rule, which changes no type. A replace that keeps the
    /// signature cannot make a stored call of the function invalid.
    /// </summary>
    public bool HasSameSignature(InlineFunctionDefinition other)
    {
        if (ReturnType != other.ReturnType || ReturnElementType != other.ReturnElementType ||
            Parameters.Count != other.Parameters.Count)
            return false;

        for (int i = 0; i < Parameters.Count; i++)
        {
            if (Parameters[i].Type != other.Parameters[i].Type || Parameters[i].ElementType != other.Parameters[i].ElementType)
                return false;
        }

        return true;
    }

    /// <summary>
    /// True when <paramref name="other"/> computes the same thing: the same signature, the same
    /// parameter names (the body reads them), the same NULL rule and the same source text. A replace
    /// that changes none of these leaves the result of every call as it was.
    /// </summary>
    public bool HasSameMeaning(InlineFunctionDefinition other)
    {
        if (!HasSameSignature(other) || NullInput != other.NullInput ||
            !string.Equals(Source, other.Source, StringComparison.Ordinal) ||
            !string.Equals(Language, other.Language, StringComparison.OrdinalIgnoreCase))
            return false;

        for (int i = 0; i < Parameters.Count; i++)
        {
            if (!string.Equals(Parameters[i].Name, other.Parameters[i].Name, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    /// <summary>True when the two records are the same version of the same function.</summary>
    public bool IsSameRecord(InlineFunctionDefinition other) =>
        CreatedAt == other.CreatedAt
        && Generation == other.Generation
        && UpdatedAt == other.UpdatedAt
        && string.Equals(SourceSha256, other.SourceSha256, StringComparison.Ordinal)
        && string.Equals(OwnerId, other.OwnerId, StringComparison.Ordinal)
        && string.Equals(Owner, other.Owner, StringComparison.Ordinal);
}
