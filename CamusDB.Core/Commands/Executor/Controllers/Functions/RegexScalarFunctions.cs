
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.RegularExpressions;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Registers the Phase-1 PostgreSQL-compatible <c>regexp_*</c> scalar functions:
/// <c>regexp_like</c>, <c>regexp_match</c>, <c>regexp_replace</c>, <c>regexp_count</c>,
/// <c>regexp_instr</c>, <c>regexp_substr</c>, and <c>regexp_split_to_array</c>.
///
/// <para>All functions reuse <see cref="RegexMatcher"/>'s compiled-regex cache and per-match
/// timeout, so the <see cref="CamusDBConfig.RegexMatchTimeoutMs"/> and
/// <see cref="CamusDBConfig.RegexCacheMaxEntries"/> YAML keys also cover these functions.</para>
///
/// <para>The underlying regex engine is .NET <see cref="System.Text.RegularExpressions"/>,
/// not POSIX ERE — see the operator documentation for engine differences and the supported
/// flag characters (<c>i</c>, <c>c</c>, <c>m</c>/<c>n</c>, <c>s</c>, <c>x</c>, <c>g</c>).
/// Back-references in <c>regexp_replace</c> use PostgreSQL syntax (<c>\1</c>…<c>\9</c>,
/// <c>\&amp;</c>) which is translated to .NET syntax (<c>$1</c>…<c>$9</c>, <c>$0</c>) before
/// the replacement is applied.</para>
///
/// <para>Phase-2 set-returning functions (<c>regexp_matches</c>, <c>regexp_split_to_table</c>)
/// are not registered; calling them returns a clear "not supported" error. They require
/// SRF/LATERAL infrastructure that does not exist yet.</para>
/// </summary>
internal static class RegexScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_like",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = EvaluateRegexpLike,
            InferReturnType = _ => ColumnType.Bool,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_match",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = EvaluateRegexpMatch,
            InferReturnType = _ => ColumnType.Array,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_replace",
            MinArity = 3,
            MaxArity = 4,
            Evaluator = EvaluateRegexpReplace,
            InferReturnType = _ => ColumnType.String,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_count",
            MinArity = 2,
            MaxArity = 4,
            Evaluator = EvaluateRegexpCount,
            InferReturnType = _ => ColumnType.Integer64,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_instr",
            MinArity = 2,
            MaxArity = 7,
            Evaluator = EvaluateRegexpInstr,
            InferReturnType = _ => ColumnType.Integer64,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_substr",
            MinArity = 2,
            MaxArity = 6,
            Evaluator = EvaluateRegexpSubstr,
            InferReturnType = _ => ColumnType.String,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_split_to_array",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = EvaluateRegexpSplitToArray,
            InferReturnType = _ => ColumnType.Array,
            IsVolatile = false,
        });

        // Phase-2 set-returning functions: explicitly unsupported with a clear error.
        // They require SRF/LATERAL infrastructure (a planner node that expands one input row
        // into N output rows). Use regexp_match / regexp_split_to_array for the scalar equivalents.
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_matches",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = (name, _) => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{name}' is a set-returning function and is not yet supported. Use regexp_match for the first match's groups."),
            InferReturnType = _ => ColumnType.Array,
            IsVolatile = false,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "regexp_split_to_table",
            MinArity = 2,
            MaxArity = 3,
            Evaluator = (name, _) => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{name}' is a set-returning function and is not yet supported. Use regexp_split_to_array for a scalar equivalent."),
            InferReturnType = _ => ColumnType.String,
            IsVolatile = false,
        });
    }

    // ── regexp_like(text, pattern [, flags]) → bool ────────────────────────────

    private static ColumnValue EvaluateRegexpLike(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);

        (RegexOptions opts, _) = ParseFlags(calledName, args, 2);
        Regex re = RegexMatcher.GetRegex(args[1].StrValue!, opts);
        bool matched = RegexMatcher.GuardTimeout(() => re.IsMatch(args[0].StrValue!));
        return ColumnValue.FromBool(matched);
    }

    // ── regexp_match(text, pattern [, flags]) → text[] | NULL ─────────────────

    private static ColumnValue EvaluateRegexpMatch(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);

        (RegexOptions opts, _) = ParseFlags(calledName, args, 2);
        Regex re = RegexMatcher.GetRegex(args[1].StrValue!, opts);
        Match m = RegexMatcher.GuardTimeout(() => re.Match(args[0].StrValue!));

        if (!m.Success)
            return ColumnValue.Null;

        // No capture groups → one-element array with the whole match (PostgreSQL behavior).
        if (m.Groups.Count <= 1)
            return ColumnValue.FromArray(ColumnType.String, [new ColumnValue(ColumnType.String, m.Value)]);

        // Capture groups → array of groups[1..]; non-participating group → Null element.
        List<ColumnValue> groups = new(m.Groups.Count - 1);
        for (int i = 1; i < m.Groups.Count; i++)
        {
            Group g = m.Groups[i];
            groups.Add(g.Success
                ? new ColumnValue(ColumnType.String, g.Value)
                : ColumnValue.Null);
        }
        return ColumnValue.FromArray(ColumnType.String, groups);
    }

    // ── regexp_replace(text, pattern, replacement [, flags]) → text ───────────

    private static ColumnValue EvaluateRegexpReplace(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null || args[2].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);
        RequireString(calledName, 2, args[2]);

        (RegexOptions opts, bool global) = ParseFlags(calledName, args, 3);
        Regex re = RegexMatcher.GetRegex(args[1].StrValue!, opts);
        string dotnetReplacement = TranslateReplacement(args[2].StrValue!);

        string result = RegexMatcher.GuardTimeout(() =>
            global
                ? re.Replace(args[0].StrValue!, dotnetReplacement)
                : re.Replace(args[0].StrValue!, dotnetReplacement, count: 1));

        return new ColumnValue(ColumnType.String, result);
    }

    // ── regexp_count(text, pattern [, start [, flags]]) → integer ────────────

    private static ColumnValue EvaluateRegexpCount(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);

        int start = 1;
        string? flagsStr = null;

        if (args.Count >= 3)
        {
            if (args[2].Type != ColumnType.Null)
            {
                start = ReadInt32Arg(calledName, 2, args[2], "start");
                if (start < 1)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Function '{calledName}' argument 'start' must be >= 1, got {start}");
            }
        }

        if (args.Count >= 4 && args[3].Type != ColumnType.Null)
        {
            RequireString(calledName, 3, args[3]);
            flagsStr = args[3].StrValue;
        }

        (RegexOptions opts, _) = RegexMatcher.ParseFlags(flagsStr);
        Regex re = RegexMatcher.GetRegex(args[1].StrValue!, opts);
        string subject = args[0].StrValue!;
        // Match from the start offset without slicing, so an anchored '^' still refers to the true
        // start of the string (or line, with 'm') rather than the offset — matching PostgreSQL and
        // the regexp_instr/regexp_substr functions.
        int offset = start <= 1 ? 0 : Math.Min(start - 1, subject.Length);
        int count = RegexMatcher.GuardTimeout(() => re.Matches(subject, offset).Count);
        return new ColumnValue(ColumnType.Integer64, (long)count);
    }

    // ── regexp_instr(text, pattern [, start [, N [, endoption [, flags [, subexpr]]]]]) → integer ─

    private static ColumnValue EvaluateRegexpInstr(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);

        int start = 1, nth = 1, endOption = 0, subexpr = 0;
        string? flagsStr = null;

        if (args.Count >= 3 && args[2].Type != ColumnType.Null)
        {
            start = ReadInt32Arg(calledName, 2, args[2], "start");
            if (start < 1)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'start' must be >= 1, got {start}");
        }
        if (args.Count >= 4 && args[3].Type != ColumnType.Null)
        {
            nth = ReadInt32Arg(calledName, 3, args[3], "N");
            if (nth < 1)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'N' must be >= 1, got {nth}");
        }
        if (args.Count >= 5 && args[4].Type != ColumnType.Null)
        {
            endOption = ReadInt32Arg(calledName, 4, args[4], "endoption");
            if (endOption is not 0 and not 1)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'endoption' must be 0 or 1, got {endOption}");
        }
        if (args.Count >= 6 && args[5].Type != ColumnType.Null)
        {
            RequireString(calledName, 5, args[5]);
            flagsStr = args[5].StrValue;
        }
        if (args.Count >= 7 && args[6].Type != ColumnType.Null)
        {
            subexpr = ReadInt32Arg(calledName, 6, args[6], "subexpr");
            if (subexpr < 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'subexpr' must be >= 0, got {subexpr}");
        }

        (RegexOptions opts, _) = RegexMatcher.ParseFlags(flagsStr);
        Regex re = RegexMatcher.GetRegex(args[1].StrValue!, opts);
        string subject = args[0].StrValue!;
        int offset = start <= 1 ? 0 : Math.Min(start - 1, subject.Length);

        MatchCollection matches = RegexMatcher.GuardTimeout(() => re.Matches(subject, offset));
        if (matches.Count < nth)
            return new ColumnValue(ColumnType.Integer64, 0L); // no N-th match → return 0

        Match m = matches[nth - 1];

        if (subexpr == 0)
        {
            // Whole match position.
            int pos = endOption == 0 ? m.Index : m.Index + m.Length;
            return new ColumnValue(ColumnType.Integer64, (long)(pos + 1)); // 1-based
        }
        else
        {
            // Capture group position.
            if (subexpr >= m.Groups.Count || !m.Groups[subexpr].Success)
                return new ColumnValue(ColumnType.Integer64, 0L);
            Group g = m.Groups[subexpr];
            int pos = endOption == 0 ? g.Index : g.Index + g.Length;
            return new ColumnValue(ColumnType.Integer64, (long)(pos + 1));
        }
    }

    // ── regexp_substr(text, pattern [, start [, N [, flags [, subexpr]]]]) → text | NULL ──

    private static ColumnValue EvaluateRegexpSubstr(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);

        int start = 1, nth = 1, subexpr = 0;
        string? flagsStr = null;

        if (args.Count >= 3 && args[2].Type != ColumnType.Null)
        {
            start = ReadInt32Arg(calledName, 2, args[2], "start");
            if (start < 1)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'start' must be >= 1, got {start}");
        }
        if (args.Count >= 4 && args[3].Type != ColumnType.Null)
        {
            nth = ReadInt32Arg(calledName, 3, args[3], "N");
            if (nth < 1)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'N' must be >= 1, got {nth}");
        }
        if (args.Count >= 5 && args[4].Type != ColumnType.Null)
        {
            RequireString(calledName, 4, args[4]);
            flagsStr = args[4].StrValue;
        }
        if (args.Count >= 6 && args[5].Type != ColumnType.Null)
        {
            subexpr = ReadInt32Arg(calledName, 5, args[5], "subexpr");
            if (subexpr < 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' argument 'subexpr' must be >= 0, got {subexpr}");
        }

        (RegexOptions opts, _) = RegexMatcher.ParseFlags(flagsStr);
        Regex re = RegexMatcher.GetRegex(args[1].StrValue!, opts);
        string subject = args[0].StrValue!;
        int offset = start <= 1 ? 0 : Math.Min(start - 1, subject.Length);

        MatchCollection matches = RegexMatcher.GuardTimeout(() => re.Matches(subject, offset));
        if (matches.Count < nth)
            return ColumnValue.Null;

        Match m = matches[nth - 1];

        if (subexpr == 0)
            return new ColumnValue(ColumnType.String, m.Value);

        if (subexpr >= m.Groups.Count || !m.Groups[subexpr].Success)
            return ColumnValue.Null;

        return new ColumnValue(ColumnType.String, m.Groups[subexpr].Value);
    }

    // ── regexp_split_to_array(text, pattern [, flags]) → text[] ──────────────

    private static ColumnValue EvaluateRegexpSplitToArray(string calledName, IReadOnlyList<ColumnValue> args)
    {
        if (args[0].Type == ColumnType.Null || args[1].Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireString(calledName, 0, args[0]);
        RequireString(calledName, 1, args[1]);

        (RegexOptions opts, _) = ParseFlags(calledName, args, 2);
        string subject = args[0].StrValue!;
        string pattern = args[1].StrValue!;

        string[] parts;

        if (pattern.Length == 0)
        {
            // Empty pattern → split into individual characters (PostgreSQL parity).
            // .NET Regex.Split("abc", "") gives ["", "a", "b", "c", ""] which differs.
            parts = subject.Length == 0
                ? [""]
                : subject.Select(c => c.ToString()).ToArray();
        }
        else
        {
            Regex re = RegexMatcher.GetRegex(pattern, opts);
            parts = RegexMatcher.GuardTimeout(() => re.Split(subject));
        }

        List<ColumnValue> elements = parts.Select(p => new ColumnValue(ColumnType.String, p)).ToList<ColumnValue>();
        return ColumnValue.FromArray(ColumnType.String, elements);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads the optional flags argument at <paramref name="flagsArgIndex"/> from
    /// <paramref name="args"/> and delegates to <see cref="RegexMatcher.ParseFlags"/>.
    /// Returns default (no flags) when the argument is absent or NULL.
    /// </summary>
    private static (RegexOptions Options, bool Global) ParseFlags(
        string calledName, IReadOnlyList<ColumnValue> args, int flagsArgIndex)
    {
        if (args.Count <= flagsArgIndex || args[flagsArgIndex].Type == ColumnType.Null)
            return RegexMatcher.ParseFlags(null);

        RequireString(calledName, flagsArgIndex, args[flagsArgIndex]);
        return RegexMatcher.ParseFlags(args[flagsArgIndex].StrValue);
    }

    /// <summary>
    /// Translates a PostgreSQL-style back-reference replacement string to .NET syntax:
    /// <c>\1</c>…<c>\9</c> → <c>$1</c>…<c>$9</c>, <c>\&amp;</c> → <c>$0</c>,
    /// <c>\\</c> → literal backslash.
    /// <para>A literal <c>$</c> in the PostgreSQL replacement (PostgreSQL never treats <c>$</c> as a
    /// group reference — only backslash does) is escaped to <c>$$</c> so .NET does not
    /// misinterpret <c>$1</c> etc. as a group substitution.</para>
    /// </summary>
    private static string TranslateReplacement(string pgReplacement)
    {
        System.Text.StringBuilder sb = new(pgReplacement.Length + 4);
        for (int i = 0; i < pgReplacement.Length; i++)
        {
            char c = pgReplacement[i];
            if (c == '\\' && i + 1 < pgReplacement.Length)
            {
                char next = pgReplacement[i + 1];
                if (next >= '1' && next <= '9')
                {
                    sb.Append('$');
                    sb.Append(next);
                    i++;
                    continue;
                }
                if (next == '&')
                {
                    sb.Append("$0");
                    i++;
                    continue;
                }
                if (next == '\\')
                {
                    // Literal backslash: backslash is not special in a .NET replacement, so emit one.
                    sb.Append('\\');
                    i++;
                    continue;
                }
            }
            if (c == '$')
            {
                // '$' is literal in a PostgreSQL replacement but significant in .NET ($1, $0, $$,
                // ${name}, $`, $', $_, $+). Escape it so it is emitted literally.
                sb.Append("$$");
                continue;
            }
            sb.Append(c);
        }
        return sb.ToString();
    }

    private static void RequireString(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

    private static void RequireInteger(string calledName, int argumentIndex, ColumnValue argument)
        => ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.Integer64);

    /// <summary>
    /// Validates that argument <paramref name="argumentIndex"/> is an integer and fits in a 32-bit
    /// <see cref="int"/>, returning it. Guards against a large bigint silently truncating (which could
    /// even wrap past a lower-bound check) when used as a position/count argument.
    /// </summary>
    private static int ReadInt32Arg(string calledName, int argumentIndex, ColumnValue argument, string argName)
    {
        RequireInteger(calledName, argumentIndex, argument);
        long value = argument.LongValue;
        if (value is < int.MinValue or > int.MaxValue)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' argument '{argName}' is out of range: {value}");
        return (int)value;
    }
}
