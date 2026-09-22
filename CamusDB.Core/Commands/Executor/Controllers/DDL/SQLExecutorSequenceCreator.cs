/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Turns a parsed sequence statement into its ticket, and in doing so is the one place that reads
/// the option list.
///
/// <para><b>The grammar hands over a flat chain of words and numbers, not structured options.</b>
/// That is deliberate — see the sequence productions in the grammar — and it makes this class the
/// whole option language: which words exist, how many values each takes, which combinations
/// contradict each other, and which are parsed only so they can be refused with a message instead
/// of a syntax error.</para>
///
/// <para><b>Every option word is a plain identifier.</b> A user's column named <c>cache</c> or
/// <c>cycle</c> keeps working, and the price is that a misspelled option reaches this code rather
/// than the tokenizer — so each rejection names what was expected rather than only what was
/// wrong.</para>
/// </summary>
internal sealed class SQLExecutorSequenceCreator
{
    /// <summary>
    /// What one option word can be followed by. The grammar cannot express this, because the words
    /// are identifiers; the shape is checked here as the chain is walked.
    /// </summary>
    private enum OptionWord
    {
        Start,
        Increment,
        MinValue,
        MaxValue,
        Cache,
        Cycle,
        Restart,
        No,
        Owned,
    }

    /// <summary>
    /// What <c>CREATE SEQUENCE</c> uses when the statement writes no <c>CACHE</c> clause.
    /// </summary>
    /// <remarks>
    /// <para><b>One, matching PostgreSQL</b> — so a plain <c>CREATE SEQUENCE</c> is gap-free: each
    /// value is reserved durably before it is handed out, and a restart, a leadership change or an
    /// eviction loses nothing. The cost is one Raft commit, with its fsync, per value drawn.</para>
    ///
    /// <para>The alternative was to leave it unset and follow the node-wide block size, which is a
    /// thousand. That is far faster, but it means a plain sequence skips up to a thousand values
    /// every restart — behaviour no other database gives by default, arriving without the
    /// statement saying anything about it. A caller who wants the throughput asks for it by name
    /// with <c>CACHE 1000</c>, which is the direction a surprise should run in.</para>
    ///
    /// <para>This applies at creation only. A sequence created before this default recorded no
    /// cache of its own and still follows the node-wide setting; <c>ALTER SEQUENCE … CACHE 1</c>
    /// moves it.</para>
    /// </remarks>
    private const int DefaultCacheSize = 1;

    internal CreateSequenceTicket CreateCreateSequenceTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst?.yytext is not { Length: > 0 } sequenceName)
            throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Invalid CREATE SEQUENCE AST");

        ParsedSequenceOptions options = ParseOptions(ast.rightAst, allowRestart: false);

        // PostgreSQL's defaults, and the ones a user who writes no options expects: start at 1,
        // step by 1, no ceiling, and — see below — a cache of one.
        long increment = options.Increment ?? 1;
        long minValue = options.MinValue ?? (options.NoMinValue ? long.MinValue + increment : 1);
        long startValue = options.StartValue ?? minValue;

        return new CreateSequenceTicket(
            ticket.DatabaseName,
            sequenceName,
            startValue,
            increment,
            minValue,
            options.NoMaxValue ? null : options.MaxValue,
            options.CacheSize ?? DefaultCacheSize,
            ifNotExists: ast.nodeType == NodeType.CreateSequenceIfNotExists);
    }

    internal static DropSequenceTicket CreateDropSequenceTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst?.yytext is not { Length: > 0 } sequenceName)
            throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Invalid DROP SEQUENCE AST");

        return new DropSequenceTicket(
            ticket.DatabaseName, sequenceName, ifExists: ast.nodeType == NodeType.DropSequenceIfExists);
    }

    internal static RenameSequenceTicket CreateRenameSequenceTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst?.yytext is not { Length: > 0 } sequenceName ||
            ast.rightAst?.yytext is not { Length: > 0 } newName)
            throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Invalid ALTER SEQUENCE … RENAME TO AST");

        return new RenameSequenceTicket(ticket.DatabaseName, sequenceName, newName);
    }

    internal AlterSequenceTicket CreateAlterSequenceTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst?.yytext is not { Length: > 0 } sequenceName)
            throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Invalid ALTER SEQUENCE AST");

        ParsedSequenceOptions options = ParseOptions(ast.rightAst, allowRestart: true);

        return new AlterSequenceTicket(
            ticket.DatabaseName,
            sequenceName,
            options.StartValue,
            options.Increment,
            options.MinValue,
            options.NoMinValue,
            options.MaxValue,
            options.NoMaxValue,
            options.CacheSize,
            removeCacheSize: false,
            options.Restart,
            options.RestartWith);
    }

    /// <summary>The options a statement named, before any default is filled in.</summary>
    private sealed class ParsedSequenceOptions
    {
        internal long? StartValue;
        internal long? Increment;
        internal long? MinValue;
        internal bool NoMinValue;
        internal long? MaxValue;
        internal bool NoMaxValue;
        internal int? CacheSize;
        internal bool Restart;
        internal long? RestartWith;
    }

    /// <summary>
    /// Walks the option chain left to right and folds it into <see cref="ParsedSequenceOptions"/>.
    /// </summary>
    /// <remarks>
    /// Left to right matters: the chain the grammar builds is left-deep, and reading it in source
    /// order is what makes "the last spelling of an option wins" true rather than accidental.
    /// </remarks>
    private static ParsedSequenceOptions ParseOptions(NodeAst? optionChain, bool allowRestart)
    {
        ParsedSequenceOptions options = new();

        List<NodeAst> words = [];
        Flatten(optionChain, words);

        int index = 0;
        while (index < words.Count)
        {
            NodeAst node = words[index];

            if (node.nodeType != NodeType.SequenceOptionWord)
                throw Unexpected(node);

            OptionWord word = ResolveWord(node.yytext!);
            index++;

            switch (word)
            {
                case OptionWord.Start:
                    SkipNoiseWord(words, ref index, "with");
                    options.StartValue = TakeNumber(words, ref index, "START");
                    break;

                case OptionWord.Increment:
                    SkipNoiseWord(words, ref index, "by");
                    options.Increment = TakeNumber(words, ref index, "INCREMENT");
                    break;

                case OptionWord.MinValue:
                    options.MinValue = TakeNumber(words, ref index, "MINVALUE");
                    options.NoMinValue = false;
                    break;

                case OptionWord.MaxValue:
                    options.MaxValue = TakeNumber(words, ref index, "MAXVALUE");
                    options.NoMaxValue = false;
                    break;

                case OptionWord.Cache:
                    long cache = TakeNumber(words, ref index, "CACHE");
                    if (cache is < 1 or > int.MaxValue)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidSequenceDefinition,
                            $"CACHE must be at least 1, got {cache}");
                    options.CacheSize = (int)cache;
                    break;

                case OptionWord.Restart:
                    if (!allowRestart)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidSequenceDefinition,
                            "RESTART is only accepted by ALTER SEQUENCE; use START WITH on CREATE SEQUENCE");
                    options.Restart = true;
                    SkipNoiseWord(words, ref index, "with");
                    // RESTART with no value returns the sequence to its recorded start value, which
                    // is why the catalog keeps that value at all.
                    options.RestartWith = TryTakeNumber(words, ref index);
                    break;

                case OptionWord.No:
                    ApplyNoForm(words, ref index, options);
                    break;

                case OptionWord.Cycle:
                    throw new CamusDBException(
                        CamusDBErrorCodes.FeatureNotSupported,
                        "CYCLE is not supported. A sequence value is guaranteed unique for the life of the " +
                        "sequence, and wrapping the counter back to its minimum would reissue values that " +
                        "committed rows already hold. Write NO CYCLE, or raise MAXVALUE.");

                case OptionWord.Owned:
                    throw new CamusDBException(
                        CamusDBErrorCodes.FeatureNotSupported,
                        "OWNED BY is not supported as a standalone clause. A sequence becomes owned by a " +
                        "column only through SERIAL or GENERATED AS IDENTITY.");

                default:
                    throw Unexpected(node);
            }
        }

        return options;
    }

    /// <summary>
    /// Applies <c>NO MINVALUE</c>, <c>NO MAXVALUE</c> or <c>NO CYCLE</c>. <c>NO CYCLE</c> is the
    /// engine's only behavior, so it is accepted and does nothing — unlike bare <c>CYCLE</c>, which
    /// is refused rather than silently ignored.
    /// </summary>
    private static void ApplyNoForm(List<NodeAst> words, ref int index, ParsedSequenceOptions options)
    {
        if (index >= words.Count || words[index].nodeType != NodeType.SequenceOptionWord)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                "Expected NO MINVALUE, NO MAXVALUE or NO CYCLE");

        string following = words[index].yytext!;
        index++;

        if (string.Equals(following, "minvalue", StringComparison.OrdinalIgnoreCase))
        {
            options.NoMinValue = true;
            options.MinValue = null;
            return;
        }

        if (string.Equals(following, "maxvalue", StringComparison.OrdinalIgnoreCase))
        {
            options.NoMaxValue = true;
            options.MaxValue = null;
            return;
        }

        if (string.Equals(following, "cycle", StringComparison.OrdinalIgnoreCase))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidSequenceDefinition,
            $"Expected NO MINVALUE, NO MAXVALUE or NO CYCLE, got 'NO {following}'");
    }

    /// <summary>
    /// Consumes an optional noise word such as the <c>WITH</c> of <c>START WITH</c> or the
    /// <c>BY</c> of <c>INCREMENT BY</c>. Both spellings are legal SQL, so the word is skipped when
    /// present rather than required or refused.
    /// </summary>
    private static void SkipNoiseWord(List<NodeAst> words, ref int index, string noise)
    {
        if (index < words.Count
            && words[index].nodeType == NodeType.SequenceOptionWord
            && string.Equals(words[index].yytext, noise, StringComparison.OrdinalIgnoreCase))
            index++;
    }

    private static long TakeNumber(List<NodeAst> words, ref int index, string option)
    {
        long? value = TryTakeNumber(words, ref index);

        if (value is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"{option} requires a numeric value");

        return value.Value;
    }

    private static long? TryTakeNumber(List<NodeAst> words, ref int index)
    {
        if (index >= words.Count || words[index].nodeType != NodeType.Integer)
            return null;

        if (!long.TryParse(words[index].yytext, out long value))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidSequenceDefinition,
                $"'{words[index].yytext}' is not a 64-bit integer");

        index++;
        return value;
    }

    private static OptionWord ResolveWord(string word) => word.ToLowerInvariant() switch
    {
        "start" => OptionWord.Start,
        "increment" => OptionWord.Increment,
        "minvalue" => OptionWord.MinValue,
        "maxvalue" => OptionWord.MaxValue,
        "cache" => OptionWord.Cache,
        "cycle" => OptionWord.Cycle,
        "restart" => OptionWord.Restart,
        "no" => OptionWord.No,
        "owned" => OptionWord.Owned,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidSequenceDefinition,
            $"'{word}' is not a sequence option. Accepted: START [WITH] n, INCREMENT [BY] n, " +
            "MINVALUE n, NO MINVALUE, MAXVALUE n, NO MAXVALUE, CACHE n, NO CYCLE, RESTART [WITH n].")
    };

    private static CamusDBException Unexpected(NodeAst node)
        => new(
            CamusDBErrorCodes.InvalidSequenceDefinition,
            $"Unexpected value '{node.yytext}' in the sequence option list");

    /// <summary>
    /// Flattens the left-deep option chain into source order.
    /// </summary>
    /// <remarks>
    /// Iterative rather than recursive: the chain's depth is the number of option words, which
    /// nothing bounds for a caller who repeats an option, and a recursive walk would put that count
    /// on the stack of the thread serving the request.
    /// </remarks>
    private static void Flatten(NodeAst? node, List<NodeAst> into)
    {
        if (node is null)
            return;

        Stack<NodeAst> pending = new();
        pending.Push(node);

        while (pending.Count > 0)
        {
            NodeAst current = pending.Pop();

            if (current.nodeType == NodeType.SequenceOptionList)
            {
                if (current.rightAst is not null)
                    pending.Push(current.rightAst);
                if (current.leftAst is not null)
                    pending.Push(current.leftAst);
                continue;
            }

            into.Add(current);
        }
    }
}
