
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Entrypoint for the SQL Parser
/// </summary>
internal partial class sqlParser
{
    public sqlParser() : base(null) { }

    public NodeAst Parse(string sqlStatement)
    {
        // Feed the SQL string straight into the scanner via SetSource(string). This avoids the
        // per-parse Encoding.Default.GetBytes(...) byte[] copy plus MemoryStream that the previous
        // path allocated, and reads the statement's chars directly — which also sidesteps the
        // Encoding.Default round-trip that could mangle non-ASCII identifiers/string literals.
        var scanner = new sqlScanner();
        scanner.SetSource(sqlStatement, 0);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new CamusDBException(CamusDBErrorCodes.SqlSyntaxError, scanner.YYError);

        NodeAst ast = CurrentSemanticValue.n;

        // Both passes run here, before the tree leaves the parser: the rebalance assigns child slots,
        // which is only allowed until the tree is shared (the parser cache hands one instance to
        // concurrent executions), and the depth check must precede every recursive walker.
        if (ast is not null)
        {
            ExpressionChains.Normalize(ast);
            StatementDepthGuard.Enforce(ast);
        }

        return ast!;
    }

    /// <summary>
    /// The one guidance message every malformed SHOW RANGES / SHOW RANGE input gets. Naming all
    /// four accepted shapes is deliberate: the words RANGES, RANGE and ROW are matched as plain
    /// identifiers so they stay usable as column names, which means a typo reaches the parse action
    /// rather than the tokenizer and produces no positional hint of its own.
    /// </summary>
    private const string ShowRangesGuidance =
        "Expected: SHOW RANGES FROM TABLE <table>, SHOW RANGES FROM INDEX <table>@<index>, "
        + "SHOW RANGE FROM TABLE <table> FOR ROW (<value>, ...), or "
        + "SHOW RANGE FROM INDEX <table>@<index> FOR ROW (<value>, ...)";

    /// <summary>
    /// Validates the word after SHOW and enforces the plural/singular pairing: the plural RANGES
    /// form lists every span and takes no FOR ROW clause, while the singular RANGE form names one
    /// span and requires one. Accepting either word in either shape would let
    /// <c>SHOW RANGES ... FOR ROW</c> parse into a statement whose name contradicts its result.
    /// </summary>
    private static void RequireShowRangesWord(string? word, bool plural)
    {
        if (!string.Equals(word, plural ? "ranges" : "range", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, ShowRangesGuidance);
    }

    /// <summary>ROW is likewise a plain identifier, so the parse action is what pins the word.</summary>
    private static void RequireRowWord(string? word)
    {
        if (!string.Equals(word, "row", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, ShowRangesGuidance);
    }

    /// <summary>
    /// USAGE in <c>SHOW INDEX USAGE</c> is a plain identifier, so the parse action is what pins the
    /// word; anything else after <c>SHOW INDEX</c> is told the two accepted forms.
    /// </summary>
    private static void RequireIndexUsageWord(string? word)
    {
        if (!string.Equals(word, "usage", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "Expected: SHOW INDEX USAGE [FROM <table>] or SHOW INDEX FROM <table>");
    }

    /// <summary>
    /// Validates the word that opens a <c>FLUSH</c> statement. FLUSH is matched as a plain identifier
    /// so the word stays usable as a table and column name, which means a typo reaches this action
    /// rather than the tokenizer and carries no positional hint of its own — hence the message names
    /// both accepted statements.
    /// </summary>
    private static void RequireFlushWord(string? word)
    {
        if (!string.Equals(word, "flush", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "Expected: FLUSH PRIVILEGES or FLUSH SESSIONS");
    }

    /// <summary>
    /// The <see cref="NodeType.OnConflict"/> node of <c>UPSERT INTO t …</c>, after checking that the
    /// statement's first word is UPSERT. UPSERT is a plain identifier, so a misspelled statement reaches
    /// this action and not the tokenizer.
    /// </summary>
    private static NodeAst UpsertClause(string? word)
    {
        if (!string.Equals(word, "upsert", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "Expected: UPSERT INTO <table> ..., got '" + word + " INTO'");

        return new NodeAst(NodeType.OnConflict, null, null, null, null, null, null, null, OnConflictUpsert);
    }

    /// <summary>The <see cref="NodeAst.yytext"/> of an <see cref="NodeType.OnConflict"/> node for DO NOTHING.</summary>
    internal const string OnConflictNothing = "nothing";

    /// <summary>The <see cref="NodeAst.yytext"/> of an <see cref="NodeType.OnConflict"/> node for DO UPDATE.</summary>
    internal const string OnConflictUpdate = "update";

    /// <summary>The <see cref="NodeAst.yytext"/> of the <see cref="NodeType.OnConflict"/> node of an UPSERT.</summary>
    internal const string OnConflictUpsert = "upsert";

    /// <summary>
    /// Joins the target and the action of an <c>ON CONFLICT</c> clause into one
    /// <see cref="NodeType.OnConflict"/> node, after checking the CONFLICT word. DO UPDATE without a
    /// target is refused here, as PostgreSQL refuses it: without an arbiter index the statement cannot
    /// say which stored row the SET list changes.
    /// </summary>
    private static NodeAst OnConflictClause(string? conflictWord, NodeAst? target, NodeAst action)
    {
        if (!string.Equals(conflictWord, "conflict", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "Expected: ON CONFLICT after the INSERT row source, got 'ON " + conflictWord + "'");

        if (target is null && action.yytext == OnConflictUpdate)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "ON CONFLICT DO UPDATE requires a conflict target: a column list such as ON CONFLICT (id), or ON CONFLICT ON CONSTRAINT <name>");

        return new NodeAst(NodeType.OnConflict, target, action.rightAst, action.extendedOne, null, null, null, null, action.yytext);
    }

    /// <summary>The action part of <c>ON CONFLICT … DO NOTHING</c>, after checking the two plain words.</summary>
    private static NodeAst ConflictDoNothing(string? doWord, string? nothingWord)
    {
        if (!string.Equals(doWord, "do", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(nothingWord, "nothing", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "Expected: DO NOTHING or DO UPDATE SET ..., got '" + doWord + " " + nothingWord + "'");

        return new NodeAst(NodeType.OnConflict, null, null, null, null, null, null, null, OnConflictNothing);
    }

    /// <summary>The action part of <c>ON CONFLICT … DO UPDATE SET … [WHERE …]</c>, after checking the DO word.</summary>
    private static NodeAst ConflictDoUpdate(string? doWord, NodeAst setList, NodeAst? where)
    {
        if (!string.Equals(doWord, "do", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput, "Expected: DO UPDATE SET ..., got '" + doWord + " UPDATE'");

        return new NodeAst(NodeType.OnConflict, null, setList, where, null, null, null, null, OnConflictUpdate);
    }

    /// <summary>
    /// Builds a <see cref="NodeType.CreateTrigger"/> node after checking the plain-identifier words
    /// (TRIGGER, BEFORE or AFTER, EXTERNAL NAME) and refusing a repeated event. The timing travels
    /// in <c>yytext</c> ("before" or "after"), the comma-joined events in <c>extendedOne.yytext</c>,
    /// and the EXTERNAL NAME string literal in <c>extendedTwo</c>, still quoted as the user wrote it.
    /// </summary>
    private static NodeAst CreateTriggerStatement(
        string? triggerWord, NodeAst name, NodeAst table, string? timing, string events,
        string? externalWord, string? nameWord, NodeAst externalName)
    {
        const string guidance =
            "Expected: CREATE TRIGGER <name> ON <table> {BEFORE|AFTER} {INSERT|UPDATE|DELETE}[, ...] EXTERNAL NAME '<Namespace.Type>'";

        if (!string.Equals(triggerWord, "trigger", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(externalWord, "external", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(nameWord, "name", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, guidance);

        string normalizedTiming;
        if (string.Equals(timing, "before", StringComparison.OrdinalIgnoreCase))
            normalizedTiming = "before";
        else if (string.Equals(timing, "after", StringComparison.OrdinalIgnoreCase))
            normalizedTiming = "after";
        else
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, guidance);

        string[] parts = events.Split(',');
        if (parts.Distinct(StringComparer.Ordinal).Count() != parts.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "CREATE TRIGGER names an event more than once");

        NodeAst eventNode = new(NodeType.String, null, null, null, null, null, null, null, events);
        return new NodeAst(NodeType.CreateTrigger, name, table, eventNode, externalName, null, null, null, normalizedTiming);
    }

    /// <summary>Builds a <see cref="NodeType.DropTrigger"/> node after checking the word TRIGGER.</summary>
    private static NodeAst DropTriggerStatement(string? triggerWord, NodeAst name, NodeAst table)
    {
        if (!string.Equals(triggerWord, "trigger", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Expected: DROP TRIGGER <name> ON <table>");

        return new NodeAst(NodeType.DropTrigger, name, table, null, null, null, null, null, null);
    }

    private const string ExtensionGuidance =
        "Expected: CREATE EXTENSION [IF NOT EXISTS] <name> [VERSION '<version>'], ALTER EXTENSION <name> UPDATE [TO '<version>'] or DROP EXTENSION [IF EXISTS] <name>";

    /// <summary>
    /// Builds a <see cref="NodeType.CreateExtension"/> node after checking the plain-identifier words
    /// EXTENSION and VERSION. <c>leftAst</c> is the extension name and <c>rightAst</c> the version
    /// string literal, still quoted, or null when the statement names no version. <c>yytext</c> is
    /// "if not exists" for the IF NOT EXISTS form.
    /// </summary>
    private static NodeAst CreateExtensionStatement(string? extensionWord, NodeAst name, string? versionWord, NodeAst? version, bool ifNotExists)
    {
        if (!string.Equals(extensionWord, "extension", StringComparison.OrdinalIgnoreCase) ||
            (version is not null && !string.Equals(versionWord, "version", StringComparison.OrdinalIgnoreCase)))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, ExtensionGuidance);

        return new NodeAst(NodeType.CreateExtension, name, version, null, null, null, null, null, ifNotExists ? "if not exists" : null);
    }

    /// <summary>
    /// Builds a <see cref="NodeType.AlterExtensionUpdate"/> node after checking the word EXTENSION.
    /// <c>rightAst</c> is the target version literal, or null for the highest loaded version.
    /// </summary>
    private static NodeAst AlterExtensionStatement(string? extensionWord, NodeAst name, NodeAst? version)
    {
        if (!string.Equals(extensionWord, "extension", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, ExtensionGuidance);

        return new NodeAst(NodeType.AlterExtensionUpdate, name, version, null, null, null, null, null, null);
    }

    /// <summary>
    /// Builds a <see cref="NodeType.DropExtension"/> node after checking the word EXTENSION.
    /// <c>yytext</c> is "if exists" for the IF EXISTS form.
    /// </summary>
    private static NodeAst DropExtensionStatement(string? extensionWord, NodeAst name, bool ifExists)
    {
        if (!string.Equals(extensionWord, "extension", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, ExtensionGuidance);

        return new NodeAst(NodeType.DropExtension, name, null, null, null, null, null, null, ifExists ? "if exists" : null);
    }

    /// <summary>
    /// Builds a <see cref="NodeType.Call"/> node after checking the word CALL. <c>leftAst</c> is the
    /// procedure name identifier and <c>rightAst</c> the argument list, null for no arguments.
    /// </summary>
    private static NodeAst CallStatement(string? callWord, string? procedure, NodeAst? arguments)
    {
        if (!string.Equals(callWord, "call", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Expected: CALL <procedure>(<arguments>)");

        NodeAst name = new(NodeType.Identifier, null, null, null, null, null, null, null, procedure);
        return new NodeAst(NodeType.Call, name, arguments, null, null, null, null, null, null);
    }

    /// <summary>
    /// Validates the word written where a column's type belongs when that word is a plain
    /// identifier rather than a type token. Only the serial shorthands are accepted there.
    /// </summary>
    /// <remarks>
    /// SERIAL is matched as an identifier so the word stays usable as a table and column name,
    /// which means every mistyped type name — <c>varchar</c>, <c>text</c>, <c>int</c> — also lands
    /// here. The message therefore names the shorthand rather than only rejecting the word, because
    /// the parser cannot tell the two mistakes apart.
    /// </remarks>
    private static void RequireSerialTypeWord(string? word)
    {
        if (string.Equals(word, "serial", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(word, "bigserial", StringComparison.OrdinalIgnoreCase))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"'{word}' is not a column type. Use one of the declared types, or SERIAL for an " +
            "auto-numbered int64 column.");
    }

    /// <summary>
    /// One clause of a foreign key: <c>ON DELETE</c>, <c>ON UPDATE</c>, <c>MATCH</c>,
    /// <c>[NOT] DEFERRABLE</c> or <c>INITIALLY</c>. The node's text is <c>kind:value</c>, for example
    /// <c>on_delete:cascade</c>, so the executor reads every clause the same way whether it came from
    /// the column-level or the table-level form.
    /// </summary>
    /// <remarks>
    /// The parser only checks that the words form a clause. Whether CamusDB supports the clause is
    /// the executor's decision, so an unsupported but well-formed clause gets a clear "not supported"
    /// error rather than a syntax error.
    /// </remarks>
    private static NodeAst ForeignKeyOption(string kind, string value) =>
        new(NodeType.ForeignKeyOption, null, null, null, null, null, null, null, kind + ":" + value);

    /// <summary>The single-word referential actions. NO ACTION, SET NULL and SET DEFAULT have their own tokens.</summary>
    private static string ForeignKeyActionWord(string? word)
    {
        if (string.Equals(word, "restrict", StringComparison.OrdinalIgnoreCase))
            return "restrict";

        if (string.Equals(word, "cascade", StringComparison.OrdinalIgnoreCase))
            return "cascade";

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Expected NO ACTION, RESTRICT, CASCADE, SET NULL or SET DEFAULT, got '{word}'");
    }

    /// <summary><c>MATCH SIMPLE | FULL | PARTIAL</c>, matched as two plain identifiers.</summary>
    private static NodeAst ForeignKeyMatchOption(string? first, string? second)
    {
        if (!string.Equals(first, "match", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Expected a foreign key clause, got '{first} {second}'");

        string? kind = second?.ToLowerInvariant();

        if (kind is not ("simple" or "full" or "partial"))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Expected MATCH SIMPLE, MATCH FULL or MATCH PARTIAL, got 'MATCH {second}'");

        return ForeignKeyOption("match", kind);
    }

    /// <summary><c>INITIALLY DEFERRED | IMMEDIATE</c>.</summary>
    private static NodeAst ForeignKeyInitiallyOption(string? word)
    {
        string? kind = word?.ToLowerInvariant();

        if (kind is not ("deferred" or "immediate"))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Expected INITIALLY DEFERRED or INITIALLY IMMEDIATE, got 'INITIALLY {word}'");

        return ForeignKeyOption("initially", kind);
    }

    /// <summary>
    /// Builds the AST of <c>DROP INDEX [IF EXISTS] &lt;table&gt;.&lt;index&gt;</c>: the same shape as
    /// <c>ALTER TABLE &lt;table&gt; DROP INDEX &lt;index&gt;</c> (<c>leftAst</c> = table, <c>rightAst</c> =
    /// index). The grammar's qualified identifier joins its parts with a dot, so the split happens here.
    /// A bare name is refused: index names are unique per table only, and there is no per-database
    /// index registry to resolve one against. A name with two dots is refused as well.
    /// </summary>
    /// <remarks>
    /// The split is on the joined text, so a quoted table name that contains a dot cannot use this form;
    /// <c>DROP INDEX &lt;index&gt; ON &lt;table&gt;</c> covers it. <c>COMMENT ON INDEX</c> has the same limit.
    /// </remarks>
    private static NodeAst DropIndexQualified(NodeType nodeType, string? qualifiedName)
    {
        string name = qualifiedName ?? "";
        int dot = name.IndexOf('.');

        if (dot <= 0 || dot == name.Length - 1 || name.IndexOf('.', dot + 1) >= 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"DROP INDEX requires a table: use DROP INDEX <index> ON <table> or DROP INDEX <table>.<index>, got '{name}'");

        NodeAst table = new(
            NodeType.Identifier, null, null, null, null, null, null, null,
            name[..dot]);

        NodeAst index = new(
            NodeType.Identifier, null, null, null, null, null, null, null,
            name[(dot + 1)..]);

        return new NodeAst(nodeType, table, index, null, null, null, null, null, null);
    }

    /// <summary>
    /// Splits the single <c>table@index</c> token the scanner produced into the relation name and
    /// the index name. The scanner matches the pair as one token so the '@' is never seen as a bind
    /// placeholder; the split belongs here because the token's text is the only place both halves
    /// survive. The scanner's pattern guarantees exactly one '@' with an identifier on each side.
    /// </summary>
    /// <param name="qualifiedName">The token text, e.g. <c>users@users_pkey</c>.</param>
    /// <param name="forRowValues">The FOR ROW value list, or null for the all-spans form.</param>
    private static NodeAst QualifiedIndexRanges(string qualifiedName, NodeAst? forRowValues)
    {
        int separator = qualifiedName.IndexOf('@');

        NodeAst relation = new(
            NodeType.Identifier, null, null, null, null, null, null, null,
            qualifiedName[..separator]);

        NodeAst index = new(
            NodeType.Identifier, null, null, null, null, null, null, null,
            qualifiedName[(separator + 1)..]);

        return new NodeAst(NodeType.ShowRanges, relation, index, forRowValues, null, null, null, null, null);
    }
}
