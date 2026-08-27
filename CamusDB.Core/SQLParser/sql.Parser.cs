
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

        return CurrentSemanticValue.n;
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
