
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Represents a node in the SQL parse tree produced by <see cref="SQLParserProcessor.Parse"/>.
/// </summary>
/// <remarks>
/// <b>Immutability invariant:</b> a <see cref="NodeAst"/> returned by
/// <see cref="SQLParserProcessor.Parse"/> must be treated as immutable after it is returned.
/// Identifier text is preserved verbatim in the exact case the user wrote it; the parser does
/// not fold case. Case-insensitive table/column/index matching is done at lookup time via
/// case-insensitive comparers, so callers must compare identifier text case-insensitively rather
/// than assume it was lower-cased. Downstream transformations
/// (<c>SubqueryRewriter</c>, binders, planners) must <b>construct new nodes</b> rather than
/// modifying fields of an existing node. This invariant is what makes sharing a single cached
/// <see cref="NodeAst"/> across concurrent executions of the same SQL text safe.
/// <para>
/// <see cref="Null"/>, <see cref="True"/>, <see cref="TypeInteger64"/>, and other static leaf
/// sentinels are shared across all parsed trees; they must never be mutated in place.
/// </para>
/// </remarks>
public sealed class NodeAst
{
	public NodeType nodeType;

    public NodeAst? leftAst;

    public NodeAst? rightAst;

    public NodeAst? extendedOne;

	public NodeAst? extendedTwo;

    public NodeAst? extendedThree;

    public NodeAst? extendedFour;

    public NodeAst? extendedFive;

    public NodeAst? extendedSix;

    /// <summary>
    /// Seventh optional child slot, appended after <see cref="extendedSix"/>. Currently carries the
    /// <c>AS OF SYSTEM TIME</c> value node (a <see cref="NodeType.String"/>, <see cref="NodeType.Integer"/>,
    /// or <see cref="NodeType.Placeholder"/>) on a <see cref="NodeType.Select"/> node; null when the
    /// SELECT has no time-travel clause. Kept as a trailing defaulted constructor parameter so the
    /// dozens of existing positional <c>new(...)</c> call sites in the grammar remain unchanged.
    /// </summary>
    public NodeAst? extendedSeven;

    public string? yytext;

    public NodeAst(
        NodeType nodeType,
        NodeAst? leftAst,
        NodeAst? rightAst,
        NodeAst? extendedOne,
        NodeAst? extendedTwo,
        NodeAst? extendedThree,
        NodeAst? extendedFour,
        NodeAst? extendedFive,
        string? yytext,
        NodeAst? extendedSix = null,
        NodeAst? extendedSeven = null
    )
	{
		this.nodeType = nodeType;
		this.leftAst = leftAst;
		this.rightAst = rightAst;
		this.extendedOne = extendedOne;
		this.extendedTwo = extendedTwo;
        this.extendedThree = extendedThree;
        this.extendedFour = extendedFour;
        this.extendedFive = extendedFive;
        this.extendedSix = extendedSix;
        this.extendedSeven = extendedSeven;
        this.yytext = yytext;
	}

    public static NodeAst FromLong(long value) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, value.ToString());

    // ── Literal sentinels ────────────────────────────────────────────────────

    public static readonly NodeAst Null = Leaf(NodeType.Null, "null");
    public static readonly NodeAst True = Leaf(NodeType.Bool, "true");
    public static readonly NodeAst False = Leaf(NodeType.Bool, "false");

    // ── Expression sentinels ───────────────────────────────────────────────────

    public static readonly NodeAst ExprAllFields = Leaf(NodeType.ExprAllFields);
    public static readonly NodeAst ExprDefault = Leaf(NodeType.ExprDefault);

    // ── DDL type sentinels ───────────────────────────────────────────────────

    public static readonly NodeAst TypeObjectId = Leaf(NodeType.TypeObjectId);
    public static readonly NodeAst TypeString = Leaf(NodeType.TypeString);
    public static readonly NodeAst TypeInteger64 = Leaf(NodeType.TypeInteger64);
    public static readonly NodeAst TypeFloat64 = Leaf(NodeType.TypeFloat64);
    public static readonly NodeAst TypeBool = Leaf(NodeType.TypeBool);
    public static readonly NodeAst TypeFloat32 = Leaf(NodeType.TypeFloat32);
    public static readonly NodeAst TypeBytes = Leaf(NodeType.TypeBytes);
    public static readonly NodeAst TypeDate = Leaf(NodeType.TypeDate);
    public static readonly NodeAst TypeDateTime = Leaf(NodeType.TypeDateTime);
    public static readonly NodeAst TypeUuid = Leaf(NodeType.TypeUuid);
    // TypeArray and TypeStringSized carry child data — construct them dynamically (no shared sentinel).

    // ── DDL constraint sentinels ───────────────────────────────────────────────

    public static readonly NodeAst ConstraintNull = Leaf(NodeType.ConstraintNull);
    public static readonly NodeAst ConstraintNotNull = Leaf(NodeType.ConstraintNotNull);
    public static readonly NodeAst ConstraintPrimaryKey = Leaf(NodeType.ConstraintPrimaryKey);
    public static readonly NodeAst ConstraintUnique = Leaf(NodeType.ConstraintUnique);

    // ── Transaction statement sentinels ──────────────────────────────────────

    public static readonly NodeAst Begin = Leaf(NodeType.Begin);
    public static readonly NodeAst Commit = Leaf(NodeType.Commit);
    public static readonly NodeAst Rollback = Leaf(NodeType.Rollback);

    /// <summary>SET TRANSACTION mode: <c>ReadWrite</c> (stored in <see cref="leftAst"/>).</summary>
    public static readonly NodeAst TransactionModeReadWrite = Leaf(NodeType.String, "ReadWrite");

    /// <summary>SET TRANSACTION mode: <c>ReadOnly</c> (stored in <see cref="leftAst"/>).</summary>
    public static readonly NodeAst TransactionModeReadOnly = Leaf(NodeType.String, "ReadOnly");

    // ── SHOW statement sentinels (no filter argument) ──────────────────────────

    public static readonly NodeAst ShowTables = Leaf(NodeType.ShowTables);
    public static readonly NodeAst ShowDatabase = Leaf(NodeType.ShowDatabase);
    public static readonly NodeAst ShowDatabases = Leaf(NodeType.ShowDatabases);

    private static NodeAst Leaf(NodeType nodeType, string? yytext = null) =>
        new(nodeType, null, null, null, null, null, null, null, yytext);
}
