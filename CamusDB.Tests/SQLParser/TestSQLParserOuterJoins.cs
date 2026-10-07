/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The join forms the grammar accepts and the kind each one stores on its join node: inner, left
/// outer, right outer and cross. The forms that stay syntax errors (FULL, NATURAL, USING, a cross
/// join with ON) are asserted too, so a later grammar change cannot accept them by accident. The
/// wire codec and the view-body renderer must both carry the kind through, because a dropped kind
/// turns a LEFT JOIN back into an inner join with no error.
/// </summary>
public sealed class TestSQLParserOuterJoins
{
    private static NodeAst From(string sql) => SQLParserProcessor.Parse(sql).rightAst!;

    [Test]
    public void LeftJoin_ReadsAsLeft()
    {
        NodeAst join = From("SELECT a.id, b.id FROM a LEFT JOIN b ON b.a_id = a.id");

        Assert.AreEqual(NodeType.Join, join.nodeType);
        Assert.AreEqual(JoinAstKind.Kind.Left, JoinAstKind.Read(join));
        Assert.AreEqual(NodeType.ExprEquals, join.extendedOne!.nodeType, "ON predicate stays in extendedOne");
        Assert.AreEqual("a", join.leftAst!.leftAst!.yytext);
        Assert.AreEqual("b", join.rightAst!.leftAst!.yytext);
    }

    [Test]
    public void LeftOuterJoin_ReadsAsLeft()
    {
        NodeAst join = From("SELECT a.id FROM a LEFT OUTER JOIN b ON b.a_id = a.id");
        Assert.AreEqual(JoinAstKind.Kind.Left, JoinAstKind.Read(join));
    }

    [Test]
    public void RightJoin_AndRightOuterJoin_ReadAsRight()
    {
        Assert.AreEqual(JoinAstKind.Kind.Right, JoinAstKind.Read(From("SELECT a.id FROM a RIGHT JOIN b ON b.a_id = a.id")));
        Assert.AreEqual(JoinAstKind.Kind.Right, JoinAstKind.Read(From("SELECT a.id FROM a RIGHT OUTER JOIN b ON b.a_id = a.id")));
    }

    [Test]
    public void CrossJoin_ReadsAsCross_AndCarriesNoOn()
    {
        NodeAst join = From("SELECT a.id FROM a CROSS JOIN b");

        Assert.AreEqual(NodeType.Join, join.nodeType);
        Assert.AreEqual(JoinAstKind.Kind.Cross, JoinAstKind.Read(join));
        Assert.IsNull(join.extendedOne, "a cross join has no ON predicate");
        Assert.AreEqual("a", join.leftAst!.leftAst!.yytext);
        Assert.AreEqual("b", join.rightAst!.leftAst!.yytext);
    }

    [Test]
    public void MixedChain_EachNodeReadsItsOwnKind()
    {
        // ((((a JOIN b) LEFT JOIN c) CROSS JOIN d) JOIN e): left-deep, kinds read from the top down.
        NodeAst joinE = From(
            "SELECT a.id FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.b_id = b.id CROSS JOIN d JOIN e ON e.d_id = d.id");

        Assert.AreEqual(JoinAstKind.Kind.Inner, JoinAstKind.Read(joinE));
        Assert.AreEqual("e", joinE.rightAst!.leftAst!.yytext);

        NodeAst joinD = joinE.leftAst!;
        Assert.AreEqual(JoinAstKind.Kind.Cross, JoinAstKind.Read(joinD));
        Assert.IsNull(joinD.extendedOne);
        Assert.AreEqual("d", joinD.rightAst!.leftAst!.yytext);

        NodeAst joinC = joinD.leftAst!;
        Assert.AreEqual(JoinAstKind.Kind.Left, JoinAstKind.Read(joinC));
        Assert.AreEqual("c", joinC.rightAst!.leftAst!.yytext);

        NodeAst joinB = joinC.leftAst!;
        Assert.AreEqual(JoinAstKind.Kind.Inner, JoinAstKind.Read(joinB));
        Assert.AreEqual("b", joinB.rightAst!.leftAst!.yytext);
        Assert.AreEqual(NodeType.TableReference, joinB.leftAst!.nodeType);
    }

    [Test]
    public void PlainJoin_AndInnerJoin_ReadAsInner()
    {
        Assert.AreEqual(JoinAstKind.Kind.Inner, JoinAstKind.Read(From("SELECT a.id FROM a JOIN b ON b.a_id = a.id")));
        Assert.AreEqual(JoinAstKind.Kind.Inner, JoinAstKind.Read(From("SELECT a.id FROM a INNER JOIN b ON b.a_id = a.id")));
    }

    [Test]
    public void JoinNodeWithoutKindWord_ReadsAsInner()
    {
        // A node built by code that predates the kind word (an old stored body, a fixture) is inner.
        NodeAst legacy = new(NodeType.Join, null, null, null, null, null, null, null, null);
        Assert.AreEqual(JoinAstKind.Kind.Inner, JoinAstKind.Read(legacy));
    }

    [Test]
    public void CrossJoinWithOn_IsSyntaxError()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT a.id FROM a CROSS JOIN b ON b.a_id = a.id"));
    }

    [Test]
    public void FullOuterJoin_IsRefused()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SELECT a.id FROM a FULL OUTER JOIN b ON b.a_id = a.id"))!;
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code);
        StringAssert.Contains("FULL", ex.Message);
    }

    [Test]
    public void OuterJoinWithoutSide_IsSyntaxErrorThatNamesTheAcceptedForms()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SELECT x.id FROM a x OUTER JOIN b ON b.a_id = x.id"))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
        StringAssert.Contains("LEFT OUTER JOIN", ex.Message);
    }

    [TestCase("SELECT a.id FROM a FULL JOIN b ON b.a_id = a.id")]
    [TestCase("SELECT a.id FROM a full JOIN b ON b.a_id = a.id")]
    [TestCase("SELECT a.id FROM (SELECT id FROM a) FULL JOIN b ON b.a_id = a.id")]
    public void FullJoin_IsRefused_NotParsedAsAnAliasedInnerJoin(string sql)
    {
        // FULL is a plain identifier, so without this guard "a FULL JOIN b" would be an inner join
        // of a table aliased "full" and would silently drop the unmatched rows.
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code, sql);
        StringAssert.Contains("FULL", ex.Message);
    }

    [Test]
    public void FullJoin_AfterAnOnCondition_IsSyntaxError()
    {
        // After an ON condition the word cannot be an alias, so the chained form never reaches the
        // guard: it is a plain syntax error.
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("SELECT a.id FROM a JOIN c ON c.id = a.id FULL JOIN b ON b.a_id = a.id"))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
    }

    [TestCase("SELECT full.id FROM a AS full JOIN b ON b.a_id = full.id")]
    [TestCase("SELECT full.id FROM a AS full INNER JOIN b ON b.a_id = full.id")]
    [TestCase("SELECT full.id FROM a AS `full` JOIN b ON b.a_id = full.id")]
    [TestCase("SELECT full.id FROM a `full` JOIN b ON b.a_id = full.id")]
    [TestCase("SELECT full.id FROM a full INNER JOIN b ON b.a_id = full.id")]
    [TestCase("SELECT full.id FROM (SELECT id FROM a) AS full JOIN b ON b.a_id = full.id")]
    public void AliasNamedFull_IsAcceptedWhenItCannotBeAFullJoin(string sql)
    {
        // Written with AS, quoted, or before INNER JOIN the word is unambiguously an alias: no FULL
        // form exists in those positions, so the guard must not refuse it.
        NodeAst join = From(sql);
        Assert.AreEqual(NodeType.Join, join.nodeType, sql);
        Assert.AreEqual(JoinAstKind.Kind.Inner, JoinAstKind.Read(join), sql);
        Assert.AreEqual("full", join.leftAst!.rightAst!.yytext, sql);
    }

    [TestCase("SELECT full.id FROM a full LEFT JOIN b ON b.a_id = full.id", JoinAstKind.Kind.Left)]
    [TestCase("SELECT full.id FROM a full RIGHT OUTER JOIN b ON b.a_id = full.id", JoinAstKind.Kind.Right)]
    [TestCase("SELECT full.id FROM a full CROSS JOIN b", JoinAstKind.Kind.Cross)]
    public void BareAliasNamedFull_BeforeAnotherJoinForm_IsAnAlias(string sql, JoinAstKind.Kind kind)
    {
        NodeAst join = From(sql);
        Assert.AreEqual(kind, JoinAstKind.Read(join), sql);
        Assert.AreEqual("full", join.leftAst!.rightAst!.yytext, sql);
    }

    [Test]
    public void AliasNamedFull_IsUsableOutsideAJoin()
    {
        Assert.AreEqual("full", SQLParserProcessor.Parse("SELECT full.id FROM a full").rightAst!.rightAst!.yytext);
    }

    [Test]
    public void NaturalJoin_AndUsing_AreSyntaxErrors()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT a.id FROM a NATURAL JOIN b"));
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT a.id FROM a JOIN b USING (id)"));
    }

    [Test]
    public void MatchFull_StillParsesAsForeignKeyOption()
    {
        // FULL stays a plain identifier: reserving it for FULL OUTER JOIN would break this clause.
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE child (id int64 PRIMARY KEY, parent_id int64, " +
            "FOREIGN KEY (parent_id) REFERENCES parent (id) MATCH FULL)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
    }

    [Test]
    public void WireCodec_RoundTripsTheJoinKind()
    {
        NodeAst join = From("SELECT a.id, b.id FROM a LEFT JOIN b ON b.a_id = a.id");

        NodeAst restored = NodeAstWireCodec.Deserialize(NodeAstWireCodec.Serialize(join));

        Assert.AreEqual(NodeType.Join, restored.nodeType);
        Assert.AreEqual(join.yytext, restored.yytext);
        Assert.AreEqual(JoinAstKind.Kind.Left, JoinAstKind.Read(restored));
        Assert.AreEqual(NodeType.ExprEquals, restored.extendedOne!.nodeType);
    }

    private static string Render(string sql) => ViewBodyRenderer.RenderSelect(SQLParserProcessor.Parse(sql));

    [Test]
    public void ViewBodyRenderer_RendersEachKind_AndIsAFixedPoint()
    {
        string left = Render("SELECT a.id, b.id FROM a LEFT JOIN b ON b.a_id = a.id");
        Assert.AreEqual("SELECT a.id, b.id FROM a LEFT OUTER JOIN b ON b.a_id = a.id", left);
        Assert.AreEqual(left, Render(left), "re-rendering a left join must be a fixed point");
        Assert.AreEqual(JoinAstKind.Kind.Left, JoinAstKind.Read(From(left)), "the rendered body must re-parse as left");

        string right = Render("SELECT a.id FROM a RIGHT JOIN b ON b.a_id = a.id");
        Assert.AreEqual("SELECT a.id FROM a RIGHT OUTER JOIN b ON b.a_id = a.id", right);
        Assert.AreEqual(right, Render(right));

        string cross = Render("SELECT a.id FROM a CROSS JOIN b");
        Assert.AreEqual("SELECT a.id FROM a CROSS JOIN b", cross);
        Assert.AreEqual(cross, Render(cross));

        string inner = Render("SELECT a.id FROM a JOIN b ON b.a_id = a.id");
        Assert.AreEqual("SELECT a.id FROM a INNER JOIN b ON b.a_id = a.id", inner, "inner rendering is unchanged");
    }
}
