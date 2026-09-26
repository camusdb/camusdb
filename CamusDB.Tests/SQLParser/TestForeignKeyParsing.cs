/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Foreign-key syntax: the column-level <c>REFERENCES</c> form, the table-level <c>FOREIGN KEY</c> form,
/// <c>ALTER TABLE … ADD FOREIGN KEY</c>, every clause, the tickets built from them, and the validator's
/// refusals. Nothing here needs a database.
/// </summary>
[TestFixture]
public sealed class TestForeignKeyParsing
{
    private const string Database = "testdb";

    // ── Parser ──────────────────────────────────────────────────────────────

    [Test]
    public void ColumnLevelReferenceParses()
    {
        NodeAst reference = SingleColumnConstraint(
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))", "city",
            NodeType.ConstraintForeignKey);

        Assert.AreEqual("cities", reference.leftAst!.yytext);
        Assert.AreEqual("name", reference.rightAst!.yytext);
        Assert.IsNull(reference.yytext, "An unnamed reference carries no name");
    }

    [Test]
    public void NamedColumnLevelReferenceWithoutColumnListParses()
    {
        NodeAst reference = SingleColumnConstraint(
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string CONSTRAINT weather_city_fk REFERENCES cities)", "city",
            NodeType.ConstraintForeignKey);

        Assert.AreEqual("weather_city_fk", reference.yytext);
        Assert.AreEqual("cities", reference.leftAst!.yytext);
        Assert.IsNull(reference.rightAst, "No column list means the parent's primary key");
    }

    /// <summary>
    /// The ambiguity the grammar is shaped around: after REFERENCES, NOT could start NOT DEFERRABLE or
    /// NOT NULL, and a plain word could start MATCH or STORAGE. Each must reach the right constraint.
    /// </summary>
    [TestCase("city string REFERENCES cities NOT NULL", NodeType.ConstraintNotNull)]
    [TestCase("city string REFERENCES cities(name) NOT DEFERRABLE NOT NULL", NodeType.ConstraintNotNull)]
    [TestCase("city string REFERENCES cities(name) ON DELETE NO ACTION STORAGE PLAIN", NodeType.ConstraintStorage)]
    [TestCase("city string REFERENCES cities(name) ON DELETE RESTRICT STORAGE PLAIN", NodeType.ConstraintStorage)]
    [TestCase("city string REFERENCES cities(name) MATCH SIMPLE DEFAULT ('x')", NodeType.ConstraintDefault)]
    [TestCase("city string NOT NULL REFERENCES cities(name) ON UPDATE CASCADE", NodeType.ConstraintNotNull)]
    public void ColumnLevelReferenceCoexistsWithOtherConstraints(string column, NodeType other)
    {
        List<NodeAst> constraints = ColumnConstraints($"CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, {column})", "city");

        Assert.AreEqual(1, constraints.Count(c => c.nodeType == NodeType.ConstraintForeignKey));
        Assert.AreEqual(1, constraints.Count(c => c.nodeType == other), $"'{other}' was not parsed as its own constraint");
    }

    [Test]
    public void TableLevelConstraintParses()
    {
        NodeAst fk = TableConstraint(
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, b int64, " +
            "CONSTRAINT child_ab_fk FOREIGN KEY (a, b) REFERENCES parent (x, y) ON DELETE RESTRICT MATCH SIMPLE)");

        Assert.AreEqual("child_ab_fk", fk.yytext);
        Assert.AreEqual(new[] { "a", "b" }, Names(fk.leftAst));
        Assert.AreEqual("parent", fk.rightAst!.yytext);
        Assert.AreEqual(new[] { "x", "y" }, Names(fk.extendedOne));
        Assert.AreEqual(new[] { "on_delete:restrict", "match:simple" }, Clauses(fk.extendedTwo));
    }

    [Test]
    public void UnnamedTableLevelConstraintWithoutColumnListParses()
    {
        NodeAst fk = TableConstraint(
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, parent_id int64, FOREIGN KEY (parent_id) REFERENCES parent)");

        Assert.IsNull(fk.yytext);
        Assert.IsNull(fk.extendedOne);
        Assert.IsNull(fk.extendedTwo);
    }

    [TestCase("ALTER TABLE child ADD FOREIGN KEY (a) REFERENCES parent (x) ON UPDATE NO ACTION", null)]
    [TestCase("ALTER TABLE child ADD CONSTRAINT child_a_fk FOREIGN KEY (a) REFERENCES parent (x) ON UPDATE NO ACTION", "child_a_fk")]
    public void AlterTableAddForeignKeyParses(string sql, string? name)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(NodeType.AlterTableAddConstraintForeignKey, ast.nodeType);
        Assert.AreEqual("child", ast.leftAst!.yytext);

        NodeAst fk = ast.rightAst!;
        Assert.AreEqual(NodeType.CreateTableConstraintForeignKey, fk.nodeType);
        Assert.AreEqual(name, fk.yytext);
        Assert.AreEqual(new[] { "on_update:no_action" }, Clauses(fk.extendedTwo));
    }

    [TestCase("ON DELETE NO ACTION", "on_delete:no_action")]
    [TestCase("ON DELETE RESTRICT", "on_delete:restrict")]
    [TestCase("ON DELETE CASCADE", "on_delete:cascade")]
    [TestCase("ON DELETE SET NULL", "on_delete:set_null")]
    [TestCase("ON DELETE SET DEFAULT", "on_delete:set_default")]
    [TestCase("on update no   action", "on_update:no_action")]
    [TestCase("ON UPDATE CASCADE", "on_update:cascade")]
    [TestCase("MATCH SIMPLE", "match:simple")]
    [TestCase("match full", "match:full")]
    [TestCase("MATCH PARTIAL", "match:partial")]
    [TestCase("DEFERRABLE", "deferrable:true")]
    [TestCase("NOT DEFERRABLE", "deferrable:false")]
    [TestCase("INITIALLY DEFERRED", "initially:deferred")]
    [TestCase("INITIALLY IMMEDIATE", "initially:immediate")]
    public void EachClauseParsesInBothPositions(string clause, string expected)
    {
        NodeAst table = TableConstraint(
            $"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, FOREIGN KEY (a) REFERENCES parent (x) {clause})");
        Assert.AreEqual(new[] { expected }, Clauses(table.extendedTwo), "table level");

        List<NodeAst> column = ColumnConstraints(
            $"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64 REFERENCES parent (x) {clause})", "a");
        Assert.AreEqual(new[] { expected }, column.Where(c => c.nodeType == NodeType.ForeignKeyOption).Select(c => c.yytext), "column level");
    }

    [TestCase("ON DELETE NOTHING", "NOTHING")]
    [TestCase("ON UPDATE SET", null)]
    [TestCase("MATCH EVERYTHING", "EVERYTHING")]
    [TestCase("INITIALLY LATER", "LATER")]
    [TestCase("SOMETIMES LATER", "SOMETIMES")]
    public void MisspelledClauseIsRefused(string clause, string? word)
    {
        string sql = $"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, FOREIGN KEY (a) REFERENCES parent (x) {clause})";

        CamusDBException exception = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;

        if (word is not null)
            Assert.That(exception.Message, Does.Contain(word), "The error must name the word it did not accept");
    }

    [Test]
    public void NoAndActionStayUsableAsSeparateIdentifiers()
    {
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT no, action FROM t"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("CREATE TABLE match (id int64 PRIMARY KEY NOT NULL, no int64, action string)"));
    }

    [Test]
    public void NewNodesRoundTripThroughTheWireCodec()
    {
        string[] statements =
        [
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64 CONSTRAINT c1 REFERENCES parent (x) ON DELETE SET NULL, " +
                "b int64, FOREIGN KEY (a, b) REFERENCES parent (x, y) MATCH FULL DEFERRABLE INITIALLY DEFERRED)",
            "ALTER TABLE child ADD CONSTRAINT c2 FOREIGN KEY (b) REFERENCES parent ON UPDATE CASCADE"
        ];

        foreach (string sql in statements)
        {
            NodeAst ast = SQLParserProcessor.Parse(sql);
            string wire = NodeAstWireCodec.Serialize(ast);

            Assert.AreEqual(wire, NodeAstWireCodec.Serialize(NodeAstWireCodec.Deserialize(wire)), sql);
        }
    }

    // ── Tickets ─────────────────────────────────────────────────────────────

    [Test]
    public void ColumnLevelReferenceBuildsANamedForeignKey()
    {
        ForeignKeyInfo fk = CreateTicket("CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))").ForeignKeys.Single();

        Assert.AreEqual("weather_city_fkey", fk.Name, "PostgreSQL's default name");
        Assert.AreEqual(new[] { "city" }, fk.Columns);
        Assert.AreEqual("cities", fk.ReferencedTable);
        Assert.AreEqual(new[] { "name" }, fk.ReferencedColumns);
        Assert.AreEqual(ForeignKeyAction.NoAction, fk.OnDelete);
        Assert.AreEqual(ForeignKeyAction.NoAction, fk.OnUpdate);
        Assert.AreEqual(ForeignKeyMatch.Simple, fk.Match);
        Assert.IsFalse(fk.Deferrable);
        Assert.IsFalse(fk.InitiallyDeferred);
    }

    [Test]
    public void OmittedReferencedColumnsMeanThePrimaryKey()
    {
        ForeignKeyInfo fk = CreateTicket("CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities)").ForeignKeys.Single();

        Assert.IsEmpty(fk.ReferencedColumns);
    }

    [Test]
    public void CompositeTableLevelConstraintKeepsBothColumnOrders()
    {
        ForeignKeyInfo fk = CreateTicket(
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, b int64, FOREIGN KEY (b, a) REFERENCES parent (y, x) ON DELETE RESTRICT ON UPDATE CASCADE)")
            .ForeignKeys.Single();

        Assert.AreEqual("child_b_a_fkey", fk.Name);
        Assert.AreEqual(new[] { "b", "a" }, fk.Columns);
        Assert.AreEqual(new[] { "y", "x" }, fk.ReferencedColumns);
        Assert.AreEqual(ForeignKeyAction.Restrict, fk.OnDelete);
        Assert.AreEqual(ForeignKeyAction.Cascade, fk.OnUpdate);
    }

    [Test]
    public void SelfReferenceBuildsAForeignKeyToTheSameTable()
    {
        ForeignKeyInfo fk = CreateTicket(
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees(id))").ForeignKeys.Single();

        Assert.AreEqual("employees", fk.ReferencedTable);
        Assert.AreEqual(new[] { "manager_id" }, fk.Columns);
    }

    [Test]
    public void ForeignKeysKeepSourceOrderAndClausesStayWithTheirReference()
    {
        CreateTableTicket ticket = CreateTicket(
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, " +
            "a int64 REFERENCES p1 ON DELETE RESTRICT REFERENCES p2 ON DELETE CASCADE, " +
            "b int64, FOREIGN KEY (b) REFERENCES p3)");

        Assert.AreEqual(new[] { "p1", "p2", "p3" }, ticket.ForeignKeys.Select(f => f.ReferencedTable));
        Assert.AreEqual(ForeignKeyAction.Restrict, ticket.ForeignKeys[0].OnDelete);
        Assert.AreEqual(ForeignKeyAction.Cascade, ticket.ForeignKeys[1].OnDelete);
        Assert.AreEqual(ForeignKeyAction.NoAction, ticket.ForeignKeys[2].OnDelete);
    }

    [Test]
    public void DefaultNamesAvoidEveryOtherConstraintName()
    {
        CreateTableTicket ticket = CreateTicket(
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64 CONSTRAINT child_a_fkey NOT NULL, " +
            "FOREIGN KEY (a) REFERENCES p1, FOREIGN KEY (a) REFERENCES p2, CONSTRAINT child_a_fkey2 CHECK (a > 0))");

        Assert.AreEqual(new[] { "child_a_fkey1", "child_a_fkey3" }, ticket.ForeignKeys.Select(f => f.Name));
    }

    [Test]
    public void ExplicitNameThatRepeatsAnotherConstraintIsRefused()
    {
        CamusDBException exception = Assert.Throws<CamusDBException>(() => CreateTicket(
            "CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, CONSTRAINT dup CHECK (a > 0), CONSTRAINT dup FOREIGN KEY (a) REFERENCES p)"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        Assert.That(exception.Message, Does.Contain("dup"));
    }

    [TestCase("a int64 ON DELETE CASCADE", CamusDBErrorCodes.InvalidInput, "must follow a REFERENCES")]
    [TestCase("a int64 REFERENCES p ON DELETE CASCADE ON DELETE RESTRICT", CamusDBErrorCodes.InvalidInput, "more than once")]
    [TestCase("a int64 REFERENCES p DEFERRABLE NOT DEFERRABLE", CamusDBErrorCodes.InvalidInput, "more than once")]
    [TestCase("a int64 REFERENCES p (x, y)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "at most one")]
    [TestCase("a int64, b int64, FOREIGN KEY (a, b) REFERENCES p (x)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "2 referencing")]
    [TestCase("a int64, FOREIGN KEY (a, a) REFERENCES p (x, y)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "more than once")]
    [TestCase("a int64, FOREIGN KEY (missing) REFERENCES p (x)", CamusDBErrorCodes.UnknownColumn, "missing")]
    [TestCase("a int64 REFERENCES otherdb.p (x)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "current database")]
    public void MalformedDeclarationIsRefusedWithItsCode(string items, string code, string fragment)
    {
        CamusDBException exception = Assert.Throws<CamusDBException>(() =>
            CreateTicket($"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, {items})"))!;

        Assert.AreEqual(code, exception.Code);
        Assert.That(exception.Message, Does.Contain(fragment));
    }

    [Test]
    public void ReferenceQualifiedWithTheCurrentDatabaseDropsTheQualifier()
    {
        ForeignKeyInfo fk = CreateTicket($"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64 REFERENCES {Database}.parent (x))").ForeignKeys.Single();

        Assert.AreEqual("parent", fk.ReferencedTable);
    }

    [Test]
    public void AlterTableTicketCarriesTheConstraintAndAvoidsExistingNames()
    {
        TableSchema child = new()
        {
            Id = "C",
            Name = "child",
            Columns =
            [
                new TableColumnSchema("c-id", "id", ColumnType.Integer64, true, null),
                new TableColumnSchema("c-a", "a", ColumnType.Integer64, false, null, notNullConstraintName: null)
            ],
            CheckConstraints = [new CheckConstraintSchema { Name = "child_a_fkey", Expression = "a > 0", ReferencedColumns = ["a"] }]
        };

        const string sql = "ALTER TABLE child ADD FOREIGN KEY (a) REFERENCES parent (x) ON DELETE RESTRICT";
        AlterConstraintTicket ticket = new SQLExecutorAlterConstraintCreator().CreateAlterConstraintTicket(
            new ExecuteSQLTicket(txnState: null!, database: Database, sql: sql, parameters: null), SQLParserProcessor.Parse(sql), child);

        Assert.AreEqual(AlterConstraintOperation.AddForeignKey, ticket.Operation);
        Assert.AreEqual("child_a_fkey1", ticket.ConstraintName);
        Assert.AreEqual("child_a_fkey1", ticket.ForeignKey!.Name);
        Assert.AreEqual(ForeignKeyAction.Restrict, ticket.ForeignKey.OnDelete);
    }

    [Test]
    public void AddColumnWithAReferenceIsRefused()
    {
        const string sql = "ALTER TABLE child ADD COLUMN p int64 REFERENCES parent (x)";

        CamusDBException exception = Assert.Throws<CamusDBException>(() => new SQLExecutorAlterTableCreator().CreateAlterTableTicket(
            new ExecuteSQLTicket(txnState: null!, database: Database, sql: sql, parameters: null), SQLParserProcessor.Parse(sql)))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.That(exception.Message, Does.Contain("ADD FOREIGN KEY"));
    }

    // ── Validator ───────────────────────────────────────────────────────────

    [TestCase("ON DELETE CASCADE", "ON DELETE CASCADE")]
    [TestCase("ON DELETE SET NULL", "ON DELETE SET NULL")]
    [TestCase("ON DELETE SET DEFAULT", "ON DELETE SET DEFAULT")]
    [TestCase("ON UPDATE CASCADE", "ON UPDATE CASCADE")]
    [TestCase("ON UPDATE SET NULL", "ON UPDATE SET NULL")]
    [TestCase("ON UPDATE SET DEFAULT", "ON UPDATE SET DEFAULT")]
    [TestCase("MATCH FULL", "MATCH FULL")]
    [TestCase("MATCH PARTIAL", "MATCH PARTIAL")]
    [TestCase("DEFERRABLE", "deferred checking")]
    [TestCase("INITIALLY DEFERRED", "deferred checking")]
    public void UnsupportedClauseIsRefusedByName(string clause, string fragment)
    {
        CamusDBException exception = Assert.Throws<CamusDBException>(() => Validate(
            $"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, FOREIGN KEY (a) REFERENCES parent (x) {clause})"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.That(exception.Message, Does.Contain(fragment));
        Assert.That(exception.Message, Does.Not.Contain("cannot be created yet"), "The clause must be named, not the interim refusal");
    }

    /// <summary>
    /// Nothing stores or enforces a constraint yet, so even a fully supported declaration is refused.
    /// A table created with a constraint that silently does nothing would be worse than the error.
    /// </summary>
    [TestCase("")]
    [TestCase("ON DELETE NO ACTION ON UPDATE RESTRICT MATCH SIMPLE NOT DEFERRABLE INITIALLY IMMEDIATE")]
    public void SupportedDeclarationIsRefusedUntilConstraintsCanBeCreated(string clauses)
    {
        CamusDBException exception = Assert.Throws<CamusDBException>(() => Validate(
            $"CREATE TABLE child (id int64 PRIMARY KEY NOT NULL, a int64, FOREIGN KEY (a) REFERENCES parent (x) {clauses})"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.That(exception.Message, Does.Contain("cannot be created yet"));
    }

    [Test]
    public void TableWithoutForeignKeysStillValidates()
    {
        Assert.DoesNotThrow(() => Validate("CREATE TABLE plain (id int64 PRIMARY KEY NOT NULL, a int64)"));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static CreateTableTicket CreateTicket(string sql) =>
        new SQLExecutorCreateTableCreator().CreateCreateTableTicket(
            new ExecuteSQLTicket(txnState: null!, database: Database, sql: sql, parameters: null), SQLParserProcessor.Parse(sql));

    private static void Validate(string sql) => new CommandValidator(new CamusDBOptions()).Validate(CreateTicket(sql));

    /// <summary>The constraints of one column, flattened in source order.</summary>
    private static List<NodeAst> ColumnConstraints(string sql, string column)
    {
        NodeAst item = Items(SQLParserProcessor.Parse(sql).rightAst!)
            .Single(i => i.nodeType == NodeType.CreateTableItem && i.leftAst!.yytext == column);

        List<NodeAst> constraints = [];
        Flatten(item.extendedOne!, NodeType.CreateTableFieldConstraintList, constraints);
        return constraints;
    }

    private static NodeAst SingleColumnConstraint(string sql, string column, NodeType type) =>
        ColumnConstraints(sql, column).Single(c => c.nodeType == type);

    private static NodeAst TableConstraint(string sql) =>
        Items(SQLParserProcessor.Parse(sql).rightAst!).Single(i => i.nodeType == NodeType.CreateTableConstraintForeignKey);

    private static List<NodeAst> Items(NodeAst list)
    {
        List<NodeAst> items = [];
        Flatten(list, NodeType.CreateTableItemList, items);
        return items;
    }

    private static void Flatten(NodeAst node, NodeType listType, List<NodeAst> into)
    {
        if (node.nodeType == listType)
        {
            Flatten(node.leftAst!, listType, into);
            Flatten(node.rightAst!, listType, into);
            return;
        }

        into.Add(node);
    }

    private static string[] Names(NodeAst? list)
    {
        if (list is null)
            return [];

        List<NodeAst> nodes = [];
        Flatten(list, NodeType.IndexIdentifierList, nodes);
        return [.. nodes.Select(n => n.yytext!)];
    }

    private static string[] Clauses(NodeAst? list)
    {
        if (list is null)
            return [];

        List<NodeAst> nodes = [];
        Flatten(list, NodeType.ForeignKeyOptionList, nodes);
        return [.. nodes.Select(n => n.yytext!)];
    }
}
