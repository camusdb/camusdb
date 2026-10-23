/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The proposer-side check that a schema change does not store a call to an extension function whose
/// extension is no longer installed.
///
/// <para><b>The race it closes.</b> A statement that stores a function call (a column default, a CHECK
/// constraint, a view or materialized-view body) resolves the function when the statement starts,
/// before it takes the schema lock. A <c>DROP EXTENSION</c> can commit in between: its own dependency
/// check ran under the lock and saw nothing, because the new object did not exist yet. Without this
/// check the object would be created with a call that no longer resolves, and nothing would stop the
/// drop afterwards.</para>
///
/// <para><b>Where it runs.</b> Inside <c>SchemaDeltaApplier.ValidateSchemaDelta</c>, which every
/// proposer calls under the schema lock against the current schema, just before it proposes. Proposals
/// are ordered by schema version, so the drop and the create are checked against each other in log
/// order. It never runs in the replicated apply: a follower's loaded files must not decide whether a
/// committed delta applies.</para>
///
/// <para><b>Which names it checks.</b> Only a name that is not a built-in and that one of this engine's
/// loaded extensions defines (plain or qualified) as a function, an aggregate or a table function; a
/// view body can call all three. Every other name (built-in aggregates, sequence functions, built-in
/// table functions, built-in scalars) is out of scope, so the check cannot refuse a statement that never used
/// an extension. Such a name must resolve against the extensions installed in the schema the delta is
/// validated against; otherwise the delta is refused with
/// <see cref="CamusDBErrorCodes.ExtensionNotInstalled"/>.</para>
/// </summary>
internal static class ExtensionReferenceGuard
{
    /// <summary>
    /// Throws when <paramref name="entry"/> stores a call to a function of a loaded extension that is
    /// not installed in <paramref name="schema"/>. A no-op without an ambient function scope or with no
    /// loaded extension, and for every op that stores no expression.
    /// </summary>
    public static void Check(Schema schema, SchemaChangeLogEntry entry)
    {
        ExtensionRegistry? registry = ExtensionFunctionScope.Current?.Registry;

        if (registry is null || registry.IsEmpty)
            return;

        List<string>? defaults = null;
        List<string>? expressions = null;
        List<string>? statements = null;

        switch (entry.Op)
        {
            case SchemaOp.CreateTable:
            {
                SchemaCreateTablePayload payload = SchemaDeltaApplier.DecodePayload<SchemaCreateTablePayload>(entry);

                foreach (SchemaColumnPayload column in payload.Columns)
                {
                    if (column.DefaultFunction is { Length: > 0 } function)
                        (defaults ??= []).Add(function);
                }

                if (payload.CheckConstraints is { } checks)
                {
                    foreach (CheckConstraintSchema check in checks)
                        (expressions ??= []).Add(check.Expression);
                }

                if (payload.ViewDefinition?.Sql is { Length: > 0 } body)
                    (statements ??= []).Add(body);

                break;
            }

            case SchemaOp.AddColumn:
            {
                if (SchemaDeltaApplier.DecodePayload<SchemaAlterColumnPayload>(entry).Column.DefaultFunction is { Length: > 0 } function)
                    (defaults ??= []).Add(function);

                break;
            }

            case SchemaOp.AddCheckConstraint:
                (expressions ??= []).Add(SchemaDeltaApplier.DecodePayload<SchemaCheckConstraintPayload>(entry).Expression);
                break;

            case SchemaOp.CreateView or SchemaOp.ReplaceView:
            {
                if (SchemaDeltaApplier.DecodePayload<SchemaViewPayload>(entry).Definition?.Sql is { Length: > 0 } body)
                    (statements ??= []).Add(body);

                break;
            }

            case SchemaOp.SetViewDefinition:
            {
                if (SchemaDeltaApplier.DecodePayload<SchemaSetViewDefinitionPayload>(entry).Definition?.Sql is { Length: > 0 } body)
                    (statements ??= []).Add(body);

                break;
            }

            default:
                return;
        }

        if (defaults is null && expressions is null && statements is null)
            return;

        InstalledExtensionSet installed = InstalledExtensionSet.Build(schema.Extensions, registry);

        void Require(string name)
        {
            string lowered = name.ToLowerInvariant();

            if (ScalarFunctionEvaluator.IsBuiltInScalarFunction(lowered) || !IsExtensionFunction(registry, lowered))
                return;

            if (installed.HasFunction(lowered) || installed.HasAggregate(lowered) || installed.HasTableFunction(lowered))
                return;

            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNotInstalled,
                $"Function '{name}' belongs to an extension that is no longer installed in this database; the extension was dropped while the statement ran");
        }

        if (defaults is not null)
        {
            foreach (string function in defaults)
                Require(function);
        }

        void RequireAll(NodeAst? ast)
        {
            NodeAstWalk.Any(ast, node =>
            {
                if (node.nodeType is NodeType.ExprFuncCall or NodeType.TableFunctionReference && node.leftAst?.yytext is { } name)
                    Require(name);

                return false;
            });
        }

        if (expressions is not null)
        {
            foreach (string expression in expressions)
            {
                if (expression.Length > 0)
                    RequireAll(SQLParserProcessor.ParseCondition(expression));
            }
        }

        if (statements is not null)
        {
            foreach (string statement in statements)
                RequireAll(SQLParserProcessor.Parse(statement));
        }
    }

    /// <summary>
    /// True when a loaded extension defines <paramref name="name"/> as a function, an aggregate or a
    /// table function, plain or as <c>extension.member</c>.
    /// </summary>
    private static bool IsExtensionFunction(ExtensionRegistry registry, string name)
    {
        int dot = name.IndexOf('.');

        foreach (LoadedExtension extension in registry.Extensions)
        {
            if (dot < 0)
            {
                if (extension.DefinesCallable(name))
                    return true;
            }
            else if (string.Equals(extension.Name, name[..dot], StringComparison.OrdinalIgnoreCase) &&
                     extension.DefinesCallable(name[(dot + 1)..]))
                return true;
        }

        return false;
    }
}
