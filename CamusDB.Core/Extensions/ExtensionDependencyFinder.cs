/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Finds the schema objects of a database that use members of an installed extension, so that
/// <c>DROP EXTENSION</c> can refuse while they exist and <c>ALTER EXTENSION ... UPDATE</c> can refuse
/// a version that lacks a member they use.
///
/// <para><b>Computed from the schema, not recorded.</b> The objects that can call a member keep the
/// call in their own definition: a column default names its function, a CHECK constraint and a view
/// or materialized-view body keep their SQL text, and a trigger names its extension and type. Reading
/// them when the statement runs, under the schema lock, gives the same answer a recorded edge list
/// would, with no second copy that a replay or a crash could leave out of step.</para>
///
/// <para><b>Names.</b> A call matches a function or aggregate member by its plain name or by the
/// qualified form <c>extension.member</c>, and a table-function member by the same names in a
/// <c>FROM</c> reference. A view body is the only stored object that can name a table function or an
/// aggregate: a default and a CHECK refuse both. Plain names are unambiguous within a database: two installed extensions
/// cannot define the same function name, and no extension can shadow a built-in.</para>
/// </summary>
internal static class ExtensionDependencyFinder
{
    /// <summary>
    /// Describes each object that uses one of <paramref name="members"/> of <paramref name="extension"/>,
    /// for example <c>view v_report</c> or <c>trigger audit on table orders</c>. Empty when nothing
    /// depends on them.
    /// </summary>
    public static List<string> Find(
        Schema schema,
        ExtensionDefinition extension,
        IEnumerable<ExtensionMemberDefinition> members,
        Func<string, NodeAst> parseStatement)
    {
        // Scalar functions and aggregates are both call nodes; a table function is a FROM reference.
        HashSet<string> functionNames = new(StringComparer.OrdinalIgnoreCase);
        HashSet<string> tableFunctionNames = new(StringComparer.OrdinalIgnoreCase);
        HashSet<string> triggerTypes = new(StringComparer.Ordinal);

        foreach (ExtensionMemberDefinition member in members)
        {
            if (member.Kind is ExtensionMemberKind.Function or ExtensionMemberKind.Aggregate)
            {
                functionNames.Add(member.Name);
                functionNames.Add(extension.Name + "." + member.Name);
            }
            else if (member.Kind == ExtensionMemberKind.TableFunction)
            {
                tableFunctionNames.Add(member.Name);
                tableFunctionNames.Add(extension.Name + "." + member.Name);
            }
            else if (member.Kind == ExtensionMemberKind.Trigger)
                triggerTypes.Add(member.Name);
        }

        return FindUsers(schema, functionNames, tableFunctionNames, extension.Name, triggerTypes, parseStatement);
    }

    /// <summary>
    /// Describes each object that calls the function <paramref name="functionName"/>: a column default,
    /// a CHECK constraint, a view or a materialized view. <c>DROP FUNCTION</c> refuses while the list
    /// is not empty, and so does a <c>CREATE OR REPLACE FUNCTION</c> that changes the function's
    /// parameter types or result type. An inline function has no qualified name and no trigger.
    /// </summary>
    public static List<string> FindCallers(Schema schema, string functionName, Func<string, NodeAst> parseStatement)
    {
        HashSet<string> functionNames = new(StringComparer.OrdinalIgnoreCase) { functionName };
        return FindUsers(schema, functionNames, new HashSet<string>(StringComparer.OrdinalIgnoreCase), null, new HashSet<string>(StringComparer.Ordinal), parseStatement);
    }

    /// <summary>
    /// The scan behind <see cref="Find"/> and <see cref="FindCallers"/>: every default, CHECK, view and
    /// materialized view that calls one of the names, and every trigger of
    /// <paramref name="extensionName"/> whose type is one of <paramref name="triggerTypes"/>.
    /// </summary>
    private static List<string> FindUsers(
        Schema schema,
        HashSet<string> functionNames,
        HashSet<string> tableFunctionNames,
        string? extensionName,
        HashSet<string> triggerTypes,
        Func<string, NodeAst> parseStatement)
    {
        List<string> dependents = [];

        if (functionNames.Count == 0 && tableFunctionNames.Count == 0 && triggerTypes.Count == 0)
            return dependents;

        bool callsAny = functionNames.Count > 0 || tableFunctionNames.Count > 0;

        bool Calls(NodeAst? ast) =>
            ast is not null && NodeAstWalk.Any(ast, node =>
                node.leftAst?.yytext is { } name &&
                ((node.nodeType == NodeType.ExprFuncCall && functionNames.Contains(name)) ||
                 (node.nodeType == NodeType.TableFunctionReference && tableFunctionNames.Contains(name))));

        foreach (TableSchema table in schema.Tables.Values)
        {
            if (functionNames.Count > 0)
            {
                if (table.Columns is { } columns)
                {
                    foreach (TableColumnSchema column in columns)
                    {
                        if (column.DefaultFunction is { } function && functionNames.Contains(function))
                            dependents.Add($"the default of column {table.Name}.{column.Name}");
                    }
                }

                if (table.CheckConstraints is { } checks)
                {
                    foreach (CheckConstraintSchema check in checks)
                    {
                        NodeAst? condition = check.ParsedCondition
                            ?? (string.IsNullOrEmpty(check.Expression) ? null : SQLParserProcessor.ParseCondition(check.Expression));

                        if (Calls(condition))
                            dependents.Add($"CHECK constraint {check.Name} on table {table.Name}");
                    }
                }
            }

            if (callsAny && table.ViewDefinition is { Sql.Length: > 0 } definition && Calls(parseStatement(definition.Sql)))
                dependents.Add($"materialized view {table.Name}");

            if (table.Triggers is { } triggers)
            {
                foreach (TriggerDefinition trigger in triggers)
                {
                    if (extensionName is not null && string.Equals(trigger.ExtensionName, extensionName, StringComparison.OrdinalIgnoreCase) &&
                        triggerTypes.Contains(trigger.TypeName))

                        dependents.Add($"trigger {trigger.Name} on table {table.Name}");
                }
            }
        }

        if (callsAny)
        {
            foreach (ViewSchema view in schema.Views.Values)
            {
                if (view.Definition is { Sql.Length: > 0 } definition && Calls(parseStatement(definition.Sql)))
                    dependents.Add($"view {view.Name}");
            }
        }

        return dependents;
    }

    /// <summary>The members of <paramref name="installed"/> that <paramref name="target"/> does not define.</summary>
    public static List<ExtensionMemberDefinition> MembersMissingFrom(ExtensionDefinition installed, ExtensionDefinition target)
    {
        List<ExtensionMemberDefinition> missing = [];

        foreach (ExtensionMemberDefinition member in installed.Members)
        {
            if (!target.HasMember(member.Kind, member.Name))
                missing.Add(member);
        }

        return missing;
    }
}
