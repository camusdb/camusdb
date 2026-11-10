/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The SQL text of a trigger record, for <c>SHOW TRIGGERS</c>, <c>SHOW CREATE TRIGGER</c> and a
/// logical dump. Both forms render as a statement that parses back to the same record: the DLL form
/// with <c>EXTERNAL NAME '...'</c>, the inline form with <c>LANGUAGE csharp AS $$ body $$</c>. The
/// body is written exactly as it was stored; when it holds <c>$$</c>, the dollar quotes carry a tag
/// the body does not hold (<see cref="InlineFunctionSignature.DollarQuoteFor"/>).
/// </summary>
internal static class TriggerStatementRenderer
{
    /// <summary>The events in the fixed order INSERT, UPDATE, DELETE, comma-separated: <c>INSERT, DELETE</c>.</summary>
    public static string Events(TriggerEventSet events)
    {
        StringBuilder text = new(24);

        if ((events & TriggerEventSet.Insert) != 0)
            text.Append("INSERT");

        if ((events & TriggerEventSet.Update) != 0)
            text.Append(text.Length > 0 ? ", " : "").Append("UPDATE");

        if ((events & TriggerEventSet.Delete) != 0)
            text.Append(text.Length > 0 ? ", " : "").Append("DELETE");

        return text.ToString();
    }

    /// <summary>The <c>CREATE TRIGGER</c> statement that creates <paramref name="trigger"/> again on <paramref name="tableName"/>.</summary>
    public static string RenderCreate(TriggerDefinition trigger, string tableName)
    {
        StringBuilder text = new(trigger.Source.Length + 128);

        text.Append("CREATE TRIGGER ").Append(InlineFunctionSignature.SqlName(trigger.Name))
            .Append(" ON ").Append(InlineFunctionSignature.SqlName(tableName))
            .Append(trigger.Timing == TriggerFireTiming.Before ? " BEFORE " : " AFTER ")
            .Append(Events(trigger.Events));

        if (trigger.IsInline)
        {
            string quote = InlineFunctionSignature.DollarQuoteFor(trigger.Source);
            text.Append(" LANGUAGE ").Append(trigger.Language).Append(" AS ").Append(quote).Append(trigger.Source).Append(quote);
        }
        else
        {
            text.Append(" EXTERNAL NAME ").Append(SQLParser.SqlStringLiteral.Quote(trigger.TypeName));
        }

        return text.ToString();
    }
}
