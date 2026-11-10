/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Tests.Extensions;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// Shared setup for the tests that run inline functions through SQL: the engine under test has
/// <see cref="CamusDBOptions.InlineFunctionsEnabled"/> and <see cref="CamusDBOptions.InlineTriggersEnabled"/>
/// on. The engine fixes its settings when it is
/// built, so a test that needs another setting builds its own engine with <c>Options with { ... }</c>.
/// </summary>
public abstract class InlineFunctionTestBase : ExtensionTestBase
{
    protected const string Slugify =
        "CREATE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s.Trim().ToLowerInvariant().Replace(' ', '-'); $$";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with { InlineFunctionsEnabled = true, InlineTriggersEnabled = true };

    protected static async Task<string> Code(System.Func<Task> action) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await action())!.Code;

    protected static async Task<string?> Text(CommandExecutor executor, string db, string sql) =>
        (await Scalar(executor, db, sql)).StrValue;

    protected static async Task<long> Number(CommandExecutor executor, string db, string sql) =>
        (await Scalar(executor, db, sql)).LongValue;

    /// <summary>The row of <c>SHOW FUNCTIONS</c> for <paramref name="name"/>.</summary>
    protected static async Task<Dictionary<string, ColumnValue>?> ShownFunction(CommandExecutor executor, string db, string name) =>
        (await ExecQuery(executor, db, "SHOW FUNCTIONS")).SingleOrDefault(row => row["name"].StrValue == name);
}
