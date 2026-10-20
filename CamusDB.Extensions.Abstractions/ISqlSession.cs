/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

namespace CamusDB.Extensions;

/// <summary>
/// Runs SQL inside the transaction of the statement that called a trigger or a procedure.
///
/// <para><b>Allowed statements.</b> SELECT, INSERT, UPDATE and DELETE only. DDL and transaction
/// control (BEGIN, COMMIT, ROLLBACK) fail.</para>
///
/// <para><b>Privileges.</b> The SQL runs with the privileges of the user whose statement fired the
/// trigger or called the procedure.</para>
///
/// <para><b>Limits.</b> Writes count against the transaction's mutation limit. A trigger whose SQL
/// fires more triggers is allowed, up to a fixed nesting depth of 16; deeper nesting fails the
/// statement.</para>
///
/// <para><b>Parameters.</b> A placeholder <c>@name</c> in the SQL takes the value of the
/// dictionary entry <c>name</c> (without the <c>@</c>). Values are plain CLR values of the value
/// mapping, or <c>null</c> for SQL NULL.</para>
/// </summary>
public interface ISqlSession
{
    /// <summary>Runs an INSERT, UPDATE or DELETE and returns the number of affected rows.</summary>
    Task<long> ExecuteAsync(string sql, IReadOnlyDictionary<string, object?>? parameters = null);

    /// <summary>
    /// Runs a SELECT and returns its rows. The rows are copies and stay valid after the call.
    /// </summary>
    Task<IReadOnlyList<IRow>> QueryAsync(string sql, IReadOnlyDictionary<string, object?>? parameters = null);
}
