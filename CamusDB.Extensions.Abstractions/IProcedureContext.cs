/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading;

namespace CamusDB.Extensions;

/// <summary>
/// What a procedure receives for one <c>CALL</c>.
/// </summary>
public interface IProcedureContext
{
    /// <summary>The SQL name of the procedure, as the statement called it.</summary>
    string ProcedureName { get; }

    /// <summary>
    /// The evaluated arguments, in order, as plain CLR values of the value mapping
    /// (<c>long</c>, <c>double</c>, <c>float</c>, <c>bool</c>, <c>string</c>, <c>Guid</c>,
    /// <c>byte[]</c>, <c>DateTime</c>, <c>decimal</c>), or <c>null</c> for SQL NULL.
    /// </summary>
    IReadOnlyList<object?> Arguments { get; }

    /// <summary>Runs SQL inside the caller's transaction. See <see cref="ISqlSession"/>.</summary>
    ISqlSession Sql { get; }

    /// <summary>
    /// Signals that the statement was cancelled. Cancellation is cooperative: the server cannot
    /// stop a procedure that does not observe this token. In this version the server does not cancel a
    /// statement that writes, so the token does not fire yet; observe it anyway, so a later server can
    /// stop long work cleanly.
    /// </summary>
    CancellationToken CancellationToken { get; }
}
