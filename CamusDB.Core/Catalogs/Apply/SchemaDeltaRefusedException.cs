/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Raised by an apply arm when a committed delta's preconditions no longer hold against the schema it
/// lands on — the delta was proposed against a snapshot that another delta, committed first, has
/// since moved.
///
/// <para><b>Proposer and apply.</b> In the proposer's dry-run (<see
/// cref="SchemaDeltaApplier.ValidateSchemaDelta"/>, under the schema lock) it is an ordinary error:
/// the statement is refused before anything is proposed, and the user sees the code and message.
/// In a live apply or a replay it is <b>not</b> an error. The entry is committed, so every node
/// meets it at the same log position against the same schema and reaches the same refusal; the
/// entry becomes a no-op that still consumes its version (<see
/// cref="SchemaDeltaApplier.ApplyOrRefuse"/>), and the proposer reports the refusal to its caller
/// once its own apply has run. Letting the exception escape a live apply instead would stop that
/// node's schema pipeline at this entry, which is far worse than the race it reports.</para>
///
/// <para>An arm that raises this must raise it <b>before</b> it changes anything, so a refused entry
/// leaves the schema exactly as it found it.</para>
/// </summary>
internal sealed class SchemaDeltaRefusedException : CamusDBException
{
    public SchemaDeltaRefusedException(string code, string message) : base(code, message)
    {
    }
}
