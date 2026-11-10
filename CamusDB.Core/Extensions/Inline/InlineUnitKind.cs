/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// The two shapes of inline C# that the server compiles. They share the pipeline and differ in what
/// the body sees: a function body gets its parameters and returns one value; a trigger body gets the
/// row of the event and a SQL session, and may await that session. The budget frame of a function is
/// thread-static; the frame of a trigger is an object the body carries, because the code after an
/// await can run on another thread.
/// </summary>
internal enum InlineUnitKind
{
    Function,

    Trigger,
}
