/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Extensions;

/// <summary>
/// Counts how deeply extension code is nested in the current async flow: a trigger whose SQL fires
/// another trigger, a procedure whose SQL fires a trigger, and so on. A trigger that writes to its
/// own table would otherwise recurse until the stack or the mutation limit gave out.
///
/// <para>The limit is a fixed constant (<see cref="MaxDepth"/>), not a setting: it guards against
/// runaway recursion, not a workload shape anyone tunes. A call past it fails the statement with
/// <see cref="CamusDBErrorCodes.ExtensionNestingTooDeep"/>.</para>
///
/// <para><b>Why an <see cref="AsyncLocal{T}"/>.</b> The nested SQL re-enters the engine through its
/// public entry points, which carry no depth parameter. The value is incremented inside the async
/// method that calls the extension, so the nested flow sees the higher depth and the increment is
/// undone automatically when that method returns.</para>
/// </summary>
internal static class ExtensionNesting
{
    /// <summary>The deepest allowed nesting of extension calls in one statement.</summary>
    public const int MaxDepth = 16;

    private static readonly AsyncLocal<int> depth = new();

    /// <summary>The nesting depth of the current flow; 0 outside any extension call.</summary>
    public static int Depth => depth.Value;

    /// <summary>
    /// Enters one more level. Call it at the start of the async method that calls extension code;
    /// the level ends when that method returns. Throws past <see cref="MaxDepth"/>.
    /// </summary>
    public static void Enter(string what)
    {
        int current = depth.Value;

        if (current >= MaxDepth)
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNestingTooDeep,
                $"{what} is nested more than {MaxDepth} levels deep (a trigger or procedure whose SQL fires itself again?)");

        depth.Value = current + 1;
    }
}
