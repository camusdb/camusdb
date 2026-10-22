/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Extensions;

/// <summary>
/// Binds a lazy query cursor to the extension scope frame of the statement that created it. The query entry
/// points return a cursor that the caller pulls later, in whatever flow it is in by then: an async
/// helper may have created the cursor and returned (its scope is gone), or the caller may have entered
/// another engine in between (its scope is the other engine's). The wrapper makes every
/// <c>MoveNextAsync</c> enter the creating statement's frame first, so a row is always evaluated with
/// the functions of the engine and the database that planned it.
///
/// <para><b>Cost.</b> Nothing is wrapped until some engine in the process has loaded an extension
/// or some database has installed one (<see cref="ExtensionRegistry.AnyLoaded"/>): a server without
/// extensions returns its cursors as they are. Once wrapping is on, every cursor wraps, so it enters
/// its own frame and cannot see another engine's or another database's functions.</para>
/// </summary>
internal static class ExtensionScopedCursor
{
    public static IAsyncEnumerable<T> Bind<T>(IAsyncEnumerable<T> cursor, ExtensionScopeFrame? frame) =>
        ExtensionRegistry.AnyLoaded ? new ScopedEnumerable<T>(cursor, frame) : cursor;

    private sealed class ScopedEnumerable<T>(IAsyncEnumerable<T> inner, ExtensionScopeFrame? frame) : IAsyncEnumerable<T>
    {
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            ExtensionFunctionScope.Enter(frame);
            return new ScopedEnumerator<T>(inner.GetAsyncEnumerator(cancellationToken), frame);
        }
    }

    private sealed class ScopedEnumerator<T>(IAsyncEnumerator<T> inner, ExtensionScopeFrame? frame) : IAsyncEnumerator<T>
    {
        public T Current => inner.Current;

        /// <summary>
        /// Entered in this synchronous method, so the inner iterator's synchronous part and every
        /// continuation it schedules (which capture the context at their await) run in this scope.
        /// </summary>
        public ValueTask<bool> MoveNextAsync()
        {
            ExtensionFunctionScope.Enter(frame);
            return inner.MoveNextAsync();
        }

        public ValueTask DisposeAsync()
        {
            ExtensionFunctionScope.Enter(frame);
            return inner.DisposeAsync();
        }
    }
}
