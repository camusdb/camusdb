
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// Binds a lazy query cursor to the transaction whose statement created it, so every row is read with
/// that transaction's private schema. The query entry point returns the cursor and the caller pulls it
/// later, outside the statement's async scope; without this, a row of a table the transaction created
/// would be read against the shared schema, where the table does not exist.
///
/// <para>Each <c>MoveNextAsync</c> names the transaction, starts the inner step, and restores the
/// caller's name before it returns. The inner step captured the named flow at its first await, so it
/// keeps it, and the caller's flow is left as it was.</para>
///
/// <para>Wraps only a statement of a transaction that has schema changes; every other cursor is
/// returned as it is.</para>
/// </summary>
internal static class TransactionSchemaScopedCursor
{
    public static IAsyncEnumerable<T> Bind<T>(IAsyncEnumerable<T> cursor, KvTransaction? tx) =>
        tx?.SchemaChanges is TransactionSchemaOverlay ? new ScopedEnumerable<T>(cursor, tx) : cursor;

    private sealed class ScopedEnumerable<T>(IAsyncEnumerable<T> inner, KvTransaction tx) : IAsyncEnumerable<T>
    {
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            KvTransaction? previous = TransactionSchemaScope.Swap(tx);
            try
            {
                return new ScopedEnumerator<T>(inner.GetAsyncEnumerator(cancellationToken), tx);
            }
            finally
            {
                TransactionSchemaScope.Swap(previous);
            }
        }
    }

    private sealed class ScopedEnumerator<T>(IAsyncEnumerator<T> inner, KvTransaction tx) : IAsyncEnumerator<T>
    {
        public T Current => inner.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            KvTransaction? previous = TransactionSchemaScope.Swap(tx);
            try
            {
                return inner.MoveNextAsync();
            }
            finally
            {
                TransactionSchemaScope.Swap(previous);
            }
        }

        public ValueTask DisposeAsync()
        {
            KvTransaction? previous = TransactionSchemaScope.Swap(tx);
            try
            {
                return inner.DisposeAsync();
            }
            finally
            {
                TransactionSchemaScope.Swap(previous);
            }
        }
    }
}
