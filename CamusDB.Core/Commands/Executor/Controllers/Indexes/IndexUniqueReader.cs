
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexUniqueReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BTree<ColumnValue, BTreeTuple?>> ReadUnique(BufferPoolHandler bufferpool, int offset)
    {
        //Console.WriteLine("***");

        IndexUniqueNodeReader reader = new(bufferpool);

        BTree<ColumnValue, BTreeTuple?> index = new(offset, reader);

        using IDisposable readerLock = await index.ReaderWriterLock.ReaderLockAsync();

        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.size = Serializator.ReadInt32(data, ref pointer);

        int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

        if (rootPageOffset > -1)
        {
            BTreeNode<ColumnValue, BTreeTuple?>? node = await reader.GetNode(rootPageOffset);
            if (node is not null)
                index.root = node;
        }

        return index;
    }
}
