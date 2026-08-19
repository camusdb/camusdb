/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// How a node sizes the cache budgets it does not have an explicit value for: the RocksDB shared
/// block cache, its memtable sub-budget, and the key/value actor caches.
///
/// <para>The profile only supplies <em>defaults</em>. Every budget it covers has its own
/// <c>kahuna.*</c> key, and an explicit key always wins over the profile — so a profile can be
/// combined with a hand-tuned override without the two fighting.</para>
///
/// <para>It deliberately does not touch worker counts, IO threads, or anything else that changes how
/// the node behaves rather than how much it caches: a smaller cache makes a workload slower, whereas
/// fewer workers makes it concurrent in a different way, and only the first belongs behind a memory
/// switch.</para>
/// </summary>
public enum MemoryProfile
{
    /// <summary>
    /// Sizes the caches from the machine's available memory (container limits respected) — roughly
    /// 16% of RAM across both cache layers. The right choice for a node that owns its box, and the
    /// historical behavior.
    /// </summary>
    Prod = 0,

    /// <summary>
    /// Small fixed budgets — about 96 MiB of caches, for a node that shares a laptop with an IDE, a
    /// compiler, and the application under development. Correctness is unaffected; a working set
    /// larger than the block cache is served from disk instead of memory, so throughput on a real
    /// dataset is materially lower. Not for production.
    /// </summary>
    Dev = 1,
}
