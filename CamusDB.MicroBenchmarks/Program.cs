
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Running;
using CamusDB.MicroBenchmarks;

// `probe <name>` runs the one-shot restore measurements in InsertRestoreProbes instead of a
// BenchmarkDotNet job, and `probe exprscan` runs the statement-depth measurements in
// ExpressionDepthProbes; everything else is handed to the switcher unchanged.
if (args.Length > 0 && args[0] == "probe" && args.Length > 1 && args[1] is "exprscan" or "exprchild")
    return await ExpressionDepthProbes.RunAsync(args);

if (args.Length > 0 && args[0] == "probe")
    return await InsertRestoreProbes.RunAsync(args);

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;
