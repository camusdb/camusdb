
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;

using BenchmarkDotNet.Attributes;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Cost of one exact distance evaluation — that is, the per-row cost of a nearest-neighbour scan,
/// because exact search evaluates the metric once for every row it examines.
///
/// <para>Read the result as the floor on a scan: a table of N rows costs at least N times the figure
/// here, before any I/O or decode. That number is what decides whether an approximate index is worth
/// its correctness cost, so it is measured rather than guessed.</para>
///
/// <para>The functions are invoked through <see cref="ScalarFunctionRegistry"/> rather than called
/// directly, so the measurement includes the argument list and result <see cref="ColumnValue"/> the
/// engine really allocates. 768 dimensions is the common embedding width; 1536 is the other one.</para>
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class VectorDistanceBenchmarks
{
    [Params(128, 768, 1_536)]
    public int Dimensions { get; set; }

    private ScalarFunctionEvaluatorDelegate _l2 = null!;
    private ScalarFunctionEvaluatorDelegate _innerProduct = null!;
    private ScalarFunctionEvaluatorDelegate _cosine = null!;

    private ColumnValue[] _operands = null!;

    [GlobalSetup]
    public void Setup()
    {
        ScalarFunctionRegistry registry = ScalarFunctionRegistry.CreateDefault();

        _l2 = Resolve(registry, "l2_distance");
        _innerProduct = Resolve(registry, "inner_product");
        _cosine = Resolve(registry, "cosine_distance");

        // A fixed seed keeps successive runs comparable; the values themselves do not affect cost,
        // because every element is read and multiplied regardless of magnitude.
        Random random = new(20260819);
        _operands = [PackRandom(random, Dimensions), PackRandom(random, Dimensions)];
    }

    private static ScalarFunctionEvaluatorDelegate Resolve(ScalarFunctionRegistry registry, string name)
    {
        if (!registry.TryGet(name, out ScalarFunctionDescriptor descriptor))
            throw new InvalidOperationException($"Scalar function '{name}' is not registered");

        return descriptor.Evaluator;
    }

    private static ColumnValue PackRandom(Random random, int dimensions)
    {
        byte[] bytes = new byte[dimensions * 4];

        for (int i = 0; i < dimensions; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), (float)(random.NextDouble() * 2d - 1d));

        return new ColumnValue(bytes);
    }

    [Benchmark(Baseline = true)]
    public ColumnValue L2Distance() => _l2("l2_distance", _operands);

    [Benchmark]
    public ColumnValue InnerProduct() => _innerProduct("inner_product", _operands);

    /// <summary>Reads the same elements as the other two but accumulates three sums, not one.</summary>
    [Benchmark]
    public ColumnValue CosineDistance() => _cosine("cosine_distance", _operands);
}
