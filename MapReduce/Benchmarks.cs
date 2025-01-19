using BenchmarkDotNet.Attributes;

namespace MapReduce;

[HtmlExporter]
public class Benchmarks
{
    private static readonly MapReducer MapReducer = new();

    private static readonly string[] Array1 = ArraysCreator.CreateRandomStringArray(100);
    private static readonly string[] Array2 = ArraysCreator.CreateRandomStringArray(10_000);
    private static readonly string[] Array3 = ArraysCreator.CreateRandomStringArray(1_000_000);
    private static readonly string[] Array4 = ArraysCreator.CreateRandomStringArray(10_000_000);

    public static object[] ArraysForBenchmarks =>
    [
        Array1,
        Array2,
        Array3,
        Array4,
    ];

    [Benchmark]
    [ArgumentsSource(nameof(ArraysForBenchmarks))]
    public Dictionary<string, int> BenchmarkForThreeNodes(string[] input)
    {
        return MapReducer.ProcessMapReduce(input, 3);
    }

    [Benchmark]
    [ArgumentsSource(nameof(ArraysForBenchmarks))]
    public Dictionary<string, int> BenchmarkForTenNodes(string[] input)
    {
        return MapReducer.ProcessMapReduce(input, 10);
    }
}