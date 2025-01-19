namespace MapReduce;

public static class ArraysCreator
{
    private static readonly Random Random = new();

    private static readonly string[] Strings = Enumerable
        .Range(0, 100)
        .Select(element => $"{element}")
        .ToArray();

    public static string[] CreateRandomStringArray(int length) => Enumerable
        .Range(0, length)
        .Select(_ => Strings[Random.Next(0, Strings.Length)])
        .ToArray();
}