using System.Collections.Concurrent;

namespace MapReduce;

public class MapReducer
{
    public Dictionary<string, int> ProcessMapReduce(
        string[] input,
        int numberOfNodes)
    {
        var partitionSize = input.Length / numberOfNodes;

        var parts = new List<string[]>();
        var arrayIndex = 0;
        while (arrayIndex < input.Length)
        {
            var currentPartitionEnd = arrayIndex + partitionSize;
            if (currentPartitionEnd < input.Length)
            {
                parts.Add(input[arrayIndex..currentPartitionEnd]);
                arrayIndex = currentPartitionEnd;
                continue;
            }

            parts.Add(input[arrayIndex..]);
            arrayIndex = currentPartitionEnd;
        }

        var mappedPartitions = new ConcurrentBag<string[]>();

        var mappingActions = new Action[parts.Count];
        var index = 0;
        foreach (var part in parts)
        {
            mappingActions[index] = () =>
            {
                var mappedArray = part
                    .Select(Map)
                    .ToArray();

                mappedPartitions.Add(mappedArray);
            };

            index++;
        }

        Parallel.Invoke(mappingActions);

        var countedWords = new ConcurrentBag<Dictionary<string, int>>();

        var countingActions = new Action[parts.Count];
        index = 0;
        foreach (var part in mappedPartitions)
        {
            countingActions[index] = () => countedWords.Add(Reduce(part));

            index++;
        }

        Parallel.Invoke(countingActions);

        var numberOfWorkingThreads = 0;

        while (countedWords.Count != 1 || numberOfWorkingThreads != 0)
        {
            if (countedWords.Count >= 2)
            {
                countedWords.TryTake(out var firstDictionary);
                countedWords.TryTake(out var secondDictionary);

                Interlocked.Increment(ref numberOfWorkingThreads);
                Task.Run(() => Merge(
                    firstDictionary!,
                    secondDictionary!,
                    countedWords,
                    ref numberOfWorkingThreads));
            }
        }

        countedWords.TryTake(out var result);

        return result!;
    }

    private static string Map(string input)
    {
        return input
            .Trim()
            .ToLower();
    }

    private static Dictionary<string, int> Reduce(IReadOnlyCollection<string> input)
    {
        var result = new Dictionary<string, int>();

        foreach (var word in input)
        {
            if (result.TryAdd(word, 1) is false)
            {
                result[word]++;
            }
        }

        return result;
    }

    private static void Merge(
        Dictionary<string, int> countedWords1,
        Dictionary<string, int> countedWords2,
        ConcurrentBag<Dictionary<string, int>> parts,
        ref int numberOfWorkingThreads)
    {
        var result = new Dictionary<string, int>(countedWords1.Count + countedWords2.Count);

        foreach (var (word, count) in countedWords1)
        {
            if (countedWords2.TryGetValue(word, out var wordCountInSecondDictionary))
            {
                result[word] = wordCountInSecondDictionary + count;
                countedWords2.Remove(word);
                continue;
            }

            result[word] = count;
        }

        foreach (var (word, count) in countedWords2)
        {
            result[word] = count;
        }

        parts.Add(result);

        Interlocked.Decrement(ref numberOfWorkingThreads);
    }
}