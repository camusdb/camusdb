/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// The size and speed of the <c>jsonb</c> cell format against JSON text, on public corpora.
///
/// <para><c>probe jsonbsize &lt;dir&gt;</c> prints the space table: for each corpus file, the minified
/// text, the cell, and each one after the engine's LZ4 rule (<see cref="LargeValueCompression"/>:
/// 256-byte floor, 12% minimum saving). A file that holds a top-level array is also measured element
/// by element, because a row holds one document, not one file. A <c>.ndjson</c> file is one document per
/// line.</para>
///
/// <para>The BenchmarkDotNet class needs the environment variable <c>CAMUSDB_JSON_CORPUS</c> (the same
/// directory). It times parse to a cell, a cell to text, and one <c>-&gt;</c> on the first, middle and
/// last key of a 10-key and a 1,000-key object, with <see cref="JsonDocument"/> parse plus the same
/// lookup as the baseline. The corpora are not in the repository.</para>
/// </summary>
[MemoryDiagnoser]
public class JsonbCodecBenchmarks
{
    private byte[] twitterText = [];
    private byte[] twitterCell = [];
    private byte[] smallText = [];
    private byte[] smallCell = [];
    private byte[] wideText = [];
    private byte[] wideCell = [];

    [Params("k0", "k5", "k9")]
    public string SmallKey { get; set; } = "k0";

    [GlobalSetup]
    public void Setup()
    {
        string directory = Environment.GetEnvironmentVariable("CAMUSDB_JSON_CORPUS")
            ?? throw new InvalidOperationException("set CAMUSDB_JSON_CORPUS to the corpus directory");

        twitterText = Minify(File.ReadAllBytes(Path.Combine(directory, "twitter.json")));
        twitterCell = JsonbEncoder.FromUtf8(twitterText);

        smallText = Encoding.UTF8.GetBytes("{" + string.Join(", ", Enumerable.Range(0, 10).Select(i => $"\"k{i}\": {i}")) + "}");
        smallCell = JsonbEncoder.FromUtf8(smallText);

        wideText = Encoding.UTF8.GetBytes("{" + string.Join(", ", Enumerable.Range(0, 1000).Select(i => $"\"key{i:D4}\": {{\"v\": {i}}}")) + "}");
        wideCell = JsonbEncoder.FromUtf8(wideText);
    }

    [Benchmark]
    public byte[] ParseTwitterToCell() => JsonbEncoder.FromUtf8(twitterText);

    [Benchmark]
    public string TwitterCellToText() => JsonbText.ToText(JsonbDocument.Item(twitterCell));

    [Benchmark]
    public int TwitterJsonDocumentParse()
    {
        using JsonDocument document = JsonDocument.Parse(twitterText);
        return document.RootElement.GetPropertyCount();
    }

    [Benchmark]
    public int SmallObjectLookup_Cell() =>
        JsonbDocument.TryGetField(JsonbDocument.Item(smallCell), SmallKey, out Range range) ? range.Start.Value : -1;

    [Benchmark]
    public int SmallObjectLookup_JsonDocument()
    {
        using JsonDocument document = JsonDocument.Parse(smallText);
        return document.RootElement.TryGetProperty(SmallKey, out JsonElement value) ? value.GetInt32() : -1;
    }

    [Benchmark]
    public int WideObjectLookup_Cell()
    {
        string key = SmallKey switch { "k0" => "key0000", "k5" => "key0500", _ => "key0999" };
        return JsonbDocument.TryGetField(JsonbDocument.Item(wideCell), key, out Range range) ? range.Start.Value : -1;
    }

    [Benchmark]
    public int WideObjectLookup_JsonDocument()
    {
        string key = SmallKey switch { "k0" => "key0000", "k5" => "key0500", _ => "key0999" };
        using JsonDocument document = JsonDocument.Parse(wideText);
        return document.RootElement.TryGetProperty(key, out JsonElement value) ? value.GetRawText().Length : -1;
    }

    private static byte[] Minify(byte[] text)
    {
        using JsonDocument document = JsonDocument.Parse(text);
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
            document.WriteTo(writer);
        return stream.ToArray();
    }

    /// <summary>The space table described on the class.</summary>
    public static int RunSizeProbe(string directory)
    {
        Console.WriteLine("| Corpus | Documents | Minified text | Cell | Text + LZ4 | Cell + LZ4 |");
        Console.WriteLine("|---|---|---|---|---|---|");

        foreach (string path in Directory.GetFiles(directory).OrderBy(p => p, StringComparer.Ordinal))
        {
            string name = Path.GetFileName(path);
            List<byte[]> documents = [];

            if (name.EndsWith(".ndjson", StringComparison.Ordinal))
            {
                foreach (string line in File.ReadLines(path))
                    if (line.Trim().Length > 0)
                        documents.Add(Encoding.UTF8.GetBytes(line));
                Report(name + " (each line)", documents);
                continue;
            }

            if (!name.EndsWith(".json", StringComparison.Ordinal))
                continue;

            byte[] whole = File.ReadAllBytes(path);
            Report(name + " (whole file)", [whole]);

            using JsonDocument document = JsonDocument.Parse(whole);
            JsonElement root = document.RootElement;
            JsonElement? rows = root.ValueKind == JsonValueKind.Array ? root
                : root.ValueKind == JsonValueKind.Object && root.TryGetProperty("statuses", out JsonElement statuses) ? statuses
                : null;

            if (rows is JsonElement array && array.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement element in array.EnumerateArray())
                    documents.Add(Encoding.UTF8.GetBytes(element.GetRawText()));
                Report(name + " (each element)", documents);
            }
        }

        return 0;
    }

    private static void Report(string label, List<byte[]> documents)
    {
        long text = 0, cell = 0, textLz = 0, cellLz = 0;
        foreach (byte[] raw in documents)
        {
            byte[] minified = Minify(raw);
            byte[] encoded = JsonbEncoder.FromUtf8(minified);
            text += minified.Length;
            cell += encoded.Length;
            textLz += LargeValueCompression.TryCompress(minified, 12, out byte[]? a) ? a!.Length : minified.Length;
            cellLz += LargeValueCompression.TryCompress(encoded, 12, out byte[]? b) ? b!.Length : encoded.Length;
        }

        string Cell(long bytes) => $"{bytes:N0} B ({100.0 * bytes / text:F0}%)";
        Console.WriteLine($"| {label} | {documents.Count:N0} | {text:N0} B | {Cell(cell)} | {Cell(textLz)} | {Cell(cellLz)} |");
    }
}
