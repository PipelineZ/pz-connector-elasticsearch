using System.Collections.Concurrent;
using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>Pins the <see cref="ActivitySource"/> the Elastic client emits spans on, so the name the
/// host opts in to (<c>Program.cs</c>) is the one the library really uses, and records which tag
/// keys those spans carry (the SDK strips the ones that can hold user data before export).</summary>
[Collection("elasticsearch")]
[Trait("Category", "Docker")]
public sealed class EsActivitySourceTests
{
    private readonly EsFixture _es;
    private readonly ITestOutputHelper _output;

    public EsActivitySourceTests(EsFixture es, ITestOutputHelper output)
    {
        _es = es;
        _output = output;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task Reads_and_writes_emit_spans_on_the_Elastic_Transport_source()
    {
        var index = await _es.CreateIndexAsync("\"id\":{\"type\":\"long\"}");
        await _es.BulkAsync(index, [("a", """{"id":1}""")]);
        var target = EsFixture.NewName("spans");

        var seen = new ConcurrentDictionary<string, ConcurrentBag<string>>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                var tags = seen.GetOrAdd(a.Source.Name, _ => []);
                foreach (var tag in a.TagObjects)
                {
                    tags.Add(tag.Key);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var connection = new ConnectorConfig(_es.ConnectionConfig());
        ISourceConnector sourceConnector = new EsConnector();
        await using (var source = await sourceConnector.OpenAsync(connection, CancellationToken.None))
        {
            var partitions = await source.PlanReadAsync(new DatasetSpec("elasticsearch", index, new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
            foreach (var partition in partitions)
            {
                await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
                {
                    batch.Dispose();
                }
            }
        }

        ISinkConnector sinkConnector = new EsConnector();
        await using (var sink = await sinkConnector.OpenAsync(connection, CancellationToken.None))
        {
            var schema = new Schema([new Field("id", Int64Type.Default, false)], null);
            await using var session = await sink.BeginWriteAsync(new OutputSpec("elasticsearch", target, "append", "fail_on_change", new Dictionary<string, object?>()), schema, CancellationToken.None);
            using var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build()], 1);
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        foreach (var (name, keys) in seen.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            _output.WriteLine($"{name}: {string.Join(", ", keys.Distinct().Order(StringComparer.Ordinal))}");
        }

        Assert.Contains("Elastic.Transport", seen.Keys);
    }
}
