using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>Sink behavior beyond the TestKit contract: composite merge keys, what a replace leaves
/// behind, the refusals, and the shape of a bulk rejection.</summary>
[Collection("elasticsearch")]
[Trait("Category", "Docker")]
public sealed class EsSinkBehaviorTests
{
    private static readonly Schema Schema = new(
    [
        new Field("tenant", StringType.Default, true),
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
    ], null);

    private readonly EsFixture _es;

    public EsSinkBehaviorTests(EsFixture es)
    {
        _es = es;
        DockerFacts.SkipUnlessDocker();
    }

    private static RecordBatch Batch(params (string Tenant, long Id, string Name)[] rows) => new(Schema,
    [
        new StringArray.Builder().AppendRange(rows.Select(r => r.Tenant)).Build(),
        new Int64Array.Builder().AppendRange(rows.Select(r => r.Id)).Build(),
        new StringArray.Builder().AppendRange(rows.Select(r => r.Name)).Build(),
    ], rows.Length);

    private async Task<ISink> OpenAsync(EsConnector? connector = null) =>
        await ((ISinkConnector)(connector ?? new EsConnector())).OpenAsync(new ConnectorConfig(_es.ConnectionConfig()), CancellationToken.None);

    private static OutputSpec Spec(string output, string mode, params string[] keys) =>
        new("elasticsearch", output, mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys };

    private static async Task CommitAsync(ISink sink, OutputSpec spec, params (string Tenant, long Id, string Name)[] rows)
    {
        await using var session = await sink.BeginWriteAsync(spec, Schema, CancellationToken.None);
        using (var batch = Batch(rows))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.CommitAsync(CancellationToken.None);
    }

    [SkippableFact]
    public async Task Merge_with_a_composite_key_upserts_by_the_joined_id()
    {
        var index = EsFixture.NewName("merge");
        await using var sink = await OpenAsync();
        await CommitAsync(sink, Spec(index, "merge", "tenant", "id"), ("acme", 1, "one"), ("acme", 2, "two"), ("beta", 1, "uno"));
        await CommitAsync(sink, Spec(index, "merge", "tenant", "id"), ("acme", 1, "ONE"));

        var docs = await _es.SearchAllAsync(index);
        Assert.Equal(["acme|1", "acme|2", "beta|1"], docs.Select(d => d.Id).Order());
        Assert.Equal("ONE", docs.Single(d => d.Id == "acme|1").Source.GetProperty("name").GetString());
    }

    [SkippableFact]
    public async Task Bulk_sizing_splits_requests_and_every_row_still_lands()
    {
        var index = EsFixture.NewName("append");
        await using var sink = await OpenAsync();
        var spec = new OutputSpec("elasticsearch", index, "append", "fail_on_change", new Dictionary<string, object?> { ["bulk_size"] = 7L });
        await CommitAsync(sink, spec, Enumerable.Range(0, 50).Select(i => ("t", (long)i, $"n{i}")).ToArray());

        Assert.Equal(50, await _es.CountAsync(index));
    }

    [SkippableFact]
    public async Task Replace_swaps_the_alias_deletes_the_old_index_and_copies_its_mapping()
    {
        var alias = EsFixture.NewName("orders");
        var first = await _es.CreateIndexAsync("\"tenant\":{\"type\":\"keyword\"},\"id\":{\"type\":\"long\"},\"name\":{\"type\":\"text\"}");
        await _es.BulkAsync(first, [(null, """{"tenant":"old","id":9,"name":"stale"}""")]);
        await _es.AddAliasAsync(first, alias);

        var time = new FakeTime(new DateTimeOffset(2026, 9, 8, 12, 0, 0, TimeSpan.Zero));
        var connector = new EsConnector(null, time, new Random(7));
        await using var sink = await OpenAsync(connector);
        await CommitAsync(sink, Spec(alias, "replace"), ("acme", 1, "one"), ("acme", 2, "two"));

        var backing = await _es.AliasIndicesAsync(alias);
        Assert.Single(backing);
        Assert.StartsWith(alias + "-pz-20260908120000000-", backing[0]);
        Assert.False(await _es.IndexExistsAsync(first));
        Assert.Equal(2, await _es.CountAsync(alias));
        Assert.Equal("text", (await _es.GetMappingPropertiesAsync(backing[0])).GetProperty("name").GetProperty("type").GetString());

        // A second replace swaps again and leaves exactly one index behind the alias.
        await CommitAsync(sink, Spec(alias, "replace"), ("acme", 3, "three"));
        var again = await _es.AliasIndicesAsync(alias);
        Assert.Single(again);
        Assert.NotEqual(backing[0], again[0]);
        Assert.False(await _es.IndexExistsAsync(backing[0]));
        Assert.Equal(1, await _es.CountAsync(alias));
    }

    [SkippableFact]
    public async Task Replace_into_an_absent_name_creates_the_alias_and_abort_deletes_the_staging_index()
    {
        var alias = EsFixture.NewName("fresh");
        await using var sink = await OpenAsync();

        string staging;
        await using (var session = await sink.BeginWriteAsync(Spec(alias, "replace"), Schema, CancellationToken.None))
        {
            staging = ((EsWriteSession)session).TargetIndex;
            Assert.True(await _es.IndexExistsAsync(staging));
            using (var batch = Batch(("t", 1, "x")))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.AbortAsync(CancellationToken.None);
        }

        Assert.False(await _es.IndexExistsAsync(staging));
        Assert.Empty(await _es.AliasIndicesAsync(alias));

        await CommitAsync(sink, Spec(alias, "replace"), ("t", 1, "x"));
        Assert.Single(await _es.AliasIndicesAsync(alias));
        Assert.Equal(1, await _es.CountAsync(alias));
    }

    [SkippableFact]
    public async Task Replace_refuses_a_concrete_index_of_the_output_name()
    {
        var index = await _es.CreateIndexAsync(EsFixture.RowsMapping);
        await using var sink = await OpenAsync();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(Spec(index, "replace"), Schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Equal($"elasticsearch: output '{index}': '{index}' is an index, not an alias; replace swaps an alias so the old data disappears "
            + "atomically -- reindex it behind an alias first", ex.Message);
    }

    [SkippableFact]
    public async Task A_rejected_bulk_item_fails_the_write_with_its_type_and_reason()
    {
        var index = await _es.CreateIndexAsync("\"tenant\":{\"type\":\"keyword\"},\"id\":{\"type\":\"long\"},\"name\":{\"type\":\"integer\"}");
        await using var sink = await OpenAsync();
        await using var session = await sink.BeginWriteAsync(Spec(index, "merge", "id"), Schema, CancellationToken.None);
        using var batch = Batch(("t", 5, "not a number"));
        await session.WriteBatchAsync(batch, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.CommitAsync(CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith($"elasticsearch: output '{index}': bulk of 1 document(s) into '{index}': 1 of 1 item(s) rejected; first (document '5'): document_parsing_exception: ", ex.Message);
    }

    [SkippableFact]
    public async Task Round_trip_through_the_source_keeps_ids_and_values()
    {
        var index = EsFixture.NewName("rt");
        await using var sink = await OpenAsync();
        await CommitAsync(sink, Spec(index, "merge", "id"), ("acme", 1, "one"), ("acme", 2, "two"));

        ISourceConnector connector = new EsConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_es.ConnectionConfig()), CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("elasticsearch", index, new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
        var rows = new List<(string Id, long Value, string Name)>();
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            var names = batch.Schema.FieldsList.Select(f => f.Name).ToList();
            for (var r = 0; r < batch.Length; r++)
            {
                rows.Add((((StringArray)batch.Column(names.IndexOf("_id"))).GetString(r),
                    ((Int64Array)batch.Column(names.IndexOf("id"))).GetValue(r)!.Value,
                    ((StringArray)batch.Column(names.IndexOf("name"))).GetString(r)));
            }

            batch.Dispose();
        }

        Assert.Equal([("1", 1L, "one"), ("2", 2L, "two")], rows.OrderBy(r => r.Value));
    }

    private sealed class FakeTime(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
