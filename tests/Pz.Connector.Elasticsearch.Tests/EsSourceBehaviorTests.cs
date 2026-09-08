using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>Source behavior the TestKit contract does not pin: the value rules against real
/// documents, the query and watermark filters, paging, pruning, and the error texts a user sees.</summary>
[Collection("elasticsearch")]
[Trait("Category", "Docker")]
public sealed class EsSourceBehaviorTests
{
    private readonly EsFixture _es;

    public EsSourceBehaviorTests(EsFixture es)
    {
        _es = es;
        DockerFacts.SkipUnlessDocker();
    }

    private async Task<List<RecordBatch>> ReadAsync(DatasetSpec spec, ReadHints? hints = null, BatchOptions? options = null)
    {
        ISourceConnector connector = new EsConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_es.ConnectionConfig()), CancellationToken.None);
        var partitions = await source.PlanReadAsync(spec, hints ?? ReadHints.None, CancellationToken.None);
        var batches = new List<RecordBatch>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(options ?? BatchOptions.Default, CancellationToken.None))
            {
                batches.Add(batch);
            }
        }

        return batches;
    }

    private static DatasetSpec Spec(string index, Dictionary<string, object?>? options = null) => new("elasticsearch", index, options ?? new());

    private static List<T> Column<T>(List<RecordBatch> batches, string name, Func<IArrowArray, int, T> read)
    {
        var result = new List<T>();
        foreach (var batch in batches)
        {
            var i = batch.Schema.FieldsList.ToList().FindIndex(f => f.Name == name);
            for (var row = 0; row < batch.Length; row++)
            {
                result.Add(read(batch.Column(i), row));
            }
        }

        return result;
    }

    private static string? Str(IArrowArray a, int row) => ((StringArray)a).GetString(row);

    [SkippableFact]
    public async Task Objects_flatten_nested_and_json_fields_land_as_json_text_and_custom_dates_as_text()
    {
        var index = await _es.CreateIndexAsync("\"id\":{\"type\":\"long\"},\"addr\":{\"properties\":{\"city\":{\"type\":\"keyword\"},\"geo\":{\"type\":\"geo_point\"}}},\"items\":{\"type\":\"nested\",\"properties\":{\"sku\":{\"type\":\"keyword\"}}},\"tags\":{\"type\":\"keyword\"},\"day\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd\"},\"ts\":{\"type\":\"date\"}");
        await _es.BulkAsync(index, [("a", """{"id":1,"addr":{"city":"Lyon","geo":{"lat":45.7,"lon":4.8}},"items":[{"sku":"x"},{"sku":"y"}],"tags":["p","q"],"day":"2026-09-08","ts":"2026-09-08T10:11:12Z"}""")]);

        var batches = await ReadAsync(Spec(index, new() { ["json_fields"] = new List<object?> { "tags" } }));

        Assert.Equal(["addr.city", "addr.geo", "day", "id", "items", "tags", "ts", "_id"], batches[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Equal("Lyon", Column(batches, "addr.city", Str)[0]);
        Assert.Equal("""{"lat":45.7,"lon":4.8}""", Column(batches, "addr.geo", Str)[0]);
        Assert.Equal("2026-09-08", Column(batches, "day", Str)[0]);
        Assert.Equal("""[{"sku":"x"},{"sku":"y"}]""", Column(batches, "items", Str)[0]);
        Assert.Equal("""["p","q"]""", Column(batches, "tags", Str)[0]);
        Assert.Equal(new DateTimeOffset(2026, 9, 8, 10, 11, 12, TimeSpan.Zero), Column(batches, "ts", (a, r) => ((TimestampArray)a).GetTimestamp(r)!.Value)[0]);
        Assert.Equal("a", Column(batches, "_id", Str)[0]);
        foreach (var b in batches) b.Dispose();
    }

    [SkippableFact]
    public async Task An_array_in_a_scalar_field_fails_naming_field_document_and_the_escape_hatch()
    {
        var index = await _es.CreateIndexAsync("\"id\":{\"type\":\"long\"},\"tags\":{\"type\":\"keyword\"}");
        await _es.BulkAsync(index, [("r4", """{"id":1,"tags":["p","q"]}""")]);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(Spec(index)));

        Assert.False(ex.IsTransient);
        Assert.Equal($"elasticsearch: dataset '{index}': field 'tags' of document 'r4' holds an array; list it under json_fields: to land it as JSON text", ex.Message);
    }

    [SkippableFact]
    public async Task Query_narrows_and_watermark_bounds_filter_on_a_date_cursor()
    {
        var index = await _es.CreateIndexAsync("\"id\":{\"type\":\"long\"},\"ts\":{\"type\":\"date\"},\"kind\":{\"type\":\"keyword\"}");
        await _es.BulkAsync(index, Enumerable.Range(0, 10).Select(i =>
            ((string?)$"d{i}", "{\"id\":" + i + ",\"ts\":\"2026-01-" + (i + 1).ToString("00") + "T00:00:00Z\",\"kind\":\"" + (i % 2 == 0 ? "even" : "odd") + "\"}")));

        var evens = await ReadAsync(Spec(index, new() { ["query"] = new Dictionary<string, object?> { ["term"] = new Dictionary<string, object?> { ["kind"] = "even" } } }));
        Assert.Equal([0, 2, 4, 6, 8], Column(evens, "id", (a, r) => ((Int64Array)a).GetValue(r)!.Value).Order());

        var strict = await ReadAsync(Spec(index) with { WatermarkCursor = "ts", WatermarkValue = "2026-01-03T00:00:00.000000" });
        Assert.Equal([3, 4, 5, 6, 7, 8, 9], Column(strict, "id", (a, r) => ((Int64Array)a).GetValue(r)!.Value).Order());

        var inclusive = await ReadAsync(Spec(index) with { WatermarkCursor = "ts", WatermarkValue = "2026-01-03T00:00:00.000000", WatermarkLowerInclusive = true });
        Assert.Equal([2, 3, 4, 5, 6, 7, 8, 9], Column(inclusive, "id", (a, r) => ((Int64Array)a).GetValue(r)!.Value).Order());

        var window = await ReadAsync(Spec(index, new() { ["query"] = """{"term":{"kind":"odd"}}""" }) with
        {
            WatermarkCursor = "ts", WatermarkValue = "2026-01-02T00:00:00.000000", WatermarkUpperBound = "2026-01-06T00:00:00.000000",
        });
        Assert.Equal([3, 5], Column(window, "id", (a, r) => ((Int64Array)a).GetValue(r)!.Value).Order());
    }

    [SkippableFact]
    public async Task Small_pages_read_to_completion_and_pruning_carries_only_the_hinted_columns()
    {
        var index = await _es.CreateIndexAsync(EsFixture.RowsMapping);
        await _es.BulkAsync(index, EsFixture.Rows(120));

        var paged = await ReadAsync(Spec(index, new() { ["page_size"] = 7L }));
        Assert.Equal(120, paged.Sum(b => b.Length));
        Assert.Equal(Enumerable.Range(0, 120).Select(i => (long)i), Column(paged, "id", (a, r) => ((Int64Array)a).GetValue(r)!.Value).Order());

        var pruned = await ReadAsync(Spec(index), new ReadHints(Columns: ["name", "_id"]));
        Assert.Equal(["name", "_id"], pruned[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(120, pruned.Sum(b => b.Length));
        Assert.Contains("n7", Column(pruned, "name", Str));

        var idOnly = await ReadAsync(Spec(index), new ReadHints(Columns: ["_id"]));
        Assert.Equal(["_id"], idOnly[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Contains("d7", Column(idOnly, "_id", Str));
    }

    [SkippableFact]
    public async Task Schema_matches_the_mapping_and_an_unknown_index_or_pattern_is_refused_with_a_hint()
    {
        ISourceConnector connector = new EsConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_es.ConnectionConfig()), CancellationToken.None);

        var index = await _es.CreateIndexAsync(EsFixture.RowsMapping);
        var schema = await source.GetSchemaAsync(Spec(index), CancellationToken.None);
        Assert.Equal(["id", "name", "pad", "_id"], schema.Schema.FieldsList.Select(f => f.Name));

        var missing = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.GetSchemaAsync(Spec("nope-" + index), CancellationToken.None));
        Assert.False(missing.IsTransient);
        Assert.Contains("index_not_found_exception", missing.Message);
        Assert.Contains("(HTTP 404; create the index or fix 'index:')", missing.Message);

        var pattern = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.GetSchemaAsync(Spec("nope-*"), CancellationToken.None));
        Assert.Contains("matches no index", pattern.Message);
    }

    [SkippableFact]
    public async Task Check_connection_reports_the_cluster_and_a_refused_port_as_a_failed_probe()
    {
        var ok = await new EsConnector().CheckConnectionAsync(new ConnectorConfig(_es.ConnectionConfig()), CancellationToken.None);
        Assert.True(ok.Ok, ok.Message);
        Assert.Contains("(9.5.3)", ok.Message);

        var refused = await new EsConnector().CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["url"] = "http://127.0.0.1:1", ["timeout"] = 2L }), CancellationToken.None);
        Assert.False(refused.Ok);
    }
}
