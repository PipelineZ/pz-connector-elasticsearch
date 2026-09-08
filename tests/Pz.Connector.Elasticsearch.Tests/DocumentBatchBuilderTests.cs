using System.Text.Json;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class DocumentBatchBuilderTests
{
    private const string Mapping = """
        {"idx":{"mappings":{"properties":{
          "b":{"type":"boolean"},"d":{"type":"double"},"dt":{"type":"date"},"dt_sec":{"type":"date","format":"epoch_second"},
          "i":{"type":"integer"},"j":{"type":"flattened"},"k":{"type":"keyword"},"l":{"type":"long"},
          "o":{"properties":{"city":{"type":"keyword"}}},"t":{"type":"text"}
        }}}}
        """;

    private static ColumnPlan Plan(params string[] jsonFields)
    {
        var errors = new List<string>();
        var dataset = EsDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?> { ["json_fields"] = jsonFields.Cast<object?>().ToList() }), errors)!;
        return EsSchemaMapper.Plan(EsSchemaMapperTests.Mappings(Mapping), dataset, "d", EsRedactor.None);
    }

    private static JsonElement Doc(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static RecordBatch One(ColumnPlan plan, string id, string json)
    {
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "d", EsRedactor.None);
        builder.Append(id, Doc(json));
        return builder.Flush()!;
    }

    [Fact]
    public void Converts_every_kind_from_its_native_json_shape()
    {
        var plan = Plan();
        using var batch = One(plan, "doc-1",
            """{"b":true,"d":1.5,"dt":"2026-09-08T10:11:12.123456789Z","dt_sec":1700000000,"i":7,"j":{"a":[1,"x"]},"k":"kw","l":9000000000,"o":{"city":"Lyon"},"t":"long text"}""");

        Assert.Equal(1, batch.Length);
        Assert.Equal(plan.Schema.FieldsList.Select(f => f.Name), batch.Schema.FieldsList.Select(f => f.Name));
        Assert.True(((BooleanArray)batch.Column(0)).GetValue(0));
        Assert.Equal(1.5, ((DoubleArray)batch.Column(1)).GetValue(0));
        Assert.Equal(new DateTimeOffset(2026, 9, 8, 10, 11, 12, TimeSpan.Zero).AddTicks(1234560), ((TimestampArray)batch.Column(2)).GetTimestamp(0));
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1700000000), ((TimestampArray)batch.Column(3)).GetTimestamp(0));
        Assert.Equal(7, ((Int32Array)batch.Column(4)).GetValue(0));
        Assert.Equal("""{"a":[1,"x"]}""", ((StringArray)batch.Column(5)).GetString(0));
        Assert.Equal("kw", ((StringArray)batch.Column(6)).GetString(0));
        Assert.Equal(9000000000L, ((Int64Array)batch.Column(7)).GetValue(0));
        Assert.Equal("Lyon", ((StringArray)batch.Column(8)).GetString(0));
        Assert.Equal("long text", ((StringArray)batch.Column(9)).GetString(0));
        Assert.Equal("doc-1", ((StringArray)batch.Column(10)).GetString(0));
    }

    [Fact]
    public void Accepts_the_coerced_spellings_elasticsearch_accepts()
    {
        var plan = Plan();
        using var batch = One(plan, "x",
            """{"b":"true","d":"2.5","dt":1700000000000,"dt_sec":"1700000000","i":"42","l":"1.0","k":12,"t":false}""");

        Assert.True(((BooleanArray)batch.Column(0)).GetValue(0));
        Assert.Equal(2.5, ((DoubleArray)batch.Column(1)).GetValue(0));
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), ((TimestampArray)batch.Column(2)).GetTimestamp(0));
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1700000000), ((TimestampArray)batch.Column(3)).GetTimestamp(0));
        Assert.Equal(42, ((Int32Array)batch.Column(4)).GetValue(0));
        Assert.Equal(1L, ((Int64Array)batch.Column(7)).GetValue(0));
        Assert.Equal("12", ((StringArray)batch.Column(6)).GetString(0));
        Assert.Equal("false", ((StringArray)batch.Column(9)).GetString(0));
    }

    [Fact]
    public void A_timestamp_without_a_zone_is_utc_and_a_json_string_column_keeps_its_quotes()
    {
        var plan = Plan("k");
        using var batch = One(plan, "x", """{"dt":"2026-01-02T03:04:05","k":"quoted"}""");

        Assert.Equal(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), ((TimestampArray)batch.Column(2)).GetTimestamp(0));
        Assert.Equal("\"quoted\"", ((StringArray)batch.Column(6)).GetString(0));
    }

    [Fact]
    public void Missing_and_null_fields_are_null_in_every_column()
    {
        var plan = Plan();
        using var batch = One(plan, "x", """{"b":null,"o":{}}""");

        for (var c = 0; c < batch.ColumnCount - 1; c++)
        {
            Assert.True(batch.Column(c).IsNull(0), $"column {c}");
        }

        Assert.Equal("x", ((StringArray)batch.Column(batch.ColumnCount - 1)).GetString(0));
    }

    [Fact]
    public void Dotted_source_keys_resolve_like_nested_objects()
    {
        var plan = Plan();
        using var batch = One(plan, "x", """{"o.city":"Nice"}""");

        Assert.Equal("Nice", ((StringArray)batch.Column(8)).GetString(0));
    }

    [Theory]
    [InlineData("""{"k":["a","b"]}""", "field 'k' of document 'r4' holds an array; list it under json_fields: to land it as JSON text")]
    [InlineData("""{"l":{"n":1}}""", "field 'l' of document 'r4' holds an object; list it under json_fields: to land it as JSON text")]
    [InlineData("""{"l":"abc"}""", "field 'l' of document 'r4' holds \"abc\" where the mapping says long")]
    [InlineData("""{"i":3000000000}""", "field 'i' of document 'r4' holds 3000000000, outside the mapped 32-bit range")]
    [InlineData("""{"d":"NaNny"}""", "field 'd' of document 'r4' holds \"NaNny\" where the mapping says double")]
    [InlineData("""{"b":"yes"}""", "field 'b' of document 'r4' holds \"yes\" where the mapping says boolean")]
    [InlineData("""{"dt":"yesterday"}""", "field 'dt' of document 'r4' holds \"yesterday\", which is not an ISO-8601 timestamp or an epoch number")]
    [InlineData("""{"l":1.5}""", "field 'l' of document 'r4' holds 1.5 where the mapping says long")]
    public void Refuses_values_the_mapping_cannot_hold_naming_field_and_document(string json, string message)
    {
        var builder = new DocumentBatchBuilder(Plan(), BatchOptions.Default, "orders", EsRedactor.None);

        var ex = Assert.Throws<PzConnectorException>(() => builder.Append("r4", Doc(json)));

        Assert.False(ex.IsTransient);
        Assert.Equal("elasticsearch: dataset 'orders': " + message, ex.Message);
    }

    [Fact]
    public void Batches_cut_by_rows_and_by_bytes_and_are_fresh_instances()
    {
        var plan = Plan().Project(["k", "_id"]);
        var byRows = new DocumentBatchBuilder(plan, new BatchOptions(MaxRowsPerBatch: 3), "d", EsRedactor.None);
        var taken = new List<RecordBatch>();
        for (var i = 0; i < 7; i++)
        {
            byRows.Append($"id{i}", Doc($$"""{"k":"v{{i}}"}"""));
            if (byRows.TryTakeBatch(out var batch))
            {
                taken.Add(batch!);
            }
        }

        var last = byRows.Flush();
        Assert.NotNull(last);
        Assert.Equal([3, 3], taken.Select(b => b.Length));
        Assert.Equal(1, last.Length);
        Assert.Equal(["k", "_id"], last.Schema.FieldsList.Select(f => f.Name));
        Assert.NotSame(taken[0], taken[1]);
        Assert.Equal("v6", ((StringArray)last.Column(0)).GetString(0));
        foreach (var b in taken.Append(last)) b.Dispose();

        var byBytes = new DocumentBatchBuilder(plan, new BatchOptions(TargetBatchBytes: 64), "d", EsRedactor.None);
        var cuts = 0;
        for (var i = 0; i < 20; i++)
        {
            byBytes.Append("id", Doc("""{"k":"0123456789012345678901234567890123456789"}"""));
            if (byBytes.TryTakeBatch(out var batch))
            {
                cuts++;
                batch!.Dispose();
            }
        }

        Assert.True(cuts >= 5, $"expected several byte-bounded batches, got {cuts}");
        byBytes.Flush()?.Dispose();
    }

    [Fact]
    public void TryGetPath_prefers_nested_then_literal_dotted_keys()
    {
        var doc = Doc("""{"a":{"b":{"c":1},"b.c":2},"x.y":{"z":3},"p":{"q.r.s":4}}""");

        Assert.True(DocumentBatchBuilder.TryGetPath(doc, ["a", "b", "c"], out var v)); Assert.Equal(1, v.GetInt32());
        Assert.True(DocumentBatchBuilder.TryGetPath(doc, ["x", "y", "z"], out v)); Assert.Equal(3, v.GetInt32());
        Assert.True(DocumentBatchBuilder.TryGetPath(doc, ["p", "q", "r", "s"], out v)); Assert.Equal(4, v.GetInt32());
        Assert.False(DocumentBatchBuilder.TryGetPath(doc, ["a", "nope"], out _));
        Assert.False(DocumentBatchBuilder.TryGetPath(doc, ["a", "b", "c", "d"], out _));
    }
}
