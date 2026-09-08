using System.Text;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsSearchBuilderTests
{
    private const string Mapping = """{"idx":{"mappings":{"properties":{"id":{"type":"long"},"name":{"type":"keyword"},"ts":{"type":"date"},"day":{"type":"date","format":"yyyy-MM-dd"},"o":{"properties":{"c":{"type":"keyword"}}}}}}}""";

    private static readonly ElasticsearchClient Client =
        new(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), new InMemoryRequestInvoker()));

    private static (EsDatasetConfig Dataset, ColumnPlan Plan) Fixture(Dictionary<string, object?>? options = null)
    {
        var errors = new List<string>();
        var dataset = EsDatasetConfig.Parse(new DatasetSpec("s", "orders", options ?? new()), errors)!;
        return (dataset, EsSchemaMapper.Plan(EsSchemaMapperTests.Mappings(Mapping), dataset, "orders", EsRedactor.None));
    }

    private static JsonElement Serialize(SearchRequest request)
    {
        using var stream = new MemoryStream();
        Client.RequestResponseSerializer.Serialize(request, stream);
        return JsonDocument.Parse(stream.ToArray()).RootElement.Clone();
    }

    private static string Unwrap(JsonElement clause) =>
        Encoding.UTF8.GetString(Convert.FromBase64String(clause.GetProperty("wrapper").GetProperty("query").GetString()!));

    [Fact]
    public void First_page_carries_pit_sort_size_source_and_no_query()
    {
        var (dataset, plan) = Fixture(new() { ["page_size"] = 50L, ["pit_keep_alive"] = "2m" });
        var body = Serialize(EsSearchBuilder.Build(dataset, plan, new DatasetSpec("s", "orders", new Dictionary<string, object?>()), "pit-1", null));

        Assert.Equal("pit-1", body.GetProperty("pit").GetProperty("id").GetString());
        Assert.Equal("2m", body.GetProperty("pit").GetProperty("keep_alive").GetString());
        Assert.Equal(50, body.GetProperty("size").GetInt32());
        Assert.Equal(JsonValueKind.Object, body.GetProperty("sort").GetProperty("_shard_doc").ValueKind);
        Assert.False(body.GetProperty("track_total_hits").GetBoolean());
        Assert.Equal(["id", "name", "ts", "day", "o.c"], body.GetProperty("_source").GetProperty("includes").EnumerateArray().Select(e => e.GetString()));
        Assert.False(body.TryGetProperty("query", out _));
        Assert.False(body.TryGetProperty("search_after", out _));
    }

    [Fact]
    public void A_projection_narrows_source_and_only_id_disables_it()
    {
        var (dataset, plan) = Fixture();
        var spec = new DatasetSpec("s", "orders", new Dictionary<string, object?>());

        // The client spells a single include as a bare string, which Elasticsearch accepts.
        var narrowed = Serialize(EsSearchBuilder.Build(dataset, plan.Project(["o.c", "_id"]), spec, "p", null));
        Assert.Equal("o.c", narrowed.GetProperty("_source").GetProperty("includes").GetString());

        var idOnly = Serialize(EsSearchBuilder.Build(dataset, plan.Project(["_id"]), spec, "p", null));
        Assert.False(idOnly.GetProperty("_source").GetBoolean());
    }

    [Fact]
    public void Search_after_carries_the_previous_sort_values_by_kind()
    {
        var (dataset, plan) = Fixture();
        var after = JsonDocument.Parse("""[123, "abc", 1.5, true, null]""").RootElement.EnumerateArray().Select(e => e.Clone()).ToList();

        var body = Serialize(EsSearchBuilder.Build(dataset, plan, new DatasetSpec("s", "orders", new Dictionary<string, object?>()), "p", after));

        Assert.Equal("""[123,"abc",1.5,true,null]""", body.GetProperty("search_after").GetRawText());
    }

    [Theory]
    [InlineData("3", false, null, """{"range":{"id":{"gt":"3"}}}""")]
    [InlineData("3", true, null, """{"range":{"id":{"gte":"3"}}}""")]
    [InlineData("3", false, "7", """{"range":{"id":{"gt":"3","lte":"7"}}}""")]
    [InlineData(null, false, "7", """{"range":{"id":{"lte":"7"}}}""")]
    [InlineData(null, false, null, null)]
    public void Cursor_range_follows_the_bounds(string? value, bool inclusive, string? upper, string? expected)
    {
        var spec = new DatasetSpec("s", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "id", WatermarkValue = value, WatermarkLowerInclusive = inclusive, WatermarkUpperBound = upper,
        };

        Assert.Equal(expected, EsSearchBuilder.CursorRangeJson(spec));
    }

    [Fact]
    public void No_cursor_means_no_range()
    {
        Assert.Null(EsSearchBuilder.CursorRangeJson(new DatasetSpec("s", "orders", new Dictionary<string, object?>()) { WatermarkValue = "3" }));
    }

    [Fact]
    public void User_query_and_range_are_wrapped_verbatim_under_a_bool_filter()
    {
        var (dataset, plan) = Fixture(new() { ["query"] = """{"term":{"name":"x"}}""" });
        var spec = new DatasetSpec("s", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "ts", WatermarkValue = "2026-09-08T10:11:12.123456", WatermarkUpperBound = "2026-09-09T00:00:00.000000",
        };

        var body = Serialize(EsSearchBuilder.Build(dataset, plan, spec, "p", null));

        var filter = body.GetProperty("query").GetProperty("bool").GetProperty("filter");
        Assert.Equal(2, filter.GetArrayLength());
        Assert.Equal("""{"term":{"name":"x"}}""", Unwrap(filter[0]));
        Assert.Equal("""{"range":{"ts":{"gt":"2026-09-08T10:11:12.123456","lte":"2026-09-09T00:00:00.000000"}}}""", Unwrap(filter[1]));
    }

    [Fact]
    public void A_lone_user_query_is_still_a_filter_clause()
    {
        var (dataset, plan) = Fixture(new() { ["query"] = new Dictionary<string, object?> { ["match_all"] = new Dictionary<string, object?>() } });

        var body = Serialize(EsSearchBuilder.Build(dataset, plan, new DatasetSpec("s", "orders", new Dictionary<string, object?>()), "p", null));

        var filter = body.GetProperty("query").GetProperty("bool").GetProperty("filter");
        Assert.Equal("""{"match_all":{}}""", Unwrap(filter.ValueKind == JsonValueKind.Array ? filter[0] : filter));
    }

    [Fact]
    public void Cursor_must_be_a_numeric_or_standard_date_column()
    {
        var (_, plan) = Fixture();
        DatasetSpec Spec(string cursor) => new("s", "orders", new Dictionary<string, object?>()) { WatermarkCursor = cursor };

        EsSearchBuilder.ValidateCursor(plan, Spec("id"), EsRedactor.None);
        EsSearchBuilder.ValidateCursor(plan, Spec("ts"), EsRedactor.None);
        EsSearchBuilder.ValidateCursor(plan, new DatasetSpec("s", "orders", new Dictionary<string, object?>()), EsRedactor.None);

        var text = Assert.Throws<PzConnectorException>(() => EsSearchBuilder.ValidateCursor(plan, Spec("name"), EsRedactor.None));
        Assert.Equal("elasticsearch: dataset 'orders': watermark cursor 'name' is mapped as 'keyword', which a range query cannot bound; "
            + "use a numeric or standard-format date field", text.Message);

        var customDate = Assert.Throws<PzConnectorException>(() => EsSearchBuilder.ValidateCursor(plan, Spec("day"), EsRedactor.None));
        Assert.Contains("mapped as 'date (yyyy-MM-dd)'", customDate.Message);

        var missing = Assert.Throws<PzConnectorException>(() => EsSearchBuilder.ValidateCursor(plan, Spec("nope"), EsRedactor.None));
        Assert.Equal("elasticsearch: dataset 'orders': watermark cursor 'nope' is not a field of the index mapping", missing.Message);
    }
}
