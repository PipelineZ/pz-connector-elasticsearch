using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsDatasetConfigTests
{
    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("search", "orders", options);

    [Fact]
    public void Defaults_read_the_entity_name_with_no_query()
    {
        var errors = new List<string>();
        var config = EsDatasetConfig.Parse(Spec(new()), errors);

        Assert.Empty(errors);
        Assert.Equal("orders", config!.Index);
        Assert.Null(config.QueryJson);
        Assert.Equal(1000, config.PageSize);
        Assert.Empty(config.JsonFields);
        Assert.Equal("5m", config.PitKeepAlive);
    }

    [Fact]
    public void Every_option_parses()
    {
        var errors = new List<string>();
        var config = EsDatasetConfig.Parse(Spec(new()
        {
            ["index"] = "orders-*",
            ["query"] = new Dictionary<string, object?>
            {
                ["bool"] = new Dictionary<string, object?>
                {
                    ["filter"] = new List<object?>
                    {
                        new Dictionary<string, object?> { ["term"] = new Dictionary<string, object?> { ["status"] = "shipped" } },
                        new Dictionary<string, object?> { ["range"] = new Dictionary<string, object?> { ["qty"] = new Dictionary<string, object?> { ["gte"] = 2L, ["lt"] = 2.5, ["boost"] = true } } },
                    },
                },
            },
            ["page_size"] = 250L,
            ["json_fields"] = new List<object?> { "tags", "items" },
            ["pit_keep_alive"] = "30s",
        }), errors);

        Assert.Empty(errors);
        Assert.Equal("orders-*", config!.Index);
        Assert.Equal("""{"bool":{"filter":[{"term":{"status":"shipped"}},{"range":{"qty":{"gte":2,"lt":2.5,"boost":true}}}]}}""", config.QueryJson);
        Assert.Equal(250, config.PageSize);
        Assert.Equal(new HashSet<string> { "tags", "items" }, config.JsonFields);
        Assert.Equal("30s", config.PitKeepAlive);
    }

    [Fact]
    public void A_query_string_must_be_a_json_object()
    {
        var errors = new List<string>();
        var ok = EsDatasetConfig.Parse(Spec(new() { ["query"] = """{ "match_all": {} }""" }), errors);
        Assert.Empty(errors);
        Assert.Equal("""{ "match_all": {} }""", ok!.QueryJson);

        Assert.Null(EsDatasetConfig.Parse(Spec(new() { ["query"] = "[1]" }), errors));
        Assert.Contains(errors, e => e.Contains("'query' must be a JSON object"));

        errors.Clear();
        Assert.Null(EsDatasetConfig.Parse(Spec(new() { ["query"] = "{nope" }), errors));
        Assert.Contains(errors, e => e.Contains("'query' is not valid JSON"));

        errors.Clear();
        Assert.Null(EsDatasetConfig.Parse(Spec(new() { ["query"] = 5L }), errors));
        Assert.Contains(errors, e => e.Contains("'query' must be a mapping"));
    }

    [Fact]
    public void Bounds_and_shapes_are_checked_and_errors_aggregate()
    {
        var errors = new List<string>();
        Assert.Null(EsDatasetConfig.Parse(Spec(new()
        {
            ["index"] = "",
            ["page_size"] = 10_001L,
            ["json_fields"] = "tags",
            ["pit_keep_alive"] = "soon",
            ["scroll"] = "1m",
        }), errors));

        Assert.Equal(5, errors.Count);
        Assert.Contains(errors, e => e.Contains("'index' must be a non-empty string"));
        Assert.Contains(errors, e => e.Contains("'page_size' must be an integer between 1 and 10000"));
        Assert.Contains(errors, e => e.Contains("'json_fields' must be a list"));
        Assert.Contains(errors, e => e.Contains("'pit_keep_alive' must be an Elasticsearch duration"));
        Assert.Contains(errors, e => e.Contains("unknown read option 'scroll'"));
        Assert.All(errors, e => Assert.StartsWith("dataset 'orders': ", e));
    }
}
