using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsOutputConfigTests
{
    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("name", StringType.Default, true),
    ], null);

    private static OutputSpec Spec(Dictionary<string, object?> options, string mode = "append", IReadOnlyList<string>? keys = null) =>
        new("search", "orders_out", mode, "fail_on_change", options) { Keys = keys ?? [] };

    [Fact]
    public void Defaults_to_the_entity_index_and_bulk_sizing()
    {
        var errors = new List<string>();
        var config = EsOutputConfig.Parse(Spec([]), errors);

        Assert.Empty(errors);
        Assert.Equal(new EsOutputConfig("orders_out", 1000, 5 * 1024 * 1024), config);
    }

    [Fact]
    public void Every_option_parses()
    {
        var errors = new List<string>();
        var config = EsOutputConfig.Parse(Spec(new() { ["index"] = "orders-v2", ["bulk_size"] = 250L, ["bulk_bytes"] = 65536L }, "merge", ["id"]), errors);

        Assert.Empty(errors);
        Assert.Equal(new EsOutputConfig("orders-v2", 250, 65536), config);
    }

    [Fact]
    public void Shapes_modes_and_unknown_options_are_refused_together()
    {
        var errors = new List<string>();
        Assert.Null(EsOutputConfig.Parse(Spec(new() { ["index"] = "", ["bulk_size"] = 0L, ["bulk_bytes"] = 10L, ["refresh"] = true }, "upsert"), errors));

        Assert.Equal(5, errors.Count);
        Assert.Contains(errors, e => e.Contains("unknown write option 'refresh'"));
        Assert.Contains(errors, e => e.Contains("mode 'upsert' is not supported"));
        Assert.Contains(errors, e => e.Contains("'index' must be a non-empty string"));
        Assert.Contains(errors, e => e.Contains("'bulk_size' must be an integer between 1 and 10000"));
        Assert.Contains(errors, e => e.Contains("'bulk_bytes' must be an integer of at least 1024"));
        Assert.All(errors, e => Assert.StartsWith("output 'orders_out': ", e));
    }

    [Fact]
    public void Schema_validation_refuses_types_outside_the_matrix_and_missing_merge_keys()
    {
        var errors = new List<string>();
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("f", FloatType.Default, true)], null);

        EsOutputConfig.ValidateSchema(Spec([], "merge", ["id", "tenant"]), schema, errors);
        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, e => e.Contains("column 'f' is Float"));
        Assert.Contains(errors, e => e.Contains("key column 'tenant' is not in the pipeline's output"));

        errors.Clear();
        EsOutputConfig.ValidateSchema(Spec([], "merge"), Schema, errors);
        Assert.Single(errors);
        Assert.Contains("mode merge needs 'keys:'", errors[0]);

        errors.Clear();
        EsOutputConfig.ValidateSchema(Spec([]), Schema, errors);
        Assert.Empty(errors);
    }
}
