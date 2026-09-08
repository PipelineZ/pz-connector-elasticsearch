using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsConnectorTests
{
    [Fact]
    public void Identity_is_elasticsearch_on_the_current_protocol_major()
    {
        var connector = new EsConnector();

        Assert.Equal("elasticsearch", connector.Info.Name);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
        Assert.False(string.IsNullOrWhiteSpace(connector.Info.Version));
    }

    [Fact]
    public void Declares_pruning_bounded_and_inclusive_watermarks_merge_and_replace()
    {
        Assert.Equal(
            ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound
            | ConnectorCapabilities.Merge | ConnectorCapabilities.ReplaceWrites,
            new EsConnector().Capabilities);
    }

    [Fact]
    public void Config_schemas_are_json_objects_listing_every_key()
    {
        var connector = new EsConnector();
        using var connection = JsonDocument.Parse(connector.ConnectionConfigSchema);
        using var dataset = JsonDocument.Parse(connector.DatasetConfigSchema);

        Assert.Equal("object", connection.RootElement.GetProperty("type").GetString());
        var keys = connection.RootElement.GetProperty("properties").EnumerateObject().Select(p => p.Name).ToList();
        Assert.Equal(["url", "api_key", "username", "password", "ca_cert", "ca_fingerprint", "insecure", "timeout", "base_dir"], keys);
        Assert.Equal("object", dataset.RootElement.GetProperty("type").GetString());
        var options = dataset.RootElement.GetProperty("properties").EnumerateObject().Select(p => p.Name).ToList();
        Assert.Equal(["index", "query", "page_size", "json_fields", "pit_keep_alive", "bulk_size", "bulk_bytes"], options);
    }

    [Fact]
    public async Task Validate_reports_every_config_error()
    {
        var result = await new EsConnector().ValidateAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["url"] = "nope", ["timeout"] = -1L }),
            CancellationToken.None);

        Assert.False(result.IsValid);
        Assert.Equal(2, result.Errors.Count);
    }

    [Fact]
    public async Task Check_reports_an_invalid_config_as_a_failed_probe()
    {
        var check = await new EsConnector().CheckConnectionAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.Contains("'url' is required", check.Message);
    }

    [Fact]
    public async Task Check_reports_a_refused_connection_as_a_failed_probe_without_throwing()
    {
        var check = await new EsConnector().CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["url"] = "http://127.0.0.1:1", ["timeout"] = 2L }), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.StartsWith("elasticsearch: checking the connection: ", check.Message);
    }
}
